#include "CodecAdvisor.h"
#include "CompressedStatisticsCollectBuffer.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/program_options.hpp>

#include <Common/ThreadPool.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/MapHelpers.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Poco/DirectoryIterator.h>

namespace DB
{

CodecAdvisor::CodecAdvisor(
    const po::variables_map & options,
    const ColumnsDescription & column_descs,
    std::string absolute_part_path_,
    size_t sample_row_number_,
    size_t max_threads_)
    : absolute_part_path(std::move(absolute_part_path_))
    , sample_row_number(sample_row_number_)
    , max_threads(max_threads_)
{
    parseCodecCandidates(options);
    setSamplingColumnFiles(absolute_part_path, column_descs);
}

void CodecAdvisor::parseCodecCandidates(const po::variables_map & options)
{
    block_size = options["block-size"].as<unsigned>();

    bool use_lz4hc = options.count("hc");
    bool use_zstd = options.count("zstd");
    std::vector<std::string> combi_codec;
    if (options.count("codec"))
        combi_codec = options["codec"].as<std::vector<std::string>>();

    if (!use_lz4hc && !use_zstd && combi_codec.empty())
        throw Exception(
            "Missing options, either --hc or --zstd or --codec options is required", ErrorCodes::BAD_ARGUMENTS);
    if ((use_lz4hc || use_zstd) && !combi_codec.empty())
        throw Exception(
            "Wrong options, codec flags like --zstd and --codec options are mutually exclusive", ErrorCodes::BAD_ARGUMENTS);
    if (!combi_codec.empty() && options.count("level"))
        throw Exception("Wrong options, --level is not compatible with --codec list", ErrorCodes::BAD_ARGUMENTS);

    std::string method_family;
    if (use_lz4hc)
        method_family = "LZ4HC";
    else if (use_zstd)
        method_family = "ZSTD";

    std::optional<int> level = std::nullopt;
    if (options.count("level"))
        level = options["level"].as<int>();

    CompressionCodecPtr codec;
    if (!combi_codec.empty())
    {
        ParserCodec codec_parser;
        std::string combi_codec_line = boost::algorithm::join(combi_codec, ",");
        auto ast = parseQuery(codec_parser, "(" + combi_codec_line + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        codec = CompressionCodecFactory::instance().get(ast, nullptr);
    }
    else
        codec = CompressionCodecFactory::instance().get(method_family, level);

    codecs_to_compare.push_back(codec);
}

/// Select column files to sample and estimate profit
void CodecAdvisor::setSamplingColumnFiles(const std::string & part_path, const ColumnsDescription & column_descs)
{
    Poco::DirectoryIterator end;
    for (Poco::DirectoryIterator it(part_path); it != end; ++it)
    {
        if (it->isFile() && endsWith(it->path(), ".bin"))
        {
            std::string file_path = it->path();
            std::string file_name = it.name();
            std::string column_name;
            if (isMapImplicitKey(file_name) && !isMapBaseFile(file_name))
                column_name = parseMapNameFromImplicitFileName(file_name);
            else if (endsWith(it->path(), ".null.bin"))
                column_name = unescapeForFileName(file_name.substr(0, file_name.size() - 9));
            else
                column_name = unescapeForFileName(file_name.substr(0, file_name.size() - 4));

            if (column_descs.has(column_name))
                column_files_to_sample.push_back(std::make_shared<SamplingColumnFile>(file_path, column_name));
        }
    }
}

void CodecAdvisor::execute()
{
    size_t part_row_count;
    std::string part_count_path = absolute_part_path + "/count.txt";
    {
        ReadBufferFromFile in(part_count_path, METADATA_FILE_BUFFER_SIZE);
        readIntText(part_row_count, in);
        assertEOF(in);
    }

    auto run_estimate_task = [&](const SamplingColumnFilePtr & column_file_to_sample) {
        std::string file_path = column_file_to_sample->file_path;
        column_file_to_sample->origin_file_size = std::filesystem::file_size(file_path) * sample_row_number / part_row_count;

        CompressedReadBufferFromFile from(std::make_unique<ReadBufferFromFile>(file_path), true, 0, column_file_to_sample->origin_file_size, true);
        CompressedStatisticsCollectBuffer to(codecs_to_compare[0], block_size); /// TODO(weiping.qw): support comparing multiple codecs after FSST is imported.
        copyData(from, to);

        column_file_to_sample->optimized_file_size = to.getCompressedBytes();
    };

    ExceptionHandler exception_handler;
    ///make queue size large enough to hold all tasks.
    ThreadPool pool(max_threads, max_threads, 100000);

    for (const auto & file : column_files_to_sample)
    {
        pool.trySchedule(
            createExceptionHandledJob(
                [&, column_file_to_sample = file]() { run_estimate_task(column_file_to_sample); }
                , exception_handler
            )
        );
    }
    pool.wait();
    /// throw if exception during collecting compression info.
    exception_handler.throwIfException();
}

void CodecAdvisor::serializeJson(WriteBuffer & buf, bool verbose)
{
    size_t total_origin_file_size = 0;
    size_t total_optimized_file_size = 0;

    std::unordered_map<std::string, size_t> column_origin_file_sizes;
    std::unordered_map<std::string, size_t> column_optimized_file_sizes;
    for (const auto & file : column_files_to_sample)
    {
        /// skip column without potential compression profit
        if (file->origin_file_size <= file->optimized_file_size)
            continue;

        total_origin_file_size += file->origin_file_size;
        total_optimized_file_size += file->optimized_file_size;
        if (verbose)
        {
            if (column_origin_file_sizes.find(file->column_name) == column_origin_file_sizes.end())
            {
                column_origin_file_sizes.emplace(file->column_name, file->origin_file_size);
                column_optimized_file_sizes.emplace(file->column_name, file->optimized_file_size);
            }
            else
            {
                column_origin_file_sizes[file->column_name] += file->origin_file_size;
                column_optimized_file_sizes[file->column_name] += file->optimized_file_size;
            }
        }
    }

    if (verbose)
    {
        bool first = true;
        writeString("\"columns\":[", buf);
        for (const auto & entry : column_origin_file_sizes)
        {
            if (first)
                first = false;
            else
                writeString(",", buf);
            std::string column_name = entry.first;
            writeString("{\"name\":\"", buf);
            writeString(column_name, buf);
            writeString("\",", buf);
            size_t column_origin_file_size = entry.second;
            size_t column_optimized_file_size = column_optimized_file_sizes[column_name];
            double column_estimated_profit =
                (column_origin_file_size == 0 || column_origin_file_size <= column_optimized_file_size)
                    ? 0 : (column_origin_file_size - column_optimized_file_size) * 100.0 / column_origin_file_size;
            writeString("\"codec\":{\"", buf);
            writeString(queryToString(codecs_to_compare[0]->getCodecDesc()), buf);
            writeString("\":{\"compression ratio\":", buf);
            writeFloatText(column_estimated_profit, buf);
            writeString("}}}", buf);
        }
        writeString("],", buf);
    }

    double estimated_profit = (total_origin_file_size - total_optimized_file_size) * 100.0 / total_origin_file_size;

    writeString("\"codec\":{\"compression ratio\":", buf);
    writeFloatText(estimated_profit, buf);
    writeString("}", buf);
}

}
