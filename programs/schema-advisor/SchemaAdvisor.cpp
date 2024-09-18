#include "Advisor/Advisor.h"
#include "CodecAdvisor.h"
#include "ColumnUsageExtractor.h"
#include "IndexAdvisor.h"
#include "MockEnvironment.h"
#include "SchemaAdvisorHelpers.h"
#include "TypeAdvisor.h"
#include "PrewhereAdvisor.h"

#include <iostream>
#include <optional>
#include <string>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Advisor/AdvisorContext.h>
#include <Core/Defines.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Poco/Util/XMLConfiguration.h>
#include <common/types.h>
#include <Common/Exception.h>
#include <Common/TerminalSize.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr size_t DEFAULT_SAMPLE_ROW_NUMBER = 1000000;
static constexpr size_t DEFAULT_MAX_THREADS = 8;
static constexpr Float64 MARK_FILTER_THRESHOLD = 0.35;
static constexpr Float64 TOP_3_MARK_FILTER_THRESHOLD = 0.65;

}

int mainEntryClickHouseSchemaAdvisor(int argc, char ** argv)
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()("help,h", "produce help message")
        /// mandatory
        ("db", po::value<std::string>()->value_name("DATABASE"), "db name")(
            "table", po::value<std::string>()->value_name("TABLE"), "table name")(
            "mode", po::value<std::string>()->value_name("MODE"),
            "mode: type, codec, type-codec, skip-index, projection, materialized-view, projection, order-by-key, sharding-key")(
            "path", po::value<std::string>()->value_name("PATH"), "main path")(
            "meta-path", po::value<std::string>()->value_name("META PATH"), "meta path")(
            "data-path-list", po::value<std::string>()->value_name("DATA PATH LIST"), "data path list, format: path1,path2")(
            "settings", po::value<std::string>()->default_value(""), "set settings, format: key1=value,key2=value2")(
            "log-level", po::value<std::string>()->default_value(""),
            "log level: trace, debug, information, notice, warning, error. Disable if emtpy.")
        /// optional
        ("part", po::value<std::string>()->value_name("PART"), "sample part name")(
            "max-threads", po::value<size_t>()->default_value(DEFAULT_MAX_THREADS), "max threads for schema advisor")(
            "sample-number", po::value<size_t>()->default_value(DEFAULT_SAMPLE_ROW_NUMBER), "sample row number")(
            "verbose", "print column compression gain ratio")
        /// codec
        ("fsst", "codec mode: use FSST instead of LZ4")("zstd", "codec mode: use ZSTD instead of LZ4")(
            "level", po::value<int>(), "codec mode: compression level for codecs specified via flags")(
            "codec", po::value<std::vector<std::string>>()->multitoken(), "codec mode: use codecs combination instead of LZ4")(
            "hc", "codec mode: use LZ4HC instead of LZ4")("none", "codec mode: use no compression instead of LZ4")(
            "block-size,b",
            po::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE),
            "codec mode: compress in blocks of specified size")
        /// skip-index
        ("query-file", po::value<std::string>()->value_name("QUERIES"), "absolute path to the query file seperated by newline")(
            "query-file-delimiter", po::value<std::string>()->value_name("DELIMITER"), "query file delimiter, default is new line.")
        /// tos
        ("tos-ak", po::value<std::string>()->value_name("TOS AK"), "tos access key")(
            "vetos-endpoint", po::value<std::string>()->value_name("VETOS ENDPOINT"), "ve tos endpoint")(
            "vetos-region", po::value<std::string>()->value_name("VETOS REGION"), "ve tos region")(
            "vetos-ak", po::value<std::string>()->value_name("VETOS AK"), "ve tos access key")(
            "vetos-sk", po::value<std::string>()->value_name("VETOS SK"), "ve tos secret key")
        /// prewhere
        ("mark_filter_threshold", po::value<Float64>()->default_value(MARK_FILTER_THRESHOLD), "threshold for mark filter ratio") ("top_3_mark_filter_threshold", po::value<Float64>()->default_value(TOP_3_MARK_FILTER_THRESHOLD), "threshold for mark filter ratio")    ("low-cardinality", "recommend low-cardinality only in type advisor") (
            "scanned_count_threshold_for_lc", po::value<Float64>()->default_value(0.035), "recommend low-cardinality only scanned count > scan_count_threshold_for_lc") (
            "cardinality_ratio_threshold_for_lc", po::value<Float64>()->default_value(0.05), "recommend low-cardinality only cardinality < sample_row_number * cardinality_ratio_threshold_for_lc");

    WriteBufferFromOwnString str_buf;
    std::unique_ptr<WriteBufferFromFileBase> stdout_buf = std::make_unique<WriteBufferFromFileDescriptor>(STDOUT_FILENO);
    bool verbose = false;

    try
    {
        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < INPUT > OUTPUT" << std::endl;
            std::cout << "Usage: " << argv[0] << " [options] INPUT OUTPUT" << std::endl;
            std::cout << desc << std::endl;
            return 0;
        }

        if (!options.count("db") || !options.count("mode"))
            throw Exception("Missing option, 'db' or 'mode' is missing", ErrorCodes::BAD_ARGUMENTS);

        // if (options.count("path") == options.count("meta-path") || options.count("meta-path") != options.count("data-path-list"))
        //     throw Exception("Missing option, either single 'path' argument or both meta path and data path list arguments are allowed", ErrorCodes::BAD_ARGUMENTS);

        std::string db_name = options["db"].as<std::string>();
        std::string advisor_mode = options["mode"].as<std::string>();

        std::string meta_path;
        std::vector<std::string> data_path_list;
        // if (options.count("path"))
        // {
        //     std::string path = options["path"].as<std::string>();
        //     if (!endsWith(path, "/"))
        //         path.append("/");
        //     meta_path = path;
        //     data_path_list.emplace_back(path);
        // }
        // else
        // {
            meta_path = options["meta-path"].as<std::string>();
            if (!endsWith(meta_path, "/"))
                meta_path.append("/");
            boost::split(data_path_list, options["data-path-list"].as<std::string>(), boost::is_any_of(" ,"));
            for(auto & path : data_path_list)
            {
                if (!endsWith(path, "/"))
                    path = path.append("/");
            }
        // }
        
        size_t sample_row_number = options["sample-number"].as<size_t>();
        size_t max_threads = options["max-threads"].as<size_t>();
        Float64 mark_filter_threshold = options["mark_filter_threshold"].as<Float64>();
        Float64 top_3_mark_filter_threshold = options["top_3_mark_filter_threshold"].as<Float64>();
        verbose = options.count("verbose");


        if (auto log_level = options["log-level"].as<std::string>(); !log_level.empty())
            setupLogging(log_level);

        // prepare mock env
        MockEnvironment env(meta_path, max_threads);

        if (advisor_mode == "codec")
        {
            if (!options.count("table"))
                throw Exception("Missing option, 'table' is missing", ErrorCodes::BAD_ARGUMENTS);

            std::string table_name = options["table"].as<std::string>();
            std::string absolute_part_path = selectPartPath(options, data_path_list, db_name, table_name, sample_row_number);
            serializeJsonPrefix(str_buf, db_name, table_name, absolute_part_path, verbose);
            ColumnsDescription columns = env.getColumnsDescription(db_name, table_name);

            CodecAdvisor codec_advisor(options, columns, absolute_part_path, sample_row_number, max_threads);
            codec_advisor.execute();
            codec_advisor.serializeJson(str_buf, verbose);
            serializeJsonSuffix(str_buf);
        }
        else if (advisor_mode == "type")
        {
            if (!options.count("table"))
                throw Exception("Missing option, 'table' is missing", ErrorCodes::BAD_ARGUMENTS);

            auto lc_only = options.count("low-cardinality");
            auto scanned_count_threshold_for_lc = options["scanned_count_threshold_for_lc"].as<Float64>();
            auto cardinality_ratio_threshold_for_lc = options["cardinality_ratio_threshold_for_lc"].as<Float64>();

            std::string table_name = options["table"].as<std::string>();
            std::string absolute_part_path = selectPartPath(options, data_path_list, db_name, table_name, sample_row_number);
            serializeJsonPrefix(str_buf, db_name, table_name, absolute_part_path, verbose);
            ColumnsDescription columns = env.getColumnsDescription(db_name, table_name);

            TypeAdvisor type_advisor(env, options, columns, absolute_part_path, sample_row_number, max_threads, lc_only, scanned_count_threshold_for_lc, cardinality_ratio_threshold_for_lc);
            type_advisor.execute();
            type_advisor.serializeJson(str_buf, verbose);
            serializeJsonSuffix(str_buf);
        }
        else if (advisor_mode == "skip-index") // currently extracts all usages for the database
        {
            serializeJsonPrefixWithDB(str_buf, db_name);
            IndexAdvisor index_advisor(env, options, sample_row_number, max_threads);
            index_advisor.execute();
            index_advisor.serializeJson(str_buf, verbose);
            serializeJsonSuffix(str_buf);
        }
        else if (advisor_mode == "prewhere") // currently extracts all usages for the database
        {
            serializeJsonPrefixWithDB(str_buf, db_name);
            PrewhereAdvisor prewhere_advisor(env, options, sample_row_number, max_threads, mark_filter_threshold, top_3_mark_filter_threshold);
            prewhere_advisor.execute();
            prewhere_advisor.serializeJson(str_buf, verbose);
            serializeJsonSuffix(str_buf);
        }
        else if (advisor_mode == "materialized-view")
        {
            Advisor advisor{ASTAdviseQuery::AdvisorType::MATERIALIZED_VIEW};
            WorkloadAdvises advises = advisor.analyze(loadQueries(options), createContext(options, env));
            serializeJson("materialized-view", "ddl", db_name, advises, str_buf, verbose);
        }
        else if (advisor_mode == "projection")
        {
            Advisor advisor{ASTAdviseQuery::AdvisorType::PROJECTION};
            WorkloadAdvises advises = advisor.analyze(loadQueries(options), createContext(options, env));
            serializeJson("projection", "ddl", db_name, advises, str_buf, verbose);
        }
        else if (advisor_mode == "order-by-key")
        {
            Advisor advisor{ASTAdviseQuery::AdvisorType::ORDER_BY};
            WorkloadAdvises advises = advisor.analyze(loadQueries(options), createContext(options, env));
            serializeJson(advisor_mode, "candidate", db_name, advises, str_buf, verbose);
        }
        else if (advisor_mode == "cluster-key")
        {
            Advisor advisor{ASTAdviseQuery::AdvisorType::CLUSTER_BY};
            WorkloadAdvises advises = advisor.analyze(loadQueries(options), createContext(options, env));
            serializeJson(advisor_mode, "candidate", db_name, advises, str_buf, verbose);
        }
        else if (advisor_mode == "column-usage")
        {
            Advisor advisor{ASTAdviseQuery::AdvisorType::COLUMN_USAGE};
            WorkloadAdvises advises = advisor.analyze(loadQueries(options), createContext(options, env));
            serializeJson(advisor_mode, "usage", db_name, advises, str_buf, verbose);
        }
        else
        {
            throw Exception("Unsupported advisor mode: " + advisor_mode, ErrorCodes::BAD_ARGUMENTS);
        }
    }
    catch (...)
    {
        serializeException(*stdout_buf, getCurrentExceptionMessage(verbose));
        return getCurrentExceptionCode();
    }
    writeString(str_buf.str(), *stdout_buf);

    return 0;
}
