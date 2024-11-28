#include "IStorageCloudFile.h"

#include <filesystem>
#include <utility>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/PartitionedBlockOutputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/StorageFactory.h>
#include <Storages/getVirtualsForStorage.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <Common/Exception.h>
#include <Common/parseGlobs.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ACCESS_DENIED;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

class FileBlockInputStream : public SourceWithProgress
{
public:
    using IteratorWrapper = std::function<const FilePartInfo *()>;

    FileBlockInputStream(
        const ContextPtr & context_,
        const FileClientPtr & client_,
        Block block_for_format_,
        std::vector<NameAndTypePair> requested_virtual_columns_,
        UInt64 max_block_size_,
        std::shared_ptr<IteratorWrapper> file_iterator_,
        ColumnsDescription columns_description_,
        const String & compression_method_,
        const String & format_name_)
        : SourceWithProgress(getHeader(block_for_format_, requested_virtual_columns_))
        , global_context(context_)
        , client(client_)
        , block_for_format(std::move(block_for_format_))
        , requested_virtual_columns(std::move(requested_virtual_columns_))
        , max_block_size(max_block_size_)
        , file_iterator(std::move(file_iterator_))
        , columns_description(std::move(columns_description_))
        , compression_method(compression_method_)
        , format_name(format_name_)
    {
        initialize(context_);
    }

    bool initialize(const ContextPtr & query_context)
    {
        current_file = (*file_iterator)();
        if (!current_file)
            return false;

        String current_file_name = current_file->name;
        LOG_TRACE(getLogger("FileBlockInputStream"), "{} start to read {}", client->type(), current_file_name);
        auto current_compression = chooseCompressionMethod(current_file_name, compression_method);
        auto current_format = FormatFactory::instance().getFormatFromFileName(current_file_name, true, format_name);
        FormatFactory::instance().checkFormatName(current_format);

        auto read_buffer = client->createReadBuffer(current_file_name);
        auto compressed_read_buf = wrapReadBufferWithCompressionMethod(std::move(read_buffer), current_compression);
        auto input_stream = query_context->getInputFormat(current_format, *compressed_read_buf, block_for_format, max_block_size);
        reader = std::make_shared<OwningBlockInputStream<ReadBuffer>>(input_stream, std::move(compressed_read_buf));
        return true;
    }


    String getName() const override { return "CloudInputFile"; }

    Chunk generate() override
    {
        while (true)
        {
            if (!reader)
                return {};
            if (auto columns_data = reader->read(); columns_data)
            {
                auto columns = columns_data.getColumns();
                auto rows = columns_data.rows();
                for (const auto & virtual_column : requested_virtual_columns)
                {
                    if (virtual_column.name == "_path")
                    {
                        auto column = std::make_shared<DataTypeString>()->createColumnConst(rows, current_file->name);
                        columns_data.insert({column->convertToFullColumnIfConst(), std::make_shared<DataTypeString>(), "_path"});
                    }
                    else if (virtual_column.name == "_file")
                    {
                        size_t last_slash_pos = current_file->name.find_last_of('/');
                        auto file_name = current_file->name.substr(last_slash_pos + 1);

                        auto column = std::make_shared<DataTypeString>()->createColumnConst(rows, std::move(file_name));
                        columns_data.insert({column->convertToFullColumnIfConst(), std::make_shared<DataTypeString>(), "_file"});
                    }
                    else if (virtual_column.name == "_size")
                    {
                        auto column = std::make_shared<DataTypeUInt64>()->createColumnConst(rows, current_file->size);
                        columns_data.insert({column->convertToFullColumnIfConst(), std::make_shared<DataTypeUInt64>(), "_size"});
                    }
                }
                return Chunk(columns_data.getColumns(), columns_data.rows());
            }
            {
                std::lock_guard lock(reader_mutex);
                reader.reset();
                if (this->empty() || !initialize(global_context))
                {
                    return {};
                }
            }
        }
    }

    Block getHeader(const Block & sample, const std::vector<NameAndTypePair> & virtual_columns) const
    {
        auto header = sample;
        for (const auto & virtual_column : virtual_columns)
            header.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});
        return header;
    }

    bool empty() { return !current_file; }

private:
    ContextPtr global_context;
    FileClientPtr client;
    Block block_for_format;
    std::vector<NameAndTypePair> requested_virtual_columns;
    UInt64 max_block_size;
    std::shared_ptr<IteratorWrapper> file_iterator;
    ColumnsDescription columns_description;
    String compression_method;
    String format_name;

    std::mutex reader_mutex;
    const FilePartInfo * current_file;

    BlockInputStreamPtr reader;
};

class FileBlockOutputStream : public IBlockOutputStream
{
public:
    FileBlockOutputStream(
        const ContextPtr & query_context,
        const FileClientPtr & client,
        const String & uri,
        const String & format,
        const Block & sample_block_,
        const CompressionMethod compression_method)
        : sample_block(sample_block_)
    {
        write_buf = wrapWriteBufferWithCompressionMethod(client->createWriteBuffer(uri), compression_method, 3);
        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, query_context);
    }

    String getName() const override { return "CloudOutputFile"; }

    Block getHeader() const override { return sample_block; }

    void write(const Block & block) override { writer->write(block); }

    void writePrefix() override { writer->doWritePrefix(); }

    void writeSuffix() override
    {
        writer->doWriteSuffix();
        writer->flush();
        write_buf->sync();
        write_buf->finalize();
    }

private:
    Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
};


class PartitionedFileBlockOutputStream : public PartitionedBlockOutputStream
{
public:
    PartitionedFileBlockOutputStream(
        const ContextPtr & context_,
        const FileClientPtr & client_,
        const ASTPtr & partition_by_,
        const Block & sample_block_,
        String uri_,
        String format_,
        const CompressionMethod compression_method_)
        : PartitionedBlockOutputStream(context_, partition_by_, sample_block_)
        , client(client_)
        , uri(std::move(uri_))
        , format(std::move(format_))
        , compression_method(compression_method_)
    {
    }

    BlockOutputStreamPtr createStreamForPartition(const String & partition_id) override
    {
        auto path = PartitionedBlockOutputStream::replaceWildcards(uri, partition_id, query_context->getPlanSegmentInstanceId().parallel_index);
        PartitionedBlockOutputStream::validatePartitionKey(path, true);
        return std::make_shared<FileBlockOutputStream>(query_context, client, path, format, sample_block, compression_method);
    }

private:
    const FileClientPtr client;
    const String uri;
    const String format;
    const CompressionMethod compression_method;
};

class Pipe;

class ReadFromCnchFile final : public ISourceStep
{
public:
    ReadFromCnchFile(
        const FileClientPtr & client_,
        FileDataPartsCNCHVector parts_,
        Names real_column_names_,
        const NamesAndTypesList & virtual_,
        const StorageID & id_,
        const SelectQueryInfo & query_info_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t num_streams_,
        const CnchFileArguments & arguments_,
        LoggerPtr log_)
        : ISourceStep(DataStream{.header = metadata_snapshot_->getSampleBlockForColumns(real_column_names_, virtual_, id_)})
        , client(client_)
        , data_parts(parts_)
        , virtuals(virtual_)
        , real_column_names(std::move(real_column_names_))
        , query_info(query_info_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , query_context(std::move(context_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
        , arguments(arguments_)
        , log(log_)
    {
    }

    String getName() const override { return "ReadFromCnchFile"; }

    Type getType() const override { return Type::ReadFromCnchFile; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override
    {
        if (data_parts.empty())
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
            return;
        }

        Pipe pipe = read(query_context, std::min(num_streams, data_parts.size()), real_column_names, max_block_size);
        if (pipe.empty())
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
            return;
        }

        for (const auto & processor : pipe.getProcessors())
            processors.emplace_back(processor);

        pipeline.init(std::move(pipe));
    }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr /*ptr*/) const override
    {
        throw Exception("ReadFromCnchFile can not copy", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    FileClientPtr client;
    FileDataPartsCNCHVector data_parts;
    NamesAndTypesList virtuals;
    Names real_column_names;

    SelectQueryInfo query_info;

    StorageMetadataPtr metadata_snapshot;

    ContextPtr query_context;

    const size_t max_block_size;
    size_t num_streams;

    CnchFileArguments arguments;
    LoggerPtr log;

    Pipe read(ContextPtr & query_context_, size_t num_streams_, const Names & column_names, const UInt64 & max_block_size_)
    {
        std::shared_ptr<FileBlockInputStream::IteratorWrapper> iterator_wrapper{nullptr};
        auto local_iterator = std::make_shared<FilesIterator>(data_parts);
        iterator_wrapper = std::make_shared<FileBlockInputStream::IteratorWrapper>([local_iterator]() { return local_iterator->next(); });

        std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
        std::vector<NameAndTypePair> requested_virtual_columns;

        for (const auto & virtual_column : virtuals)
        {
            if (column_names_set.contains(virtual_column.name))
                requested_virtual_columns.push_back(virtual_column);
        }

        LOG_TRACE(log, "read files size = {} by stream size = {}", data_parts.size(), num_streams_);
        Pipes pipes;
        for (size_t i = 0; i < num_streams_; ++i)
        {
            auto pipe = std::make_shared<FileBlockInputStream>(
                query_context_,
                client,
                metadata_snapshot->getSampleBlock(),
                requested_virtual_columns,
                max_block_size_,
                iterator_wrapper,
                metadata_snapshot->getColumns(),
                arguments.compression_method,
                arguments.format_name);
            if (!pipe->empty())
            {
                pipes.emplace_back(std::move(pipe));
            }
        }

        return Pipe::unitePipes(std::move(pipes));
    }
};

IStorageCloudFile::IStorageCloudFile(
    ContextPtr context_,
    const StorageID & table_id_,
    const ColumnsDescription & required_columns_,
    const ConstraintsDescription & constraints_,
    const FileClientPtr & client_,
    FilePartInfos files_,
    const ASTPtr & setting_changes_,
    const CnchFileArguments & arguments_,
    const CnchFileSettings & settings_)
    : IStorage(table_id_)
    , WithMutableContext(context_->getGlobalContext())
    , client(client_)
    , file_list(std::move(files_))
    , arguments(arguments_)
    , settings(settings_)
{
    StorageInMemoryMetadata metadata;
    metadata.setSettingsChanges(setting_changes_);
    metadata.setColumns(required_columns_);
    metadata.setConstraints(constraints_);
    setInMemoryMetadata(metadata);

    if (file_list.empty())
        throw Exception("Can't allow read empty file list", ErrorCodes::LOGICAL_ERROR);
    String path = arguments.url.substr(arguments.url.find('/', arguments.url.find("//") + 2));
    arguments.is_glob_path = path.find_first_of("*?{") != std::string::npos;

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()},
        {"_size", std::make_shared<DataTypeUInt64>()}};
    virtual_columns = getVirtualsForStorage(metadata.getSampleBlock().getNamesAndTypesList(), default_virtuals);
}

Pipe IStorageCloudFile::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    LOG_TRACE(log, " CloudFile column_names size = {}", column_names.size());
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(query_context), BuildQueryPipelineSettings::fromContext(query_context));
}

void IStorageCloudFile::read(
    DB::QueryPlan & query_plan,
    const DB::Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    DB::SelectQueryInfo & query_info,
    DB::ContextPtr query_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned int num_streams)
{
    if (parts.empty())
        return;

    Names real_column_names = column_names;
    NamesAndTypesList available_real_columns = storage_snapshot->metadata->getColumns().getAllPhysical();
    if (real_column_names.empty())
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));

    storage_snapshot->check(real_column_names);

    auto cloud_file_source = std::make_unique<ReadFromCnchFile>(
        client,
        parts,
        real_column_names,
        getVirtuals(),
        getStorageID(),
        query_info,
        storage_snapshot->metadata,
        query_context,
        max_block_size,
        num_streams,
        arguments,
        log);

    query_plan.addStep(std::move(cloud_file_source));
}

BlockOutputStreamPtr IStorageCloudFile::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, const ContextPtr query_context)
{
    String current_uri = file_list.front().name;
    bool has_wildcards = current_uri.find(PartitionedFileBlockOutputStream::PARTITION_ID_WILDCARD) != String::npos;
    const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
    auto partition_by_ast = insert_query ? (insert_query->partition_by ? insert_query->partition_by : arguments.partition_by) : nullptr;
    bool allow_partition_write = partition_by_ast && has_wildcards;

    auto current_compression = chooseCompressionMethod(current_uri, arguments.compression_method);
    auto current_format = FormatFactory::instance().getFormatFromFileName(current_uri, true, arguments.format_name);
    FormatFactory::instance().checkFormatName(current_format);

    if (allow_partition_write)
    {
        return std::make_shared<PartitionedFileBlockOutputStream>(
            query_context, client, partition_by_ast, metadata_snapshot->getSampleBlock(), current_uri, current_format, current_compression);
    }
    else
    {
        if (arguments.is_glob_path)
        {
            if (!has_wildcards)
                throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "URI '{}' contains globs and no `{}`, so the table is in readonly mode", arguments.url, PartitionedFileBlockOutputStream::PARTITION_ID_WILDCARD);
            if (!partition_by_ast)
            {
                current_uri = PartitionedBlockOutputStream::replaceWildcards(current_uri, "", query_context->getPlanSegmentInstanceId().parallel_index);
                LOG_TRACE(log, "URI `{}` has `{}` but no partition by so replace wildcards without parition_id to {}", arguments.url, PartitionedFileBlockOutputStream::PARTITION_ID_WILDCARD, current_uri);
            }
        }

        FileURI file_uri(current_uri);
        if (client->exist(file_uri.file_path) && !query_context->getSettingsRef().overwrite_current_file)
        {
            if (query_context->getSettingsRef().insert_new_file)
            {
                auto pos = current_uri.find_first_of('.', current_uri.find_last_of('/'));
                size_t index = file_list.size();
                String new_uri;
                do
                {
                    new_uri = current_uri.substr(0, pos) + "." + std::to_string(index)
                        + (pos == std::string::npos ? "" : current_uri.substr(pos));
                    ++index;
                } while (client->exist(new_uri));

                file_list.emplace_back(new_uri);
                current_uri = new_uri;
            }
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "File with path {} already exists. If you want to overwrite it, enable setting overwrite_current_file = 1, if you want to "
                    "create new file on each insert, enable setting insert_new_file = 1",
                    current_uri);
        }

        return std::make_shared<FileBlockOutputStream>(
            query_context, client, current_uri, current_format, metadata_snapshot->getSampleBlock(), current_compression);
    }
}

void IStorageCloudFile::loadDataParts(FileDataPartsCNCHVector & file_parts)
{
    this->parts = file_parts;
}

NamesAndTypesList IStorageCloudFile::getVirtuals() const
{
    return virtual_columns;
};
}
