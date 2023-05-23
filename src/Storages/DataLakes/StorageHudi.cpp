#include <Storages/DataLakes/StorageHudi.h>

#include <JNI/JNIMetaClient.h>
#include "JNI/JNIArrowReader.h"
#include "Core/NamesAndTypes.h"
#include "DataStreams/narrowBlockInputStreams.h"
#include "DataTypes/DataTypeString.h"
#include "Parsers/ASTLiteral.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include "Storages/DataLakes/HudiPartition.h"
#include "Storages/DataLakes/HudiSchemaConverter.h"
#include "Storages/DataLakes/JNIArrowSource.h"
#include "Storages/SelectQueryInfo.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"

#include <common/logger_useful.h>
#include <hudi.pb.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static constexpr auto HUDI_CLIENT_CLASS = "org/byconity/hudi/HudiMetaClient";
static constexpr auto HUDI_ARROW_READER_CLASS = "org/byconity/readers/HudiFileSliceArrowReaderBuilder";

RemoteFileInfo const * StorageHudiSource::SourceInfo::next() const
{
    size_t file_idx = idx.fetch_add(1);
    if (file_idx >= remote_files.size())
        return nullptr;

    return &remote_files[file_idx];
}

Block StorageHudiSource::getHeader(Block header, const SourceInfo &)
{
    return header;
}

String StorageHudiSource::getName() const 
{
    return "Hudi";
}

void StorageHudiSource::resetReader()
{
    current_file = source_info->next();
    if (current_file == nullptr)
        return;

    properties["data_file_path"] = current_file->base_file_path;
    properties["delta_file_paths"] = fmt::format("{}", fmt::join(current_file->delta_logs, ","));
    properties["data_file_length"] = std::to_string(current_file->base_file_length);
    properties["instant_time"] = current_file->instant;

    Protos::Properties params;
    WriteBufferFromOwnString wb;
    for (const auto &kv : properties)
    {
        auto * prop = params.add_properties();
        prop->set_key(kv.first);
        prop->set_value(kv.second);
        writeString(kv.first, wb);
        writeChar(':', wb);
        writeString(kv.second, wb);
        writeChar(',', wb);
    }

    LOG_DEBUG(log, "To read from remote file info {}", wb.str());

    auto jni_reader = std::make_unique<JNIArrowReader>(HUDI_ARROW_READER_CLASS, params.SerializeAsString());
    source = std::make_shared<JNIArrowSource>(header, std::move(jni_reader));
    std::cout << header.dumpStructure() << std::endl;
    pipeline = std::make_unique<QueryPipeline>();
    pipeline->init(Pipe(source));
    executor = std::make_unique<PullingPipelineExecutor>(*pipeline);
}

Chunk StorageHudiSource::generate()
{
    while (executor)
    {
        Chunk chunk;
        if (executor->pull(chunk))
        {
            /// TOOD: fix when phsical column is empty
            UInt64 num_rows = chunk.getNumRows();
            std::cout << "received " << num_rows << std::endl;
            if (source_info->need_path_column)
            {
                std::string path = fmt::format("base[{}], delta[{}]", current_file->base_file_path, fmt::join(current_file->delta_logs, ","));
                chunk.addColumn(DataTypeString().createColumnConst(num_rows, path)->convertToFullColumnIfConst());
            }

            return chunk;
        }

        source.reset();
        pipeline.reset();
        executor.reset();
        resetReader();
    }
    return {};
}

StorageHudiSource::StorageHudiSource(
    Block header_, const SourceInfoPtr & info_, const HudiTableProperties & properties_, Poco::Logger * log_)
    : SourceWithProgress(getHeader(header_, *info_)), header(header_), source_info(info_), properties(properties_), log(log_)
{
    properties["fetch_size"] = std::to_string(info_->max_block_size);
    properties["required_fields"] = fmt::format("{}", fmt::join(source_info->phsical_columns, ","));

    resetReader();
}

StorageHudi::StorageHudi(const StorageID & table_id_, const String & base_path_, ContextPtr context_)
    : IStorageDataLake(table_id_, context_), base_path(base_path_)
{
    /// TODO: add hdfs config
    Protos::HudiMetaClientParams params;
    auto * property = params.mutable_properties()->add_properties();
    property->set_key("base_path");
    property->set_value(base_path);

    jni_client = std::make_shared<JNIMetaClient>(HUDI_CLIENT_CLASS, params.SerializeAsString());
    auto hudi_meta = std::make_shared<Protos::HudiTable>();
    hudi_meta->ParseFromString(jni_client->getTable());
    HudiSchemaConverter schema_converter(hudi_meta);
    StorageInMemoryMetadata metadata = schema_converter.create();
    setInMemoryMetadata(metadata);

    buildProperties(*hudi_meta);
}

/// TODO: proper partition pruning
Strings StorageHudi::getRequiredPartitions(const StorageInMemoryMetadata & metadata, SelectQueryInfo & /*query_info*/)
{
    if (!metadata.isPartitionKeyDefined())
    {
        /// non partition table
        return {""};
    }

    String partition_path_raw = jni_client->getPartitionPaths();
    Protos::PartitionPaths partitions;
    partitions.ParseFromString(partition_path_raw);

    Strings res;
    for (const auto & partition_path : partitions.paths())
    {
        res.push_back(partition_path);
    }
    return res;
}

Pipe StorageHudi::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    Pipes pipes;
    Strings partition_paths = getRequiredPartitions(*metadata_snapshot, query_info);

    RemoteFileInfos remote_files;
    for (const auto & partition_path : partition_paths)
    {
        auto res = huidListDirectory(*jni_client, base_path, partition_path);
        remote_files.insert(remote_files.end(), std::make_move_iterator(res.begin()), std::make_move_iterator(res.end()));
    }

    auto source_info = std::make_shared<StorageHudiSource::SourceInfo>();
    source_info->max_block_size = max_block_size;
    source_info->remote_files = std::move(remote_files);

    for (const auto & col : column_names)
    {
        if (col == "_path")
            source_info->need_path_column = true;
        /// phsical column
        else
            source_info->phsical_columns.emplace_back(col);
    }

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<StorageHudiSource>(
            metadata_snapshot->getSampleBlockForColumns(source_info->phsical_columns),
            source_info,
            table_properties,
            log
        ));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    narrowPipe(pipe, num_streams);
    return pipe;
}

NamesAndTypesList StorageHudi::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
    };
}

void StorageHudi::buildProperties(const Protos::HudiTable & table)
{
    table_properties["base_path"] = base_path;
    table_properties["instant_time"] = "";
    /// only support Parquet format now
    table_properties["input_format"] = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    table_properties["serde"] = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

    WriteBufferFromString buf_col_names{table_properties["hive_column_names"]};
    WriteBufferFromString buf_col_types{table_properties["hive_column_types"]};
    bool need_delimiter = false;
    for (const auto & hive_col : table.columns())
    {
        if (std::exchange(need_delimiter, true))
        {
            writeChar(',', buf_col_names);
            writeChar('#', buf_col_types);
        }
        writeString(hive_col.name(), buf_col_names);
        writeString(hive_col.type(), buf_col_types);
    }
}

void registerStorageHudi(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_sort_order = true,
        .supports_schema_inference = true,
    };

    factory.registerStorage(
        "Hudi",
        [](const StorageFactory::Arguments & args) {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() == 1)
            {
                String base_path = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
                return StorageHudi::create(args.table_id, base_path, args.getContext());
            }
            /// TODO: supports create table from hive metastore
            throw Exception("Storage Hudi requires 1 arguments: base path", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        },
        features);
}
}
