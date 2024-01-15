#include <Storages/Hive/StorageCnchHive.h>
#if USE_HIVE

#include <thrift/TToString.h>
#include "CloudServices/CnchServerResource.h"
#include "DataStreams/narrowBlockInputStreams.h"
#include "Interpreters/ClusterProxy/SelectStreamFactory.h"
#include "Interpreters/ClusterProxy/executeQuery.h"
#include "Interpreters/InterpreterSelectQuery.h"
#include "Interpreters/SelectQueryOptions.h"
#include "Interpreters/evaluateConstantExpression.h"
#include "Interpreters/trySetVirtualWarehouse.h"
#include "MergeTreeCommon/CnchStorageCommon.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSetQuery.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/queryToString.h"
#include "Processors/Sources/NullSource.h"
#include "QueryPlan/BuildQueryPipelineSettings.h"
#include "QueryPlan/Optimizations/QueryPlanOptimizationSettings.h"
#include "QueryPlan/ReadFromPreparedSource.h"
#include "ResourceManagement/CommonData.h"
#include "Storages/AlterCommands.h"
#include "Storages/DataLakes/HudiDirectoryLister.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/Hive/HivePartition.h"
#include "Storages/Hive/HiveSchemaConverter.h"
#include "Storages/Hive/HiveWhereOptimizer.h"
#include "Storages/Hive/Metastore/HiveMetastore.h"
#include "Storages/Hive/StorageHiveSource.h"
#include "Storages/MergeTree/PartitionPruner.h"
#include "Storages/StorageFactory.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "Storages/DataLakes/HudiDirectoryLister.h"

#include <boost/lexical_cast.hpp>
#include "common/scope_guard_safe.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FORMAT;
}

StorageCnchHive::StorageCnchHive(
    const StorageID & table_id_,
    const String & hive_metastore_url_,
    const String & hive_db_name_,
    const String & hive_table_name_,
    StorageInMemoryMetadata metadata_,
    ContextPtr context_,
    std::shared_ptr<CnchHiveSettings> settings_,
    IMetaClientPtr client_from_catalog)
    : IStorage(table_id_)
    , WithContext(context_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_db_name(hive_db_name_)
    , hive_table_name(hive_table_name_)
    , storage_settings(settings_)
{
    try
    {
        hive_client
            = client_from_catalog != nullptr ? client_from_catalog : HiveMetastoreClientFactory::instance().getOrCreate(hive_metastore_url,storage_settings);
        hive_table = hive_client->getTable(hive_db_name, hive_table_name);
    }
    catch (...)
    {
        hive_exception = std::current_exception();
        // tryLogCurrentException(__PRETTY_FUNCTION__);
        return;
    }

    HiveSchemaConverter converter(context_, hive_table);

    if (metadata_.columns.empty())
    {
        converter.convert(metadata_);
        setInMemoryMetadata(metadata_);
    }
    else
    {
        converter.check(metadata_);
        setInMemoryMetadata(metadata_);
    }
}




void StorageCnchHive::startup()
{
    /// for some reason, we do not what to throw exceptions in ctor
    if (hive_exception)
    {
        std::rethrow_exception(hive_exception);
    }
}

bool StorageCnchHive::isBucketTable() const
{
    return getInMemoryMetadata().hasClusterByKey();
}

QueryProcessingStage::Enum StorageCnchHive::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const
{
    const auto & local_settings = local_context->getSettingsRef();

    if (local_settings.distributed_perfect_shard || local_settings.distributed_group_by_no_merge)
    {
        return QueryProcessingStage::Complete;
    }
    else if (auto worker_group = local_context->tryGetCurrentWorkerGroup())
    {
        size_t num_workers = worker_group->getShardsInfo().size();
        size_t result_size = (num_workers * local_settings.max_parallel_replicas);
        return result_size == 1 ? QueryProcessingStage::Complete : QueryProcessingStage::WithMergeableState;
    }
    else
    {
        return QueryProcessingStage::WithMergeableState;
    }
}

std::optional<String> StorageCnchHive::getVirtualWarehouseName(VirtualWarehouseType vw_type) const
{
    if (storage_settings)
    {
        if (vw_type == VirtualWarehouseType::Default)
        {
            /// deprecated
            if (storage_settings->cnch_vw_read.changed)
                return storage_settings->cnch_vw_read;

            return storage_settings->cnch_vw_default;
        }
        else if (vw_type == VirtualWarehouseType::Write)
        {
            return storage_settings->cnch_vw_write;
        }
    }
    return {};
}

void StorageCnchHive::collectResource(ContextPtr local_context, PrepareContextResult & result)
{
    auto worker_group = getWorkerGroupForTable(local_context, shared_from_this());
    auto cnch_resource = local_context->getCnchServerResource();
    auto txn_id = local_context->getCurrentTransactionID();
    StorageID cloud_storage_id = getStorageID();
    cloud_storage_id.table_name = cloud_storage_id.table_name + '_' + txn_id.toString();
    CloudTableBuilder builder;
    String cloud_table_sql
        = builder.setStorageID(cloud_storage_id).setMetadata(getInMemoryMetadataPtr()).setCloudEngine("CloudHive").build();

    LOG_INFO(log, "Create cloud table sql {}", cloud_table_sql);
    cnch_resource->addCreateQuery(local_context, shared_from_this(), cloud_table_sql, builder.cloudTableName());
    cnch_resource->addDataParts(getStorageUUID(), result.hive_files);
    result.local_table_name = builder.cloudTableName();
}

PrepareContextResult StorageCnchHive::prepareReadContext(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr & local_context,
    unsigned)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    HiveWhereOptimizer optimizer(metadata_snapshot, query_info);

    HivePartitions partitions = selectPartitions(local_context, metadata_snapshot, query_info, optimizer);
    HiveFiles hive_files;
    std::mutex mu;

    auto lister = getDirectoryLister();
    auto list_partition = [&](const HivePartitionPtr & partition) {
        LOG_DEBUG(log, "Fetch hive files from {}", partition->location);
        HiveFiles files = lister->list(partition);
        {
            std::lock_guard lock(mu);
            hive_files.insert(hive_files.end(), std::make_move_iterator(files.begin()), std::make_move_iterator(files.end()));
        }
    };

    // if (num_streams <= 1 || partitions.size() == 1)
    // {
    //     for (const auto & partition : partitions)
    //     {
    //         list_partition(partition);
    //     }
    // }
    // else
    // {
    //     size_t num_threads = std::min(size_t(num_streams), partitions.size());

    //     ThreadPool pool(num_threads);
    //     for (const auto & partition : partitions)
    //     {
    //         pool.scheduleOrThrowOnError([&, partition, thread_group = CurrentThread::getGroup()] {
    //             SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
    //             if (thread_group)
    //                 CurrentThread::attachTo(thread_group);
    //             list_partition(partition);
    //         });
    //     }
    //     pool.wait();
    // }
    for (const auto & partition : partitions)
    {
        list_partition(partition);
    }

    LOG_DEBUG(log, "Read from {} hive files", hive_files.size());

    PrepareContextResult result{.hive_files = std::move(hive_files)};

    collectResource(local_context, result);
    return result;
}

Pipe StorageCnchHive::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context), BuildQueryPipelineSettings::fromContext(local_context));
}

void StorageCnchHive::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    unsigned num_streams)
{
    PrepareContextResult result = prepareReadContext(column_names, metadata_snapshot, query_info, local_context, num_streams);
    Block header = InterpreterSelectQuery(query_info.query, local_context, SelectQueryOptions(processed_stage)).getSampleBlock();

    auto worker_group = getWorkerGroupForTable(local_context, shared_from_this());
    /// Return directly (with correct header) if no shard read from
    if (!worker_group || worker_group->getShardsInfo().empty() || result.hive_files.empty())
    {
        LOG_TRACE(log, " worker group empty ");
        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (CnchMergeTree)");
        query_plan.addStep(std::move(read_from_pipe));
        return;
    }

    const Scalars & scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};
    ASTPtr select_ast = CnchStorageCommonHelper::rewriteSelectQuery(query_info.query, getDatabaseName(), result.local_table_name);

    ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
        header,
        processed_stage,
        StorageID::createEmpty(), /// Don't check whether table exists in cnch-worker
        scalars,
        false,
        local_context->getExternalTables());

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, select_ast, local_context, worker_group);

    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);
}

NamesAndTypesList StorageCnchHive::getVirtuals() const
{
    return NamesAndTypesList{{"_path", std::make_shared<DataTypeString>()}, {"_file", std::make_shared<DataTypeString>()}};
}

HivePartitions StorageCnchHive::selectPartitions(
    ContextPtr local_context,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const HiveWhereOptimizer & optimizer)
{
    if (!metadata_snapshot->hasPartitionKey())
    {
        auto partition = std::make_shared<HivePartition>();
        partition->load(hive_table->sd);
        return {partition};
    }

    const auto & query_settings = local_context->getSettingsRef();
    String filter = {};
    if (query_settings.use_hive_metastore_filter && optimizer.partition_key_conds)
        filter = queryToString(optimizer.partition_key_conds);

    auto apache_hive_partitions = hive_client->getPartitionsByFilter(hive_db_name, hive_table_name, filter);

    std::optional<PartitionPruner> pruner;
    if (metadata_snapshot->hasPartitionKey() && query_settings.use_hive_partition_filter)
        pruner.emplace(metadata_snapshot, query_info, local_context, false);

    /// TODO: handle non partition key case
    HivePartitions partitions;
    partitions.reserve(apache_hive_partitions.size());
    for (const auto & apache_partition : apache_hive_partitions)
    {
        auto partition = std::make_shared<HivePartition>();
        partition->load(apache_partition, metadata_snapshot->getPartitionKey());
        bool can_be_pruned = pruner && pruner->canBePruned(partition->partition_id, partition->value);
        if (!can_be_pruned)
        {
            partitions.push_back(std::move(partition));
        }
    }

    LOG_DEBUG(log, "Read from {} partitions", partitions.size());
    return partitions;
}

void StorageCnchHive::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::MODIFY_SETTING)
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Alter of type {} is not supported by storage {}", alterTypeToString(command.type), getName());
        }
    }
}

void StorageCnchHive::alter(const AlterCommands & params, ContextPtr local_context, TableLockHolder &)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();

    params.apply(new_metadata, local_context);
    CnchHiveSettings new_settings = storage_settings ? *storage_settings : local_context->getCnchHiveSettings();
    const auto & settings_changes = new_metadata.settings_changes->as<const ASTSetQuery &>().changes;
    new_settings.applyChanges(settings_changes);

    // HiveSchemaConverter converter(local_context, hive_table);
    // converter.check(new_metadata);

    TransactionCnchPtr txn = local_context->getCurrentTransaction();
    auto action = txn->createAction<DDLAlterAction>(shared_from_this(), local_context->getSettingsRef(), local_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();
    /// replace table schema in catalog
    {
        String create_table_query = getCreateTableSql();
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(
            parser, create_table_query, local_context->getSettingsRef().max_query_size,
            local_context->getSettingsRef().max_parser_depth);

        auto & create_query = ast->as<ASTCreateQuery &>();
        if (new_metadata.settings_changes && create_query.storage)
        {
            ASTStorage & storage_ast = *create_query.storage;
            storage_ast.set(storage_ast.settings, new_metadata.settings_changes);
        }

        alter_act.setNewSchema(queryToString(ast));
    }

    txn->appendAction(action);
    txn->commitV1();
    *storage_settings = std::move(new_settings);

    setInMemoryMetadata(new_metadata);
}

std::optional<TableStatistics> StorageCnchHive::getTableStats(const Strings & columns, ContextPtr local_context)
{
    bool merge_partition_stats = local_context->getSettingsRef().merge_partition_stats;

    auto stats =  hive_client->getTableStats(hive_db_name, hive_table_name, columns, merge_partition_stats);
    if (stats)
        LOG_TRACE(log, "row_count {}", stats->row_count);
    else
        LOG_TRACE(log, "no stats");
    return stats;
}

std::shared_ptr<IDirectoryLister> StorageCnchHive::getDirectoryLister()
{
    auto disk = HiveUtil::getDiskFromURI(hive_table->sd.location, getContext(), *storage_settings);
    const auto & input_format = hive_table->sd.inputFormat;
    if (input_format == "org.apache.hudi.hadoop.HoodieParquetInputFormat")
    {
        return std::make_shared<HudiCowDirectoryLister>(disk);
    }
    else if (input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
    {
        return std::make_shared<DiskDirectoryLister>(disk, IHiveFile::FileFormat::PARQUET);
    }
    else if (input_format == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
    {
        return std::make_shared<DiskDirectoryLister>(disk, IHiveFile::FileFormat::ORC);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive format {}", input_format);
}

StorageID StorageCnchHive::prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context)
{
    size_t max_streams = local_context->getSettingsRef().max_threads;
    // if (max_block_size < local_context->getSettingsRef().max_block_size)
    //     max_streams = 1; // single block single stream.
    // if (max_streams > 1 && !isRemote())
    //     max_streams *= local_context->getSettingsRef().max_streams_to_max_threads_ratio;

    auto prepare_result = prepareReadContext(output_columns, getInMemoryMetadataPtr(), query_info, local_context, max_streams);

    StorageID storage_id = getStorageID();
    storage_id.table_name = prepare_result.local_table_name;
    return storage_id;
}

void registerStorageCnchHive(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_schema_inference = true,
    };

    factory.registerStorage(
        "CnchHive",
        [](const StorageFactory::Arguments & args) {
            ASTs & engine_args = args.engine_args;
            if (engine_args.size() != 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage CnchHive require 3 arguments: hive_metastore_url, hive_db_name and hive_table_name.");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            String hive_metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
            String hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            String hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

            StorageInMemoryMetadata metadata;
            std::shared_ptr<CnchHiveSettings> hive_settings = std::make_shared<CnchHiveSettings>(args.getContext()->getCnchHiveSettings());
            if (args.storage_def->settings)
            {
                hive_settings->loadFromQuery(*args.storage_def);
                metadata.settings_changes = args.storage_def->settings->ptr();
            }

            if (!args.columns.empty())
                metadata.setColumns(args.columns);

            metadata.setComment(args.comment);

            if (args.storage_def->partition_by)
            {
                ASTPtr partition_by_key;
                partition_by_key = args.storage_def->partition_by->ptr();
                metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());
            }

            if (args.storage_def->cluster_by)
            {
                ASTPtr cluster_by_key;
                cluster_by_key = args.storage_def->cluster_by->ptr();
                metadata.cluster_by_key = KeyDescription::getClusterByKeyFromAST(cluster_by_key, metadata.columns, args.getContext());
            }

            return StorageCnchHive::create(
                args.table_id, hive_metastore_url, hive_database, hive_table, std::move(metadata), args.getContext(), hive_settings, args.hive_client);
        },
        features);
}
}
#endif
