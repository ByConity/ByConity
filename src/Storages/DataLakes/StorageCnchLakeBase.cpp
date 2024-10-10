#include "StorageCnchLakeBase.h"

#include <CloudServices/CnchServerResource.h>
#include <DataStreams/narrowBlockInputStreams.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/PushFilterToStorage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/NullSource.h>
#include <Protos/lake_models.pb.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <ResourceManagement/CommonData.h>
#include <Storages/AlterCommands.h>
#include <Storages/DataLakes/HudiDirectoryLister.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/DirectoryLister.h>
#include <Storages/Hive/HiveFile/IHiveFile.h>
#include <Storages/Hive/HivePartition.h>
#include <Storages/Hive/HiveSchemaConverter.h>
#include <Storages/Hive/HiveWhereOptimizer.h>
#include <Storages/Hive/Metastore/HiveMetastore.h>
#include <Storages/Hive/StorageHiveSource.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <boost/lexical_cast.hpp>
#include <thrift/TToString.h>
#include <common/scope_guard_safe.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FORMAT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_PARTITIONS;
    extern const int INDEX_NOT_USED;
}

StorageCnchLakeBase::StorageCnchLakeBase(
    const StorageID & table_id_,
    const String & db_name_,
    const String & table_name_,
    ContextPtr context_,
    std::shared_ptr<CnchHiveSettings> settings_)
    : IStorage(table_id_), WithContext(context_), db_name(db_name_), table_name(table_name_), storage_settings(settings_)
{
}

StorageID StorageCnchLakeBase::prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context)
{
    auto prepare_result = prepareReadContext(output_columns, getInMemoryMetadataPtr(), query_info, local_context, maxStreams(local_context));
    StorageID storage_id = getStorageID();
    storage_id.table_name = prepare_result.local_table_name;
    return storage_id;
}

Pipe StorageCnchLakeBase::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context), BuildQueryPipelineSettings::fromContext(local_context));
}

void StorageCnchLakeBase::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    unsigned num_streams)
{
    PrepareContextResult result = prepareReadContext(column_names, storage_snapshot->metadata, query_info, local_context, num_streams);
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
        {},
        storage_snapshot,
        processed_stage,
        StorageID::createEmpty(), /// Don't check whether table exists in cnch-worker
        scalars,
        false,
        local_context->getExternalTables());

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, select_ast, local_context, worker_group);

    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);
}


QueryProcessingStage::Enum StorageCnchLakeBase::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const
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

std::optional<String> StorageCnchLakeBase::getVirtualWarehouseName(VirtualWarehouseType vw_type) const
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

void StorageCnchLakeBase::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
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

void StorageCnchLakeBase::alter(const AlterCommands & params, ContextPtr local_context, TableLockHolder &)
{
    checkAlterSettings(params);

    StorageInMemoryMetadata new_metadata = getInMemoryMetadataCopy();

    params.apply(new_metadata, local_context);
    CnchHiveSettings new_settings = storage_settings ? *storage_settings : local_context->getCnchHiveSettings();
    const auto & settings_changes = new_metadata.settings_changes->as<const ASTSetQuery &>().changes;
    new_settings.applyChanges(settings_changes);

    TransactionCnchPtr txn = local_context->getCurrentTransaction();
    auto action
        = txn->createAction<DDLAlterAction>(shared_from_this(), local_context->getSettingsRef(), local_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();
    /// replace table schema in catalog
    {
        String create_table_query = getCreateTableSql();
        alter_act.setOldSchema(create_table_query);
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(
            parser, create_table_query, local_context->getSettingsRef().max_query_size, local_context->getSettingsRef().max_parser_depth);

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

void StorageCnchLakeBase::serializeHiveFiles(Protos::ProtoHiveFiles & proto, const HiveFiles & hive_files)
{
    /// TODO: {caoliu} hack here
    if (!hive_files.empty() && hive_files.front()->partition)
    {
        proto.set_sd_url(hive_files.front()->partition->location);
    }

    for (const auto & hive_file : hive_files)
    {
        auto * proto_file = proto.add_files();
        hive_file->serialize(*proto_file);
    }
}

void StorageCnchLakeBase::collectResource(ContextPtr local_context, PrepareContextResult & result)
{
    auto worker_group = getWorkerGroupForTable(local_context, shared_from_this());
    auto cnch_resource = local_context->getCnchServerResource();
    auto txn_id = local_context->getCurrentTransactionID();
    StorageID cloud_storage_id = getStorageID();
    cloud_storage_id.table_name = cloud_storage_id.table_name + '_' + txn_id.toString();
    CloudTableBuilder builder;
    String cloud_table_sql
        = builder.setStorageID(cloud_storage_id).setMetadata(getInMemoryMetadataPtr()).setCloudEngine("CloudHive").build();

    LOG_TRACE(log, "Create cloud table sql {}", cloud_table_sql);
    cnch_resource->addCreateQuery(local_context, shared_from_this(), cloud_table_sql, builder.cloudTableName());
    cnch_resource->addDataParts(getStorageUUID(), result.hive_files);
    result.local_table_name = builder.cloudTableName();
}

void StorageCnchLakeBase::checkAlterSettings(const AlterCommands & commands) const
{
    static std::set<String> supported_settings = {"cnch_vw_default", "cnch_vw_read", "cnch_server_vw", "enable_local_disk_cache"};

    /// Check whether the value is legal for Setting.
    /// For example, we have a setting item, `SettingBool setting_test`
    /// If you submit a Alter query: "Alter table test modify setting setting_test='abc'"
    /// Then, it will throw an Exception here, because we can't convert string 'abc' to a Bool.
    auto settings_copy = *storage_settings;

    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::MODIFY_SETTING)
            continue;

        for (const auto & change : command.settings_changes)
        {
            if (!supported_settings.count(change.name))
                throw Exception("Setting " + change.name + " cannot be modified", ErrorCodes::SUPPORT_IS_DISABLED);

            settings_copy.set(change.name, change.value);
        }
    }
}
}
