/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <optional>
#include <Storages/StorageCnchMergeTree.h>

#include <Catalog/Catalog.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/CnchMergeMutateThread.h>
#include <CloudServices/CnchPartGCThread.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchServerResource.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <DaemonManager/DaemonManagerClient.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <Databases/DatabaseOnDisk.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <Interpreters/CnchSystemLog.h>
#include <MergeTreeCommon/CnchBucketTableCommon.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/CnchAttachProcessor.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndex.h>
#include <Storages/MergeTree/Index/MergeTreeSegmentBitmapIndex.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/VirtualColumnUtils.h>
#include <Transaction/CnchLock.h>
#include <Transaction/getCommitted.h>

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/CnchDataWriter.h>
#include <Core/NamesAndTypes.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/TreeRewriter.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/SelectQueryInfo.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Transaction/Actions/S3DetachMetaAction.h>
#include <brpc/controller.h>
#include <fmt/ranges.h>
#include <Common/Exception.h>
#include <Common/RowExistsColumnInfo.h>
#include <Common/parseAddress.h>
#include <common/logger_useful.h>
#include <DataTypes/ObjectUtils.h>
#include <Storages/StorageSnapshot.h>
#include <Transaction/TxnTimestamp.h>


namespace ProfileEvents
{
extern const Event CatalogTime;
extern const Event PrunePartsTime;
extern const Event TotalPartitions;
extern const Event PrunedPartitions;
extern const Event SelectedParts;
extern const Event PreloadSubmitTotalOps;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INDEX_NOT_USED;
    extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NO_SUCH_DATA_PART;
    extern const int BUCKET_TABLE_ENGINE_MISMATCH;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int READONLY_SETTING;
    extern const int UNKNOWN_CNCH_SNAPSHOT;
    extern const int INVALID_CNCH_SNAPSHOT;
    extern const int CANNOT_ASSIGN_ALTER;
}

static NameSet collectColumnsFromCommands(const AlterCommands & commands)
{
    NameSet res;
    for (const auto & command : commands)
        res.insert(command.column_name);
    return res;
}

/// Get basic select query to read from prepared pipe: remove prewhere, sampling, offset, final
static ASTPtr getBasicSelectQuery(const ASTPtr & original_query)
{
    auto query = original_query->clone();
    auto & select = query->as<ASTSelectQuery &>();
    auto & tables_in_select_query = select.refTables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables list is empty, it's a bug");
    auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no table expression, it's a bug");
    tables_element.table_expression->as<ASTTableExpression &>().final = false;
    tables_element.table_expression->as<ASTTableExpression &>().sample_size = nullptr;
    tables_element.table_expression->as<ASTTableExpression &>().sample_offset = nullptr;

    /// TODO @canh: can we just throw prewhere away?
    if (select.prewhere() && select.where())
        select.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", select.where(), select.prewhere()));
    else if (select.prewhere())
        select.setExpression(ASTSelectQuery::Expression::WHERE, select.prewhere()->clone());
    select.setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
    return query;
}

StorageCnchMergeTree::~StorageCnchMergeTree()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

StorageCnchMergeTree::StorageCnchMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach_,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeMetaBase::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : MergeTreeMetaBase(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_),
        table_id_.getNameForLogs() + " (CnchMergeTree)",
        false,
        attach_,
        [](const String &) {})
    , CnchStorageCommonHelper(table_id_, getDatabaseName(), getTableName())
{
    setServerVwName(getSettings()->cnch_server_vw);
    setProperties(metadata_, metadata_, false);
    checkTTLExpressions(metadata_, metadata_);

    String relative_table_path = getStoragePolicy(IStorage::StorageLocation::MAIN)
                                     ->getAnyDisk()
                                     ->getTableRelativePathOnDisk(UUIDHelpers::UUIDToString(table_id_.uuid));

    if (relative_data_path_.empty() || relative_table_path.empty())
    {
        MergeTreeMetaBase::setRelativeDataPath(IStorage::StorageLocation::MAIN, relative_table_path);
    }
    relative_auxility_storage_path = fs::path("auxility_store") / relative_table_path / "";
    format_version = MERGE_TREE_CHCH_DATA_STORAGTE_VERSION;

    const auto & all_columns = getInMemoryMetadataPtr()->getColumns();
    MergeTreeBitmapIndex::checkValidBitmapIndexType(all_columns);
    MergeTreeSegmentBitmapIndex::checkSegmentBitmapStorageGranularity(all_columns, getSettings());
}

/// NOTE: it involve a RPC. We have a CnchStorageCache to avoid invoking this RPC frequently.
void StorageCnchMergeTree::loadMutations()
{
    try
    {
        getContext()->getCnchCatalog()->fillMutationsByStorage(getStorageID(), mutations_by_version);

        auto print_mutations_debug_str = [&]() -> void {
            String res;
            for (auto const & [_, mutation] : mutations_by_version)
            {
                res += mutation.toString() + "\n";
            }
            if (!mutations_by_version.empty())
                LOG_TRACE(log, "All mutations:\n{}", res);
        };

        if (log->trace())
            print_mutations_debug_str();
    }
    catch(...)
    {
        LOG_WARNING(log, "Failed to fill mutations_by_version.");
    }
}

QueryProcessingStage::Enum StorageCnchMergeTree::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const
{
    const auto & settings = local_context->getSettingsRef();
    if (auto worker_group = local_context->tryGetCurrentWorkerGroup())
    {
        size_t num_workers = worker_group->getShardsInfo().size();
        size_t result_size = (num_workers * settings.max_parallel_replicas);
        return result_size == 1 ? QueryProcessingStage::Complete : QueryProcessingStage::WithMergeableState;
    }
    else
    {
        return QueryProcessingStage::WithMergeableState;
    }
}

void StorageCnchMergeTree::startup()
{
}

void StorageCnchMergeTree::shutdown()
{
}

Pipe StorageCnchMergeTree::read(
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

void StorageCnchMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    auto prepare_result = prepareReadContext(column_names, storage_snapshot->metadata, query_info, local_context);
    Block header = InterpreterSelectQuery(query_info.query, local_context, SelectQueryOptions(processed_stage)).getSampleBlock();

    auto worker_group = local_context->getCurrentWorkerGroup();
    /// Return directly (with correct header) if no shard read from
    if (!worker_group || worker_group->getShardsInfo().empty())
    {
        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (CnchMergeTree)");
        query_plan.addStep(std::move(read_from_pipe));
        return;
    }

    /// If no parts to read from - execute locally, must make sure that all stages are executed
    /// because CnchMergeTree is a high order storage
    if (prepare_result.parts.empty() && !getSettings()->enable_publish_version_on_commit)
    {
        /// Stage 1: read from source table, just assume we read everything
        const auto & source_columns = query_info.syntax_analyzer_result->required_source_columns;
        auto fetch_column_header = Block(NamesAndTypes{source_columns.begin(), source_columns.end()});
        Pipe pipe(std::make_shared<NullSource>(std::move(fetch_column_header)));
        /// Stage 2: (partial) aggregation and projection if any
        auto query = getBasicSelectQuery(query_info.query);
        // not support join query
        if (const auto & select = query->as<ASTSelectQuery>(); select && !select->join())
        {
            InterpreterSelectQuery(query, local_context, std::move(pipe), SelectQueryOptions(processed_stage)).buildQueryPlan(query_plan);
            return;
        }
    }

    auto modified_query_ast = query_info.query->clone();
    const Scalars & scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};
    ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
        header, {}, storage_snapshot, processed_stage, StorageID{"system", "one"}, scalars, false, local_context->getExternalTables());

    LOG_TRACE(log, "Original query before rewrite: {}", queryToString(query_info.query));
    modified_query_ast = rewriteSelectQuery(modified_query_ast, getDatabaseName(), prepare_result.local_table_name);

    LOG_TRACE(log, "After query rewrite: {}", queryToString(modified_query_ast));

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, modified_query_ast, local_context, worker_group);

    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);
}

PrepareContextResult StorageCnchMergeTree::prepareReadContext(
    const Names & column_names, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info, ContextPtr & local_context)
{
    auto txn = local_context->getCurrentTransaction();
    if (local_context->getServerType() == ServerType::cnch_server && txn)
        local_context->getCnchTransactionCoordinator().touchActiveTimestampByTable(getStorageID(), txn);

    auto storage_snapshot = getStorageSnapshot(metadata_snapshot, local_context);
    storage_snapshot->check(column_names);

    auto worker_group = local_context->getCurrentWorkerGroup();
    healthCheckForWorkerGroup(local_context, worker_group);

    UInt64 snapshot_ts = 0;
    if (String snapshot_name = local_context->getSettingsRef().use_snapshot.value; !snapshot_name.empty())
    {
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(getDatabaseName(), local_context);
        if (!database->supportSnapshot())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot query using snapshot {} because database of {} doesn't support snapshot",
                snapshot_name, getStorageID().getFullTableName());

        auto snapshot = database->tryGetSnapshot(snapshot_name);
        if (!snapshot)
            throw Exception(ErrorCodes::UNKNOWN_CNCH_SNAPSHOT,
                "Snapshot {} doesn't exists in db {}", snapshot_name, getDatabaseName());
        if (snapshot->has_table_uuid() && RPCHelpers::createUUID(snapshot->table_uuid()) != getStorageUUID())
            throw Exception(ErrorCodes::INVALID_CNCH_SNAPSHOT, "Snapshot {} doesn't bind to {}", snapshot_name, getStorageID().getNameForLogs());
        snapshot_ts = snapshot->commit_time();
    }

    String local_table_name = getCloudTableName(local_context);
    auto bucket_numbers = getRequiredBucketNumbers(query_info, local_context);
    UInt64 table_version = 0;
    ServerDataPartsWithDBM parts_with_dbm;

    if (getSettings()->enable_publish_version_on_commit && local_context->getSettingsRef().query_with_linear_table_version)
    {
        Stopwatch watch;
        TxnTimestamp ts = snapshot_ts ? TxnTimestamp{snapshot_ts} : local_context->getCurrentTransactionID();
        table_version = local_context->getCnchCatalog()->getCurrentTableVersion(getStorageUUID(), ts);
        ProfileEvents::increment(ProfileEvents::CatalogTime, watch.elapsedMilliseconds());

        // fill bucket numbers in order to assign by bucket
        if (bucket_numbers.empty() && isBucketTable())
        {
            Int64 total = metadata_snapshot->getBucketNumberFromClusterByKey();
            for (Int64 i = 0; i < total; ++i)
                bucket_numbers.insert(i);
        }
        LOG_INFO(log, "Total {} buckets to read in version {}", bucket_numbers.size(), table_version);
    }
    else
    {
        parts_with_dbm = selectPartsToReadWithDBM(column_names, local_context, query_info, snapshot_ts);
        if (metadata_snapshot->hasUniqueKey())
        {
            getDeleteBitmapMetaForServerParts(parts_with_dbm.first, parts_with_dbm.second);
        }
        LOG_INFO(log, "Total {} parts to read", parts_with_dbm.first.size());
    }

    auto & parts = parts_with_dbm.first;
    collectResource(local_context, table_version, parts, local_table_name, bucket_numbers, storage_snapshot);
    return {std::move(local_table_name), std::move(parts), {}, {}};
}

Strings StorageCnchMergeTree::getPartitionsByPredicate(const ASTPtr & predicate, ContextPtr local_context)
{
    auto partition_list = local_context->getCnchCatalog()->getPartitionList(shared_from_this(), local_context.get());
    if (partition_list.empty())
    {
        LOG_TRACE(log, "Return 0 partitions by predicate");
        return {};
    }

    auto where_expr = predicate->clone();
    ASTPtr actual_where_expr;
    const auto & partition_key_sample = MergeTreePartition::adjustPartitionKey(getInMemoryMetadataPtr(), local_context).sample_block;

    /// Extract the partition-related expressions before executing.
    MutableColumns columns_for_filter = partition_key_sample.cloneEmptyColumns();
    const auto & one_partition_key = partition_list[0]->value;
    for (size_t i = 0; i < one_partition_key.size(); ++i)
        columns_for_filter[i]->insert(one_partition_key[i]);

    auto filter_block = partition_key_sample.cloneWithColumns(std::move(columns_for_filter));
    auto unmodified = VirtualColumnUtils::prepareFilterBlockByPredicates({where_expr}, local_context, filter_block, actual_where_expr);

    /// If we can't extract related expressions, it means all partitions should be processed.
    if (unmodified || !actual_where_expr)
    {
        LOG_TRACE(log, "Return all partitions by predicate");
        Names all_partition_ids;
        for (const auto & partition : partition_list)
            all_partition_ids.emplace_back(partition->getID(partition_key_sample, extractNullableForPartitionID()));
        return all_partition_ids;
    }

    auto syntax_result = TreeRewriter(local_context).analyze(actual_where_expr, partition_key_sample.getNamesAndTypesList());
    auto actions = ExpressionAnalyzer(actual_where_expr, syntax_result, local_context).getActions(true);

    MutableColumns partition_columns = partition_key_sample.cloneEmptyColumns();
    for (const auto & partition : partition_list)
    {
        const auto & partition_key = partition->value;

        if (partition_key.size() != partition_key_sample.columns())
            throw Exception(
                fmt::format("Partition size({}) is different with partition key(size = {})", toString(partition_key.size()), toString(partition_key_sample.columns())),
                ErrorCodes::ILLEGAL_COLUMN);

        for (size_t i = 0; i < partition_key.size(); ++i)
        {
            partition_columns[i]->insert(partition_key[i]);
        }
    }

    auto block = partition_key_sample.cloneWithColumns(std::move(partition_columns));
    actions->execute(block);

    /// Check the result
    if (1 != block.columns())
        throw Exception("Wrong column number of WHERE clause's calculation result", ErrorCodes::LOGICAL_ERROR);

    if (block.getNamesAndTypesList().front().type->getName() != "UInt8")
        throw Exception("Wrong column type of WHERE clause's calculation result", ErrorCodes::LOGICAL_ERROR);

    Names filtered_partition_ids;
    const auto & res_column = block.getColumnsWithTypeAndName().front().column;
    for (size_t i = 0; i < partition_list.size(); ++i)
    {
        if (res_column->getBool(i))
            filtered_partition_ids.emplace_back(partition_list[i]->getID(partition_key_sample, extractNullableForPartitionID()));
    }

    LOG_TRACE(log, fmt::format("Return partitions by predicate:{}", fmt::join(filtered_partition_ids, ",")));
    return filtered_partition_ids;
}

ServerDataPartsVector StorageCnchMergeTree::getServerPartsByPredicate(
    const ASTPtr & predicate_, const std::function<ServerDataPartsVector()> & get_parts, ContextPtr local_context)
{
    const auto partition_key = MergeTreePartition::adjustPartitionKey(getInMemoryMetadataPtr(), local_context);
    const auto & partition_key_sample = partition_key.sample_block;

    /// Execute expr on block
    auto predicate = predicate_->clone();
    auto syntax_result = TreeRewriter(local_context).analyze(predicate, partition_key_sample.getNamesAndTypesList());
    ExpressionActionsPtr actions = ExpressionAnalyzer{predicate, syntax_result, local_context}.getActions(true);

    auto parts = get_parts();
    MutableColumns name_columns = partition_key_sample.cloneEmptyColumns();
    for (const auto & part : parts)
    {
        const auto & current_partition_key = part->partition().value;
        for (size_t c = 0; c < current_partition_key.size(); ++c)
        {
            name_columns[c]->insert(current_partition_key[c]);
        }
    }

    auto block = partition_key_sample.cloneWithColumns(std::move(name_columns));
    actions->execute(block);

    /// Check the result
    if (1 != block.columns())
        throw Exception("Wrong column number of WHERE clause's calculation result", ErrorCodes::LOGICAL_ERROR);

    if (block.getNamesAndTypesList().front().type->getName() != "UInt8")
        throw Exception("Wrong column type of WHERE clause's calculation result", ErrorCodes::LOGICAL_ERROR);

    /// Got the candidate parts
    ServerDataPartsVector candidate_parts;
    const auto & res_column = block.getColumnsWithTypeAndName().front().column;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        if (res_column->getBool(i))
            candidate_parts.emplace_back(parts[i]);
    }
    return candidate_parts;
}


static Block getBlockWithPartAndBucketColumn(ServerDataPartsVector & parts)
{
    auto part_column = ColumnString::create();
    auto bucket_column = ColumnInt64::create();

    for (const auto & part : parts)
    {
        part_column->insert(part->part_model_wrapper->name);
        bucket_column->insert(part->part_model().bucket_number());
    }

    Block res;
    res.insert({ColumnWithTypeAndName(std::move(part_column), std::make_shared<DataTypeString>(), "_part")});
    res.insert({ColumnWithTypeAndName(std::move(bucket_column), std::make_shared<DataTypeInt64>(), "_bucket_number")});
    return res;
}


time_t StorageCnchMergeTree::getTTLForPartition(const MergeTreePartition & partition) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasRowsTTL())
        return 0;

    /// make a copy of rows_ttl, we may rewrite it later.
    auto rows_ttl = metadata_snapshot->table_ttl.rows_ttl;

    /// Construct a block consists of partition keys then compute ttl values according to this block
    const auto & partition_key_sample = metadata_snapshot->getPartitionKey().sample_block;
    MutableColumns columns = partition_key_sample.cloneEmptyColumns();
    /// This can happen when ALTER query is implemented improperly; finish ALTER query should bypass this check.
    if (columns.size() != partition.value.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Partition key columns definition missmatch between inmemory and metastore, this is a bug, expect block ({}), got values "
            "({})\n",
            partition_key_sample.dumpNames(),
            fmt::join(partition.value, ", "));
    for (size_t i = 0; i < partition.value.size(); ++i)
        columns[i]->insert(partition.value[i]);

    auto block = partition_key_sample.cloneWithColumns(std::move(columns));

    TTLDescription::tryRewriteTTLWithPartitionKey(
        rows_ttl, metadata_snapshot->columns, metadata_snapshot->partition_key, metadata_snapshot->primary_key, getContext());
    rows_ttl.expression->execute(block);

    const auto & current = block.getByName(rows_ttl.result_column);

    const IColumn * column = current.column.get();

    if (column->size() > 1)
        throw Exception("Cannot get TTL value from table ttl ast since there are multiple ttl value", ErrorCodes::LOGICAL_ERROR);

    if (column->isNullable())
        column = static_cast<const ColumnNullable *>(column)->getNestedColumnPtr().get();

    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
    {
        const auto & date_lut = DateLUT::serverTimezoneInstance();
        return date_lut.fromDayNum(DayNum(column_date->getElement(0)));
    }
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
    {
        return column_date_time->getElement(0);
    }
    else
        throw Exception("Unexpected type of result ttl column", ErrorCodes::LOGICAL_ERROR);
}

void StorageCnchMergeTree::filterPartsByPartition(
    ServerDataPartsVector & parts, ContextPtr local_context, const SelectQueryInfo & query_info, const Names & column_names_to_return) const
{
    /// Fine grained parts pruning by:
    /// (1) partition min-max
    /// (2) min-max index
    /// (3) part name (if _part is in the query) and uuid (todo)
    /// (4) primary key (TODO)
    /// (5) block id for deduped part (TODO)

    const Settings & settings = local_context->getSettingsRef();
    std::optional<PartitionPruner> partition_pruner;
    std::optional<KeyCondition> minmax_idx_condition;
    DataTypes minmax_columns_types;
    auto metadata_snapshot = getInMemoryMetadataPtr();

    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        auto minmax_columns_names = getMinMaxColumnsNames(partition_key);
        minmax_columns_types = getMinMaxColumnsTypes(partition_key);

        minmax_idx_condition.emplace(
            query_info,
            local_context,
            minmax_columns_names,
            getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(local_context)));
        partition_pruner.emplace(metadata_snapshot, query_info, local_context, false /* strict */);

        if (settings.force_index_by_date && (minmax_idx_condition->alwaysUnknownOrTrue() && partition_pruner->isUseless()))
        {
            String msg = "Neither MinMax index by columns (";
            bool first = true;
            for (const String & col : minmax_columns_names)
            {
                if (first)
                    first = false;
                else
                    msg += ", ";
                msg += col;
            }
            msg += ") nor partition expr is used and setting 'force_index_by_date' is set";

            throw Exception(msg, ErrorCodes::INDEX_NOT_USED);
        }
    }

    /// If `_part` or `_bucket_number` virtual column is requested, we try to use it as an index.
    Block virtual_columns_block = getBlockWithPartAndBucketColumn(parts);
    bool part_filter_queried = std::any_of(column_names_to_return.begin(), column_names_to_return.end(), [](const auto & name) {
        return name == "_part" || name == "_bucket_number";
    });
    if (part_filter_queried)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, local_context);
    auto part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");

    size_t prev_sz = parts.size();
    size_t empty = 0, partition_minmax = 0, minmax_idx = 0, part_value = 0;
    std::erase_if(parts, [&](const auto & part) {
        auto base_part = part->getBasePart();
        if (base_part->isEmpty())
        {
            ++empty;
            return true;
        }
        else if (partition_pruner && partition_pruner->canBePruned(*base_part))
        {
            ++partition_minmax;
            return true;
        }
        else if (
            minmax_idx_condition
            && !minmax_idx_condition->checkInHyperrectangle(base_part->minmax_idx()->hyperrectangle, minmax_columns_types).can_be_true)
        {
            ++minmax_idx;
            return true;
        }
        else if (part_values.find(part->name()) == part_values.end())
        {
            ++part_value;
            return true;
        }

        return false;
    });

    if (parts.size() < prev_sz)
        LOG_DEBUG(
            log,
            "Parts pruning rules dropped {} parts, include {} empty parts, {} parts by partition minmax, {} parts by minmax index, {} "
            "parts by part value",
            prev_sz - parts.size(),
            empty,
            partition_minmax,
            minmax_idx,
            part_value);
}

/// Add related tables for active timestamps
static void touchActiveTimestampForInsertSelectQuery(const ASTInsertQuery & insert_query, ContextPtr local_context)
{
    if (!insert_query.select)
        return;

    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        return;

    auto & txn_coordinator = local_context->getCnchTransactionCoordinator();
    auto current_database = local_context->getCurrentDatabase();

    ASTs related_tables;
    bool has_table_func = false;
    if (const auto * select = insert_query.select.get())
        ASTSelectQuery::collectAllTables(select, related_tables, has_table_func);

    for (auto & db_and_table_ast : related_tables)
    {
        DatabaseAndTableWithAlias db_and_table(db_and_table_ast, current_database);
        if (db_and_table.database == "system" || db_and_table.database == "default")
            continue;

        if (auto table = DatabaseCatalog::instance().tryGetTable(StorageID{db_and_table.database, db_and_table.table}, local_context))
            txn_coordinator.touchActiveTimestampByTable(table->getStorageID(), txn);
    }
}

static String extractTableSuffix(const String & gen_table_name)
{
    return gen_table_name.substr(gen_table_name.find_last_of('_') + 1);
}

static std::pair<String, ASTPtr> replaceMaterializedViewQuery(StorageMaterializedView * mv, const String & table_suffix)
{
    auto query = mv->getCreateTableSql();

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);


    auto & create_query = ast->as<ASTCreateQuery &>();
    create_query.to_inner_uuid = UUIDHelpers::Nil;
    create_query.table += "_" + table_suffix;
    /// If creating MV is `CREATE MATERIALIZED VIEW AS SELECT ... ENGINE = CnchMergeTree()...`
    /// Then we change the query to CREATE MATERIALIZED VIEW TO `mv_db.`.inner.mv_name`` AS SELECT ...
    if (create_query.to_table_id.empty())
    {
        create_query.to_table_id.table_name = generateInnerTableName(mv->getStorageID());
        create_query.to_table_id.database_name = mv->getDatabaseName();
        create_query.storage = nullptr;
    }
    create_query.to_table_id.table_name += "_" + table_suffix;

    auto & inner_query = create_query.select->list_of_selects->children.at(0);
    if (!inner_query)
        throw Exception("Select query is necessary for mv table", ErrorCodes::LOGICAL_ERROR);

    auto & select_query = inner_query->as<ASTSelectQuery &>();
    select_query.replaceDatabaseAndTable(
        mv->getInMemoryMetadataPtr()->select.select_table_id.database_name,
        mv->getInMemoryMetadataPtr()->select.select_table_id.table_name + "_" + table_suffix);

    return {getTableDefinitionFromCreateQuery(ast, false), select_query.clone()};
}

NameSet StorageCnchMergeTree::genViewDependencyCreateQueries(
    const StorageID & storage_id, ContextPtr local_context, const String & table_suffix, std::set<String> & cnch_table_create_queries)
{
    NameSet create_view_sqls;
    std::set<StorageID> view_dependencies;
    auto storage = DatabaseCatalog::instance().getTable(storage_id, local_context);
    auto start_time = local_context->getTimestamp();

    auto catalog_client = local_context->getCnchCatalog();
    if (!catalog_client)
        throw Exception("Get catalog client failed", ErrorCodes::LOGICAL_ERROR);

    auto all_views_from_catalog = catalog_client->getAllViewsOn(*local_context, storage, start_time);
    if (all_views_from_catalog.empty())
        return create_view_sqls;

    for (auto & view : all_views_from_catalog)
        view_dependencies.emplace(view->getStorageID());

    for (const auto & dependence : view_dependencies)
    {
        auto table = DatabaseCatalog::instance().getTable(dependence, local_context);
        if (!table)
        {
            LOG_WARNING(log, "Table {} not found", dependence.getNameForLogs());
            continue;
        }

        if (auto * mv = dynamic_cast<StorageMaterializedView *>(table.get()))
        {
            if (mv->async())
                continue;
            auto target_table = mv->tryGetTargetTable();
            if (!target_table)
            {
                LOG_WARNING(log, "Target table for {} not exist", mv->getTargetTableName());
                continue;
            }

            /// target table should be CnchMergeTree
            auto * cnch_merge = dynamic_cast<StorageCnchMergeTree *>(target_table.get());
            if (!cnch_merge)
            {
                LOG_WARNING(log, "Table type not matched for {}, CnchMergeTree is expected", target_table->getTableName());
                continue;
            }
            auto create_target_query = target_table->getCreateTableSql();
            bool enable_staging_area = cnch_merge->getInMemoryMetadataPtr()->hasUniqueKey()
                && bool(local_context->getSettingsRef().enable_staging_area_for_write);
            auto create_local_target_query = getCreateQueryForCloudTable(
                create_target_query,
                cnch_merge->getTableName() + "_" + table_suffix,
                local_context,
                enable_staging_area,
                cnch_merge->getStorageID());
            create_view_sqls.insert(create_local_target_query);

            ASTPtr rewrite_inner_ast;
            String rewrite_mv_sql;
            std::tie(rewrite_mv_sql, rewrite_inner_ast) = replaceMaterializedViewQuery(mv, table_suffix);
            create_view_sqls.insert(rewrite_mv_sql);

            /// After rewrite try to analyze materialized view inner query and extract cnch tables which need be dispatched to worker
            ASTs related_tables;
            bool have_table_function = false;
            ASTSelectQuery::collectAllTables(rewrite_inner_ast.get(), related_tables, have_table_function);
            for (auto & db_and_table_ast : related_tables)
            {
                DatabaseAndTableWithAlias db_and_table(db_and_table_ast);
                if (auto cnch_table = DatabaseCatalog::instance().tryGetTable(StorageID(db_and_table.database, db_and_table.table), local_context))
                    cnch_table_create_queries.insert(cnch_table->getCreateTableSql());
            }
        }

        /// TODO: Check cascade view dependency
    }

    return create_view_sqls;
}

std::pair<String, const Cluster::ShardInfo *> StorageCnchMergeTree::prepareLocalTableForWrite(
    ASTInsertQuery * insert_query, ContextPtr local_context, bool enable_staging_area, bool send_query_in_normal_mode)
{
    auto generated_tb_name = getCloudTableName(local_context);
    auto local_table_name = generated_tb_name + "_write";
    if (insert_query)
        insert_query->table_id.table_name = local_table_name;

    auto create_local_tb_query = getCreateQueryForCloudTable(getCreateTableSql(), local_table_name, local_context, enable_staging_area);

    WorkerGroupHandle worker_group = local_context->getCurrentWorkerGroup();

    /// TODO: currently use only one write worker to do insert, use multiple write workers when distributed write is support
    const Settings & settings = local_context->getSettingsRef();
    int max_retry = 2, retry = 0;
    auto num_of_workers = worker_group->getShardsInfo().size();
    if (!num_of_workers)
        throw Exception("No heathy worker available", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

    std::vector<CnchWorkerClientPtr> worker_clients_for_send;
    std::size_t index = std::hash<String>{}(local_context->getCurrentQueryId() + std::to_string(retry)) % num_of_workers;
    const auto * write_shard_ptr = &(worker_group->getShardsInfo().at(index));

    if (!send_query_in_normal_mode)
    {
        worker_clients_for_send = worker_group->getWorkerClients();
    }
    else
    {
        // TODO: healthy check by rpc
        if (settings.query_worker_fault_tolerance)
        {
            ConnectionTimeouts connection_timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(local_context->getSettingsRef());

            // Perform health check for selected write_shard and retry for 2 more times if there are enough write workers.
            while (true)
            {
                LOG_TRACE(log, "Health check for worker: {}", write_shard_ptr->worker_id);
                try
                {
                    // The checking task checks whether the current connection is connected or can connect.
                    auto entry = write_shard_ptr->pool->get(connection_timeouts, &settings, true);
                    Connection * conn = &(*entry);
                    conn->tryConnect(connection_timeouts);
                    break;
                }
                catch (const NetException &)
                {
                    // Don't throw network exception, instead remove the unhealthy worker unless no more available workers or reach retry limit.
                    if (++retry > max_retry)
                        throw Exception(
                            "Cannot find healthy worker after " + std::to_string(max_retry) + " times retries.",
                            ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

                    index = (index + 1) % num_of_workers;
                    write_shard_ptr = &(worker_group->getShardsInfo().at(index));
                }
            }
        }

        auto worker_client = worker_group->getWorkerClient(index, /*skip_busy_worker*/false).second;
        worker_clients_for_send.emplace_back(worker_client);
    }

    for (auto & worker_client : worker_clients_for_send)
    {
        LOG_DEBUG(
            log,
            "Will send create query: {} to target worker: {}",
            create_local_tb_query,
            worker_client->getHostWithPorts().toDebugString());

        worker_client->sendCreateQueries(local_context, {create_local_tb_query});

        auto table_suffix = extractTableSuffix(generated_tb_name);
        std::set<String> cnch_table_create_queries;
        NameSet dependency_create_queries = genViewDependencyCreateQueries(getStorageID(), local_context, table_suffix + "_write", cnch_table_create_queries);
        for (const auto & dependency_create_query : dependency_create_queries)
            LOG_DEBUG(log, "send local table (for view write) create query {}", dependency_create_query);
        for (const auto & cnch_table_query : cnch_table_create_queries)
            LOG_DEBUG(log, "send cnch table (for view subquery join) create query {}", cnch_table_query);
        std::vector<String> dependency_create_queries_vec(dependency_create_queries.begin(), dependency_create_queries.end());
        worker_client->sendCreateQueries(local_context, dependency_create_queries_vec, cnch_table_create_queries);
    }

    if (send_query_in_normal_mode)
    {
        /// Ensure worker session local_context resource could be released
        if (auto session_resource = local_context->tryGetCnchServerResource())
        {
            std::vector<size_t> index_values{index};
            session_resource->setWorkerGroup(std::make_shared<WorkerGroupHandleImpl>(*worker_group, index_values));
        }

        if (insert_query)
        {
            String query_statement = queryToString(*insert_query);

            LOG_DEBUG(log, "Prepare execute insert query: {}", query_statement);
            /// TODO: send insert query by rpc.
            sendQueryPerShard(local_context, query_statement, *write_shard_ptr, true);
        }
    }

    return std::make_pair(local_table_name, write_shard_ptr);
}

BlockOutputStreamPtr StorageCnchMergeTree::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto modified_query_ast = query->clone();
    auto & insert_query = modified_query_ast->as<ASTInsertQuery &>();

    if (insert_query.table_id.database_name.empty())
        insert_query.table_id.database_name = local_context->getCurrentDatabase();

    return std::make_shared<CloudMergeTreeBlockOutputStream>(*this, metadata_snapshot, local_context, insert_query.is_overwrite ? insert_query.overwrite_partition : nullptr);
}

/// for insert select and insert infile
BlockInputStreamPtr
StorageCnchMergeTree::writeInWorker(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    bool enable_staging_area = metadata_snapshot->hasUniqueKey() && bool(local_context->getSettingsRef().enable_staging_area_for_write);
    if (enable_staging_area)
        LOG_DEBUG(log, "enable staging area for write");

    auto modified_query_ast = query->clone();
    auto & insert_query = modified_query_ast->as<ASTInsertQuery &>();

    if (insert_query.table_id.database_name.empty())
        insert_query.table_id.database_name = local_context->getCurrentDatabase();

    if (insert_query.select)
        touchActiveTimestampForInsertSelectQuery(insert_query, local_context);

    if (insert_query.select && local_context->getSettingsRef().restore_table_expression_in_distributed)
    {
        RestoreTableExpressionsVisitor::Data data;
        data.database = local_context->getCurrentDatabase();
        RestoreTableExpressionsVisitor(data).visit(insert_query.select);
    }

    std::pair<String, const Cluster::ShardInfo *> write_shard_info
        = prepareLocalTableForWrite(&insert_query, local_context, enable_staging_area, true);
    String query_statement = queryToString(insert_query);

    LOG_DEBUG(log, "Prepare execute insert query: {}", query_statement);
    return sendQueryPerShard(local_context, query_statement, *write_shard_info.second, true);
}

HostWithPortsVec StorageCnchMergeTree::getWriteWorkers(const ASTPtr & /**/, ContextPtr local_context)
{
    String vw_name = local_context->getSettingsRef().virtual_warehouse;
    if (vw_name.empty())
        vw_name = getSettings()->cnch_vw_write;

    if (vw_name.empty())
        throw Exception("Expected a nonempty vw name. Please specify it in query or table settings", ErrorCodes::BAD_ARGUMENTS);

    // No fixed workers for insertion, pick one randomly from worker pool
    auto vw_handle = local_context->getVirtualWarehousePool().get(vw_name);
    HostWithPortsVec res;
    for (const auto & [_, wg] : vw_handle->getAll(VirtualWarehouseHandleImpl::TryUpdate))
    {
        auto wg_hosts = wg->getHostWithPortsVec();
        res.insert(res.end(), wg_hosts.begin(), wg_hosts.end());
    }
    return res;
}

bool StorageCnchMergeTree::optimize(
    const ASTPtr & query, const StorageMetadataPtr &, const ASTPtr & partition, bool final, bool, const Names &, ContextPtr query_context)
{
    auto & optimize_query = query->as<ASTOptimizeQuery &>();
    auto enable_try = optimize_query.enable_try;
    if (optimize_query.final && query_context->getSettingsRef().disable_optimize_final)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FINAL is disabled because it is dangerous");

    auto bg_thread = query_context->tryGetCnchBGThread(CnchBGThreadType::MergeMutate, getStorageID());
    auto timeout_ms = query_context->getSettingsRef().max_execution_time.totalMilliseconds();

    if (!bg_thread)
    {
        auto daemon_manage_client = getContext()->getDaemonManagerClient();
        auto enable_sync = query_context->getSettingsRef().mutations_sync;
        auto partition_id = partition ? getPartitionIDFromQuery(partition, query_context) : "all";
        daemon_manage_client->forwardOptimizeQuery(getStorageID(), partition_id, enable_try, enable_sync, timeout_ms);
        return true;
    }

    Strings partition_ids;

    if (!partition)
        partition_ids = query_context->getCnchCatalog()->getPartitionIDs(shared_from_this(), query_context.get());
    else
        partition_ids.push_back(getPartitionIDFromQuery(partition, query_context));

    auto istorage = shared_from_this();
    auto * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(bg_thread.get());
    std::vector<String> task_ids;
    for (const auto & partition_id : partition_ids)
    {
        auto task_id = merge_mutate_thread->triggerPartMerge(istorage, partition_id, final, enable_try, false);
        if (!task_id.empty())
            task_ids.push_back(task_id);
    }

    if (query_context->getSettingsRef().mutations_sync != 0)
        merge_mutate_thread->waitTasksFinish(task_ids, timeout_ms);

    auto bg_gc_thread = query_context->tryGetCnchBGThread(CnchBGThreadType::PartGC, getStorageID());
    if (bg_gc_thread)
    {
        auto * gc_thread = dynamic_cast<CnchPartGCThread *>(bg_gc_thread.get());
        for (const auto & partition_id : partition_ids)
            gc_thread->addCandidatePartition(partition_id);
    }

    return true;
}

CheckResults StorageCnchMergeTree::checkDataCommon(const ASTPtr & query, ContextPtr local_context, ServerDataPartsVector & parts) const
{
    String local_table_name = getCloudTableName(local_context);

    auto create_table_query = getCreateQueryForCloudTable(getCreateTableSql(), local_table_name, local_context, true);

    if (local_context->getCnchCatalog())
    {
        if (const auto & check_query = query->as<ASTCheckQuery &>(); check_query.partition)
        {
            String partition_id = getPartitionIDFromQuery(check_query.partition, local_context);
            parts = local_context->getCnchCatalog()->getServerDataPartsInPartitions(
                shared_from_this(), {partition_id}, local_context->getCurrentTransactionID(), nullptr);
        }
        else
        {
            parts = getAllPartsWithDBM(local_context).first;
        }
    }

    if (parts.empty())
        return {};

    auto worker_group = getWorkerGroupForTable(*this, local_context);
    auto worker_clients = worker_group->getWorkerClients();
    const auto & shards_info = worker_group->getShardsInfo();

    size_t num_of_workers = shards_info.size();
    if (!num_of_workers)
        throw Exception("No heathy worker available.",ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

    std::mutex mutex;
    CheckResults results;

    auto assignment = assignCnchParts(worker_group, parts, local_context, getSettings());

    ThreadPool allocate_pool(std::min<UInt64>(local_context->getSettingsRef().parts_preallocate_pool_size, num_of_workers));

    for (size_t i = 0; i < num_of_workers; ++i)
    {
        auto allocate_task = [&, i] {
            auto it = assignment.find(shards_info[i].worker_id);
            if (it == assignment.end())
                return;

            auto result = worker_clients[i]->checkDataParts(local_context, *this, local_table_name, create_table_query, it->second);

            {
               std::lock_guard lock(mutex);
               results.insert(results.end(), result.begin(), result.end());
            }
        };

        allocate_pool.scheduleOrThrowOnError(allocate_task);
    }
    allocate_pool.wait();

    return results;
}

CheckResults StorageCnchMergeTree::checkData(const ASTPtr & query, ContextPtr local_context)
{
    ServerDataPartsVector parts;
    return checkDataCommon(query, local_context, parts);
}

CheckResults StorageCnchMergeTree::autoRemoveData(const ASTPtr & query, ContextPtr local_context)
{
    ServerDataPartsVector error_server_parts;
    CheckResults check_results =  checkDataCommon(query, local_context, error_server_parts);

    MergeTreeDataPartsCNCHVector error_parts;
    CheckResults error_results;
    for (size_t i=0; i<check_results.size(); i++)
    {
        if (check_results[i].success)
            continue;
        error_parts.push_back(error_server_parts[i]->toCNCHDataPart(*this));
        error_results.push_back(check_results[i]);
    }

    if (error_parts.size())
    {
        if (auto catalog = local_context->getCnchCatalog())
            catalog->clearDataPartsMeta(shared_from_this(), error_parts);
    }

    return error_results;
}

ServerDataPartsWithDBM StorageCnchMergeTree::getAllPartsWithDBM(ContextPtr local_context) const
{
    ServerDataPartsWithDBM res;
    if (local_context->getCnchCatalog())
    {
        TransactionCnchPtr cur_txn = local_context->getCurrentTransaction();
        if (cur_txn->isSecondary())
        {
            /// Get all parts in the partition list
            LOG_DEBUG(log, "Current transaction is secondary transaction, result may include uncommitted data");
            res = local_context->getCnchCatalog()->getAllServerDataPartsWithDBM(shared_from_this(), {0}, local_context.get());
            /// Fillter by commited parts and parts written by same explicit transaction
            res.first = filterPartsInExplicitTransaction(res.first, local_context);
        }
        else
        {
            res = local_context->getCnchCatalog()->getAllServerDataPartsWithDBM(
                shared_from_this(), cur_txn->getTransactionID(), local_context.get());
        }
        res.first = CnchPartsHelper::calcVisibleParts(res.first, false, CnchPartsHelper::getLoggingOption(*local_context));
    }
    LOG_INFO(log, "Number of parts get from catalog: {}", res.first.size());
    return res;
}

ServerDataPartsWithDBM StorageCnchMergeTree::getAllPartsInPartitionsWithDBM(
    const Names & column_names_to_return,
    ContextPtr local_context,
    const SelectQueryInfo & query_info,
    UInt64 snapshot_ts,
    bool staging_area) const
{
    ServerDataPartsWithDBM res;
    auto & all_parts = res.first;
    auto & all_bitmaps = res.second;

    if (auto catalog = local_context->getCnchCatalog())
    {
        TransactionCnchPtr cur_txn = local_context->getCurrentTransaction();
        if (!cur_txn)
            throw Exception("current txn is not set.", ErrorCodes::LOGICAL_ERROR);

        Stopwatch watch;

        /// ignoring snapshot because partition infos are not cleaned right now
        auto pruned_res = getPrunedPartitions(query_info, column_names_to_return, local_context);
        Strings & pruned_partitions = pruned_res.partitions;
        UInt64 total_partition_number = pruned_res.total_partition_number;

        if (cur_txn->isSecondary())
        {
            /// TODO: snapshot read in interactive txn?
            /// Get all parts in the partition list
            LOG_DEBUG(log, "Current transaction is secondary transaction, result may include uncommitted data");

            if (staging_area)
            {
                NameSet partition_filter(pruned_partitions.begin(), pruned_partitions.end());
                res = catalog->getStagedServerDataPartsWithDBM(shared_from_this(), {0}, &partition_filter);
            }
            else
                res = catalog->getServerDataPartsInPartitionsWithDBM(
                    shared_from_this(), pruned_partitions, {0}, local_context.get());
            /// Fillter by commited parts and parts written by same explicit transaction
            all_parts = filterPartsInExplicitTransaction(all_parts, local_context);
        }
        else
        {
            if (staging_area)
            {
                NameSet partition_filter(pruned_partitions.begin(), pruned_partitions.end());
                res = catalog->getStagedServerDataPartsWithDBM(
                    shared_from_this(),
                    snapshot_ts ? TxnTimestamp(snapshot_ts) : local_context->getCurrentTransactionID(),
                    &partition_filter);
            }
            else
                res = catalog->getServerDataPartsInPartitionsWithDBM(
                    shared_from_this(), pruned_partitions,
                    snapshot_ts ? TxnTimestamp(snapshot_ts) : local_context->getCurrentTransactionID(),
                    local_context.get());
            if (snapshot_ts)
            {
                auto trashed_parts_with_dbm = catalog->getTrashedPartsInPartitionsWithDBM(shared_from_this(), pruned_partitions, snapshot_ts);
                auto & trashed_parts = trashed_parts_with_dbm.first;
                auto & bitmaps = trashed_parts_with_dbm.second;
                std::move(trashed_parts.begin(), trashed_parts.end(), std::back_inserter(all_parts));
                std::move(bitmaps.begin(), bitmaps.end(), std::back_inserter(all_bitmaps));
            }
        }
        // TEST_LOG(testlog, "get dataparts in partitions.");
        LOG_DEBUG(log, "Total number of parts get from bytekv: {}", all_parts.size());
        auto catalog_time_ms = watch.elapsedMilliseconds();
        all_parts = CnchPartsHelper::calcVisibleParts(all_parts, false, CnchPartsHelper::getLoggingOption(*local_context));

        ProfileEvents::increment(ProfileEvents::CatalogTime, catalog_time_ms);
        ProfileEvents::increment(ProfileEvents::PrunePartsTime, watch.elapsedMilliseconds() - catalog_time_ms);
        ProfileEvents::increment(ProfileEvents::TotalPartitions, total_partition_number);
        ProfileEvents::increment(ProfileEvents::PrunedPartitions, pruned_partitions.size());
        ProfileEvents::increment(ProfileEvents::SelectedParts, all_parts.size());
    }

    // TEST_END(testlog, "Get pruned parts from Catalog Service");
    LOG_INFO(log, "Number of parts get from catalog: {}", all_parts.size());
    return res;
}


ServerDataPartsWithDBM StorageCnchMergeTree::selectPartsToReadWithDBM(
    const Names & column_names_to_return, ContextPtr local_context, const SelectQueryInfo & query_info, UInt64 snapshot_ts, bool staging_area) const
{
    auto parts = getAllPartsInPartitionsWithDBM(column_names_to_return, local_context, query_info, snapshot_ts, staging_area);
    filterPartsByPartition(parts.first, local_context, query_info, column_names_to_return);
    return parts;
}

MergeTreeDataPartsCNCHVector StorageCnchMergeTree::getUniqueTableMeta(TxnTimestamp ts, const Strings & input_partitions, bool force_bitmap, const std::set<Int64> & bucket_numbers)
{
    auto catalog = getContext()->getCnchCatalog();
    auto storage = shared_from_this();

    Strings partitions;
    if (!input_partitions.empty())
        partitions = input_partitions;
    else
        partitions = catalog->getPartitionIDs(storage, nullptr);

    auto cnch_parts_with_dbm = catalog->getServerDataPartsInPartitionsWithDBM(storage, partitions, ts, nullptr, Catalog::VisibilityLevel::Visible, bucket_numbers);
    auto parts = CnchPartsHelper::calcVisibleParts(cnch_parts_with_dbm.first, /*collect_on_chain=*/false);

    MergeTreeDataPartsCNCHVector res;
    res.reserve(parts.size());
    for (auto & part : parts)
        res.emplace_back(dynamic_pointer_cast<const MergeTreeDataPartCNCH>(part->getBasePart()->toCNCHDataPart(*this)));

    getDeleteBitmapMetaForCnchParts(res, cnch_parts_with_dbm.second, force_bitmap);
    return res;
}

MergeTreeDataPartsCNCHVector
StorageCnchMergeTree::getStagedParts(const TxnTimestamp & ts, const NameSet * partitions, bool skip_delete_bitmap)
{
    auto catalog = getContext()->getCnchCatalog();
    MergeTreeDataPartsCNCHVector staged_parts = catalog->getStagedParts(shared_from_this(), ts, partitions);
    auto res = CnchPartsHelper::calcVisibleParts(staged_parts, /*collect_on_chain*/ false);

    if (!skip_delete_bitmap)
        getDeleteBitmapMetaForStagedParts(res, getContext(), ts);
    return res;
}

void StorageCnchMergeTree::executeDedupForRepair(const ASTSystemQuery & query, ContextPtr local_context)
{
    if (!getInMemoryMetadataPtr()->hasUniqueKey())
        throw Exception("SYSTEM DEDUP can only be executed on table with UNIQUE KEY", ErrorCodes::BAD_ARGUMENTS);

    const auto & partition = query.partition;
    Int64 bucket_number = static_cast<Int64>(query.bucket_number);
    if (partition && !getSettings()->partition_level_unique_keys)
        throw Exception("SYSTEM DEDUP PARTITION can only be used on table with partition_level_unique_keys=1", ErrorCodes::BAD_ARGUMENTS);
    if (!partition && getSettings()->partition_level_unique_keys && query.specify_bucket)
        throw Exception("SYSTEM DEDUP BUCKET without partition can only be used on table with partition_level_unique_keys=0", ErrorCodes::BAD_ARGUMENTS);

    auto txn = local_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Transaction is not set", ErrorCodes::LOGICAL_ERROR);
    txn->setMainTableUUID(getStorageUUID());

    auto catalog = local_context->getCnchCatalog();

    CnchDedupHelper::DedupScope scope = CnchDedupHelper::DedupScope::TableDedup();
    if (partition)
    {
        if (query.specify_bucket)
        {
            CnchDedupHelper::DedupScope::BucketWithPartitionSet bucket_with_partition_set;
            bucket_with_partition_set.insert({getPartitionIDFromQuery(partition, local_context), bucket_number});
            scope = CnchDedupHelper::DedupScope::PartitionDedupWithBucket(bucket_with_partition_set);
        }
        else
        {
            NameOrderedSet partitions;
            partitions.insert(getPartitionIDFromQuery(partition, local_context));
            scope = CnchDedupHelper::DedupScope::PartitionDedup(partitions);
        }
    }
    else if (query.specify_bucket)
    {
        CnchDedupHelper::DedupScope::BucketSet buckets;
        buckets.insert(bucket_number);
        scope = CnchDedupHelper::DedupScope::TableDedupWithBucket(buckets);
    }

    auto cnch_lock = std::make_shared<CnchLockHolder>(
        local_context,
        CnchDedupHelper::getLocksToAcquire(
            scope, txn->getTransactionID(), *this, CnchDedupHelper::getWriteLockTimeout(*this, local_context)));
    txn->appendLockHolder(cnch_lock);
    cnch_lock->lock();

    TxnTimestamp ts = local_context->getTimestamp();
    MergeTreeDataPartsCNCHVector visible_parts = CnchDedupHelper::getVisiblePartsToDedup(scope, *this, ts, /*force_bitmap*/ false);
    if (scope.isBucketLock())
    {
        UInt64 expected_table_definition_hash = local_context->getSettingsRef().expected_table_definition_hash;
        std::erase_if(visible_parts, [&](const MergeTreeDataPartCNCHPtr & part) {
            if (expected_table_definition_hash > 0 && part->table_definition_hash != expected_table_definition_hash)
            {
                LOG_DEBUG(
                    log,
                    "Table definition hash {} of part {} is mismatch with expected_table_definition_hash {}, ignore it.",
                    part->table_definition_hash,
                    part->name,
                    expected_table_definition_hash);
                return true;
            }
            else if (part->bucket_number != bucket_number)
            {
                LOG_DEBUG(
                    log,
                    "Bucket number {} of part {} is mismatch with acquired bucket number {}, ignore it.",
                    part->bucket_number,
                    part->name,
                    bucket_number);
                return true;
            }
            return false;
        });
    }

    MergeTreeDataDeduper deduper(*this, local_context);
    LocalDeleteBitmaps bitmaps_to_dump
        = deduper.repairParts(txn->getTransactionID(), CnchPartsHelper::toIMergeTreeDataPartsVector(visible_parts));

    CnchDataWriter cnch_writer(*this, local_context, ManipulationType::Insert);
    if (!bitmaps_to_dump.empty())
        cnch_writer.publishStagedParts(/*staged_parts*/ {}, bitmaps_to_dump);

    txn->commitV2();
}

void StorageCnchMergeTree::waitForStagedPartsToPublish(ContextPtr local_context)
{
    UInt64 wait_timeout_seconds = local_context->getSettingsRef().receive_timeout.value.totalSeconds();
    Stopwatch timer;
    size_t staged_parts_cnt = 0;
    do
    {
        staged_parts_cnt
            = getStagedParts(local_context->getTimestamp(), /* partitions = */ nullptr, /* skip_delete_bitmap = */ true).size();
        if (!staged_parts_cnt)
            return;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    } while (timer.elapsedSeconds() < wait_timeout_seconds);
    LOG_WARNING(
        log,
        "There are still " + toString(staged_parts_cnt) + " staged parts to be published after " + toString(wait_timeout_seconds) + "s.");
}

void StorageCnchMergeTree::allocateParts(ContextPtr local_context, const ServerDataPartsVector & parts)
{
    String local_table_name = getCloudTableName(local_context);
    collectResource(local_context, /*table_version=*/0, parts, local_table_name);
}

void StorageCnchMergeTree::collectResource(
    ContextPtr local_context,
    UInt64 table_version,
    const ServerDataPartsVector & parts,
    const String & local_table_name,
    const std::set<Int64> & required_bucket_numbers,
    const StorageSnapshotPtr & storage_snapshot,
    WorkerEngineType engine_type,
    bool replicated)
{
    auto storage_uuid = getStorageUUID();
    auto cnch_resource = local_context->getCnchServerResource();
    if (local_context->getSettingsRef().send_cacheable_table_definitions)
    {
        String local_dictionary_tables;
        cnch_resource->addCacheableCreateQuery(shared_from_this(), local_table_name, engine_type, local_dictionary_tables);
    }
    else
    {
        auto create_table_query = getCreateQueryForCloudTable(
            getCreateTableSql(), local_table_name, local_context, false, std::nullopt, {}, {}, engine_type);
        cnch_resource->addCreateQuery(local_context, shared_from_this(), create_table_query, local_table_name, false);
    }

    if (table_version)
        cnch_resource->setTableVersion(storage_uuid, table_version, required_bucket_numbers);
    else
        cnch_resource->addDataParts(storage_uuid, parts, required_bucket_numbers);

    if (storage_snapshot && !storage_snapshot->object_columns.empty())
        cnch_resource->addDynamicObjectSchema(storage_uuid, storage_snapshot->object_columns);

    if (replicated)
        cnch_resource->setResourceReplicated(storage_uuid, replicated);
}

void StorageCnchMergeTree::sendPreloadTasks(ContextPtr local_context, ServerDataPartsVector parts, bool enable_parts_sync_preload, UInt64 parts_preload_level, UInt64 ts)
{
    ProfileEvents::increment(ProfileEvents::PreloadSubmitTotalOps, 1, Metrics::MetricType::Rate);
    Stopwatch timer;

    auto worker_group = getWorkerGroupForTable(*this, local_context);
    local_context->setCurrentWorkerGroup(worker_group);

    TxnTimestamp txn_id = local_context->getCurrentTransactionID();
    String create_table_query = genCreateTableQueryForWorker(txn_id.toString());

    /// reuse server resource for part allocation
    /// no worker session context is created
    auto server_resource = std::make_shared<CnchServerResource>(txn_id);
    server_resource->skipCleanWorker();
    /// all bucket numbers are required
    std::set<Int64> bucket_numbers;
    if (isBucketTable())
    {
        std::transform(parts.begin(), parts.end(), std::inserter(bucket_numbers, bucket_numbers.end()), [](const auto & part) {
            return part->part_model().bucket_number();
        });
    }
    server_resource->addCreateQuery(local_context, shared_from_this(), create_table_query, "");
    server_resource->addDataParts(getStorageUUID(), parts, bucket_numbers);

    server_resource->sendResources(local_context, [&](CnchWorkerClientPtr client, const auto & resources, const ExceptionHandlerPtr & handler) {
        std::vector<brpc::CallId> ids;
        for (const auto & resource : resources)
        {
            if (resource.server_parts.empty())
                continue;

            brpc::CallId id = client->preloadDataParts(
                local_context,
                txn_id,
                *this,
                create_table_query,
                resource.server_parts,
                handler,
                enable_parts_sync_preload,
                parts_preload_level,
                ts);
            ids.emplace_back(id);
            LOG_TRACE(
                log,
                "send preload data parts size = {}, enable_parts_sync_preload = {}, parts_preload_level = {}, submit_ts = {}, time_ms = {}",
                resource.server_parts.size(),
                enable_parts_sync_preload,
                parts_preload_level,
                ts,
                timer.elapsedMilliseconds());
        }
        return ids;
    });
}

void StorageCnchMergeTree::sendDropDiskCacheTasks(ContextPtr local_context, const ServerDataPartsVector & parts, bool sync, bool drop_vw_disk_cache)
{
    TxnTimestamp txn_id(local_context->getTimestamp());
    String create_table_query = genCreateTableQueryForWorker(txn_id.toString());

    auto worker_group = getWorkerGroupForTable(*this, local_context);
    local_context->setCurrentWorkerGroup(worker_group);

    /// reuse server resource for part allocation
    /// no worker session context is created
    auto server_resource = std::make_shared<CnchServerResource>(txn_id);
    server_resource->skipCleanWorker();
    /// all bucket numbers are required
    std::set<Int64> bucket_numbers;
    if (isBucketTable())
    {
        std::transform(parts.begin(), parts.end(), std::inserter(bucket_numbers, bucket_numbers.end()), [](const auto & part) {
            return part->part_model().bucket_number();
        });
    }
    server_resource->addCreateQuery(local_context, shared_from_this(), create_table_query, "");
    server_resource->addDataParts(getStorageUUID(), parts, bucket_numbers);
    /// TODO: async rpc?
    auto worker_action =  [&](const CnchWorkerClientPtr & client, const std::vector<AssignedResource> & resources, const ExceptionHandlerPtr &)->std::vector<brpc::CallId> {
        std::vector<brpc::CallId> ids;
        for (const auto & resource : resources)
        {
            auto data_parts = std::move(resource.server_parts);
            CnchPartsHelper::flattenPartsVector(data_parts);
            ids.emplace_back(client->dropPartDiskCache(local_context, txn_id, *this, create_table_query, data_parts, sync, drop_vw_disk_cache));
        }
        return ids;
    };
    server_resource->sendResources(local_context, worker_action);
}

void StorageCnchMergeTree::sendDropManifestDiskCacheTasks(ContextPtr local_context, String version, bool sync)
{
    auto worker_group = getWorkerGroupForTable(*this, local_context);
    std::vector<brpc::CallId> call_ids;

    for (const auto & worker_client : worker_group->getWorkerClients())
        call_ids.emplace_back(worker_client->dropManifestDiskCache(local_context, *this, version, sync));

    for (auto & call_id : call_ids)
        brpc::Join(call_id);
}

PrunedPartitions StorageCnchMergeTree::getPrunedPartitions(
    const SelectQueryInfo & query_info, const Names & column_names_to_return, ContextPtr local_context) const
{
    PrunedPartitions pruned_partitions;
    if (local_context->getCnchCatalog())
        pruned_partitions = local_context->getCnchCatalog()->getPartitionsByPredicate(local_context, shared_from_this(), query_info, column_names_to_return);
    return pruned_partitions;
}

void StorageCnchMergeTree::checkColumnsValidity(const ColumnsDescription & columns, const ASTPtr & new_settings) const
{
    MergeTreeSettingsPtr current_settings = getChangedSettings(new_settings);

    /// do not support compact map in CnchMergeTree
    if (current_settings->enable_compact_map_data == true)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Compact map is not supported in CnchMergeTree.");

    MergeTreeMetaBase::checkColumnsValidity(columns);
}

ServerDataPartsVector StorageCnchMergeTree::filterPartsInExplicitTransaction(ServerDataPartsVector & data_parts, ContextPtr local_context) const
{
    Int64 primary_txn_id = local_context->getCurrentTransaction()->getPrimaryTransactionID().toUInt64();
    TxnTimestamp start_time = local_context->getCurrentTransaction()->getStartTime();
    ServerDataPartsVector target_parts;
    std::map<TxnTimestamp, bool> success_secondary_txns;
    auto check_success_txn = [&success_secondary_txns, this](const TxnTimestamp & txn_id) -> bool {
        if (auto it = success_secondary_txns.find(txn_id); it != success_secondary_txns.end())
            return it->second;
        auto record = getContext()->getCnchCatalog()->getTransactionRecord(txn_id);
        success_secondary_txns.emplace(txn_id, record.status() == CnchTransactionStatus::Finished);
        return record.status() == CnchTransactionStatus::Finished;
    };
    std::for_each(data_parts.begin(), data_parts.end(), [&](const auto & part) {
        if (part->info().mutation == primary_txn_id && part->part_model_wrapper->part_model->has_secondary_txn_id()
            && check_success_txn(part->part_model_wrapper->part_model->secondary_txn_id()))
            target_parts.push_back(part);
    });

    auto * txn_record_cache =
        local_context->getServerType() == ServerType::cnch_server ? local_context->getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache() : nullptr;
    getVisibleServerDataParts(data_parts, start_time, &(*local_context->getCnchCatalog()), /*TransactionRecords *=*/nullptr, txn_record_cache);
    std::move(data_parts.begin(), data_parts.end(), std::back_inserter(target_parts));
    return target_parts;
}

namespace
{
    /// Conversion that is allowed for serializable key (primary key, sorting key).
    /// Key should be serialized in the same way after conversion.
    /// NOTE: The list is not complete.
    bool isSafeForKeyConversion(const IDataType * from, const IDataType * to)
    {
        if (from->getName() == to->getName())
            return true;

        /// Enums are serialized in partition key as numbers - so conversion from Enum to number is Ok.
        /// But only for types of identical width because they are serialized as binary in minmax index.
        /// But not from number to Enum because Enum does not necessarily represents all numbers.

        if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from))
        {
            if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to))
                return to_enum8->contains(*from_enum8);
            if (typeid_cast<const DataTypeInt8 *>(to))
                return true; // NOLINT
            return false;
        }

        if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from))
        {
            if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to))
                return to_enum16->contains(*from_enum16);
            if (typeid_cast<const DataTypeInt16 *>(to))
                return true; // NOLINT
            return false;
        }

        if (const auto * from_lc = typeid_cast<const DataTypeLowCardinality *>(from))
            return from_lc->getDictionaryType()->equals(*to);

        if (const auto * to_lc = typeid_cast<const DataTypeLowCardinality *>(to))
            return to_lc->getDictionaryType()->equals(*from);

        return false;
    }
    /// Special check for alters of VersionedCollapsingMergeTree version column
    void checkVersionColumnTypesConversion(const IDataType * old_type, const IDataType * new_type, const String column_name)
    {
        /// Check new type can be used as version
        if (!new_type->canBeUsedAsVersion())
            throw Exception(
                "Cannot alter version column " + backQuoteIfNeed(column_name) + " to type " + new_type->getName()
                    + " because version column must be of an integer type or of type Date or DateTime",
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);

        auto which_new_type = WhichDataType(new_type);
        auto which_old_type = WhichDataType(old_type);

        /// Check alter to different sign or float -> int and so on
        if ((which_old_type.isInt() && !which_new_type.isInt()) || (which_old_type.isUInt() && !which_new_type.isUInt())
            || (which_old_type.isDate() && !which_new_type.isDate()) || (which_old_type.isDateTime() && !which_new_type.isDateTime())
            || (which_old_type.isFloat() && !which_new_type.isFloat()))
        {
            throw Exception(
                "Cannot alter version column " + backQuoteIfNeed(column_name) + " from type " + old_type->getName() + " to type "
                    + new_type->getName() + " because new type will change sort order of version column."
                    + " The only possible conversion is expansion of the number of bytes of the current type.",
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
        }

        /// Check alter to smaller size: UInt64 -> UInt32 and so on
        if (new_type->getSizeOfValueInMemory() < old_type->getSizeOfValueInMemory())
        {
            throw Exception(
                "Cannot alter version column " + backQuoteIfNeed(column_name) + " from type " + old_type->getName() + " to type "
                    + new_type->getName() + " because new type is smaller than current in the number of bytes."
                    + " The only possible conversion is expansion of the number of bytes of the current type.",
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
        }
    }
}

void StorageCnchMergeTree::checkAlterInCnchServer(const AlterCommands & commands, ContextPtr local_context) const
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    const auto & settings = local_context->getSettingsRef();

    if (!settings.allow_non_metadata_alters)
    {
        auto mutation_commands = commands.getMutationCommands(new_metadata, settings.materialize_ttl_after_modify, getContext());

        if (!mutation_commands.empty())
            throw Exception(
                ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                "The following alter commands: '{}' will modify data on disk, but setting `allow_non_metadata_alters` is disabled",
                queryToString(mutation_commands.ast()));
    }
    commands.apply(new_metadata, getContext());

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_type_forbidden;

    /// Primary key columns can be ALTERed only if they are used in the key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    NameSet columns_alter_type_metadata_only;

    /// Columns to check that the type change is safe for partition key.
    NameSet columns_alter_type_check_safe_for_partition;

    if (old_metadata.hasPartitionKey())
    {
        /// Forbid altering columns inside partition key expressions because it can change partition ID format.
        auto partition_key_expr = old_metadata.getPartitionKey().expression;
        for (const auto & action : partition_key_expr->getActions())
        {
            for (const auto * child : action.node->children)
                columns_alter_type_forbidden.insert(child->result_name);
        }

        /// But allow to alter columns without expressions under certain condition.
        for (const String & col : partition_key_expr->getRequiredColumns())
            columns_alter_type_check_safe_for_partition.insert(col);
    }

    if (old_metadata.hasUniqueKey())
    {
        for (const String & col : old_metadata.getColumnsRequiredForUniqueKey())
            columns_alter_type_forbidden.insert(col);

        if (!merging_params.version_column.empty())
            columns_alter_type_forbidden.insert(merging_params.version_column);
    }

    for (const auto & index : old_metadata.getSecondaryIndices())
    {
        for (const String & col : index.expression->getRequiredColumns())
            columns_alter_type_forbidden.insert(col);
    }

    if (old_metadata.hasSortingKey())
    {
        auto old_sorting_key_expr = old_metadata.getSortingKey().expression;
        for (const auto & action : old_sorting_key_expr->getActions())
        {
            for (const auto * child : action.node->children)
                columns_alter_type_forbidden.insert(child->result_name);
        }
        for (const String & col : old_sorting_key_expr->getRequiredColumns())
            columns_alter_type_metadata_only.insert(col);

        /// We don't process sample_by_ast separately because it must be among the primary key columns
        /// and we don't process primary_key_expr separately because it is a prefix of sorting_key_expr.
    }
    if (!merging_params.sign_column.empty())
        columns_alter_type_forbidden.insert(merging_params.sign_column);

    /// All of the above.
    NameSet columns_in_keys;
    columns_in_keys.insert(columns_alter_type_forbidden.begin(), columns_alter_type_forbidden.end());
    columns_in_keys.insert(columns_alter_type_metadata_only.begin(), columns_alter_type_metadata_only.end());
    columns_in_keys.insert(columns_alter_type_check_safe_for_partition.begin(), columns_alter_type_check_safe_for_partition.end());

    NameSet dropped_columns;

    std::map<String, const IDataType *> old_types;
    for (const auto & column : old_metadata.getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());

    NameSet columns_already_in_alter;
    auto all_mutations = getContext()->getCnchCatalog()->getAllMutations(getStorageID());

    for (auto & mutation : all_mutations)
    {
        auto entry = CnchMergeTreeMutationEntry::parse(mutation);

        for (auto command : entry.commands)
        {
            if (!command.column_name.empty())
                columns_already_in_alter.emplace(command.column_name);
        }
    }

    NamesAndTypesList columns_to_check_conversion;
    auto name_deps = getDependentViewsByColumn(local_context);

    for (const AlterCommand & command : commands)
    {
        /// Just validate partition expression
        if (command.partition)
        {
            getPartitionIDFromQuery(command.partition, getContext());
        }

        if (command.column_name == merging_params.version_column)
        {
            /// Some type changes for version column is allowed despite it's a part of sorting key
            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                const IDataType * new_type = command.data_type.get();
                const IDataType * old_type = old_types[command.column_name];

                if (new_type)
                    checkVersionColumnTypesConversion(old_type, new_type, command.column_name);

                /// No other checks required
                continue;
            }
            else if (command.type == AlterCommand::DROP_COLUMN)
            {
                throw Exception(
                    "Trying to ALTER DROP version " + backQuoteIfNeed(command.column_name) + " column",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
            else if (command.type == AlterCommand::RENAME_COLUMN)
            {
                throw Exception(
                    "Trying to ALTER RENAME version " + backQuoteIfNeed(command.column_name) + " column",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }

        if (command.type == AlterCommand::MODIFY_ORDER_BY && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax",
                ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::MODIFY_TTL && !is_custom_partitioned)
        {
            throw Exception(
                "ALTER MODIFY TTL is not supported for default-partitioned tables created with the old syntax", ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::MODIFY_SAMPLE_BY)
        {
            if (!is_custom_partitioned)
                throw Exception(
                    "ALTER MODIFY SAMPLE BY is not supported for default-partitioned tables created with the old syntax",
                    ErrorCodes::BAD_ARGUMENTS);

            checkSampleExpression(new_metadata, getSettings()->compatibility_allow_sampling_expression_not_in_primary_key);
        }
        if (command.type == AlterCommand::ADD_INDEX && !is_custom_partitioned)
        {
            throw Exception("ALTER ADD INDEX is not supported for tables with the old syntax", ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::ADD_PROJECTION && old_metadata.hasUniqueKey())
        {
            throw Exception("ALTER ADD PROJECTION is not supported for tables with the unique index", ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::ADD_PROJECTION && !is_custom_partitioned)
        {
            throw Exception("ALTER ADD PROJECTION is not supported for tables with the old syntax", ErrorCodes::BAD_ARGUMENTS);
        }
        if (command.type == AlterCommand::RENAME_COLUMN)
        {
            if (columns_in_keys.count(command.column_name))
            {
                throw Exception(
                    "Trying to ALTER RENAME key " + backQuoteIfNeed(command.column_name) + " column which is a part of key expression",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (command.clear)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEAR COLUMN is not supported by storage {}", getName());

            if (columns_in_keys.count(command.column_name))
            {
                throw Exception(
                    "Trying to ALTER DROP key " + backQuoteIfNeed(command.column_name) + " column which is a part of key expression",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }

            if (!command.clear)
            {
                const auto & deps_mv = name_deps[command.column_name];
                if (!deps_mv.empty())
                {
                    throw Exception(
                        "Trying to ALTER DROP column " + backQuoteIfNeed(command.column_name) + " which is referenced by materialized view "
                            + toString(deps_mv),
                        ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }
            }


            if (command.partition_predicate)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEAR COLUMN IN PARTITION WHERE is not supported by storage {}", getName());

            dropped_columns.emplace(command.column_name);
        }
        else if (command.isRequireMutationStage(getInMemoryMetadata()))
        {
            /// This alter will override data on disk. Let's check that it doesn't
            /// modify immutable column.
            if (columns_alter_type_forbidden.count(command.column_name))
                throw Exception(
                    "ALTER of key column " + backQuoteIfNeed(command.column_name) + " is forbidden",
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);

            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (columns_alter_type_check_safe_for_partition.count(command.column_name))
                {
                    auto it = old_types.find(command.column_name);

                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception(
                            "ALTER of partition key column " + backQuoteIfNeed(command.column_name) + " from type " + it->second->getName()
                                + " to type " + command.data_type->getName()
                                + " is not safe because it can change the representation of partition key",
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }

                if (columns_alter_type_metadata_only.count(command.column_name))
                {
                    auto it = old_types.find(command.column_name);
                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception(
                            "ALTER of key column " + backQuoteIfNeed(command.column_name) + " from type " + it->second->getName()
                                + " to type " + command.data_type->getName()
                                + " is not safe because it can change the representation of primary key",
                            ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
                }

                if (old_metadata.getColumns().has(command.column_name))
                {
                    columns_to_check_conversion.push_back(new_metadata.getColumns().getPhysical(command.column_name));
                }
            }
        }
    }

    checkProperties(new_metadata, old_metadata);
    checkTTLExpressions(new_metadata, old_metadata);

    if (!columns_to_check_conversion.empty())
    {
        const_cast<Context &>(*local_context).setSetting("disable_str_to_array_cast", Field{true});

        auto old_header = old_metadata.getSampleBlock();
        performRequiredConversions(old_header, columns_to_check_conversion, local_context);
    }

    for (const auto & part : getDataPartsVector())
    {
        bool at_least_one_column_rest = false;
        for (const auto & column : part->getColumns())
        {
            if (!dropped_columns.count(column.name))
            {
                at_least_one_column_rest = true;
                break;
            }
        }
        if (!at_least_one_column_rest)
        {
            std::string postfix;
            if (dropped_columns.size() > 1)
                postfix = "s";
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot drop or clear column{} '{}', because all columns in part '{}' will be removed from disk. Empty parts are not "
                "allowed",
                postfix,
                boost::algorithm::join(dropped_columns, ", "),
                part->name);
        }
    }
}

void StorageCnchMergeTree::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    checkAlterInCnchServer(commands, local_context);
    checkAlterSettings(commands);
}

void StorageCnchMergeTree::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings) const
{
    for (const auto & command : commands)
    {
        if (command.type == PartitionCommand::DROP_DETACHED_PARTITION && !settings.allow_drop_detached)
            throw DB::Exception(
                "Cannot execute query: DROP DETACHED PART is disabled "
                "(see allow_drop_detached setting)",
                ErrorCodes::SUPPORT_IS_DISABLED);

        if (!partitionCommandHasWhere(command))
        {
            if (command.part)
            {
                auto part_name = command.partition->as<ASTLiteral &>().value.safeGet<String>();
                /// We are able to parse it
                MergeTreePartInfo::fromPartName(part_name, format_version);
            }
            else if (!command.parts)
            {
                /// We are able to parse it
                getPartitionIDFromQuery(command.partition, getContext());
            }
        }
    }
}

Pipe StorageCnchMergeTree::alterPartition(
    const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr query_context, const ASTPtr & )
{
    if (unlikely(!query_context->getCurrentTransaction()))
        throw Exception("Transaction is not set", ErrorCodes::LOGICAL_ERROR);

    auto current_query_context = Context::createCopy(query_context);

    for (auto & command : commands)
    {
        TransactionCnchPtr new_txn;

        SCOPE_EXIT({
            if (new_txn)
                current_query_context->getCnchTransactionCoordinator().finishTransaction(new_txn);
        });

        /// If previous transaction has been committed, need to set a new transaction
        /// For the first txn, it is handled by finishCurrentTransaction log in executeQuery to keep the same lifecycle as the query.
        if (current_query_context->getCurrentTransaction()->getStatus() == CnchTransactionStatus::Finished)
        {
            new_txn = current_query_context->getCnchTransactionCoordinator().createTransaction();
            current_query_context->setCurrentTransaction(new_txn, false);
        }

        switch (command.type)
        {
            case PartitionCommand::ATTACH_PARTITION:
            case PartitionCommand::ATTACH_DETACHED_PARTITION:
            case PartitionCommand::REPLACE_PARTITION:
            case PartitionCommand::REPLACE_PARTITION_WHERE: {
                CnchAttachProcessor processor(*this, command, current_query_context);
                processor.exec();
                break;
            }

            case PartitionCommand::DROP_PARTITION:
            case PartitionCommand::DROP_PARTITION_WHERE:
                dropPartitionOrPart(command, current_query_context);
                break;

            case PartitionCommand::INGEST_PARTITION:
                return ingestPartition(command, current_query_context);


            case PartitionCommand::RECLUSTER_PARTITION:
            case PartitionCommand::RECLUSTER_PARTITION_WHERE:
                reclusterPartition(command, current_query_context);
                break;

            default:
                IStorage::alterPartition(metadata_snapshot, commands, current_query_context);
        }
    }
    return {};
}

void StorageCnchMergeTree::reclusterPartition(const PartitionCommand & command, ContextPtr query_context)
{
    if (getInMemoryMetadataPtr()->hasUniqueKey())
        throw Exception("Table with UNIQUE KEY doesn't support recluster partition commands.", ErrorCodes::SUPPORT_IS_DISABLED);

    if (getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey())
        throw Exception("Table with user defined CLUSTER BY expression doesn't support recluster partition commands.", ErrorCodes::SUPPORT_IS_DISABLED);

    // create mutation command with partition or predicate attribute
    MutationCommand mutation_command;
    mutation_command.type = MutationCommand::Type::RECLUSTER;
    mutation_command.ast = command.ast->clone();
    mutation_command.predicate = command.type == PartitionCommand::RECLUSTER_PARTITION_WHERE ? command.partition : nullptr;
    mutation_command.partition = command.type == PartitionCommand::RECLUSTER_PARTITION ? command.partition : nullptr;

    if (mutation_command.predicate)
    {
        // if there are columns in the predicate, they must be a subset of partition key columns
        NameSet columns;
        auto idents = IdentifiersCollector::collect(mutation_command.predicate);
        for (const auto * ident : idents)
            columns.insert(ident->shortName());

        auto partition_keys = getInMemoryMetadataPtr()->getColumnsRequiredForPartitionKey();
        for (const auto & col : columns)
            if (std::find(partition_keys.begin(), partition_keys.end(), col) == partition_keys.end())
                throw Exception("Only partition key columns are allowed for reclustering partitions", ErrorCodes::BAD_ARGUMENTS);
    }

    // create mutation entry
    CnchMergeTreeMutationEntry mutation_entry;
    MutationCommands mutation_commands;
    mutation_commands.emplace_back(mutation_command);
    mutation_entry.commands = mutation_commands;
    mutation_entry.txn_id = query_context->getCurrentTransaction()->getPrimaryTransactionID().toUInt64();
    mutation_entry.commit_time = query_context->getTimestamp();
    mutation_entry.columns_commit_time = commit_time;
    query_context->getCnchCatalog()->createMutation(getStorageID(), mutation_entry.txn_id.toString(), mutation_entry.toString());
}

void StorageCnchMergeTree::alter(const AlterCommands & commands, ContextPtr local_context, TableLockHolder & /*table_lock_holder*/)
{
    const Settings & settings = local_context->getSettingsRef();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();
    auto mutation_commands = commands.getMutationCommands(old_metadata, false, local_context);

    if (settings.force_alter_conflict_check)
    {
        NameSet columns = collectColumnsFromCommands(commands);
        NameSet current_columns;
        if (!columns.empty())
        {
            auto table_id = getStorageID();
            auto catalog = local_context->getCnchCatalog();
            std::map<TxnTimestamp, CnchMergeTreeMutationEntry> exists_mutations;
            catalog->fillMutationsByStorage(table_id, exists_mutations);
            for (const auto & [t, mutation] : exists_mutations)
            {
                LOG_TRACE(log, "exists_mutation {}", mutation.toString());

                for (const auto & command : mutation.commands)
                {
                    if (!command.column_name.empty())
                        current_columns.insert(command.column_name);

                    if (!command.rename_to.empty())
                        current_columns.insert(command.rename_to);
                }
            }
        }

        for (auto & command : mutation_commands)
        {
            if ((command.type == MutationCommand::RENAME_COLUMN) || (command.type == MutationCommand::READ_COLUMN))
            {
                if (!command.column_name.empty() && current_columns.count(command.column_name))
                    throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "There are unfinished async ALTERs which might conflict on column name {}, please wait...",
                        command.column_name);
            }
        }

    }

    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();

    TransactionCnchPtr txn = local_context->getCurrentTransaction();
    auto action = txn->createActionWithLocalContext<DDLAlterAction>(local_context, shared_from_this(), local_context->getSettingsRef(), local_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();
    alter_act.setMutationCommands(mutation_commands);

    commands.apply(new_metadata, local_context);
    checkColumnsValidity(new_metadata.columns, new_metadata.settings_changes);

    {
        String create_table_query = getCreateTableSql();
        alter_act.setOldSchema(create_table_query);
        ParserCreateQuery p_create_query;
        ASTPtr ast = parseQuery(
            p_create_query,
            create_table_query,
            local_context->getSettingsRef().max_query_size,
            local_context->getSettingsRef().max_parser_depth);

        applyMetadataChangesToCreateQuery(ast, new_metadata, ParserSettings::valueOf(local_context->getSettingsRef()));
        alter_act.setNewSchema(queryToString(ast));

        LOG_DEBUG(log, "new schema for alter query: {}", alter_act.getNewSchema());
        txn->appendAction(action);
    }

    auto daemon_manager = getContext()->getDaemonManagerClient();
    const String full_name = getStorageID().getNameForLogs();
    bool dedup_worker_is_active = false;
    if (old_metadata.hasUniqueKey())
    {
        auto dedup_worker_job_info
            = daemon_manager->getDMBGJobInfo(getStorageID().uuid, CnchBGThreadType::DedupWorker, local_context->getCurrentQueryId());
        if (dedup_worker_job_info && dedup_worker_job_info->status == CnchBGThread::Status::Running)
        {
            dedup_worker_is_active = true;
            LOG_TRACE(log, "Stop dedup worker before altering table {}", full_name);
            daemon_manager->controlDaemonJob(
                getStorageID(), CnchBGThreadType::DedupWorker, CnchBGThreadAction::Stop, local_context->getCurrentQueryId());
        }
    }

    SCOPE_EXIT({
        if (dedup_worker_is_active)
        {
            LOG_TRACE(log, "Restart dedup worker no matter if ALTER succ for table {}", full_name);
            try
            {
                daemon_manager->controlDaemonJob(
                    getStorageID(), CnchBGThreadType::DedupWorker, CnchBGThreadAction::Start, local_context->getCurrentQueryId());
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to restart dedup worker for table " + full_name + " after ALTER");
            }
        }
    });

    //setProperties(new_metadata, false);
    //updateHDFSRootPaths(new_metadata.root_paths_ast);
    //setTTLExpressions(new_metadata.ttl_for_table_ast);
    //setCreateTableSql(alter_act->getNewSchema());

    txn->commitV1();
    LOG_TRACE(log, "Updated shared metadata in Catalog.");

    auto mutation_entry = alter_act.getMutationEntry();
    if (!mutation_entry.has_value())
    {
        if (!alter_act.getMutationCommands().empty())
            LOG_ERROR(log, "DDLAlterAction didn't generated mutation entry for commands.");
        return;
    }
    addMutationEntry(*mutation_entry);
    LOG_TRACE(log, "Added mutation entry {} to mutations_by_version", mutation_entry->txn_id);

    auto bg_thread = local_context->tryGetCnchBGThread(CnchBGThreadType::MergeMutate, getStorageID());
    if (bg_thread)
    {
        /// TODO(shiyuze): check ci 50010_parts_info when calls triggerPartMutate here
        if (settings.mutations_sync != 0)
        {
            try
            {
                auto timeout_ms = settings.max_execution_time.totalMilliseconds();
                auto * merge_mutate_thread = typeid_cast<CnchMergeMutateThread *>(bg_thread.get());
                merge_mutate_thread->triggerPartMutate(shared_from_this());
                merge_mutate_thread->waitMutationFinish(mutation_entry->commit_time, timeout_ms);
            }
            catch(Exception & e)
            {
                e.addMessage("(It will be scheduled in background. Check `system.mutations` and `system.manipulations` for progress)");
                throw;
            }
        }
    }
}

void StorageCnchMergeTree::checkAlterSettings(const AlterCommands & commands) const
{
    static std::set<String> supported_settings = {
        "cnch_vw_default",
        "cnch_vw_read",
        "cnch_vw_write",
        "cnch_vw_task",
        "cnch_server_vw",

        /// Setting for memory buffer
        "cnch_enable_memory_buffer",
        "cnch_memory_buffer_size",
        "min_time_memory_buffer_to_flush",
        "max_time_memory_buffer_to_flush",
        "min_bytes_memory_buffer_to_flush",
        "max_bytes_memory_buffer_to_flush",
        "min_rows_memory_buffer_to_flush",
        "max_rows_memory_buffer_to_flush",
        "max_block_size_in_memory_buffer",
        "max_bytes_to_write_wal",
        "enable_flush_buffer_with_multi_threads",
        "max_flush_threads_num",

        "gc_remove_bitmap_batch_size",
        "gc_remove_bitmap_thread_pool_size",

        "insertion_label_ttl",
        "enable_local_disk_cache",
        "enable_preload_parts",
        "enable_parts_sync_preload",
        "parts_preload_level",
        "cnch_parallel_prefetching",
        "enable_prefetch_checksums",
        "disk_cache_stealing_mode",
        "cnch_part_allocation_algorithm",

        "enable_addition_bg_task",
        "max_addition_bg_task_num",
        "max_addition_mutation_task_num",
        "max_partition_for_multi_select",

        "cnch_merge_parts_cache_timeout",
        "cnch_merge_parts_cache_min_count",
        "cnch_merge_enable_batch_select",
        "cnch_merge_max_total_rows_to_merge",
        "cnch_merge_max_total_bytes_to_merge",
        "cnch_merge_max_parts_to_merge",
        "cnch_merge_only_realtime_partition",
        "cnch_merge_select_nonadjacent_parts",
        "cnch_merge_pick_worker_algo",
        "max_refresh_materialized_view_task_num",
        "cnch_merge_round_robin_partitions_interval",
        "cnch_gc_round_robin_partitions_interval",
        "cnch_gc_round_robin_partitions_number",
        "gc_remove_part_thread_pool_size",
        "gc_remove_part_batch_size",
        "cluster_by_hint",

        "enable_hybrid_allocation",
        "min_rows_per_virtual_part",
        "part_to_vw_size_ratio"
    };

    /// Check whether the value is legal for Setting.
    /// For example, we have a setting item, `SettingBool setting_test`
    /// If you submit a Alter query: "Alter table test modify setting setting_test='abc'"
    /// Then, it will throw an Exception here, because we can't convert string 'abc' to a Bool.
    auto settings_copy = *getSettings();

    for (auto & command : commands)
    {
        if (command.type != AlterCommand::MODIFY_SETTING)
            continue;

        for (auto & change : command.settings_changes)
        {
            if (MergeTreeSettings::isReadonlySetting(change.name))
                throw Exception("Setting " + change.name + " cannot be modified", ErrorCodes::SUPPORT_IS_DISABLED);

            if (getInMemoryMetadataPtr()->hasUniqueKey() && change.name == "cnch_enable_memory_buffer" && change.value.get<Int64>() == 1)
                throw Exception("Table with UNIQUE KEY doesn't support memory buffer", ErrorCodes::SUPPORT_IS_DISABLED);

            /// Prevent set partition_level_unique_keys to 0
            if (getInMemoryMetadataPtr()->hasUniqueKey() && change.name == "partition_level_unique_keys" && change.value.get<UInt8>() == 0)
                throw Exception("Setting 'partition_level_unique_keys' can not change to table_level.", ErrorCodes::SUPPORT_IS_DISABLED);

            if (change.name.find("cnch_vw_") == 0)
                checkAlterVW(change.value.get<String>());

            // check table version related settings, can only change from 1->0
            if (change.name == "enable_publish_version_on_commit" && change.value.get<UInt8>() == 1)
                throw Exception("Change table setting `enable_publish_version_on_commit` from 0 -> 1 is not allowed.", ErrorCodes::SUPPORT_IS_DISABLED);

            settings_copy.set(change.name, change.value);
        }
    }
}

void StorageCnchMergeTree::checkAlterVW(const String & vw_name) const
{
    if (vw_name == "vw_default" || vw_name == "vw_write")
        return;

    /// Will throw VIRTUAL_WAREHOUSE_NOT_FOUND if vw not found.
    getContext()->getVirtualWarehousePool().get(vw_name);
}

void StorageCnchMergeTree::truncate(
    const ASTPtr & query, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr local_context, TableExclusiveLockHolder &)
{
    PartitionCommand command;
    command.part = false;
    const auto & ast_truncate = query->as<ASTDropQuery &>();

    /// TRUNCATE TABLE t
    if (!ast_truncate.partition && !ast_truncate.partition_predicate)
    {
        LOG_TRACE(log, "TRUNCATE WHOLE TABLE");
        command.type = PartitionCommand::DROP_PARTITION_WHERE;
        command.partition = std::make_shared<ASTLiteral>(Field(static_cast<UInt8>(1)));
    }
    /// TRUNCATE TABLE t PARTITION p1, p2, p3
    else if (ast_truncate.partition)
    {
        LOG_TRACE(log, "TRUNCATE PARTITION {}", serializeAST(*ast_truncate.partition));
        if (auto * partition_list = typeid_cast<ASTExpressionList *>(ast_truncate.partition.get()))
        {
            for (const auto & p : partition_list->children)
            {
                PartitionCommand single_command;
                single_command.type = PartitionCommand::DROP_PARTITION;
                single_command.partition = p;
                dropPartitionOrPart(single_command, local_context, nullptr, false);
            }

            auto txn = local_context->getCurrentTransaction();
            txn->commitV2();
            return;
        }
    }
    /// TRUNCATE TABLE t PARTITION WHERE predicate
    else if (ast_truncate.partition_predicate)
    {
        LOG_TRACE(log, "TRUNCATE PARTITION WHERE {}", serializeAST(*ast_truncate.partition_predicate));
        command.type = PartitionCommand::DROP_PARTITION_WHERE;
        command.partition = ast_truncate.partition_predicate->clone();
    }

    dropPartitionOrPart(command, local_context);
}

void StorageCnchMergeTree::overwritePartitions(const ASTPtr & overwrite_partition, ContextPtr local_context, CnchLockHolderPtrs * lock_holders)
{
    if (auto * partition_list = typeid_cast<ASTExpressionList *>(overwrite_partition.get()))
    {
        for (const auto & partition : partition_list->children)
            overwritePartition(partition, local_context, lock_holders);
    }
    else
    {
        overwritePartition(overwrite_partition, local_context, lock_holders);
    }
}

void StorageCnchMergeTree::overwritePartition(const ASTPtr & overwrite_partition, ContextPtr local_context, CnchLockHolderPtrs * lock_holders)
{
    PartitionCommand command;
    if (overwrite_partition)
    {
        command.type = PartitionCommand::DROP_PARTITION;
        command.partition = overwrite_partition;
    }
    else
    {
        command.type = PartitionCommand::DROP_PARTITION_WHERE;
        command.partition = std::make_shared<ASTLiteral>(Field(UInt8(1)));
    }
    command.part = false;

    dropPartitionOrPart(command, local_context, nullptr, false, lock_holders);
}

/// Main steps of DROP PARTITION
/// 1. aquire the (task_domain) partition lock, to block the merge scheduling.
/// 2. cancel existing merge tasks (make sure all tasks are aborted or committed before step 3).
/// 3. get data parts from catalog using the newest timestamp.
/// 4. generate DropRange parts based on parts from step 3.
void StorageCnchMergeTree::dropPartitionOrPart(
    const PartitionCommand & command,
    ContextPtr local_context,
    IMergeTreeDataPartsVector * dropped_parts,
    bool do_commit,
    CnchLockHolderPtrs * lock_holders,
    size_t max_threads)
{
    if (!command.part)
    {
        /// 1. acquire partition lock
        auto cur_txn = local_context->getCurrentTransaction();
        TxnTimestamp txn_id = cur_txn->getTransactionID();
        LockInfoPtr partition_lock = std::make_shared<LockInfo>(txn_id);
        partition_lock->setMode(LockMode::X);
        partition_lock->setTimeout(local_context->getSettingsRef().drop_range_memory_lock_timeout.value.totalMilliseconds()); // default 5s
        partition_lock->setUUIDAndPrefix(getStorageUUID(), LockInfo::task_domain);

        String partition_id = "all";
        if (!partitionCommandHasWhere(command))
        {
            partition_id = getPartitionIDFromQuery(command.partition, local_context);
            partition_lock->setPartition(partition_id);
        }
        /// else { lock all partitions }
        Stopwatch lock_watch;
        auto cnch_lock = std::make_shared<CnchLockHolder>(local_context, std::move(partition_lock));
        cnch_lock->lock();
        LOG_DEBUG(log, "DROP PARTITION acquired lock in {} ms", lock_watch.elapsedMilliseconds());

        if (lock_holders)
            lock_holders->push_back(cnch_lock);

        /// 2. cancel merge tasks
        auto daemon_manager_client_ptr = local_context->getDaemonManagerClient();
        if (!daemon_manager_client_ptr)
            throw Exception("Failed to get daemon manager client", ErrorCodes::SYSTEM_ERROR);

        const StorageID target_storage_id = getStorageID();
        std::optional<DaemonManager::BGJobInfo> merge_job_info = daemon_manager_client_ptr->getDMBGJobInfo(target_storage_id.uuid, CnchBGThreadType::MergeMutate, local_context->getCurrentQueryId());

        if (!merge_job_info || merge_job_info->host_port.empty())
            LOG_DEBUG(log, "Skip removing related merge tasks as there is no valid host server for table's merge job: {}", target_storage_id.getNameForLogs());
        else
        {
            auto server_client_ptr = local_context->getCnchServerClient(merge_job_info->host_port);
            if (!server_client_ptr)
                throw Exception("Failed to get server client with host port " + merge_job_info->host_port, ErrorCodes::SYSTEM_ERROR);
            if (!server_client_ptr->removeMergeMutateTasksOnPartitions(target_storage_id, {partition_id}))
            {
                auto msg = fmt::format(
                    "Failed to remove MergeMutateTasks for partitions: {}, table: {}.",
                    fmt::join({partition_id}, ","),
                    target_storage_id.getNameForLogs());

                throw Exception(msg, ErrorCodes::SYSTEM_ERROR);
            }
        }
    }

    if (!getInMemoryMetadata().hasUniqueKey() && command.staging_area)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "DROP/DETACH STAGED PARTITION/PART command is only for unique table, table {} does not have unique key.",
            getStorageID().getNameForLogs());

    /// 3. get data parts
    auto svr_parts = selectPartsByPartitionCommand(local_context, command);

    /// 4. Filter bucket
    if (command.specify_bucket)
    {
        Int64 bucket_number = static_cast<Int64>(command.bucket_number);
        UInt64 expected_table_definition_hash = local_context->getSettingsRef().expected_table_definition_hash;
        std::erase_if(svr_parts.first, [&](const ServerDataPartPtr & part) {
            if (expected_table_definition_hash > 0 && part->part_model().table_definition_hash() != expected_table_definition_hash)
            {
                LOG_DEBUG(
                    log,
                    "Table definition hash {} of part {} is mismatch with expected_table_definition_hash {}. ignore it.",
                    part->part_model().table_definition_hash(),
                    part->name(),
                    expected_table_definition_hash);
                return true;
            }
            else if (part->part_model().bucket_number() != bucket_number)
            {
                LOG_DEBUG(
                    log,
                    "Bucket number {} of part {} is mismatch with acquired bucket number {}, ignore it.",
                    part->part_model().bucket_number(),
                    part->name(),
                    bucket_number);
                return true;
            }
            return false;
        });
    }

    if (svr_parts.first.empty())
    {
        if (command.part)
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part found");
    }

    auto parts = createPartVectorFromServerParts(*this, svr_parts.first);
    dropPartsImpl(command, svr_parts, parts, local_context, do_commit, max_threads);

    if (dropped_parts != nullptr)
    {
        *dropped_parts = std::move(parts);
    }
}

void StorageCnchMergeTree::dropPartsImpl(
    const PartitionCommand & command,
    ServerDataPartsWithDBM & svr_parts_to_drop_with_dbm,
    IMergeTreeDataPartsVector & parts_to_drop,
    ContextPtr local_context,
    bool do_commit,
    size_t max_threads)
{
    bool detach = command.detach;
    bool staging_area = command.staging_area;
    auto & svr_parts_to_drop = svr_parts_to_drop_with_dbm.first;
    auto txn = local_context->getCurrentTransaction();
    if (svr_parts_to_drop.empty())
    {
        /// Note that it doesn't matter if we commit or rollback here, because there's no data anyway. But commit is more
        /// semantically correct, because there's no exception, so client side expect that the transaction is success.
        if (do_commit)
            txn->commitV2();
        return;
    }

    if (detach)
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();

        DiskType::Type remote_storage_type = getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();

        switch (remote_storage_type)
        {
            case DiskType::Type::ByteHDFS: {
                /// XXX: Detach parts will break MVCC: queries and tasks which reference those parts will fail.
                // VolumePtr hdfs_volume = getStoragePolicy(IStorage::StorageLocation::MAIN)->local_store_volume();

                // Create detached directory first
                Disks disks = getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks();
                for (DiskPtr & disk : disks)
                {
                    String detached_rel_path = fs::path(getRelativeDataPath(IStorage::StorageLocation::MAIN)) / "detached";
                    if (metadata_snapshot->hasUniqueKey())
                        detached_rel_path = fs::path(detached_rel_path) / DeleteBitmapMeta::delete_files_dir;
                    disk->createDirectories(detached_rel_path);
                }

                UndoResources undo_resources;
                auto write_undo_callback = [&](const DataPartPtr & part) {
                    UndoResource ub(
                        txn->getTransactionID(),
                        UndoResourceType::FileSystem,
                        part->getFullRelativePath(),
                        part->getFullRelativePathForDetachedPart(""));
                    ub.setDiskName(part->volume->getDisk()->getName());
                    undo_resources.push_back(ub);
                };
                for (const auto & data_part : parts_to_drop)
                {
                    data_part->enumeratePreviousParts(write_undo_callback);
                }
                local_context->getCnchCatalog()->writeUndoBuffer(
                    getCnchStorageID(), txn->getTransactionID(), undo_resources);

                if (metadata_snapshot->hasUniqueKey() && !local_context->getSettingsRef().enable_unique_table_detach_ignore_delete_bitmap)
                    getDeleteBitmapMetaForParts(parts_to_drop, svr_parts_to_drop_with_dbm.second, /*force_found*/ false);

                ThreadPool pool(std::min(parts_to_drop.size(), max_threads));
                auto callback = [&pool, &metadata_snapshot, &local_context](const DataPartPtr & part) {
                    bool create_delete_bitmap = metadata_snapshot->hasUniqueKey()
                        && !local_context->getSettingsRef().enable_unique_table_detach_ignore_delete_bitmap;
                    pool.scheduleOrThrowOnError([part, create_delete_bitmap]() {
                        part->renameToDetached("");
                        if (create_delete_bitmap)
                            part->createDeleteBitmapForDetachedPart();
                    });
                };
                for (const auto & part : parts_to_drop)
                {
                    part->enumeratePreviousParts(callback);
                }
                pool.wait();
                /// NOTE: we still need create DROP_RANGE part for detached parts,
                break;
            }
            case DiskType::Type::ByteS3: {
                MergeTreeDataPartsCNCHVector parts;
                UndoResources resources;
                for (const auto & part_to_drop : parts_to_drop)
                {
                    for (IMergeTreeDataPartPtr curr_part = part_to_drop; curr_part != nullptr; curr_part = curr_part->tryGetPreviousPart())
                    {
                        auto part = dynamic_pointer_cast<const MergeTreeDataPartCNCH>(curr_part);
                        if (part == nullptr)
                        {
                            throw Exception("Unexpected part type when detach", ErrorCodes::LOGICAL_ERROR);
                        }
                        UndoResource ub(
                            txn->getTransactionID(),
                            staging_area ? UndoResourceType::S3DetachStagedPart : UndoResourceType::S3DetachPart,
                            part->info.getPartName());
                        resources.push_back(ub);

                        parts.push_back(part);
                    }
                }

                LocalDeleteBitmaps new_bitmaps;
                if (metadata_snapshot->hasUniqueKey() && !local_context->getSettingsRef().enable_unique_table_detach_ignore_delete_bitmap)
                {
                    /// Create new base delete bitmap, it will be convenient to handle only one necessary bitmap meta.
                    getDeleteBitmapMetaForCnchParts(parts, svr_parts_to_drop_with_dbm.second, /*force_found*/ false);
                    for (size_t i = 0; i < parts.size(); ++i)
                    {
                        IMergeTreeDataPartPtr curr_part = parts[i];
                        while (curr_part)
                        {
                            auto new_bitmap = curr_part->createNewBaseDeleteBitmap(txn->getTransactionID());
                            if (new_bitmap)
                            {
                                resources.push_back(
                                    new_bitmap->getUndoResource(txn->getTransactionID(), UndoResourceType::S3DetachDeleteBitmap));
                                new_bitmaps.push_back(std::move(new_bitmap));
                            }
                            curr_part = curr_part->tryGetPreviousPart();
                        }
                    }
                }

                // Write undo buffer first
                local_context->getCnchCatalog()->writeUndoBuffer(
                    getCnchStorageID(), txn->getTransactionID(), resources);

                DeleteBitmapMetaPtrVector bitmap_metas = dumpDeleteBitmaps(*this, new_bitmaps);
                ActionPtr action;
                if (staging_area)
                    action = txn->createAction<S3DetachMetaAction>(
                        shared_from_this(), MergeTreeDataPartsCNCHVector{}, /*staged_parts*/ parts, bitmap_metas);
                else
                    action = txn->createAction<S3DetachMetaAction>(shared_from_this(), parts, MergeTreeDataPartsCNCHVector{}, bitmap_metas);
                txn->appendAction(action);
                break;
            }
            default:
                throw Exception(
                    fmt::format("Unknown storage type {} when detach", DiskType::toString(remote_storage_type)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    MutableDataPartsVector drop_ranges;
    MutableMergeTreeDataPartsCNCHVector new_visible_parts_only_commit, new_staged_parts_only_commit;

    /// Data of staged part will not be removed in GC stage due to that is also shared by published part, thus we need to fake publish the part and remove it which will remove data in GC stage.
    auto generateFakeVisiblePartForStagedPart = [&](const IMergeTreeDataPartPtr & staged_part) -> MutableMergeTreeDataPartCNCHPtr {
        Protos::DataModelPart new_part_model;
        fillPartModel(*this, *staged_part, new_part_model);
        auto txn_id = txn->getPrimaryTransactionID().toUInt64();
        new_part_model.mutable_part_info()->set_mutation(txn_id);
        new_part_model.set_txnid(txn_id);
        new_part_model.set_delete_flag(false);
        new_part_model.set_staging_txn_id(staged_part->info.mutation);
        // storage may not have part columns info (CloudMergeTree), so set columns/columns_commit_time manually
        auto new_part = createPartFromModelCommon(*this, new_part_model);
        /// Attention: commit time has been force set in createPartFromModelCommon method, we must clear commit time here. Otherwise, it will be visible event if the txn rollback.
        new_part->commit_time = IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME;
        new_part->setColumnsPtr(std::make_shared<NamesAndTypesList>(staged_part->getColumns()));
        new_part->columns_commit_time = staged_part->columns_commit_time;
        if (txn->isSecondary())
            new_part->secondary_txn_id = txn->getTransactionID();
        return new_part;
    };

    auto generateDropPart = [&](const IMergeTreeDataPartPtr & part) -> MutableMergeTreeDataPartCNCHPtr {
        auto drop_part_info = part->info;

        drop_part_info.level += 1;
        // drop_range part should belong to the primary transaction
        drop_part_info.mutation = txn->getPrimaryTransactionID().toUInt64();
        drop_part_info.hint_mutation = 0;
        auto disk = getStoragePolicy(IStorage::StorageLocation::AUXILITY)->getAnyDisk();
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + drop_part_info.getPartName(), disk);
        auto drop_part = std::make_shared<MergeTreeDataPartCNCH>(
            *this, drop_part_info.getPartName(), drop_part_info, single_disk_volume, std::nullopt);
        drop_part->partition.assign(part->partition);
        drop_part->deleted = true;
        /// Get parts chain and update `coverd_xxx` fields for metrics.
        {
            drop_part->covered_parts_rows += part->rows_count;
            auto cur_part = part;
            do
            {
                drop_part->covered_parts_size += cur_part->bytes_on_disk;
                drop_part->covered_parts_count += 1;
            } while ((cur_part = cur_part->tryGetPreviousPart()));
        }

        if (txn->isSecondary())
        {
            drop_part->secondary_txn_id = txn->getTransactionID();
        }
        return drop_part;
    };

    if (staging_area)
    {
        for (const auto & part: parts_to_drop)
        {
            /// Create fake visible part and drop for fake visible part and staged part
            new_staged_parts_only_commit.emplace_back(generateDropPart(part));
            auto fake_visible_part = generateFakeVisiblePartForStagedPart(part);
            new_visible_parts_only_commit.emplace_back(fake_visible_part);
            drop_ranges.emplace_back(generateDropPart(fake_visible_part));
        }
    }
    else
    {
        if (svr_parts_to_drop.size() == 1 || command.specify_bucket)
        {
            for (const auto & part: parts_to_drop)
                drop_ranges.emplace_back(generateDropPart(part));
        }
        else
        {
            // drop_range parts should belong to the primary transaction
            drop_ranges = createDropRangesFromParts(local_context, svr_parts_to_drop, txn);
        }
    }

    auto bitmap_tombstones = createDeleteBitmapTombstones(drop_ranges, txn->getPrimaryTransactionID());

    /// Start to dump and commit
    {
        LOG_DEBUG(
            log,
            "Start dump and commit {} parts, {} bitmaps, only commit without dump {} parts, {} staged parts.",
            drop_ranges.size(),
            bitmap_tombstones.size(),
            new_visible_parts_only_commit.size(),
            new_staged_parts_only_commit.size());

        CnchDataWriter cnch_writer(*this, local_context, ManipulationType::Drop);
        auto dumped_data = cnch_writer.dumpCnchParts(drop_ranges, bitmap_tombstones, {});
        if (staging_area)
        {
            dumped_data.parts.insert(dumped_data.parts.end(), new_visible_parts_only_commit.begin(), new_visible_parts_only_commit.end());
            dumped_data.staged_parts.insert(
                dumped_data.staged_parts.end(), new_staged_parts_only_commit.begin(), new_staged_parts_only_commit.end());
        }
        cnch_writer.commitDumpedParts(dumped_data);
    }

    if (do_commit)
        txn->commitV2();
}

StorageCnchMergeTree::MutableDataPartsVector
StorageCnchMergeTree::createDropRangesFromParts([[maybe_unused]] ContextPtr query_context, const ServerDataPartsVector & parts_to_drop,
    const TransactionCnchPtr & txn)
{
    PartitionDropInfos partition_infos;
    std::unordered_set<String> partitions;

    for (const auto & part : parts_to_drop)
    {
        partitions.insert(part->info().partition_id);
        auto [iter, inserted] = partition_infos.try_emplace(part->info().partition_id);
        if (inserted)
            iter->second.value.assign(part->partition());

        iter->second.max_block = std::max(iter->second.max_block, part->info().max_block);

        /// Get parts chain and update `coverd_xxx` fields for metrics.
        {
            iter->second.rows_count += part->rowsCount();
            auto cur_part = part;
            do
            {
                iter->second.size += cur_part->part_model().size();
                iter->second.parts_count += 1;
            } while ((cur_part = cur_part->tryGetPreviousPart()));
        }
    }

    return createDropRangesFromPartitions(partition_infos, txn);
}

StorageCnchMergeTree::MutableDataPartsVector
StorageCnchMergeTree::createDropRangesFromPartitions(const PartitionDropInfos & partition_infos, const TransactionCnchPtr & txn)
{
    MutableDataPartsVector drop_ranges;
    for (auto && [partition_id, info] : partition_infos)
    {
        MergeTreePartInfo drop_range_info(
            partition_id, 0, info.max_block, MergeTreePartInfo::MAX_LEVEL, txn->getPrimaryTransactionID(), 0 /* must be zero */);
        auto disk = getStoragePolicy(IStorage::StorageLocation::AUXILITY)->getAnyDisk();
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + drop_range_info.getPartName(), disk);
        String drop_part_name = drop_range_info.getPartName();
        auto drop_range = createPart(
            drop_part_name,
            MergeTreeDataPartType::WIDE,
            drop_range_info,
            single_disk_volume,
            drop_part_name,
            nullptr,
            StorageLocation::AUXILITY);
        drop_range->partition.assign(info.value);
        drop_range->deleted = true;
        drop_range->covered_parts_rows = info.rows_count;
        drop_range->covered_parts_size = info.size;
        drop_range->covered_parts_count = info.parts_count;
        /// If we don't have this, drop_range parts are not visible to queries in interactive
        /// transaction session
        if (txn->isSecondary())
        {
            drop_range->secondary_txn_id = txn->getTransactionID();
        }
        drop_ranges.push_back(std::move(drop_range));
    }

    return drop_ranges;
}

LocalDeleteBitmaps StorageCnchMergeTree::createDeleteBitmapTombstones(const IMutableMergeTreeDataPartsVector & drop_ranges, UInt64 txnID)
{
    LocalDeleteBitmaps bitmap_tombstones;
    if (getInMemoryMetadataPtr()->hasUniqueKey())
    {
        for (auto & part : drop_ranges)
        {
            if (part->isDropRangePart())
            {
                auto temp_bitmap
                    = LocalDeleteBitmap::createRangeTombstone(part->info.partition_id, part->info.max_block, txnID, part->bucket_number);
                bitmap_tombstones.push_back(std::move(temp_bitmap));
                LOG_DEBUG(log, "Range tombstone of delete bitmap has been created for drop range part " + part->info.getPartName());
            }
            else
            {
                auto temp_bitmap = LocalDeleteBitmap::createTombstone(part->info, txnID, part->bucket_number);
                bitmap_tombstones.push_back(std::move(temp_bitmap));
                LOG_DEBUG(log, "Tombstone of delete bitmap has been created for part {}", part->info.getPartName());
            }
        }
    }
    return bitmap_tombstones;
}

StoragePolicyPtr StorageCnchMergeTree::getStoragePolicy(StorageLocation location) const
{
    String policy_name = (location == StorageLocation::MAIN ? getSettings()->storage_policy : getContext()->getCnchAuxilityPolicyName());
    return getContext()->getStoragePolicy(policy_name);
}

const String & StorageCnchMergeTree::getRelativeDataPath(StorageLocation location) const
{
    return location == StorageLocation::MAIN ? MergeTreeMetaBase::getRelativeDataPath(location) : relative_auxility_storage_path;
}

std::set<Int64> StorageCnchMergeTree::getRequiredBucketNumbers(const SelectQueryInfo & query_info, ContextPtr local_context) const
{
    if (!isBucketTable())
        return {};

    std::set<Int64> bucket_numbers;
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->getWhere();
    const Settings & settings = local_context->getSettingsRef();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    // if number of bucket columns of this table > 1, skip optimisation
    if (settings.optimize_skip_unused_shards && where_expression
        && metadata_snapshot->getColumnsForClusterByKey().size() == 1)
    {
        // get constant actions of the expression
        Block sample_block = metadata_snapshot->getSampleBlock();
        NamesAndTypesList source_columns = sample_block.getNamesAndTypesList();

        auto syntax_result = TreeRewriter(local_context).analyze(where_expression, source_columns);
        ExpressionActionsPtr const_actions = ExpressionAnalyzer{where_expression, syntax_result, local_context}.getConstActions();
        Names required_source_columns = syntax_result->requiredSourceColumns();

        // Delete all unneeded columns
        for (const auto & delete_column : sample_block.getNamesAndTypesList())
        {
            if (std::find(required_source_columns.begin(), required_source_columns.end(), delete_column.name)
                == required_source_columns.end())
            {
                sample_block.erase(delete_column.name);
            }
        }

        const_actions->execute(sample_block);

        //replace constant values as literals in AST using visitor
        if (sample_block)
        {
            InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(sample_block);
            visitor.visit(where_expression);
        }

        size_t limit = settings.optimize_skip_unused_shards_limit;
        if (!limit || limit > SSIZE_MAX)
        {
            throw Exception(
                "optimize_skip_unused_shards_limit out of range (0, " + std::to_string(SSIZE_MAX) + "]", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        // Increment limit so that when limit reaches 0, it means that the limit has been exceeded
        ++limit;

        // NOTE: check for cluster by columns in where clause done in evaluateExpressionOverConstantCondition
        const auto & blocks
            = evaluateExpressionOverConstantCondition(where_expression, metadata_snapshot->getClusterByKey().expression, limit);

        if (!limit)
        {
            LOG_INFO(
                log,
                "Number of values for cluster_by key exceeds optimize_skip_unused_shards_limit = "
                    + std::to_string(settings.optimize_skip_unused_shards_limit)
                    + ", try to increase it, but note that this may increase query processing time.");
        }

        if (blocks)
        {
            for (const auto & block : *blocks)
            {
                // Get bucket number and add to results array
                Block block_copy = block;
                prepareBucketColumn(
                    block_copy,
                    metadata_snapshot->getColumnsForClusterByKey(),
                    metadata_snapshot->getSplitNumberFromClusterByKey(),
                    metadata_snapshot->getWithRangeFromClusterByKey(),
                    metadata_snapshot->getBucketNumberFromClusterByKey(),
                    local_context,
                    metadata_snapshot->getIsUserDefinedExpressionFromClusterByKey());
                auto bucket_number
                    = block_copy.getByPosition(block_copy.columns() - 1).column->getInt(0); // this block only contains one row
                bucket_numbers.insert(bucket_number);
            }
        }
    }
    return bucket_numbers;
}

StorageCnchMergeTree * StorageCnchMergeTree::checkStructureAndGetCnchMergeTree(const StoragePtr & source_table, ContextPtr local_context) const
{
    StorageCnchMergeTree * src_data = dynamic_cast<StorageCnchMergeTree *>(source_table.get());
    if (!src_data)
        throw Exception(
            "Table " + source_table->getStorageID().getFullTableName() + " is not StorageCnchMergeTree", ErrorCodes::BAD_ARGUMENTS);

    auto metadata = getInMemoryMetadataPtr();
    auto src_metadata = src_data->getInMemoryMetadataPtr();
    Names minmax_column_names = getMinMaxColumnsNames(metadata->getPartitionKey());

    /// Columns order matters if table havs more than one minmax index column.
    if (!metadata->getColumns().getAllPhysical().isCompatableWithKeyColumns(
            src_metadata->getColumns().getAllPhysical(), minmax_column_names))
    {
        throw Exception("Tables have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
    }

    auto query_to_string = [](const ASTPtr & ast) {
        if (ast == nullptr)
        {
            return std::string("");
        }

        WriteBufferFromOwnString out;
        formatAST(*ast, out, false, true, true);
        return out.str();
    };

    if (query_to_string(metadata->getSortingKeyAST()) != query_to_string(src_metadata->getSortingKeyAST()))
        throw Exception("Tables have different ordering", ErrorCodes::BAD_ARGUMENTS);

    if (query_to_string(metadata->getSamplingKeyAST()) != query_to_string(src_metadata->getSamplingKeyAST()))
        throw Exception("Tables have different sample by key", ErrorCodes::BAD_ARGUMENTS);

    if (query_to_string(metadata->getPartitionKeyAST()) != query_to_string(src_metadata->getPartitionKeyAST()))
        throw Exception("Tables have different partition key", ErrorCodes::BAD_ARGUMENTS);

    if (format_version != src_data->format_version)
        throw Exception("Tables have different format_version", ErrorCodes::BAD_ARGUMENTS);

    // check root path of source and destination table
    Disks tgt_disks = getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks();
    Disks src_disks = src_data->getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks();
    std::set<String> tgt_path_set;
    for (const DiskPtr & disk : tgt_disks)
    {
        tgt_path_set.insert(disk->getPath());
    }
    for (const DiskPtr & disk : src_disks)
    {
        if (!tgt_path_set.count(disk->getPath()))
            throw Exception("source table and destination table have different hdfs root path", ErrorCodes::BAD_ARGUMENTS);
    }

    // If target table is a bucket table, ensure that source table is a bucket table
    // or if the source table is a bucket table, ensure the table_definition_hash is the same before proceeding to drop parts
    // Can remove this check if rollback has been implemented
    bool skip_table_definition_hash_check = local_context->getSettingsRef().allow_attach_parts_with_different_table_definition_hash
                                                        && !getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey();
    if (isBucketTable() && !skip_table_definition_hash_check
        && (!src_data->isBucketTable() || getTableHashForClusterBy() != src_data->getTableHashForClusterBy()))
    {
        LOG_DEBUG(
            log,
            fmt::format(
                "{}.{} table_definition hash [{}] is different from target table's "
                "table_definition hash [{}]",
                src_data->getDatabaseName(),
                src_data->getTableName(),
                src_data->getTableHashForClusterBy().toString(),
                getTableHashForClusterBy().toString()));
        throw Exception(
            "Source table is not a bucket table or has a different CLUSTER BY definition from the target table. ",
            ErrorCodes::BUCKET_TABLE_ENGINE_MISMATCH);
    }

    return src_data;
}

ServerDataPartsWithDBM StorageCnchMergeTree::selectPartsByPartitionCommand(ContextPtr local_context, const PartitionCommand & command)
{
    /// The members of `command` have different meaning depending on the types of partition command:
    /// 1. <COMMAND> PART:
    ///    - command.part = true
    ///    - command.partition is ASTLiteral of part name
    /// 2. <COMMAND> PARTITION <ID>:
    ///    - command.part = false
    ///    - command.partition is partition expression
    /// 3. <COMMAND> PARTITION WHERE:
    ///    - command.part = false;
    ///    - command.partition is the WHERE predicate (should only includes partition column)

    /// Implementation: reuse selectPartsToRead(). Actually, this is an overkill, because the predicates is usually not
    /// too complicated. Howerver, this is the only way to avoid repeating same code in StorageCnchMergeTree.
    SelectQueryInfo query_info;
    Names column_names_to_return;
    ASTPtr query = std::make_shared<ASTSelectQuery>();
    auto * select = query->as<ASTSelectQuery>();
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    select->select()->children.push_back(std::make_shared<ASTLiteral>(1));
    select->replaceDatabaseAndTable(getStorageID());
    /// create a fake query: SELECT 1 FROM TBL WHERE ... as following
    ASTPtr where;
    if (command.part)
    {
        /// Predicate: WHERE _part = value, with value from command.partition
        auto lhs = std::make_shared<ASTIdentifier>("_part");
        auto rhs = command.partition->clone();
        where = makeASTFunction("equals", std::move(lhs), std::move(rhs));
        column_names_to_return.push_back("_part");
    }
    else if (!partitionCommandHasWhere(command))
    {
        if (const auto & ast_literal = command.partition->as<ASTLiteral>())
        {
            String partition_id = ast_literal->value.get<String>();
            /// Predicate: WHERE _partition_id = value, with value is literal partition id.
            auto lhs = std::make_shared<ASTIdentifier>("_partition_id");
            auto rhs = std::make_shared<ASTLiteral>(Field(partition_id));
            where = makeASTFunction("equals", std::move(lhs), std::move(rhs));
            column_names_to_return.push_back("_partition_id");
        }
        else
        {
            const auto & partition = command.partition->as<const ASTPartition &>();
            if (!partition.id.empty())
            {
                /// Predicate: WHERE _partition_id = value, with value is partition.id
                auto lhs = std::make_shared<ASTIdentifier>("_partition_id");
                auto rhs = std::make_shared<ASTLiteral>(Field(partition.id));
                where = makeASTFunction("equals", std::move(lhs), std::move(rhs));
                column_names_to_return.push_back("_partition_id");
            }
            else
            {
                /// Predicate: WHERE _partition_value = value, with value is partition.value
                auto lhs = std::make_shared<ASTIdentifier>("_partition_value");
                auto rhs = partition.value->clone();
                if (partition.fields_count == 1)
                    rhs = makeASTFunction("tuple", std::move(rhs));
                where = makeASTFunction("equals", std::move(lhs), std::move(rhs));
                column_names_to_return.push_back("_partition_value");
            }
        }
    }
    else
    {
        /// Predicate: WHERE xxx with xxx is command.partition
        where = command.partition->clone();
        /// NOTE: Where condition may contains _partition_id or _partition_value, so we must run a filter of partition.
        /// Adding _partition_id to `column_names_to_return` trigger the partition prune in ::selectPartitionsByPredicate.
        column_names_to_return.push_back("_partition_id");
    }

    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_snapshot = getStorageSnapshot(metadata_snapshot, local_context);
    /// So this step will throws if WHERE expression contains columns not in partition key, and it's a good thing
    auto source_columns = metadata_snapshot->partition_key.sample_block.getNamesAndTypesList();
    source_columns.push_back(NameAndTypePair("_part", std::make_shared<DataTypeString>()));
    source_columns.push_back(NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()));
    source_columns.push_back(NameAndTypePair("_partition_value", getPartitionValueType()));
    TreeRewriterResult syntax_analyzer_result(source_columns, {}, storage_snapshot, true);
    auto analyzed_result = TreeRewriter(local_context).analyzeSelect(query, std::move(syntax_analyzer_result));
    SelectQueryExpressionAnalyzer analyzer(query, analyzed_result, local_context, metadata_snapshot);
    analyzer.makeSetsForIndex(select->where());
    query_info.query = std::move(query);
    query_info.sets = std::move(analyzer.getPreparedSets());
    query_info.syntax_analyzer_result = std::move(analyzed_result);
    return selectPartsToReadWithDBM(column_names_to_return, local_context, query_info, /*snapshot_ts=*/0, /*staging_area=*/command.staging_area);
}

String StorageCnchMergeTree::genCreateTableQueryForWorker(const String & suffix)
{
    String worker_table_name = getTableName();

    if (!suffix.empty())
    {
        worker_table_name += '_';
        for (const auto & c : suffix)
        {
            if (c != '-')
                worker_table_name += c;
        }
    }

    return getCreateQueryForCloudTable(getCreateTableSql(), worker_table_name);
}

std::optional<UInt64> StorageCnchMergeTree::totalRows(const ContextPtr & query_context) const
{
    const auto & metadata_snapshot = getInMemoryMetadataPtr();
    auto partition_sample_block = getInMemoryMetadataPtr()->getPartitionKey().sample_block;

    /// Prune partitions by partition level TTL even there is no WHERE condition.
    std::optional<NameSet> partition_ids = std::nullopt;
    if (canFilterPartitionByTTL())
    {
        partition_ids = std::make_optional<NameSet>();
        auto partition_list = query_context->getCnchCatalog()->getPartitionList(shared_from_this(), query_context.get());
        if (partition_list.empty())
            return 0;
        auto num_total_partition = partition_list.size();

        filterPartitionByTTL(partition_list, query_context->tryGetCurrentTransactionID().toSecond());
        if (partition_list.empty())
            return 0;
        partition_ids->reserve(partition_list.size());

        for (const auto & p : partition_list)
            partition_ids->insert(p->getID(partition_sample_block, extractNullableForPartitionID()));

        LOG_TRACE(log, "[TrivialCount] after filter partition by TTL {}/{}", partition_ids->size(), num_total_partition);
    }

    auto parts_with_dbm = getAllPartsWithDBM(query_context);
    if (parts_with_dbm.first.empty())
        return 0;
    if (metadata_snapshot->hasUniqueKey())
        getDeleteBitmapMetaForServerParts(parts_with_dbm.first, parts_with_dbm.second);
    size_t rows = 0;
    for (const auto & part : parts_with_dbm.first)
    {
        if (partition_ids.has_value() && !partition_ids->contains(part->partition().getID(partition_sample_block, extractNullableForPartitionID())))
            continue;
        rows += part->rowsCount() - part->deletedRowsCount(*this);
    }

    LOG_TRACE(log, "[TrivialCount] calculate total_rows from metadata {}", rows);
    return rows;
}

std::optional<UInt64> StorageCnchMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr local_context) const
{
    /// Similar to selectPartsToRead, but will return {} if the predicate is not a partition predicate or _part
    auto column_names_to_return = query_info.syntax_analyzer_result->requiredSourceColumns();
    auto parts_with_dbm = getAllPartsInPartitionsWithDBM(column_names_to_return, local_context, query_info);
    auto & parts = parts_with_dbm.first;
    if (parts.empty())
        return 0;
    const auto & metadata_snapshot = getInMemoryMetadataPtr();
    if (metadata_snapshot->hasUniqueKey())
        getDeleteBitmapMetaForServerParts(parts, parts_with_dbm.second);

    bool partition_column_valid = std::any_of(column_names_to_return.begin(), column_names_to_return.end(), [](const auto & name) {
        return name == "_partition_id" || name == "_partition_value";
    });

    if (partition_column_valid)
    {
        auto partition_list = local_context->getCnchCatalog()->getPartitionList(shared_from_this(), local_context.get());
        Block partition_block = getPartitionBlockWithVirtualColumns(partition_list);
        ASTPtr expression_ast;

        /// Generate valid expressions for filtering
        partition_column_valid = partition_column_valid
            && VirtualColumnUtils::prepareFilterBlockWithQuery(query_info.query, local_context, partition_block, expression_ast);
    }

    PartitionPruner partition_pruner(metadata_snapshot, query_info, local_context, true /* strict */);

    if (!partition_column_valid && partition_pruner.isUseless())
        return {};

    Block virtual_columns_block = getBlockWithPartAndBucketColumn(parts);
    bool part_column_queried
        = std::any_of(column_names_to_return.begin(), column_names_to_return.end(), [](const auto & name) { return name == "_part"; });
    if (part_column_queried)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, local_context);
    auto part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");
    if (part_values.empty())
        return 0;

    size_t rows = 0;
    for (const auto & part : parts)
    {
        if ((part_values.empty() || part_values.find(part->name()) != part_values.end())
            && !partition_pruner.canBePruned(*part))
        {
            rows += part->rowsCount() - part->deletedRowsCount(*this);
        }
    }

    LOG_TRACE(log, "Shortcut: calculate [partition] total_rows from metadata {}", rows);
    return rows;
}

void StorageCnchMergeTree::checkMutationIsPossible(const MutationCommands & commands, const Settings & /*settings*/) const
{
    static std::unordered_set<MutationCommand::Type> supported_mutations =
    {
        MutationCommand::MATERIALIZE_INDEX,
        MutationCommand::DELETE,
        MutationCommand::FAST_DELETE,
        MutationCommand::UPDATE,
    };

    bool contains_delete = false;
    for (const auto & command : commands)
    {
        if(!supported_mutations.contains(command.type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED , "StorageCnchMergeTree doesn't support mutation of type {}\n", command.type);

        if (command.column_name == RowExistsColumn::ROW_EXISTS_COLUMN.name
            || (command.type == MutationCommand::RENAME_COLUMN && command.rename_to == RowExistsColumn::ROW_EXISTS_COLUMN.name))
        {
            throw Exception("Column `_row_exists` is not allowed to appears in any mutation command as it's reserved for DELETE mutation.", ErrorCodes::BAD_ARGUMENTS);
        }

        if (command.type == MutationCommand::DELETE || command.type == MutationCommand::FAST_DELETE)
        {
            if (getInMemoryMetadataPtr()->hasUniqueKey() && !getSettings()->enable_delete_mutation_on_unique_table)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`ALTER TABLE DELETE` is disabled for unique table. To delete data from unique table, please use `DELETE FROM` or set table setting `enable_delete_mutation_on_unique_table` before running `ALTER TABLE DELETE`.");

            if (contains_delete)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "It's not allowed to execute multiple DELETE|FASTDELETE commands");
            contains_delete = true;
        }
        else if (contains_delete)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "It's not allowed to execute DELETE|FASTDELETE with other commands");
        }
    }
}

void StorageCnchMergeTree::mutate(const MutationCommands & commands, ContextPtr query_context)
{
    if (commands.empty())
        return;

    /// Check whether PARTITION (ID) is valid. Will throw exception if partition is an invalid value.
    for (const auto & c : commands)
    {
        if (c.partition)
        {
            auto p = getPartitionIDFromQuery(c.partition, query_context);
            LOG_TRACE(log, "Extract partition id from command: {}", p);
        }
    }

    auto txn = query_context->getCurrentTransaction();
    auto action = txn->createAction<DDLAlterAction>(shared_from_this(), query_context->getSettingsRef(), query_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();
    alter_act.setMutationCommands(commands);
    txn->appendAction(std::move(action));
    txn->commitV1();

    auto mutation_entry = alter_act.getMutationEntry();
    if (!mutation_entry.has_value())
    {
        if (!commands.empty())
            LOG_ERROR(log, "DDLAlterAction didn't generated mutation entry for commands.");
        return;
    }
    addMutationEntry(*mutation_entry);

    auto bg_thread = query_context->tryGetCnchBGThread(CnchBGThreadType::MergeMutate, getStorageID());
    if (bg_thread)
    {
        if (query_context->getSettingsRef().mutations_sync != 0)
        {
            try
            {
                auto timeout_ms = query_context->getSettingsRef().max_execution_time.totalMilliseconds();
                auto * merge_mutate_thread = typeid_cast<CnchMergeMutateThread *>(bg_thread.get());
                merge_mutate_thread->triggerPartMutate(shared_from_this());
                merge_mutate_thread->waitMutationFinish(mutation_entry->commit_time, timeout_ms);
            }
            catch(Exception & e)
            {
                e.addMessage("(It will be scheduled in background. Check `system.mutations` and `system.manipulations` for progress)");
                throw;
            }
        }
    }
}

void StorageCnchMergeTree::resetObjectColumns(ContextPtr query_context)
{
    object_columns = object_schemas.assembleSchema(query_context, getInMemoryMetadataPtr());
    LOG_TRACE(log, "Global object schema snapshot:" + object_columns.toString());
}

void StorageCnchMergeTree::appendObjectPartialSchema(const TxnTimestamp & txn_id, ObjectPartialSchema partial_schema)
{
    object_schemas.appendPartialSchema(txn_id, partial_schema);
}

void StorageCnchMergeTree::resetObjectSchemas(const ObjectAssembledSchema & assembled_schema, const ObjectPartialSchemas & partial_schemas)
{
    object_schemas.reset(assembled_schema, partial_schemas);
}

void StorageCnchMergeTree::refreshAssembledSchema(const ObjectAssembledSchema & assembled_schema,  std::vector<TxnTimestamp> txn_ids)
{
    object_schemas.refreshAssembledSchema(assembled_schema, txn_ids);
}

std::unique_ptr<MergeTreeSettings> StorageCnchMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

StorageID StorageCnchMergeTree::prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context)
{
    auto prepare_result = prepareReadContext(output_columns, getInMemoryMetadataPtr(), query_info, local_context);

    StorageID storage_id = getStorageID();
    storage_id.table_name = prepare_result.local_table_name;
    return storage_id;
}

StorageID StorageCnchMergeTree::prepareTableWrite(ContextPtr local_context)
{
    auto prepare_result = prepareLocalTableForWrite(nullptr, local_context, false, false);
    StorageID storage_id = getStorageID();
    storage_id.table_name = prepare_result.first;
    return storage_id;
}

} // end namespace DB
