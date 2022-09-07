#include <Storages/StorageCnchMergeTree.h>

#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchServerResource.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <Databases/DatabaseOnDisk.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <MergeTreeCommon/CnchBucketTableCommon.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/VirtualColumnUtils.h>
#include <Transaction/getCommitted.h>

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Core/NamesAndTypes.h>
#include <Core/QueryProcessingStage.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>


namespace ProfileEvents
{
extern const Event CatalogTime;
extern const Event TotalPartitions;
extern const Event PrunedPartitions;
extern const Event SelectedParts;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int INDEX_NOT_USED;
    extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
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
        relative_data_path_.empty() ? UUIDHelpers::UUIDToString(table_id_.uuid) : relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_),
        false,
        attach_,
        [](const String &) {})
    , CnchStorageCommonHelper(table_id_, getDatabaseName(), getTableName())
{
    local_store_volume = getContext()->getStoragePolicy(getSettings()->cnch_local_storage_policy.toString());
    relative_local_store_path = fs::path("store");
}

QueryProcessingStage::Enum StorageCnchMergeTree::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const
{
    const auto & settings = local_context->getSettingsRef();

    if (settings.distributed_perfect_shard || settings.distributed_group_by_no_merge)
    {
        return QueryProcessingStage::Complete;
    }
    else if (getSettings()->cnch_enable_memory_buffer)
    {
        return QueryProcessingStage::WithMergeableState;
    }
    else if (auto worker_group = local_context->tryGetCurrentWorkerGroup())
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
    /// LOG_DEBUG(log, "Startup cnch table " << getLogName());
    try
    {
        /// FIXME: add this after merging daemon manager
        // if (this->settings.cnch_enable_memory_buffer)
        // {
        //     if (auto daemon_manager = global_context.getDaemonManagerClient(); daemon_manager)
        //         daemon_manager->controlDaemonJob(getStorageID(), CnchBGThreadType::MemoryBuffer, Protos::ControlDaemonJobReq::Start);
        // }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void StorageCnchMergeTree::shutdown()
{
    /// LOG_DEBUG(log, "Shutdown cnch table " << getLogName());
}

Pipe StorageCnchMergeTree::read(
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

void StorageCnchMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    auto txn = local_context->getCurrentTransaction();
    if (local_context->getServerType() == ServerType::cnch_server && txn && txn->isReadOnly())
        local_context->getCnchTransactionCoordinator().touchActiveTimestampByTable(getStorageID(), txn);


    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    Block header = InterpreterSelectQuery(query_info.query, local_context, SelectQueryOptions(processed_stage)).getSampleBlock();

    auto worker_group = local_context->getCurrentWorkerGroup();
    healthCheckForWorkerGroup(local_context, worker_group);
    auto parts = selectPartsToRead(column_names, local_context, query_info);
    LOG_INFO(log, "Number of parts to read: {}", parts.size());

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
    if (parts.empty())
    {
        /// Stage 1: read from source table, just assume we read everything
        const auto & source_columns = query_info.syntax_analyzer_result->required_source_columns;
        auto fetch_column_header = Block(NamesAndTypes{source_columns.begin(), source_columns.end()});
        Pipe pipe(std::make_shared<NullSource>(std::move(fetch_column_header)));
        /// Stage 2: (partial) aggregation and projection if any
        auto query = getBasicSelectQuery(query_info.query);
        InterpreterSelectQuery(query, local_context, std::move(pipe), SelectQueryOptions(processed_stage)).buildQueryPlan(query_plan);
        return;
    }

    if (metadata_snapshot->hasUniqueKey())
    {
        // Previous, we need to sort parts since the partial part and base part are not guarantee to be together,
        // the getDeleteBitmapMetaForParts function needs to handle the partial->base chain.
        // Now, we can remove sort logic since we don't expand the partial->base part chain after chain construction,
        // only partial part is useful in getDeleteBitmapMetaForParts for unique table.
        getDeleteBitmapMetaForParts(parts, local_context, local_context->getCurrentTransactionID());
    }


    String local_table_name = getCloudTableName(local_context);
    auto bucket_numbers = getRequiredBucketNumbers(query_info, local_context);
    collectResource(local_context, parts, local_table_name, bucket_numbers);

    // bool need_read_memory_buffer = this->getSettings()->cnch_enable_memory_buffer && !metadata_snapshot->hasUniqueKey() &&
    //                                 !local_context->getSettingsRef().cnch_skip_memory_buffers;

    LOG_TRACE(log, "Original query before rewrite: {}", queryToString(query_info.query));
    auto modified_query_ast = rewriteSelectQuery(query_info.query, getDatabaseName(), local_table_name);

    const Scalars & scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};

    ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
        header,
        processed_stage,
        StorageID::createEmpty(), /// Don't check whether table exists in cnch-worker
        scalars,
        false,
        local_context->getExternalTables());

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, modified_query_ast, local_context, worker_group);

    /// FIXME: support memory buffer
    // if (need_read_memory_buffer)
    // {
    //     ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
    //         header, processed_stage, QualifiedTableName{getDatabaseName(), getTableName()}, local_context.getExternalTables(), true, nullptr);

    //     const auto & buffer_workers = getMemoryBufferWorkers(local_context);
    //     if (buffer_workers.empty())
    //         throw Exception("No memory buffer found for " + getStorageID().getNameForLogs() + " while try to read buffers", ErrorCodes::LOGICAL_ERROR);

    //     auto vw_name = "#temp_buffer_vw";
    //     auto worker_group_name = "#temp_buffer_wg";
    //     auto buffer_worker_group = std::make_shared<WorkerGroupHandleImpl>(worker_group_name, WorkerGroupHandleSource::TEMP, vw_name, buffer_workers, local_context);

    //     LOG_TRACE(
    //         log,
    //         "Create temporary worker group: " << buffer_worker_group->getQualifiedName()
    //                                           << " with size: " << buffer_worker_group->getHostWithPortsVec().size());

    //     auto streams_from_buffers = ClusterProxy::executeQuery(select_stream_factory, buffer_worker_group, query_info.query, local_context, settings);
    //     streams.insert(streams.end(), streams_from_buffers.begin(), streams_from_buffers.end());
    // }

    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);
}

Strings StorageCnchMergeTree::selectPartitionsByPredicate(
    const SelectQueryInfo & query_info,
    std::vector<std::shared_ptr<MergeTreePartition>> & partition_list,
    const Names & column_names_to_return,
    ContextPtr local_context)
{
    /// Coarse grained partition prunner: filter out the partition which will definately not sastify the query predicate. The benefit
    /// is 2-folded: (1) we can prune data parts and (2) we can reduce numbers of calls to catalog to get parts 's metadata.
    /// Note that this step still leaves false-positive parts. For example, the partition key is `toMonth(date)` and the query
    /// condition is `date > '2022-02-22' and date < '2022-03-22'` then this step won't eliminate any partition.

    /// The partition pruning rules come from 3 types:
    /// (1) TTL
    /// (2) Columns in predicate that exactly match the partition key
    /// (3) `_partition_id` or `_partition_value` if they're in predicate

    /// (1) Prune partition by partition level TTL
    TTLTableDescription table_ttl = getInMemoryMetadata().getTableTTLs();
    if (table_ttl.definition_ast)
    {
        TxnTimestamp start_ts = local_context->getCurrentTransactionID();
        time_t query_time = start_ts.toSecond();
        size_t prev_sz = partition_list.size();
        std::erase_if(partition_list, [&](const auto & partition) {
            time_t metadata_ttl = getTTLForPartition(*partition);
            return metadata_ttl && metadata_ttl < query_time;
        });
        if (partition_list.size() < prev_sz)
            LOG_DEBUG(log, "TTL rules droped {} expired partitions", prev_sz - partition_list.size());
    }

    const auto partition_key = MergeTreePartition::adjustPartitionKey(getInMemoryMetadataPtr(), local_context);
    const auto & partition_key_expr = partition_key.expression;
    const auto & partition_key_sample = partition_key.sample_block;
    if (local_context->getSettingsRef().enable_partition_prune && partition_key_sample.columns() > 0)
    {
        /// (2) Prune partitions if there's a column in predicate that exactly match the partition key
        Names partition_key_columns;
        for (const auto & name : partition_key_sample)
        {
            partition_key_columns.emplace_back(name.name);
        }

        KeyCondition partition_condition(query_info, local_context, partition_key_columns, partition_key_expr);
        DataTypes result;
        result.reserve(partition_key_sample.getDataTypes().size());
        for (const auto & data_type : partition_key_sample.getDataTypes())
        {
            result.push_back(DataTypeFactory::instance().get(data_type->getName(), data_type->getFlags()));
        }
        size_t prev_sz = partition_list.size();
        std::erase_if(partition_list, [&](const auto & partition) {
            const auto & partition_value = partition->value;
            std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
            auto res = partition_condition.mayBeTrueInRange(partition_key_columns.size(), index_value.data(), index_value.data(), result);
            LOG_TRACE(
                log,
                "Key condition {} is {} in [ ({}) - ({}) )",
                partition_condition.toString(),
                res,
                fmt::join(index_value, " "),
                fmt::join(index_value, " "));
            return !res;
        });
        if (partition_list.size() < prev_sz)
            LOG_DEBUG(log, "Query predicates on physical columns droped {} partitions", prev_sz - partition_list.size());

        /// (3) Prune partitions if there's `_partition_id` or `_partition_value` in query predicate
        bool has_partition_column = std::any_of(column_names_to_return.begin(), column_names_to_return.end(), [](const auto & name) {
            return name == "_partition_id" || name == "_partition_value";
        });

        if (has_partition_column && !partition_list.empty())
        {
            Block partition_block = getBlockWithVirtualPartitionColumns(partition_list);
            ASTPtr expression_ast;

            /// Generate valid expressions for filtering
            VirtualColumnUtils::prepareFilterBlockWithQuery(query_info.query, local_context, partition_block, expression_ast);

            /// Generate list of partition id that fit the query predicate
            NameSet partition_ids;
            if (expression_ast)
            {
                VirtualColumnUtils::filterBlockWithQuery(query_info.query, partition_block, local_context, expression_ast);
                partition_ids = VirtualColumnUtils::extractSingleValueFromBlock<String>(partition_block, "_partition_id");
            }
            /// Prunning
            prev_sz = partition_list.size();
            std::erase_if(partition_list, [this, &partition_ids](const auto & partition) {
                return partition_ids.find(partition->getID(*this)) == partition_ids.end();
            });
            if (partition_list.size() < prev_sz)
                LOG_DEBUG(
                    log,
                    "Query predicates on `_partition_id` and `_partition_value` droped {} partitions",
                    prev_sz - partition_list.size());
        }
    }
    Strings res_partitions;
    for (const auto & partition : partition_list)
        res_partitions.emplace_back(partition->getID(*this));

    return res_partitions;
}

static Block getBlockWithPartColumn(ServerDataPartsVector & parts)
{
    auto column = ColumnString::create();

    for (const auto & part : parts)
        column->insert(part->part_model_wrapper->name);

    return Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "_part")};
}

time_t StorageCnchMergeTree::getTTLForPartition(const MergeTreePartition & partition) const
{
    TTLTableDescription table_ttl = getInMemoryMetadata().getTableTTLs();
    if (!table_ttl.definition_ast)
        return 0;

    /// Construct a block consists of partition keys then compute ttl values according to this block
    const auto & partition_key_sample = getInMemoryMetadata().getPartitionKey().sample_block;
    MutableColumns columns = partition_key_sample.cloneEmptyColumns();
    const auto & partition_key = partition.value;
    /// This can happen when ALTER query is implemented improperly; finish ALTER query should bypass this check.
    if (columns.size() != partition_key.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Partition key columns definition missmatch between inmemory and metastore, this is a bug");
    for (size_t i = 0; i < partition_key.size(); ++i)
        columns[i]->insert(partition_key[i]);

    auto names_and_types_list = partition_key_sample.getNamesAndTypesList();
    Block block;
    auto nt_iter = names_and_types_list.begin();
    auto column_iter = columns.begin();
    while (nt_iter != names_and_types_list.end() && column_iter != columns.end())
    {
        block.insert({std::move(*column_iter), nt_iter->type, nt_iter->name});
        nt_iter++;
        column_iter++;
    }

    table_ttl.rows_ttl.expression->execute(block);

    const auto & current = block.getByName(table_ttl.rows_ttl.result_column);

    const IColumn * column = current.column.get();

    if (column->size() > 1)
        throw Exception("Cannot get TTL value from table ttl ast since there are multiple ttl value", ErrorCodes::LOGICAL_ERROR);

    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
    {
        const auto & date_lut = DateLUT::instance();
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
    /// Fine grained parts prunning by:
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

    /// If `_part` virtual column is requested, we try to use it as an index.
    Block virtual_columns_block = getBlockWithPartColumn(parts);
    bool part_column_queried
        = std::any_of(column_names_to_return.begin(), column_names_to_return.end(), [](const auto & name) { return name == "_part"; });
    if (part_column_queried)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, local_context);
    auto part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");

    size_t prev_sz = parts.size();
    size_t empty = 0, partition_minmax = 0, minmax_idx = 0, part_value = 0;
    std::erase_if(parts, [&](const auto & part) {
        if (part->isEmpty())
        {
            ++empty;
            return true;
        }
        else if (partition_pruner && partition_pruner->canBePruned(*part))
        {
            ++partition_minmax;
            return true;
        }
        else if (
            minmax_idx_condition
            && !minmax_idx_condition->checkInHyperrectangle(part->minmax_idx()->hyperrectangle, minmax_columns_types).can_be_true)
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
            "Parts prunning rules droped {} parts, include {} empty parts, {} parts by partition minmax, {} parts by minmax index, {} "
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
    if (auto * select_query = insert_query.select->as<ASTSelectQuery>())
        select_query->collectAllTables(related_tables, has_table_func);
    else if (auto * select_with_union = insert_query.select->as<ASTSelectWithUnionQuery>())
        select_with_union->collectAllTables(related_tables, has_table_func);

    for (auto & db_and_table_ast : related_tables)
    {
        DatabaseAndTableWithAlias db_and_table(db_and_table_ast, current_database);
        if (db_and_table.database == "system" || db_and_table.database == "default")
            continue;

        if (auto table = DatabaseCatalog::instance().tryGetTable(StorageID{db_and_table.database, db_and_table.table}, local_context))
            txn_coordinator.touchActiveTimestampByTable(table->getStorageID(), txn);
    }
}

static String replaceMaterializedViewQuery(StorageMaterializedView * mv, const String & table_suffix)
{
    auto query = mv->getCreateTableSql();

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto & create_query = ast->as<ASTCreateQuery &>();
    create_query.table += "_" + table_suffix;
    create_query.to_table_id.table_name += "_" + table_suffix;

    auto & inner_query = create_query.select->list_of_selects->children.at(0);
    if (!inner_query)
        throw Exception("Select query is necessary for mv table", ErrorCodes::LOGICAL_ERROR);

    auto & select_query = inner_query->as<ASTSelectQuery &>();
    select_query.replaceDatabaseAndTable(
        mv->getInMemoryMetadataPtr()->select.select_table_id.database_name,
        mv->getInMemoryMetadataPtr()->select.select_table_id.table_name + "_" + table_suffix);

    return getObjectDefinitionFromCreateQuery(ast, false);
}

String StorageCnchMergeTree::extractTableSuffix(const String & gen_table_name)
{
    return gen_table_name.substr(gen_table_name.find_last_of('_') + 1);
}

Names StorageCnchMergeTree::genViewDependencyCreateQueries(
    const StorageID & storage_id, ContextPtr local_context, const String & table_suffix)
{
    Names create_view_sqls;
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
            create_view_sqls.emplace_back(create_local_target_query);
            create_view_sqls.emplace_back(replaceMaterializedViewQuery(mv, table_suffix));
        }

        /// TODO: Check cascade view dependency
    }

    return create_view_sqls;
}

BlockOutputStreamPtr
StorageCnchMergeTree::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
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

    if (insert_query.select || insert_query.in_file)
    {
        if (insert_query.select && local_context->getSettingsRef().restore_table_expression_in_distributed)
        {
            RestoreTableExpressionsVisitor::Data data;
            data.database = local_context->getCurrentDatabase();
            RestoreTableExpressionsVisitor(data).visit(insert_query.select);
        }

        auto generated_tb_name = getCloudTableName(local_context);
        auto local_table_name = generated_tb_name + "_write";
        insert_query.table_id.table_name = local_table_name;

        auto create_local_tb_query = getCreateQueryForCloudTable(getCreateTableSql(), local_table_name, local_context, enable_staging_area);

        String query_statement = queryToString(insert_query);

        WorkerGroupHandle worker_group = local_context->getCurrentWorkerGroup();

        /// TODO: currently use only one write worker to do insert, use multiple write workers when distributed write is support
        const Settings & settings = local_context->getSettingsRef();
        int max_retry = 2, retry = 0;
        auto num_of_workers = worker_group->getShardsInfo().size();
        if (!num_of_workers)
            throw Exception("No heathy worker available", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

        std::size_t index = std::hash<String>{}(local_context->getCurrentQueryId() + std::to_string(retry)) % num_of_workers;
        auto * write_shard_ptr = &(worker_group->getShardsInfo().at(index));

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

        LOG_DEBUG(log, "Target write worker found: {}", write_shard_ptr->worker_id);

        LOG_DEBUG(log, "Prepare execute create query: {}", create_local_tb_query);
        auto worker_client = worker_group->getWorkerClients().at(index);
        worker_client->sendCreateQueries(local_context, {create_local_tb_query});

        auto table_suffix = extractTableSuffix(generated_tb_name);
        Names dependency_create_queries = genViewDependencyCreateQueries(getStorageID(), local_context, table_suffix + "_write");
        for (const auto & dependency_create_querie : dependency_create_queries)
            LOG_DEBUG(log, "{}", dependency_create_querie);
        worker_client->sendCreateQueries(local_context, dependency_create_queries);

        /// Ensure worker session local_context rsource could be released
        if (auto session_resource = local_context->tryGetCnchServerResource())
        {
            std::vector<size_t> index_values{index};
            session_resource->setWorkerGroup(std::make_shared<WorkerGroupHandleImpl>(*worker_group, index_values));
        }

        LOG_DEBUG(log, "Prepare execute insert query: {}", query_statement);
        /// TODO: send insert query by rpc.
        sendQueryPerShard(local_context, query_statement, *write_shard_ptr);

        return nullptr;
    }
    else
    {
        /// FIXME: add after cloud output stream is supported
        return std::make_shared<CloudMergeTreeBlockOutputStream>(
            *this, metadata_snapshot, local_context, local_store_volume, relative_local_store_path, enable_staging_area);
    }
}

HostWithPortsVec StorageCnchMergeTree::getWriteWorkers(const ASTPtr & /**/, ContextPtr local_context)
{
    using ResourceManagement::VirtualWarehouseType;
    if (getSettings()->cnch_enable_memory_buffer)
    {
        /// If enabled memory buffer, the table must have several fixed WRITE workers.
        /// Get the list and pick one from them
        HostWithPortsVec memory_buffers;

        /// FIXME: add after memory buffer is supported
        // auto bmptr = getContext()->tryGetMemoryBufferManager(getStorageID());
        // auto buffer_manager = dynamic_cast<MemoryBufferManager*>(bmptr.get());
        // if (buffer_manager)
        // {
        //     memory_buffers = buffer_manager->getWorkerListWithBuffer();
        // }
        // else
        // {
        //     /// Try get memory buffer from remote server
        //     String host_port
        //         = global_context.getDaemonManagerClient()->getDaemonThreadServer(getStorageID(), CnchBGThreadType::MemoryBuffer);
        //     if (host_port.empty())
        //         throw Exception("No memory buffer manager found for targert table", ErrorCodes::LOGICAL_ERROR);

        //     auto server_client = global_context.getCnchServerClient(host_port);
        //     memory_buffers = server_client->getWorkerListWithBuffer(getStorageID(), true);
        // }

        // if (memory_buffers.empty())
        //     throw Exception("Memory buffer is empty, can't choose a write worker", ErrorCodes::LOGICAL_ERROR);

        return memory_buffers;
    }
    else
    {
        String vw_name = local_context->getSettingsRef().virtual_warehouse_write;
        if (vw_name.empty())
            vw_name = getSettings()->cnch_vw_write;

        if (vw_name.empty())
            throw Exception("Expected a nonempty vw name. Please specify it in query or table settings", ErrorCodes::BAD_ARGUMENTS);

        // No fixed workers for insertion, pick one randomly from worker pool
        auto vw_handle = local_context->getVirtualWarehousePool().get(vw_name);
        HostWithPortsVec res;
        for (const auto & [_, wg] : vw_handle->getAll())
        {
            auto wg_hosts = wg->getHostWithPortsVec();
            res.insert(res.end(), wg_hosts.begin(), wg_hosts.end());
        }
        return res;
    }
}

CheckResults StorageCnchMergeTree::checkDataCommon(const ASTPtr & query, ContextPtr local_context, ServerDataPartsVector & parts)
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
            parts = getAllParts(local_context);
        }
    }

    if (parts.empty())
        return {};

    auto worker_group = getWorkerGroupForTable(*this, local_context);
    auto worker_clients = worker_group->getWorkerClients();
    const auto & shards_info = worker_group->getShardsInfo();

    size_t num_of_workers = shards_info.size();
    if (!num_of_workers)
        throw Exception("No heathy worker available.", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

    CheckResults results;
    /// FIXME: add after supporting part allocation and rpc
    // std::mutex mutex;

    // auto assignment = assignCnchParts(worker_group, parts);

    // ThreadPool allocate_pool(std::min<UInt64>(local_context.getSettingsRef().parts_preallocate_pool_size, num_of_workers));

    // for (size_t i = 0; i < num_of_workers; ++i)
    // {
    //     auto allocate_task = [&, i]
    //     {
    //         auto it = assignment.find(shards_info[i].worker_id);
    //         if (it == assignment.end())
    //             return;

    //         auto result = worker_clients[i]->checkDataParts(local_context.getCurrentCnchXID(), *this, local_table_name, create_table_query, it->second);

    //         {
    //            std::lock_guard lock(mutex);
    //            results.insert(results.end(), result.begin(), result.end());
    //         }
    //     };

    //     allocate_pool.schedule(allocate_task);
    // }

    // /// wait for finish of part preallocation
    // allocate_pool.wait();

    return results;
}

CheckResults StorageCnchMergeTree::checkData(const ASTPtr & query, ContextPtr local_context)
{
    ServerDataPartsVector parts;
    return checkDataCommon(query, local_context, parts);
}

ServerDataPartsVector StorageCnchMergeTree::getAllParts(ContextPtr local_context)
{
    // TEST_START(testlog);

    if (local_context->getCnchCatalog())
    {
        TransactionCnchPtr cur_txn = local_context->getCurrentTransaction();
        ServerDataPartsVector all_parts
            = local_context->getCnchCatalog()->getAllServerDataParts(shared_from_this(), cur_txn->getStartTime(), nullptr);
        return cur_txn->getLatestCheckpointWithVersionChain(all_parts, local_context);
    }

    // TEST_END(testlog, "Get all parts from Catalog Service");

    return {};
}

ServerDataPartsVector
StorageCnchMergeTree::selectPartsToRead(const Names & column_names_to_return, ContextPtr local_context, const SelectQueryInfo & query_info)
{
    ServerDataPartsVector data_parts;

    // TEST_START(testlog);

    if (local_context->getCnchCatalog())
    {
        TransactionCnchPtr cur_txn = local_context->getCurrentTransaction();
        Stopwatch watch;
        auto partition_list = local_context->getCnchCatalog()->getPartitionList(shared_from_this(), local_context.get());
        // TEST_LOG(testlog, "get partition list.");
        Strings pruned_partitions = selectPartitionsByPredicate(query_info, partition_list, column_names_to_return, local_context);
        // TEST_LOG(testlog, "select partitions by predicate.");
        if (cur_txn->isSecondary())
        {
            /// Get all parts in the partition list
            LOG_DEBUG(log, "Current transaction is secondary transaction, result may include uncommited data");
            data_parts = local_context->getCnchCatalog()->getServerDataPartsInPartitions(
                shared_from_this(), pruned_partitions, {0}, local_context.get());
            /// Fillter by commited parts and parts written by same explicit transaction
            filterPartsInExplicitTransaction(data_parts, local_context);
        }
        else
        {
            data_parts = local_context->getCnchCatalog()->getServerDataPartsInPartitions(
                shared_from_this(), pruned_partitions, local_context->getCurrentTransactionID(), local_context.get());
        }
        // TEST_LOG(testlog, "get dataparts in partitions.");
        LOG_DEBUG(log, "Total number of parts get from bytekv: {}", data_parts.size());
        data_parts = cur_txn->getLatestCheckpointWithVersionChain(data_parts, local_context);

        ProfileEvents::increment(ProfileEvents::CatalogTime, watch.elapsedMilliseconds());
        ProfileEvents::increment(ProfileEvents::TotalPartitions, partition_list.size());
        ProfileEvents::increment(ProfileEvents::PrunedPartitions, pruned_partitions.size());
        ProfileEvents::increment(ProfileEvents::SelectedParts, data_parts.size());
    }

    // TEST_END(testlog, "Get pruned parts from Catalog Service");

    LOG_INFO(log, "Number of parts get from catalog: {}", data_parts.size());

    /// Prune parts
    filterPartsByPartition(data_parts, local_context, query_info, column_names_to_return);
    return data_parts;
}

MergeTreeDataPartsCNCHVector StorageCnchMergeTree::getStagedParts(const TxnTimestamp & /*ts*/, const NameSet * /*partitions*/) // NOLINT
{
    return {};
    /// FIXME:
    // auto catalog = getContext()->getCnchCatalog();
    // MergeTreeDataPartsCNCHVector staged_parts = catalog->getStagedParts(shared_from_this(), ts, partitions);
    // return CnchPartsHelper::calcVisibleParts(staged_parts, /*collect_on_chain*/false);
}

void StorageCnchMergeTree::getDeleteBitmapMetaForParts(
    const ServerDataPartsVector & parts, ContextPtr local_context, TxnTimestamp start_time)
{
    if (!local_context->getCnchCatalog())
        return;

    std::set<String> request_partitions;
    for (const auto & part : parts)
    {
        const auto & partition_id = part->part_model_wrapper->info->partition_id;
        request_partitions.insert(partition_id);
    }

    /// NOTE: Get all the bitmap meta needed only once from kv instead of getting many times for every partition to save time.
    Stopwatch watch;
    auto all_bitmaps = local_context->getCnchCatalog()->getDeleteBitmapsInPartitions(
        shared_from_this(), {request_partitions.begin(), request_partitions.end()}, start_time);
    ProfileEvents::increment(ProfileEvents::CatalogTime, watch.elapsedMilliseconds());
    LOG_DEBUG(
        log,
        "Get delete bitmap meta for total {} parts took: {} ms read {} number of bitmap meta",
        parts.size(),
        watch.elapsedMilliseconds(),
        all_bitmaps.size());

    DeleteBitmapMetaPtrVector bitmaps;
    /// FIXME:
    // CnchPartsHelper::calcVisibleDeleteBitmaps(all_bitmaps, bitmaps);

    /// Both the parts and bitmaps are sorted in (partitioin_id, min_block, max_block, commit_time) order
    auto bitmap_it = bitmaps.begin();
    for (const auto & part : parts)
    {
        /// search for the first bitmap
        while (bitmap_it != bitmaps.end() && !(*bitmap_it)->sameBlock(part->info()))
            bitmap_it++;

        if (bitmap_it == bitmaps.end())
            throw Exception("Delete bitmap metadata of " + part->name() + " is not found", ErrorCodes::LOGICAL_ERROR);

        /// add all visible bitmaps (from new to old) part
        bool found_base = false;
        auto list_it = part->delete_bitmap_metas.before_begin();
        for (auto bitmap_meta = *bitmap_it; bitmap_meta; bitmap_meta = bitmap_meta->tryGetPrevious())
        {
            list_it = part->delete_bitmap_metas.insert_after(list_it, bitmap_meta->getModel());
            if (bitmap_meta->getType() == DeleteBitmapMetaType::Base)
            {
                found_base = true;
                break;
            }
        }
        if (!found_base)
            throw Exception("Base delete bitmap of " + part->name() + " is not found", ErrorCodes::LOGICAL_ERROR);

        bitmap_it++;
    }
}

void StorageCnchMergeTree::collectResource(ContextPtr local_context, ServerDataPartsVector & parts, const String & local_table_name, const std::set<Int64> & required_bucket_numbers)
{
    auto cnch_resource = local_context->getCnchServerResource();
    auto create_table_query = getCreateQueryForCloudTable(getCreateTableSql(), local_table_name, local_context);

    cnch_resource->setWorkerGroup(local_context->getCurrentWorkerGroup());
    cnch_resource->addCreateQuery(local_context, shared_from_this(), create_table_query, local_table_name);

    // if (local_context.getSettingsRef().enable_virtual_part)
    //     setVirtualPartSize(local_context, parts, worker_group->getReadWorkers().size());

    cnch_resource->addDataParts(getStorageUUID(), parts, required_bucket_numbers);
}

UInt64 StorageCnchMergeTree::getTimeTravelRetention()
{
    return getSettings()->time_travel_retention_days;
}

void StorageCnchMergeTree::addCheckpoint(const Protos::Checkpoint & /*checkpoint*/)
{
    /// FIXME: add after it was supported
    // getContext()->getCnchCatalog()->addCheckpoint(shared_from_this(), checkpoint);
}

void StorageCnchMergeTree::removeCheckpoint(const Protos::Checkpoint & checkpoint)
{
    Protos::Checkpoint new_checkpoint(checkpoint);
    new_checkpoint.set_status(Protos::Checkpoint::Removing);
    /// FIXME: add after it was supported
    // getContext()->getCnchCatalog()->markCheckpoint(shared_from_this(), new_checkpoint);
}

void StorageCnchMergeTree::filterPartsInExplicitTransaction(ServerDataPartsVector & data_parts, ContextPtr local_context)
{
    Int64 primary_txn_id = local_context->getCurrentTransaction()->getPrimaryTransactionID().toUInt64();
    TxnTimestamp start_time = local_context->getCurrentTransaction()->getStartTime();

    std::map<TxnTimestamp, bool> success_secondary_txns;
    auto check_success_txn = [&success_secondary_txns, this](const TxnTimestamp & txn_id) -> bool {
        if (auto it = success_secondary_txns.find(txn_id); it != success_secondary_txns.end())
            return it->second;
        auto record = getContext()->getCnchCatalog()->getTransactionRecord(txn_id);
        success_secondary_txns.emplace(txn_id, record.status() == CnchTransactionStatus::Finished);
        return record.status() == CnchTransactionStatus::Finished;
    };
    std::erase_if(data_parts, [&](const auto & part) {
        return !(
            part->info().mutation == primary_txn_id && part->part_model_wrapper->part_model->has_secondary_txn_id()
            && check_success_txn(part->part_model_wrapper->part_model->secondary_txn_id()));
    });
    getCommittedServerDataParts(data_parts, start_time, &(*local_context->getCnchCatalog()));
}

void StorageCnchMergeTree::checkAlterIsPossible(const AlterCommands & /*commands*/, ContextPtr /*local_context*/) const
{
    //checkAlterInCnchServer(commands, local_context);
    //checkAlterSettings(commands);
}

void StorageCnchMergeTree::alter(const AlterCommands & commands, ContextPtr local_context, TableLockHolder & /*table_lock_holder*/)
{
    auto table_id = getStorageID();

    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    TransactionCnchPtr txn = local_context->getCurrentTransaction();
    auto action = txn->createAction<DDLAlterAction>(shared_from_this());
    auto alter_act = action->as<DDLAlterAction>();
    alter_act->setMutationCommands(commands.getMutationCommands(old_metadata, false, local_context));

    commands.apply(table_id, new_metadata, local_context);
    checkColumnsValidity(new_metadata.columns);

    {
        String create_table_query = getCreateTableSql();
        ParserCreateQuery p_create_query;
        ASTPtr ast = parseQuery(
            p_create_query,
            create_table_query,
            local_context->getSettingsRef().max_query_size,
            local_context->getSettingsRef().max_parser_depth);

        applyMetadataChangesToCreateQuery(ast, new_metadata);
        alter_act->setNewSchema(queryToString(ast));

        LOG_DEBUG(log, "new schema for alter query: {}", alter_act->getNewSchema());
        txn->appendAction(action);
    }

    //setProperties(new_metadata, false);
    //updateHDFSRootPaths(new_metadata.root_paths_ast);
    //setTTLExpressions(new_metadata.ttl_for_table_ast);
    //setCreateTableSql(alter_act->getNewSchema());

    txn->commitV1();
    LOG_TRACE(log, "Updated shared metadata in Catalog.");
}

void StorageCnchMergeTree::truncate(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /* metadata_snapshot */,
    ContextPtr /* local_context */,
    TableExclusiveLockHolder &)
{
    //if (forwardQueryToServerIfNeeded(local_context, getStorageUUID()))
    //    return;
}

StoragePolicyPtr StorageCnchMergeTree::getLocalStoragePolicy() const
{
    return local_store_volume;
}

const String & StorageCnchMergeTree::getLocalStorePath() const
{
    return relative_local_store_path;
}

Block StorageCnchMergeTree::getBlockWithVirtualPartitionColumns(const std::vector<std::shared_ptr<MergeTreePartition>> & partition_list) const
{
    DataTypePtr partition_value_type = getPartitionValueType();
    bool has_partition_value = typeid_cast<const DataTypeTuple *>(partition_value_type.get());
    Block block{
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_partition_id"),
        ColumnWithTypeAndName(partition_value_type->createColumn(), partition_value_type, "_partition_value")};


    MutableColumns columns = block.mutateColumns();

    auto & partition_id_column = columns[0];
    auto & partition_value_column = columns[1];

    for (const auto & partition : partition_list)
    {
        partition_id_column->insert(partition->getID(*this));
        Tuple tuple(partition->value.begin(), partition->value.end());
        if (has_partition_value)
            partition_value_column->insert(std::move(tuple));
    }
    block.setColumns(std::move(columns));
    if (!has_partition_value)
        block.erase(block.getPositionByName("_partition_value"));
    return block;
}

std::set<Int64> StorageCnchMergeTree::getRequiredBucketNumbers(const SelectQueryInfo & query_info, ContextPtr local_context) const
{
    std::set<Int64> bucket_numbers;
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->getWhere();
    const Settings & settings = local_context->getSettingsRef();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    // if number of bucket columns of this table > 1, skip optimisation
    if (settings.optimize_skip_unused_shards && where_expression && isBucketTable() && metadata_snapshot->getColumnsForClusterByKey().size() == 1)
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
        const auto & blocks = evaluateExpressionOverConstantCondition(where_expression, metadata_snapshot->getClusterByKey().expression, limit);

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
                prepareBucketColumn(block_copy, metadata_snapshot->getColumnsForClusterByKey(), metadata_snapshot->getSplitNumberFromClusterByKey(), metadata_snapshot->getWithRangeFromClusterByKey(), metadata_snapshot->getBucketNumberFromClusterByKey(), local_context);
                auto bucket_number = block_copy.getByPosition(block_copy.columns() - 1).column->getInt(0); // this block only contains one row
                bucket_numbers.insert(bucket_number);
            }
        }
    }
    return bucket_numbers;
}
} // end namespace DB
