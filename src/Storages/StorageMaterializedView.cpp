/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Storages/StorageMaterializedView.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTPartition.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/predicateExpressionsUtils.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/executeQuery.h>
#include <Optimizer/SelectQueryInfoHelper.h>

#include <Access/AccessFlags.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/MergeTree/MergeTreePartition.h>

#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Databases/DatabasesCommon.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <QueryPlan/SettingQuotaAndLimitsStep.h>
#include <QueryPlan/ExpressionStep.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
    extern const int CYCLIC_MATERIALIZED_VIEW_CHAIN;
}

/// Remove columns from target_header that does not exists in src_header
static void removeNonCommonColumns(const Block & src_header, Block & target_header)
{
    std::set<size_t> target_only_positions;
    for (const auto & column : target_header)
    {
        if (!src_header.has(column.name))
            target_only_positions.insert(target_header.getPositionByName(column.name));
    }
    target_header.erase(target_only_positions);
}

StorageMaterializedView::StorageMaterializedView(
    const StorageID & table_id_,
    ContextPtr local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage(table_id_), WithMutableContext(local_context->getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    /// If the destination table is not set, use inner table
    has_inner_table = query.to_table_id.empty();
    if (has_inner_table && !query.storage)
        throw Exception(
            "You must specify where to save results of a MaterializedView query: either ENGINE or an existing table in a TO clause",
            ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for MATERIALIZED VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

    auto select = SelectQueryDescription::getSelectQueryFromASTForMatView(query.select->clone(), local_context);
    storage_metadata.setSelectQuery(select);
    setInMemoryMetadata(storage_metadata);

    bool point_to_itself_by_uuid = has_inner_table && query.to_inner_uuid != UUIDHelpers::Nil
                                                   && query.to_inner_uuid == table_id_.uuid;
    bool point_to_itself_by_name = !has_inner_table && query.to_table_id.database_name == table_id_.database_name
                                                    && query.to_table_id.table_name == table_id_.table_name;
    if (point_to_itself_by_uuid || point_to_itself_by_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Materialized view {} cannot point to itself", table_id_.getFullTableName());

    if (!has_inner_table)
    {
        target_table_id = query.to_table_id;
    }
    else if (attach_)
    {
        /// If there is an ATTACH request, then the internal table must already be created.
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);
    }
    else
    {
        /// We will create a query to create an internal table.
        auto create_context = Context::createCopy(local_context);
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->database = getStorageID().database_name;
        manual_create_query->table = generateInnerTableName(getStorageID());
        manual_create_query->uuid = query.to_inner_uuid;

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, query.columns_list->columns->ptr());

        manual_create_query->set(manual_create_query->columns_list, new_columns_list);
        manual_create_query->set(manual_create_query->storage, query.storage->ptr());

        create_context->setCurrentTransaction(nullptr, false);
        executeQuery(serializeAST(*manual_create_query), create_context, true);

        target_table_id = DatabaseCatalog::instance().getTable({manual_create_query->database, manual_create_query->table}, getContext())->getStorageID();
    }

    if (!select.select_table_id.empty())
        DatabaseCatalog::instance().addDependency(select.select_table_id, getStorageID());
}

QueryProcessingStage::Enum StorageMaterializedView::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr &,
    SelectQueryInfo & query_info) const
{
    const auto & target_metadata = getTargetTable()->getInMemoryMetadataPtr();
    return getTargetTable()->getQueryProcessingStage(local_context, to_stage, getTargetTable()->getStorageSnapshot(target_metadata, local_context), query_info);
}

Pipe StorageMaterializedView::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context),
        BuildQueryPipelineSettings::fromContext(local_context));
}

void StorageMaterializedView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto target_storage_snapshot = storage->getStorageSnapshot(target_metadata_snapshot, local_context);

    if (query_info.order_optimizer)
        query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

    storage->read(query_plan, column_names, target_storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (query_plan.isInitialized())
    {
        auto mv_header = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, local_context, processed_stage);
        auto target_header = query_plan.getCurrentDataStream().header;

        /// No need to convert columns that does not exists in MV
        removeNonCommonColumns(mv_header, target_header);

        /// No need to convert columns that does not exists in the result header.
        ///
        /// Distributed storage may process query up to the specific stage, and
        /// so the result header may not include all the columns from the
        /// materialized view.
        removeNonCommonColumns(target_header, mv_header);

        if (!blocksHaveEqualStructure(mv_header, target_header))
        {
            auto converting_actions = ActionsDAG::makeConvertingActions(target_header.getColumnsWithTypeAndName(),
                                                                        mv_header.getColumnsWithTypeAndName(),
                                                                        ActionsDAG::MatchColumnsMode::Name);
            auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), converting_actions);
            converting_step->setStepDescription("Convert target table structure to MaterializedView structure");
            query_plan.addStep(std::move(converting_step));
        }

        StreamLocalLimits limits;
        SizeLimits leaf_limits;

        /// Add table lock for destination table.
        auto adding_limits_and_quota = std::make_unique<SettingQuotaAndLimitsStep>(
                query_plan.getCurrentDataStream(),
                storage,
                std::move(lock),
                limits,
                leaf_limits,
                nullptr,
                nullptr);

        adding_limits_and_quota->setStepDescription("Lock destination table for MaterializedView");
        query_plan.addStep(std::move(adding_limits_and_quota));
    }
}

BlockOutputStreamPtr StorageMaterializedView::write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr local_context)
{
    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto view_metatdata_snapshot = getInMemoryMetadataPtr();
    auto stream = storage->write(query, target_metadata_snapshot, local_context);

    stream->addTableLock(lock);

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    stream = std::make_shared<AddingDefaultBlockOutputStream>(
        stream,
        view_metatdata_snapshot->getSampleBlock(/*include_func_columns*/ true),
        target_metadata_snapshot->getColumns(),
        local_context);

    return stream;
}

void StorageMaterializedView::drop()
{
    auto table_id = getStorageID();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().removeDependency(select_query.select_table_id, table_id);

    dropInnerTableIfAny(true, getContext());
}

void StorageMaterializedView::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    if (has_inner_table && tryGetTargetTable())
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);
}

void StorageMaterializedView::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    if (has_inner_table)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Truncate, getContext(), local_context, target_table_id, true);
}

void StorageMaterializedView::checkStatementCanBeForwarded() const
{
    if (!has_inner_table)
        throw Exception(
            "MATERIALIZED VIEW targets existing table " + target_table_id.getFullTableName() + ". "
            + "Execute the statement directly on it.", ErrorCodes::INCORRECT_QUERY);
}

bool StorageMaterializedView::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    auto storage_ptr = getTargetTable();
    auto metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
    return getTargetTable()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, local_context);
}

void StorageMaterializedView::alter(
    const AlterCommands & params,
    ContextPtr local_context,
    TableLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);

    /// start modify query
    if (local_context->getSettingsRef().allow_experimental_alter_materialized_view_structure)
    {
        const auto & new_select = new_metadata.select;
        const auto & old_select = old_metadata.getSelectQuery();

        DatabaseCatalog::instance().updateDependency(old_select.select_table_id, table_id, new_select.select_table_id, table_id);

        new_metadata.setSelectQuery(new_select);
    }
    /// end modify query

    DatabaseCatalog::instance().getDatabase(table_id.database_name, local_context)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    const auto & settings = local_context->getSettingsRef();
    if (settings.allow_experimental_alter_materialized_view_structure)
    {
        for (const auto & command : commands)
        {
            if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_QUERY)
                throw Exception(
                    "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                    ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (!command.isCommentAlter())
                throw Exception(
                    "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                    ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}

void StorageMaterializedView::checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const
{
    checkStatementCanBeForwarded();
    getTargetTable()->checkMutationIsPossible(commands, settings);
}

Pipe StorageMaterializedView::alterPartition(
    const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    return getTargetTable()->alterPartition(metadata_snapshot, commands, local_context);
}

void StorageMaterializedView::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const
{
    checkStatementCanBeForwarded();
    getTargetTable()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings);
}

void StorageMaterializedView::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    checkStatementCanBeForwarded();
    getTargetTable()->mutate(commands, local_context);
}

void StorageMaterializedView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    bool from_atomic_to_atomic_database = old_table_id.hasUUID() && new_table_id.hasUUID();

    if (!from_atomic_to_atomic_database && has_inner_table && tryGetTargetTable())
    {
        auto new_target_table_name = generateInnerTableName(new_table_id);
        auto rename = std::make_shared<ASTRenameQuery>();

        ASTRenameQuery::Table from;
        assert(target_table_id.database_name == old_table_id.database_name);
        from.database = target_table_id.database_name;
        from.table = target_table_id.table_name;

        ASTRenameQuery::Table to;
        to.database = new_table_id.database_name;
        to.table = new_target_table_name;

        ASTRenameQuery::Element elem;
        elem.from = from;
        elem.to = to;
        rename->elements.emplace_back(elem);

        InterpreterRenameQuery(rename, getContext()).execute();
        target_table_id.database_name = new_table_id.database_name;
        target_table_id.table_name = new_target_table_name;
    }

    IStorage::renameInMemory(new_table_id);
    if (from_atomic_to_atomic_database && has_inner_table)
    {
        assert(target_table_id.database_name == old_table_id.database_name);
        target_table_id.database_name = new_table_id.database_name;
    }
    const auto & select_query = metadata_snapshot->getSelectQuery();
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    DatabaseCatalog::instance().updateDependency(select_query.select_table_id, old_table_id, select_query.select_table_id, getStorageID());
}

void StorageMaterializedView::shutdown()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    /// Make sure the dependency is removed after DETACH TABLE
    if (!select_query.select_table_id.empty())
        DatabaseCatalog::instance().removeDependency(select_query.select_table_id, getStorageID());
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    checkStackSize();
    return DatabaseCatalog::instance().getTable(target_table_id, getContext());
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    checkStackSize();
    return DatabaseCatalog::instance().tryGetTable(target_table_id, getContext());
}

Strings StorageMaterializedView::getDataPaths() const
{
    if (auto table = tryGetTargetTable())
        return table->getDataPaths();
    return {};
}

ActionLock StorageMaterializedView::getActionLock(StorageActionBlockType type)
{
    if (has_inner_table)
    {
        if (auto target_table = tryGetTargetTable())
            return target_table->getActionLock(type);
    }
    return ActionLock{};
}

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            args.table_id, args.getLocalContext(), args.query,
            args.columns, args.attach);
    });
}


static BlockInputStreamPtr generateInput(ASTPtr query, const Block & result_header, const String & column_name, const String & column_value, ContextPtr local_context)
{
    // construct partition or part predicate
    ASTPtr equals_identifier = std::make_shared<ASTIdentifier>(column_name);
    ASTPtr equals_literal = std::make_shared<ASTLiteral>(column_value);
    ASTPtr equals_function = makeASTFunction("equals", equals_identifier, equals_literal);
    auto & select_query = query->as<ASTSelectQuery &>();
    if (select_query.where())
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, composeAnd(ASTs{select_query.where(), equals_function}));
    else
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(equals_function));
    InterpreterSelectQuery select(query, local_context, SelectQueryOptions());
    BlockInputStreamPtr in = std::make_shared<MaterializingBlockInputStream>(select.execute().getInputStream());
    in = std::make_shared<SquashingBlockInputStream>(
        in, local_context->getSettingsRef().min_insert_block_size_rows, local_context->getSettingsRef().min_insert_block_size_bytes);
    in = std::make_shared<ConvertingBlockInputStream>(in, result_header, ConvertingBlockInputStream::MatchColumnsMode::Name);
    return in;
}


bool StorageMaterializedView::isRefreshable(bool cascading) const
{
    /// Creates a dictionary `aliases`: alias -> ASTPtr
    Aliases aliases;
    DebugASTLog<false> ast_log;
    auto query = getInnerQuery();
    QueryAliasesVisitor::Data query_aliases_data{aliases};
    QueryAliasesVisitor(query_aliases_data, ast_log.stream()).visit(query);

    auto target_table = getTargetTable();
    auto target_partition_key = target_table->getInMemoryMetadataPtr()->getPartitionKey().expression_list_ast;
    if (!target_partition_key)
        throw Exception("View's target table had not specified partition key.", ErrorCodes::LOGICAL_ERROR);

    /// Normalize the target partition expression, replace aliases
    const auto & settings = getContext()->getSettingsRef();
    QueryNormalizer::Data normalizer_data(aliases, {}, false, settings, false);
    QueryNormalizer(normalizer_data).visit(target_partition_key);

    auto select_table = DatabaseCatalog::instance().getTable(getInMemoryMetadataPtr()->select.select_table_id, getContext());
    auto select_partition_key = select_table->getInMemoryMetadataPtr()->getPartitionKey().expression_list_ast;
    if (!select_partition_key)
        throw Exception("Base table had not specified partition key.", ErrorCodes::LOGICAL_ERROR);

    auto target_partition_expr_list = typeid_cast<ASTExpressionList &>(*target_partition_key);
    auto select_partition_expr_list = typeid_cast<ASTExpressionList &>(*select_partition_key);

    if (target_partition_expr_list.children.empty())
        throw Exception("View's target table had not specified any partition column.", ErrorCodes::LOGICAL_ERROR);

    if (select_partition_expr_list.children.empty())
        throw Exception("Base table had not specified any partition column.", ErrorCodes::LOGICAL_ERROR);

    if (target_partition_expr_list.children.size() != select_partition_expr_list.children.size())
        return false;

    for (size_t i = 0; i < target_partition_expr_list.children.size(); ++i)
    {
        if (target_partition_expr_list.children[i]->getColumnName() != select_partition_expr_list.children[i]->getColumnName())
            return false;
    }

    /// if cascading, check dependencies of this view
    if (cascading)
    {
        Dependencies dependencies = DatabaseCatalog::instance().getDependencies(getStorageID());
        for (const auto & database_table : dependencies)
        {
            auto dependent_table = DatabaseCatalog::instance().getTable(database_table, getContext());
            auto & materialized_view = dynamic_cast<StorageMaterializedView &>(*dependent_table);
            if (!materialized_view.isRefreshable(cascading))
                return false;
        }
    }

    return true;
}

/// TODO: Async mode is useless when atomic parameter refreshing ensure only one refresh task to execute.
///       Temporarily only support sync refresh mode later provide parallel solution.
void StorageMaterializedView::refresh(const ASTPtr & partition,  ContextMutablePtr local_context, bool /*async*/)
{
    if (local_context->getServerType() == ServerType::cnch_server)
    {
        if (!partition)
           throw Exception("Refresh materialized view need with partition parameter example: refresh materialized view mv.events_aggregate_view partition '2022-06-14'", ErrorCodes::LOGICAL_ERROR);
        StoragePtr select_table = DatabaseCatalog::instance().getTable(getInMemoryMetadataPtr()->select.select_table_id, local_context);
        StoragePtr target_table = DatabaseCatalog::instance().getTable(target_table_id, local_context);
        if (target_table && select_table)
        {
            auto * cnch_target_table = dynamic_cast<StorageCnchMergeTree*>(target_table.get());
            auto * cnch_select_table = dynamic_cast<StorageCnchMergeTree*>(select_table.get());
            if (cnch_target_table && cnch_select_table)
            {
                refreshCnchImpl(partition, local_context);
                return;
            }
        }
    }

    if (!partition)
    {
        if (!getTargetTable()->getInMemoryMetadataPtr()->getPartitionKeyAST())
        {
            String refreshing_partition_id = "all";

            /// Truncate target table
            auto target_table = getTargetTable();
            auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
            {
                 auto table_lock = target_table->lockExclusively(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
                 ASTPtr invalid_ast;
                 target_table->truncate(invalid_ast, metadata_snapshot, local_context, table_lock);
            }

            /// Refresh all table

            BlockOutputStreamPtr out;
            auto view_context = Context::createCopy(local_context);
            bool cascading = local_context->getSettingsRef().cascading_refresh_materialized_view;
            if (cascading)
                out = std::make_shared<PushingToViewsBlockOutputStream>(target_table, metadata_snapshot, view_context, ASTPtr());
            else
                out = write(ASTPtr(), metadata_snapshot, local_context);
            InterpreterSelectQuery select(getInnerQuery(), local_context, SelectQueryOptions());
            BlockInputStreamPtr in = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
            in = std::make_shared<SquashingBlockInputStream>(
                in, local_context->getSettingsRef().min_insert_block_size_rows, local_context->getSettingsRef().min_insert_block_size_bytes);
            in = std::make_shared<ConvertingBlockInputStream>(in, out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Name);
            out->writePrefix();
            copyData(*in, *out);
            out->writeSuffix();
        }
        else
        {
            MergeTreeData::DataPartsVector parts;
            auto select_table = DatabaseCatalog::instance().getTable(getInMemoryMetadataPtr()->select.select_table_id, local_context);
            if (auto * merge_tree = dynamic_cast<MergeTreeData *>(select_table.get()))
            {
                parts = merge_tree->getDataPartsVector();
                FormatSettings format_settings;
                for (const auto & part : parts)
                {
                    WriteBufferFromOwnString buf;
                    part->partition.serializeText(*merge_tree, buf, format_settings);
                    const String & part_name = buf.str();
                    LOG_DEBUG(&Poco::Logger::get("refresh"), "all partition name-{}", part_name);
                    const char * begin = part_name.data();
                    const char * end = part_name.data() + part_name.size();
                    size_t max_query_size = local_context->getSettingsRef().max_query_size;
                    Tokens tokens(begin, end);
                    IParser::Pos token_iterator(tokens, max_query_size);
                    ASTPtr part_ast;
                    Expected expected;
                    bool parse_res = ParserPartition(ParserSettings::valueOf(local_context->getSettingsRef()))
                                         .parse(token_iterator, part_ast, expected);
                    if (!parse_res)
                        continue;
                    refreshImpl(part_ast, local_context);
                }
            }
        }
    }
    else
        refreshImpl(partition, local_context);
}

bool StorageMaterializedView::checkPartitionExpr(StoragePtr target_table, ASTPtr partition_expr, ContextMutablePtr local_context)
{
    auto * cnch_target_table = dynamic_cast<StorageCnchMergeTree*>(target_table.get());
    if (!cnch_target_table)
        return false;

    StorageMetadataPtr meta_ptr = cnch_target_table->getInMemoryMetadataPtr();
    if (!meta_ptr.get())
        return false;

    const auto partition_key = MergeTreePartition::adjustPartitionKey(meta_ptr, local_context);

    IdentifierNameSet id_set;
    partition_expr->collectIdentifierNames(id_set);

    for (const auto & name : id_set)
    {
        LOG_TRACE(&Poco::Logger::get("checkPartitionExpr"), "partition_expr name: {}", name);
    }

    IdentifierNameSet id_set_target;
    partition_key.expression_list_ast->collectIdentifierNames(id_set_target);

    for (const auto & name : id_set_target)
    {
        LOG_TRACE(&Poco::Logger::get("checkPartitionExpr"), "target table partition key name: {}", name);
        if (id_set.count(name))
            return true;
    }

    return false;
}

//  refresh mv where toDate(ts) > '2023-10-01' and toDate(ts) < '2024-10-01'
void StorageMaterializedView::refreshWhere(ASTPtr partition_expr, ContextMutablePtr local_context, bool /*async*/)
{
    auto target_table = getTargetTable();
    auto * cnch_target_table = dynamic_cast<StorageCnchMergeTree*>(target_table.get());
    if (!cnch_target_table)
        throw Exception("Materialized view target table is not CnchMergeTree", ErrorCodes::LOGICAL_ERROR);
    
    const auto & mv_select_query = getInMemoryMetadataPtr()->getSelectQuery();
    auto select_table = DatabaseCatalog::instance().getTable(mv_select_query.select_table_id, local_context);
    if (!select_table)
        throw Exception("Materialized view select table " + mv_select_query.select_table_id.getFullTableName() +
                         " is not exist.", ErrorCodes::LOGICAL_ERROR);

    auto * cnch_select_table = dynamic_cast<StorageCnchMergeTree*>(select_table.get());
    if (!cnch_select_table)
        throw Exception("Materialized view select table is not CnchMergeTree", ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(&Poco::Logger::get("refreshWhere"), "partition_expr: {}", serializeAST(*partition_expr));
    if (!checkPartitionExpr(target_table, partition_expr, local_context))
        throw Exception("Refresh Materialized view without partition key", ErrorCodes::LOGICAL_ERROR);

    auto select_query = std::make_shared<ASTSelectQuery>();
    auto select_expr = std::make_shared<ASTExpressionList>();
    Names column_names_to_return;
    select_expr->children.push_back(std::make_shared<ASTAsterisk>());
    column_names_to_return.push_back("*");

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr));
    select_query->replaceDatabaseAndTable(mv_select_query.select_table_id);
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(partition_expr));

    SelectQueryInfo query_info = buildSelectQueryInfoForQuery(select_query, local_context);

    auto required_partitions = cnch_select_table->getPrunedPartitions(query_info, column_names_to_return, local_context).partitions;

    if (local_context->getSettingsRef().max_threads_to_refresh_by_partition > 1)
    {
        ExceptionHandler exception_handler;
        ThreadPool thread_pool(local_context->getSettingsRef().max_threads_to_refresh_by_partition);
        for (const auto & part_string: required_partitions)
        {
            LOG_DEBUG(&Poco::Logger::get("refreshWhere"), "thread_pool begin: {}", part_string);
            thread_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                    [this, local_context, part_string]()
                    {
                        auto refresh_context = Context::createCopy(local_context);
                        LOG_DEBUG(&Poco::Logger::get("refreshWhere"), "refresh partition begin: {}", part_string);
                        auto partition = std::make_shared<ASTPartition>();
                        partition->id = part_string;
                        refreshCnchImpl(partition, refresh_context);
                    },
                    exception_handler));
        }
        thread_pool.wait();
        exception_handler.throwIfException();
        LOG_DEBUG(&Poco::Logger::get("refreshWhere"), "refresh partition end.");
    }
    else
    {
        for (const auto & part_string: required_partitions)
        {
            LOG_DEBUG(&Poco::Logger::get("refreshWhere"), "refresh partition begin: {}", part_string);

            auto partition = std::make_shared<ASTPartition>();
            partition->id = part_string;
            refreshCnchImpl(partition, local_context);
        }
    }
}

void StorageMaterializedView::refreshCnchImpl(const ASTPtr & partition, ContextMutablePtr local_context)
{
    /** Compose the operation into a sequence of command within an interactive transaction session
        - BEGIN
        - ALTER TABLE <target> DROP PARTITION <target partition>
        - INSERT INTO <target> SELECT ...
        - COMMIT
    */

    /// BEGIN
    auto & txn_coordinator = local_context->getCnchTransactionCoordinator();
    auto explicit_txn = txn_coordinator.createTransaction(CreateTransactionOption().setType(CnchTransactionType::Explicit));

    const_cast<Context &>(*local_context).setCurrentTransaction(explicit_txn, true);

    auto create_command_context = [local_context, partition]() {
        auto command_context = Context::createCopy(local_context);
        command_context->setCurrentTransaction(nullptr, false);
        command_context->setCurrentVW(nullptr);
        command_context->setCurrentWorkerGroup(nullptr);
        command_context->setSessionContext(local_context);
        command_context->setQueryContext(command_context);
        String query_id0 = fmt::format("{}_{}", command_context->getCurrentQueryId(), serializeAST(*partition));
        command_context->setCurrentQueryId(query_id0);
        return command_context;
    };

    /// DROP
    auto drop_context = create_command_context();
    bool cascading = drop_context->getSettingsRef().cascading_refresh_materialized_view;
    PartitionCommand drop_command;
    drop_command.type = PartitionCommand::DROP_PARTITION;
    drop_command.partition = partition;
    drop_command.detach = false;
    drop_command.cascading = cascading;

    PartitionCommands drop_commands;
    drop_commands.emplace_back(std::move(drop_command));
    auto target_table = DatabaseCatalog::instance().getTable(target_table_id, local_context);
    std::stringstream alter_query_ss;
    alter_query_ss << "ALTER TABLE " << target_table_id.getFullTableName()
                   << (cascading ? " CASCADING " : " ") << "DROP " << "PARTITION " << serializeAST(*partition, true);

    LOG_DEBUG(&Poco::Logger::get("refreshCnchImpl"), "Drop partition query: {}", alter_query_ss.str());
    BlockIO drop_io;
    try
    {
        drop_io = executeQuery(alter_query_ss.str(), drop_context, true);
        if (drop_io.pipeline.initialized())
        {
            auto & pipeline = drop_io.pipeline;
            PullingAsyncPipelineExecutor executor(pipeline);
            Block block;
            while (executor.pull(block)) {}
        }
        else if (drop_io.in)
        {
            AsynchronousBlockInputStream async_in(drop_io.in);
            async_in.readPrefix();
            while (true)
            {
                const auto block = async_in.read();
                if (!block)
                    break;
            }
            async_in.readSuffix();
        }
        drop_io.onFinish();
    }
    catch (...)
    {
        drop_io.onException();
        throw;
    }

    // INSERT
    auto insert_context = create_command_context();
    auto select_table = DatabaseCatalog::instance().getTable(getInMemoryMetadataPtr()->select.select_table_id, local_context);
    if (!select_table)
        throw Exception("Materialized view select table " + getInMemoryMetadataPtr()->select.select_table_id.getFullTableName() +
                         " is not exist.", ErrorCodes::LOGICAL_ERROR);
    auto * merge_tree = dynamic_cast<MergeTreeMetaBase *>(select_table.get());
    if (!merge_tree)
        throw Exception("Select table " + select_table->getStorageID().getFullTableName() + " is not merge tree engine.", ErrorCodes::LOGICAL_ERROR);
    String partition_id = merge_tree->getPartitionIDFromQuery(partition, local_context);
    ASTPtr equals_identifier = std::make_shared<ASTIdentifier>("_partition_id");
    ASTPtr equals_literal = std::make_shared<ASTLiteral>(partition_id);
    ASTPtr equals_function = makeASTFunction("equals", equals_identifier, equals_literal);

    auto query = getInnerQuery();
    auto & select_query = query->as<ASTSelectQuery &>();
    if (select_query.where())
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", select_query.where(), std::move(equals_function)));
    else
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(equals_function));

    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->table_id = target_table_id;
    insert_query->select = select_query.clone();
    insert_query->children.push_back(insert_query->select);

    LOG_DEBUG(&Poco::Logger::get("refreshCnchImpl"), "Refresh insert query: {}",  queryToString(insert_query));
    BlockIO insert_io;
    try
    {
        insert_io = executeQuery(serializeAST(*insert_query), insert_context, true);
        if (insert_io.pipeline.initialized())
        {
            auto & pipeline = insert_io.pipeline;
            PullingAsyncPipelineExecutor executor(pipeline);
            Block block;
            while (executor.pull(block)) {}
        }
        else if (insert_io.in)
        {
            AsynchronousBlockInputStream async_in(insert_io.in);
            async_in.readPrefix();
            while (true)
            {
                const auto block = async_in.read();
                if (!block)
                    break;
            }
            async_in.readSuffix();
        }
        insert_io.onFinish();
    }
    catch (...)
    {
        insert_io.onException();
        throw;
    }

    /// COMMIT
    txn_coordinator.commitV2(explicit_txn);
}

void StorageMaterializedView::refreshImpl(const ASTPtr & partition, ContextPtr local_context)
{
    bool cascading = local_context->getSettingsRef().cascading_refresh_materialized_view;
    if (!isRefreshable(cascading))
        throw Exception("Materialized view" + backQuoteIfNeed(getStorageID().getDatabaseName()) + "." + backQuoteIfNeed(getStorageID().getTableName()) +
                            " is not refreshable.", ErrorCodes::LOGICAL_ERROR);

    auto select_table = DatabaseCatalog::instance().getTable(getInMemoryMetadataPtr()->select.select_table_id, local_context);

    auto * merge_tree = dynamic_cast<MergeTreeData *>(select_table.get());
    if (!merge_tree)
        throw Exception("Select table " + backQuoteIfNeed(select_table->getStorageID().getDatabaseName()) + "." + backQuoteIfNeed(select_table->getStorageID().getTableName()) +
                            " is not merge tree engine.", ErrorCodes::LOGICAL_ERROR);
    String refreshing_partition_id = merge_tree->getPartitionIDFromQuery(partition, local_context);

    MergeTreeData::DataPartsVector parts;
    parts = merge_tree->getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, refreshing_partition_id);
    size_t rows = 0;
    for (auto & part : parts)
        rows += part->rows_count;
    if (rows == 0)
        throw Exception("There is no data of this partition in the base table. So no data can be used to refresh the view.", ErrorCodes::LOGICAL_ERROR);

    /// First drop the old partition
    PartitionCommand drop_command;
    drop_command.type = PartitionCommand::DROP_PARTITION;
    drop_command.partition = partition;
    drop_command.detach = false;
    drop_command.cascading = cascading;

    PartitionCommands drop_commands;
    drop_commands.emplace_back(std::move(drop_command));

    auto target_table = getTargetTable();
    auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
    // construct the alter query string
    std::stringstream alter_query_ss;
    alter_query_ss << "ALTER TABLE " << backQuoteIfNeed(target_table->getStorageID().getDatabaseName()) << "." << backQuoteIfNeed(target_table->getStorageID().getTableName())
                   << (cascading ? " CASCADING " : " ") << "DROP " << "PARTITION " << serializeAST(*partition, true);

    const String alter_query_str = alter_query_ss.str();

    LOG_DEBUG(&Poco::Logger::get("refreshImpl"), "drop partition command: {}", alter_query_str);

    const char * begin = alter_query_str.data();
    const char * end = alter_query_str.data() + alter_query_str.size();

    ParserQuery parser(end, ParserSettings::CLICKHOUSE);
    auto ast = parseQuery(parser, begin, end, "", 0, 0);
    target_table->alterPartition(metadata_snapshot, drop_commands, local_context);

    /// Then write new data
    // We need special context for materialized view insertions
    bool disable_deduplication_for_children = select_table->supportsDeduplication();
    auto view_context = Context::createCopy(local_context);
    if (disable_deduplication_for_children)
        view_context->setSetting("insert_deduplicate", false);

    BlockOutputStreamPtr out;
    if (cascading)
        out = std::make_shared<PushingToViewsBlockOutputStream>(target_table, metadata_snapshot, view_context, ASTPtr());
    else
        out = write(ASTPtr(), metadata_snapshot, local_context);

    if (rows <= local_context->getSettingsRef().max_rows_to_refresh_by_partition)
    {
        auto in = generateInput(getInnerQuery(), out->getHeader(), "_partition_id", refreshing_partition_id, local_context);

        out->writePrefix();
        copyData(*in, *out);
        out->writeSuffix();
        LOG_DEBUG(&Poco::Logger::get("refreshImpl"), "write view table from original table partition-{}, with rows-{}" , refreshing_partition_id,  std::to_string(rows));
    }
    else
    {
        out->writePrefix();

        for (auto & part : parts)
        {
            auto in = generateInput(getInnerQuery(), out->getHeader(), "_part", part->name, local_context);
            copyData(*in, *out);
            LOG_DEBUG(&Poco::Logger::get("refreshImpl"), "write view table from original table partition-{} with max_rows_to_refresh_by_partition-{} < {} rows" ,
                      part->name, std::to_string(local_context->getSettingsRef().max_rows_to_refresh_by_partition), std::to_string(rows));
        }

        out->writeSuffix();
    }
}

}


