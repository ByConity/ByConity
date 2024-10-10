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

#include <filesystem>
#include <Interpreters/InterpreterInsertQuery.h>
#include <CloudServices/CnchServerResource.h>
#include <CloudServices/CnchWorkerResource.h>
#include <Access/AccessFlags.h>
#include <CloudServices/CnchServerResource.h>
#include <Columns/ColumnNullable.h>
#include <Core/Types.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <DataStreams/CheckConstraintsFilterBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/LockHoldBlockInputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/TransactionWrapperBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3Common.h>
#include <IO/SnappyReadBuffer.h>
#include <Interpreters/Context.h>
#include <IO//RAReadBufferFromS3.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/processColumnTransformers.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserQuery.h>
#include <Processors/Sources/SinkToOutputStream.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ProcessorToOutputStream.h>
#include <Processors/Transforms/getSourceFromFromASTInsertQuery.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMaterializedView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/FilePathMatcher.h>
#include <Common/HDFSFilePathMatcher.h>
#include <Common/LocalFilePathMatcher.h>
#include <Common/S3FilePathMatcher.h>
#include <Common/checkStackSize.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
}

InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, ContextPtr context_, bool allow_materialized_, bool no_squash_, bool no_destination_, AccessType access_type_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
    , access_type(access_type_)
{
    checkStackSize();
}

static NameSet genViewDependencyCreateQueries(StoragePtr storage, ContextPtr local_context)
{
    NameSet create_view_sqls;
    std::set<StorageID> view_dependencies;
    auto start_time = local_context->getTimestamp();

    auto catalog_client = local_context->getCnchCatalog();
    if (!catalog_client)
        throw Exception("get catalog client failed", ErrorCodes::LOGICAL_ERROR);

    auto all_views_from_catalog = catalog_client->getAllViewsOn(*local_context, storage, start_time);
    if (all_views_from_catalog.empty())
        return create_view_sqls;

    for (auto & view : all_views_from_catalog)
        view_dependencies.emplace(view->getStorageID());

    for (const auto & dependence : view_dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(dependence, local_context);
        if (!table)
        {
            LOG_WARNING(getLogger("InterpreterInsertQuery::genViewDependencyCreateQueries"), "table {} not found", dependence.getNameForLogs());
            continue;
        }

        if (auto * mv = dynamic_cast<StorageMaterializedView*>(table.get()))
        {
            if (mv->async())
               continue;
            auto target_table = DatabaseCatalog::instance().tryGetTable(mv->getTargetTableId(), local_context);
            if (!target_table)
            {
                LOG_WARNING(getLogger("InterpreterInsertQuery::genViewDependencyCreateQueries"), "target table for {} not exist", mv->getStorageID().getNameForLogs());
                continue;
            }

            /// target table should be CnchMergeTree
            auto * target_cnch_merge = dynamic_cast<StorageCnchMergeTree*>(target_table.get());
            if (!target_cnch_merge)
            {
                LOG_WARNING(getLogger("InterpreterInsertQuery::genViewDependencyCreateQueries"), "table type not matched for {}, CnchMergeTree is expected",
                            target_table->getStorageID().getNameForLogs());
                continue;
            }
            auto create_target_query = target_table->getCreateTableSql();
            auto create_local_target_query = target_cnch_merge->getCreateQueryForCloudTable(create_target_query, target_cnch_merge->getTableName());
            create_view_sqls.insert(create_local_target_query);
            create_view_sqls.insert(mv->getCreateTableSql());
        }
    }

    return create_view_sqls;
}


StoragePtr InterpreterInsertQuery::getTable(ASTInsertQuery & query)
{
    if (query.table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(query.table_function, getContext());
        return table_function_ptr->execute(query.table_function, getContext(), table_function_ptr->getName());
    }

    if (getContext()->getServerType() == ServerType::cnch_worker && !query.select)
    {
        query.table_id = getContext()->resolveStorageID(query.table_id);
        auto storage = DatabaseCatalog::instance().tryGetTable(query.table_id, getContext());
        if (auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
        {
            auto create_query = cnch_table->getCreateQueryForCloudTable(
                cnch_table->getCreateTableSql(),
                query.table_id.table_name,
                nullptr,
                false,
                std::nullopt,
                Strings{},
                query.table_id.database_name);
            LOG_TRACE(getLogger(__PRETTY_FUNCTION__), "Worker side create query: {}", create_query);

            NameSet view_create_sqls = genViewDependencyCreateQueries(storage, getContext());
            if (!view_create_sqls.empty())
            {
                ContextMutablePtr mutable_context = const_pointer_cast<Context>(getContext());
                if (!mutable_context->tryGetCnchWorkerResource())
                    mutable_context->initCnchWorkerResource();
                view_create_sqls.insert(create_query);
                for (const auto & create_sql : view_create_sqls)
                    mutable_context->getCnchWorkerResource()->executeCreateQuery(mutable_context, create_sql);
                if (auto worker_txn = dynamic_pointer_cast<CnchWorkerTransaction>(mutable_context->getCurrentTransaction()); worker_txn)
                {
                    if (view_create_sqls.size() > 1)
                    {
                        worker_txn->enableExplicitCommit();
                        worker_txn->setExplicitCommitStorageID(storage->getStorageID());
                    }

                    mutable_context->setCurrentTransaction(worker_txn);
                }
                return mutable_context->getCnchWorkerResource()->getTable(query.table_id);
            }
            else
            {
                query.table_id = getContext()->resolveStorageID(query.table_id);
                return DatabaseCatalog::instance().getTable(query.table_id, getContext());
            }
        }
        else
            return storage;
    }
    else
    {
        query.table_id = getContext()->resolveStorageID(query.table_id);
        auto storage = DatabaseCatalog::instance().tryGetTable(query.table_id, getContext());
        if (storage)
            return storage;

        storage = tryGetTableInWorkerResource(query.table_id);
        if (storage)
            return storage;
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot find table {} in server", query.table_id.getNameForLogs());
    }
}

StoragePtr InterpreterInsertQuery::tryGetTableInWorkerResource(const StorageID & table_id)
{
    /// in some case of bitengine, server will write data into dict table and the targe table
    /// can only be found in worker_resource
    auto try_get_table_from_worker_resource = [&table_id](const auto & context) -> StoragePtr {
        if (auto worker_resource = context->tryGetCnchWorkerResource(); worker_resource)
            return worker_resource->getTable(table_id);
        else
            return nullptr;
    };
    auto storage = try_get_table_from_worker_resource(getContext());
    if (storage)
        return storage;
    else if (auto query_context = getContext()->getQueryContext())
    {
        storage = try_get_table_from_worker_resource(query_context);
        if (storage)
            return storage;
    }
    return nullptr;
}

Block InterpreterInsertQuery::getSampleBlock(
    const ASTInsertQuery & query, const StoragePtr & table, const StorageMetadataPtr & metadata_snapshot) const
{
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (no_destination)
            return metadata_snapshot->getSampleBlockWithVirtuals(table->getVirtuals());
        else
            return metadata_snapshot->getSampleBlockNonMaterialized();
    }

    Block table_sample_non_materialized;
    if (no_destination)
        table_sample_non_materialized = metadata_snapshot->getSampleBlockWithVirtuals(table->getVirtuals());
    else
        table_sample_non_materialized = metadata_snapshot->getSampleBlockNonMaterialized(/*include_func_columns*/ true);

    Block table_sample = metadata_snapshot->getSampleBlock(/*include_func_columns*/ true);

    const auto columns_ast = processColumnTransformers(getContext()->getCurrentDatabase(), table, metadata_snapshot, query.columns);

    /// Form the block based on the column names from the query
    Block res;
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();

        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception(
                "No such column " + current_name + " in table " + table->getStorageID().getNameForLogs(),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!allow_materialized && !table_sample_non_materialized.has(current_name))
            throw Exception("Cannot insert column " + current_name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        if (res.has(current_name))
            throw Exception("Column " + current_name + " specified more than once", ErrorCodes::DUPLICATE_COLUMN);

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


/** A query that just reads all data without any complex computations or filetering.
  * If we just pipe the result to INSERT, we don't have to use too many threads for read.
  */
static bool isTrivialSelect(const ASTPtr & select)
{
    if (auto * select_query = select->as<ASTSelectQuery>())
    {
        const auto & tables = select_query->tables();

        if (!tables)
            return false;

        const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();

        if (tables_in_select_query.children.size() != 1)
            return false;

        const auto & child = tables_in_select_query.children.front();
        const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
        const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

        if (table_expr.subquery)
            return false;

        /// Note: how to write it in more generic way?
        return (
            !select_query->distinct && !select_query->limit_with_ties && !select_query->prewhere() && !select_query->where()
            && !select_query->groupBy() && !select_query->having() && !select_query->orderBy() && !select_query->limitBy());
    }
    /// This query is ASTSelectWithUnionQuery subquery
    return false;
};


BlockIO InterpreterInsertQuery::execute()
{
    const Settings & settings = getContext()->getSettingsRef();
    auto & insert_query = query_ptr->as<ASTInsertQuery &>();

    BlockIO res;

    StoragePtr table = getTable(insert_query);
    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings.lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    if (!metadata_snapshot->hasUniqueKey())
    {
        if (insert_query.is_replace)
            throw Exception("REPLACE INTO statement only supports table with UNIQUE KEY.", ErrorCodes::NOT_IMPLEMENTED);
        if (insert_query.insert_ignore)
            throw Exception("INSERT IGNORE statement only supports table with UNIQUE KEY.", ErrorCodes::NOT_IMPLEMENTED);
    }
    else if (insert_query.insert_ignore)
    {
        auto mutable_context = const_pointer_cast<Context>(getContext());
        mutable_context->setSetting("dedup_key_mode", String("ignore"));
    }

    auto query_sample_block = getSampleBlock(insert_query, table, metadata_snapshot);
    if (!insert_query.table_function)
        getContext()->checkAccess(access_type, insert_query.table_id, query_sample_block.getNames());

    bool is_distributed_insert_select = false;

    if (insert_query.select && table->isRemote() &&
        (settings.parallel_distributed_insert_select || settings.distributed_perfect_shard))
    {
        // Distributed INSERT SELECT
        if (auto maybe_pipeline = table->distributedWrite(insert_query, getContext()))
        {
            res.pipeline = std::move(*maybe_pipeline);
            is_distributed_insert_select = true;
        }
    }

    StorageCnchMergeTree * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree*>(table.get());
    CnchLockHolderPtrs lock_holders;
    if (getContext()->getServerType() == ServerType::cnch_server && cnch_merge_tree)
    {
        /// handle insert overwrite first
        if (insert_query.is_overwrite)
        {
            cnch_merge_tree->overwritePartitions(insert_query.overwrite_partition, getContext(), &lock_holders);
        }
    }
    /// Directly forward the query to cnch worker if select or infile
    if (getContext()->getServerType() == ServerType::cnch_server && (insert_query.select || insert_query.in_file) && cnch_merge_tree)
    {
        /// Handle the insert commit for insert select/infile case in cnch server.
        BlockInputStreamPtr in = cnch_merge_tree->writeInWorker(query_ptr, metadata_snapshot, getContext());

        auto txn = getContext()->getCurrentTransaction();
        txn->setMainTableUUID(table->getStorageUUID());

        if (const auto * cnch_table = dynamic_cast<const StorageCnchMergeTree *>(table.get());
            cnch_table && cnch_table->commitTxnInWriteSuffixStage(txn->getDedupImplVersion(getContext()), getContext()))
        {
            /// for unique table, insert select|infile is committed from worker side
            res.in = std::move(in);
        }
        else
            res.in = std::make_shared<TransactionWrapperBlockInputStream>(in, std::move(txn));

        if (insert_query.is_overwrite && !lock_holders.empty())
        {
            /// Make sure lock is release after txn commit
            res.in = std::make_shared<LockHoldBlockInputStream>(res.in, std::move(lock_holders));
        }
        return res;
    }

    BlockOutputStreams out_streams;
    if (!is_distributed_insert_select || insert_query.watch)
    {
        size_t out_streams_size = 1;
        if (insert_query.select)
        {
            auto insert_select_context = Context::createCopy(getContext());
            /// Cannot use trySetVirtualWarehouseAndWorkerGroup, because it only works in server node
            if (auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(table.get()))
            {
                auto worker_group = getWorkerGroupForTable(*cloud_table, insert_select_context);
                insert_select_context->setCurrentWorkerGroup(worker_group);
                /// set worker group for select query
                insert_select_context->initCnchServerResource(insert_select_context->getCurrentTransactionID());
                LOG_DEBUG(
                    getLogger("VirtualWarehouse"),
                    "Set worker group {} for table {}", worker_group->getQualifiedName(), cloud_table->getStorageID().getNameForLogs());
            }

            bool is_trivial_insert_select = false;

            if (settings.optimize_trivial_insert_select)
            {
                const auto & select_query = insert_query.select->as<ASTSelectWithUnionQuery &>();
                const auto & selects = select_query.list_of_selects->children;
                const auto & union_modes = select_query.list_of_modes;

                /// ASTSelectWithUnionQuery is not normalized now, so it may pass some queries which can be Trivial select queries
                const auto mode_is_all = [](const auto & mode) { return mode == SelectUnionMode::UNION_ALL; };

                is_trivial_insert_select = std::all_of(union_modes.begin(), union_modes.end(), std::move(mode_is_all))
                    && std::all_of(selects.begin(), selects.end(), isTrivialSelect);
            }

            if (insert_select_context->getServerType() == ServerType::cnch_worker)
            {
                Settings new_settings = insert_select_context->getSettings();
                new_settings.prefer_cnch_catalog = true;
                insert_select_context->setSettings(std::move(new_settings));
            }

            if (is_trivial_insert_select)
            {
                /** When doing trivial INSERT INTO ... SELECT ... FROM table,
                  * don't need to process SELECT with more than max_insert_threads
                  * and it's reasonable to set block size for SELECT to the desired block size for INSERT
                  * to avoid unnecessary squashing.
                  */

                Settings new_settings = insert_select_context->getSettings();

                new_settings.max_threads = std::max<UInt64>(1, settings.max_insert_threads);

                if (table->prefersLargeBlocks())
                {
                    if (settings.min_insert_block_size_rows)
                        new_settings.max_block_size = settings.min_insert_block_size_rows;
                    if (settings.min_insert_block_size_bytes)
                        new_settings.preferred_block_size_bytes = settings.min_insert_block_size_bytes;
                }

                insert_select_context->setSettings(new_settings);

                InterpreterSelectWithUnionQuery interpreter_select{
                    insert_query.select, insert_select_context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};
                res = interpreter_select.execute();
            }
            else
            {
                /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
                InterpreterSelectWithUnionQuery interpreter_select{
                    insert_query.select, insert_select_context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};
                res = interpreter_select.execute();
            }

            res.pipeline.dropTotalsAndExtremes();

            if (table->supportsParallelInsert(getContext()) && settings.max_insert_threads > 1)
                out_streams_size = std::min(size_t(settings.max_insert_threads), res.pipeline.getNumStreams());

            res.pipeline.resize(out_streams_size);

            /// Allow to insert Nullable into non-Nullable columns, NULL values will be added as defaults values.
            if (getContext()->getSettingsRef().insert_null_as_default)
            {
                const auto & input_columns = res.pipeline.getHeader().getColumnsWithTypeAndName();
                const auto & query_columns = query_sample_block.getColumnsWithTypeAndName();
                const auto & output_columns = metadata_snapshot->getColumns();

                if (input_columns.size() == query_columns.size())
                {
                    for (size_t col_idx = 0; col_idx < query_columns.size(); ++col_idx)
                    {
                        /// Change query sample block columns to Nullable to allow inserting nullable columns, where NULL values will be substituted with
                        /// default column values (in AddingDefaultsTransform), so all values will be cast correctly.
                        if (isNullableOrLowCardinalityNullable(input_columns[col_idx].type) && !isNullableOrLowCardinalityNullable(query_columns[col_idx].type) && output_columns.has(query_columns[col_idx].name))
                            query_sample_block.setColumn(col_idx, ColumnWithTypeAndName(makeNullableOrLowCardinalityNullable(query_columns[col_idx].column), makeNullableOrLowCardinalityNullable(query_columns[col_idx].type), query_columns[col_idx].name));
                    }
                }
            }
        }
        else if (insert_query.watch)
        {
            InterpreterWatchQuery interpreter_watch{ insert_query.watch, getContext() };
            res = interpreter_watch.execute();
            res.pipeline.init(Pipe(std::make_shared<SourceFromInputStream>(std::move(res.in))));
        }

        for (size_t i = 0; i < out_streams_size; i++)
        {
            /// We create a pipeline of several streams, into which we will write data.
            BlockOutputStreamPtr out;

            /// NOTE: we explicitly ignore bound materialized views when inserting into Kafka Storage.
            ///       Otherwise we'll get duplicates when MV reads same rows again from Kafka.
            if (table->noPushingToViews() && !no_destination)
                out = table->write(query_ptr, metadata_snapshot, getContext());
            else
                out = std::make_shared<PushingToViewsBlockOutputStream>(table, metadata_snapshot, getContext(), query_ptr, no_destination);

            /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.

            /// Checking constraints. It must be done after calculation of all defaults, so we can check them on calculated columns.
            BlockOutputStreamPtr constraint_filter_stream = nullptr;
            if (const auto & constraints = metadata_snapshot->getConstraints(); !constraints.empty())
            {
                if (getContext()->getSettingsRef().constraint_skip_violate)
                    constraint_filter_stream = out = std::make_shared<CheckConstraintsFilterBlockOutputStream>(
                        insert_query.table_id, out, out->getHeader(), metadata_snapshot->getConstraints(), getContext());
                else
                    out = std::make_shared<CheckConstraintsBlockOutputStream>(
                        insert_query.table_id, out, out->getHeader(), metadata_snapshot->getConstraints(), getContext());
            }

            bool null_as_default = insert_query.select && getContext()->getSettingsRef().insert_null_as_default;

            /// Actually we don't know structure of input blocks from query/table,
            /// because some clients break insertion protocol (columns != header)
            out = std::make_shared<AddingDefaultBlockOutputStream>(
                out, query_sample_block, metadata_snapshot->getColumns(), getContext(), null_as_default);

            /// It's important to squash blocks as early as possible (before other transforms),
            ///  because other transforms may work inefficient if block size is small.

            /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
            /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
            if (!(settings.insert_distributed_sync && table->isRemote()) && !no_squash && !insert_query.watch)
            {
                bool table_prefers_large_blocks = table->prefersLargeBlocks();

                out = std::make_shared<SquashingBlockOutputStream>(
                    out,
                    out->getHeader(),
                    table_prefers_large_blocks ? settings.min_insert_block_size_rows : settings.max_block_size,
                    table_prefers_large_blocks ? settings.min_insert_block_size_bytes : 0);
            }

            auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
            /// XXX: A tricky way to record the actual written metrics as it's hard to pass this info among streams;
            /// Luckily, only CheckConstraintsFilterBlockOutputStream may reduce the rows, so we add this logic for it
            if (constraint_filter_stream)
                out_wrapper->setConstraintsFilterStream(constraint_filter_stream);

            out_wrapper->setProcessListElement(getContext()->getProcessListElement());
            out_streams.emplace_back(std::move(out_wrapper));
        }
    }

    /// What type of query: INSERT or INSERT SELECT or INSERT WATCH?
    if (is_distributed_insert_select)
    {
        /// Pipeline was already built.
    }
    else if (insert_query.in_file)
    {
        // read data stream from in_file, and copy it to out
        // Special handling in_file based on url type:
        String uristr = typeid_cast<const ASTLiteral &>(*insert_query.in_file).value.safeGet<String>();
        // create Datastream based on Format:
        String format = insert_query.format;
        if (format.empty())
            format = getContext()->getDefaultFormat();

        /// It can be compressed and compression method maybe specified in query
        String compression_method;
        if (insert_query.compression)
        {
            const auto & compression_method_node = insert_query.compression->as<ASTLiteral &>();
            compression_method = compression_method_node.value.safeGet<std::string>();
        }

        auto input_stream = buildInputStreamFromSource(getContext(), metadata_snapshot->getColumns(), out_streams.at(0)->getHeader(),
            settings, uristr, format, false, compression_method);

        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(input_stream, out_streams.at(0));
        res.out = nullptr;
    }
    else if (insert_query.select || insert_query.watch)
    {
        const auto & header = out_streams.at(0)->getHeader();
        auto actions_dag = ActionsDAG::makeConvertingActions(
            res.pipeline.getHeader().getColumnsWithTypeAndName(),
            header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto actions = std::make_shared<ExpressionActions>(
            actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

        res.pipeline.addSimpleTransform(
            [&](const Block & in_header) -> ProcessorPtr { return std::make_shared<ExpressionTransform>(in_header, actions); });

        if (settings.insert_select_with_profiles)
        {
            res.pipeline.addSimpleTransform([&](const Block &, QueryPipeline::StreamType type) -> ProcessorPtr
            {
                if (type != QueryPipeline::StreamType::Main)
                    return nullptr;

                auto stream = std::move(out_streams.back());
                out_streams.pop_back();

                auto res_stream = std::make_shared<ProcessorToOutputStream>(std::move(stream), "inserted_rows");
                res_stream->setProgressCallback(this->getContext()->getProgressCallback());
                return res_stream;
            });
        }
        else
        {
            res.pipeline.setSinks([&](const Block &, QueryPipeline::StreamType type) -> ProcessorPtr
            {
                if (type != QueryPipeline::StreamType::Main)
                    return nullptr;

                auto stream = std::move(out_streams.back());
                out_streams.pop_back();

                return std::make_shared<SinkToOutputStream>(std::move(stream));
            });
        }

        if (!allow_materialized)
        {
            for (const auto & column : metadata_snapshot->getColumns())
                if (column.default_desc.kind == ColumnDefaultKind::Materialized && header.has(column.name))
                    throw Exception(
                        "Cannot insert column " + column.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (insert_query.data && !insert_query.has_tail) /// can execute without additional data
    {
        auto pipe = getSourceFromFromASTInsertQuery(query_ptr, nullptr, query_sample_block, getContext(), nullptr);
        res.pipeline.init(std::move(pipe));
        res.pipeline.resize(1);
        res.pipeline.setSinks([&](const Block &, Pipe::StreamType) { return std::make_shared<SinkToOutputStream>(out_streams.at(0)); });
    }
    else
        res.out = std::move(out_streams.at(0));

    res.pipeline.addStorageHolder(table);
    if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table.get()))
    {
        if (auto inner_table = mv->tryGetTargetTable())
            res.pipeline.addStorageHolder(inner_table);
    }

    table->setUpdateTimeNow();
    if (insert_query.is_overwrite && !lock_holders.empty())
    {
        /// Make sure lock is release after txn commit
        res.in = std::make_shared<LockHoldBlockInputStream>(res.in, std::move(lock_holders));
    }
    return res;
}


StorageID InterpreterInsertQuery::getDatabaseTable() const
{
    return query_ptr->as<ASTInsertQuery &>().table_id;
}


void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr context_) const
{
    elem.query_kind = "Insert";
    const auto & insert_table = context_->getInsertionTable();
    if (!insert_table.empty())
    {
        elem.query_databases.insert(insert_table.getDatabaseName());
        elem.query_tables.insert(insert_table.getFullNameNotQuoted());
    }
}

void parseFuzzyName(const ContextPtr & context_ptr, std::vector<String> & file_path_list, const String & source_uri, const String & scheme)
{
    // Assume no query and fragment in uri, todo, add sanity check
    String fuzzy_file_name;
    String uri_prefix = source_uri.substr(0, source_uri.find_last_of('/'));
    if (uri_prefix.length() == source_uri.length())
    {
        fuzzy_file_name = source_uri;
        uri_prefix.clear();
    }
    else
    {
        uri_prefix += "/";
        fuzzy_file_name = source_uri.substr(uri_prefix.length());
    }

    auto max_files = context_ptr->getSettingsRef().fuzzy_max_files;
    std::vector<String> parent_list = parseDescription(fuzzy_file_name, 0, fuzzy_file_name.length(), ',', max_files);
    for (const auto & fuzzy_name : parent_list)
    {
        std::vector<String> child_list = parseDescription(fuzzy_name, 0, fuzzy_name.length(), '|', max_files);
        for (const auto & star_name : child_list)
        {
            String full_path = uri_prefix + star_name;
            if (star_name.find_first_of("*?{") == std::string::npos)
            {
                file_path_list.emplace_back(full_path);
                continue;
            }

            std::shared_ptr<FilePathMatcher> matcher;
            if (scheme.empty() || scheme == "file")
            {
                matcher = std::make_shared<LocalFilePathMatcher>();
            }
#if USE_HDFS
            else if (DB::isHdfsOrCfsScheme(scheme))
            {
                matcher = std::make_shared<HDFSFilePathMatcher>(full_path, context_ptr);
            }
#endif
#if USE_AWS_S3
            else if (isS3URIScheme(scheme))
            {
                matcher = std::make_shared<S3FilePathMatcher>(full_path, context_ptr);
            }
#endif
            else
            {
                file_path_list.emplace_back(full_path);
            }

            if (matcher)
            {
                // match files
                String match_path = matcher->removeSchemeAndPrefix(full_path);
                Strings match_file_list = matcher->regexMatchFiles("/", match_path);
                file_path_list.insert(file_path_list.end(), match_file_list.begin(), match_file_list.end());
            }

            if (file_path_list.size() > max_files)
                throw Exception(uri_prefix + fuzzy_file_name + " generates too many files, please modify the value of fuzzy_max_files.", ErrorCodes::BAD_ARGUMENTS);
        }
    }
}

BlockInputStreamPtr InterpreterInsertQuery::buildInputStreamFromSource(
    const ContextPtr context_ptr,
    const ColumnsDescription & columns,
    const Block & sample,
    const Settings & settings,
    const String & source_uri,
    const String & format,
    bool is_enable_squash,
    const String & compression_method)
{
    Poco::URI uri(source_uri);
    const String & scheme = uri.getScheme();

    BlockInputStreams inputs;
    {
        std::vector<String> file_path_list;
        parseFuzzyName(context_ptr, file_path_list, source_uri, scheme);

        for (auto & file_path : file_path_list)
        {
            std::unique_ptr<ReadBuffer> read_buf = nullptr;

            if (scheme.empty() || scheme == "file")
            {
                read_buf = std::make_unique<ReadBufferFromFile>(Poco::URI(file_path).getPath());
            }
#if USE_HDFS
            else if (DB::isHdfsOrCfsScheme(scheme))
            {
                ReadSettings read_settings;
                read_settings.remote_throttler = context_ptr->getProcessList().getHDFSDownloadThrottler();
                read_buf = std::make_unique<ReadBufferFromByteHDFS>(file_path, context_ptr->getHdfsConnectionParams(), read_settings);
            }
#endif
#if USE_AWS_S3
            else if (isS3URIScheme(scheme))
            {
                S3::URI s3_uri(file_path);
                String endpoint = s3_uri.endpoint.empty() ? context_ptr->getSettingsRef().s3_endpoint.toString() : s3_uri.endpoint;
                String bucket = s3_uri.bucket;
                String key = s3_uri.key;
                S3::S3Config s3_cfg(
                    endpoint,
                    context_ptr->getSettingsRef().s3_region.toString(),
                    bucket,
                    context_ptr->getSettingsRef().s3_ak_id.toString(),
                    context_ptr->getSettingsRef().s3_ak_secret.toString(),
                    "",
                    "",
                    context_ptr->getSettingsRef().s3_use_virtual_hosted_style);
                const std::shared_ptr<Aws::S3::S3Client> client = s3_cfg.create();
                read_buf = std::make_unique<ReadBufferFromS3>(client, bucket, key, context_ptr->getReadSettings());
            }
#endif
            else
            {
                throw Exception("URI scheme " + scheme + " is not supported with insert statement yet", ErrorCodes::NOT_IMPLEMENTED);
            }

            read_buf = wrapReadBufferWithCompressionMethod(
                std::move(read_buf), chooseCompressionMethod(file_path, compression_method), settings.snappy_format_blocked);

            inputs.emplace_back(std::make_shared<OwningBlockInputStream<ReadBuffer>>(
                context_ptr->getInputStreamByFormatNameAndBuffer(
                    format,
                    *read_buf,
                    sample, // sample_block
                    settings.max_insert_block_size,
                    columns),
                std::move(read_buf)));
        }
    }

    if (inputs.empty())
        throw Exception("Input files is empty.", ErrorCodes::LOGICAL_ERROR);

    auto stream = inputs[0];
    if (inputs.size() > 1)
    {
        // Squash is used to generate larger(less part to merge later)
        stream = std::make_shared<SquashingBlockInputStream>(
            std::make_shared<UnionBlockInputStream>(inputs, nullptr, settings.max_distributed_connections),
            settings.min_insert_block_size_rows,
            settings.min_insert_block_size_bytes);
    }

    if (is_enable_squash)
        stream = std::make_shared<SquashingBlockInputStream>(
            stream,
            settings.min_insert_block_size_rows,
            settings.min_insert_block_size_bytes);

    return stream;
}

}
