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

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/checkStackSize.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/StorageValues.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageView.h>
#include <common/logger_useful.h>

#if USE_RDKAFKA
#include <Storages/Kafka/StorageCloudKafka.h>
#endif

namespace DB
{

PushingToViewsBlockOutputStream::PushingToViewsBlockOutputStream(
    const StoragePtr & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    const ASTPtr & query_ptr_,
    bool no_destination)
    : WithContext(context_)
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , log(getLogger("PushingToViewsBlockOutputStream"))
    , query_ptr(query_ptr_)
{
    checkStackSize();

    /** TODO This is a very important line. At any insertion into the table one of streams should own lock.
      * Although now any insertion into the table is done via PushingToViewsBlockOutputStream,
      *  but it's clear that here is not the best place for this functionality.
      */
    addTableLock(
        storage->lockForShare(getContext()->getInitialQueryId(), getContext()->getSettingsRef().lock_acquire_timeout));

    /// If the "root" table deduplicates blocks, there are no need to make deduplication for children
    /// Moreover, deduplication for AggregatingMergeTree children could produce false positives due to low size of inserting blocks
    bool disable_deduplication_for_children = false;
    if (!getContext()->getSettingsRef().deduplicate_blocks_in_dependent_materialized_views)
        disable_deduplication_for_children = !no_destination && storage->supportsDeduplication();

    auto table_id = storage->getStorageID();

    /// TODO: In order to execute insert values in cnch server view dependencies should be
    /// get from catalog service rather than in local context, because each query will destroy
    /// database object and remove view dependency in local context. There should be a local cache.
    Dependencies dependencies;
    if (getContext()->getServerType() == ServerType::cnch_server)
    {
        auto start_time = getContext()->getTimestamp();
        auto catalog_client = getContext()->getCnchCatalog();
        if (!catalog_client)
           throw Exception("get catalog client failed", ErrorCodes::LOGICAL_ERROR);

        auto all_views_from_catalog = catalog_client->getAllViewsOn(*getContext(), storage, start_time);
        for (auto & view : all_views_from_catalog)
        {
            dependencies.emplace_back(view->getDatabaseName(), view->getTableName());
            auto & mutable_context = const_cast<Context &>(*getContext());
            mutable_context.addSessionView({view->getDatabaseName(), view->getTableName()}, view);
        }
    }
    else
        dependencies = DatabaseCatalog::instance().getDependencies(table_id);

    /// We need special context for materialized views insertions
    if (!dependencies.empty())
    {
        select_context = Context::createCopy(context);
        insert_context = Context::createCopy(context);

        const auto & insert_settings = insert_context->getSettingsRef();

        // Do not deduplicate insertions into MV if the main insertion is Ok
        if (disable_deduplication_for_children)
            insert_context->setSetting("insert_deduplicate", Field{false});

        // Separate min_insert_block_size_rows/min_insert_block_size_bytes for children
        if (insert_settings.min_insert_block_size_rows_for_materialized_views)
            insert_context->setSetting("min_insert_block_size_rows", insert_settings.min_insert_block_size_rows_for_materialized_views.value);
        if (insert_settings.min_insert_block_size_bytes_for_materialized_views)
            insert_context->setSetting("min_insert_block_size_bytes", insert_settings.min_insert_block_size_bytes_for_materialized_views.value);
    }

    for (const auto & database_table : dependencies)
    {
        StoragePtr view_table;
        if (getContext()->getServerType() == ServerType::cnch_server)
        {
            auto & mutable_context = const_cast<Context &>(*getContext());
            view_table = mutable_context.getSessionView(StorageID(database_table.getDatabaseName(), database_table.getTableName()));
        }
        else
            view_table = DatabaseCatalog::instance().getTable(database_table, getContext());

        if(!view_table)
           continue;

        auto dependent_metadata_snapshot = view_table->getInMemoryMetadataPtr();

        ASTPtr query;
        BlockOutputStreamPtr out;
        query = dependent_metadata_snapshot->getSelectQuery().inner_query;

        if (dynamic_cast<const StorageView *>(view_table.get()))
           continue;

        Block output_header;
        if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(view_table.get()))
        {
            if (materialized_view->async())
                continue;
            addTableLock(materialized_view->lockForShare(getContext()->getInitialQueryId(), getContext()->getSettingsRef().lock_acquire_timeout));

            StoragePtr target_table;
            if (getContext()->getServerType() == ServerType::cnch_server)
            {
                auto & mutable_context = const_cast<Context &>(*getContext());
                target_table = mutable_context.getSessionView(materialized_view->getTargetTableId());
            }
            else
                target_table = DatabaseCatalog::instance().getTable(materialized_view->getTargetTableId(), getContext());

            if(!target_table)
               continue;

            auto * cnch_target_table = dynamic_cast<StorageCnchMergeTree*>(target_table.get());
            if (cnch_target_table && getContext()->getServerType() == ServerType::cnch_server)
            {
                auto target_metadata_snapshot = cnch_target_table->getInMemoryMetadataPtr();
                out = std::make_shared<CloudMergeTreeBlockOutputStream>(*cnch_target_table, target_metadata_snapshot, insert_context);
                output_header = target_metadata_snapshot->getSampleBlockNonMaterialized(true);
            }
            else
            {
                auto target_table_id = target_table->getStorageID();
                auto target_metadata_snapshot = target_table->getInMemoryMetadataPtr();

                std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
                insert->table_id = target_table_id;

                /// Get list of columns we get from select query.
                auto header = InterpreterSelectQuery(query, select_context, SelectQueryOptions().analyze())
                    .getSampleBlock();

                /// Select columns without materialized columns and columns function of uniq merge tree.
                auto list = std::make_shared<ASTExpressionList>();
                const auto & target_table_columns = target_metadata_snapshot->getSampleBlockNonMaterialized(true);
                for (const auto & column : header)
                {
                    if (target_table_columns.has(column.name))
                        list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
                }

                insert->columns = std::move(list);
                ASTPtr insert_query_ptr(insert.release());
                InterpreterInsertQuery interpreter(insert_query_ptr, insert_context);
                BlockIO io = interpreter.execute();
                out = io.out;
                output_header = out->getHeader();
            }
        }
        else if (dynamic_cast<const StorageLiveView *>(view_table.get()))
        {
            out = std::make_shared<PushingToViewsBlockOutputStream>(
                view_table, dependent_metadata_snapshot, insert_context, ASTPtr(), true);
            output_header = dependent_metadata_snapshot->getSampleBlockNonMaterialized(true);
        }
        else
        {
            out = std::make_shared<PushingToViewsBlockOutputStream>(view_table, dependent_metadata_snapshot, insert_context, ASTPtr());
            output_header = dependent_metadata_snapshot->getSampleBlockNonMaterialized(true);
        }

        views.emplace_back(ViewInfo{std::move(query), database_table, std::move(out), nullptr, 0, output_header});
    }

    /// Do not push to destination table if the flag is set
    if (!no_destination)
    {
        output = storage->write(query_ptr, storage->getInMemoryMetadataPtr(), getContext());
        replicated_output = dynamic_cast<ReplicatedMergeTreeBlockOutputStream *>(output.get());
    }

    /// Insert values query execute in server side in order to commit server transaction once
    /// disable destination table transaction commit , related views to commit transactions
    /// TODO: find better solution, this implementation is a little trick
    if (getContext()->getServerType() == ServerType::cnch_server && !views.empty())
    {
        bool all_cloud_output = true;
        for (const auto & view_info : views)
        {
            auto * cloud_output_stream = dynamic_cast<CloudMergeTreeBlockOutputStream *>(view_info.out.get());
            if (!cloud_output_stream)
                all_cloud_output = all_cloud_output && false;
        }

        if (output)
        {
            auto * dest_output_stream = dynamic_cast<CloudMergeTreeBlockOutputStream *>(output.get());
            if (!dest_output_stream)
                all_cloud_output = all_cloud_output && false;
        }

        if (all_cloud_output)
        {
            auto * dest_output_stream = dynamic_cast<CloudMergeTreeBlockOutputStream *>(output.get());
            dest_output_stream->disableTransactionCommit();
            for(const auto & view: views)
            {
                auto * cloud_output_stream = dynamic_cast<CloudMergeTreeBlockOutputStream *>(view.out.get());
                cloud_output_stream->disableTransactionCommit();
            }
            explicit_commit_txn = true;
        }
    }
}


Block PushingToViewsBlockOutputStream::getHeader() const
{
    /// If we don't write directly to the destination
    /// then expect that we're inserting with precalculated virtual columns
    if (output)
        return metadata_snapshot->getSampleBlock(/*include_func_columns*/ true);
    else
        return metadata_snapshot->getSampleBlockWithVirtuals(storage->getVirtuals());
}


void PushingToViewsBlockOutputStream::write(const Block & block)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(block);

    if (auto * live_view = dynamic_cast<StorageLiveView *>(storage.get()))
    {
        StorageLiveView::writeIntoLiveView(*live_view, block, getContext());
    }
    else
    {
        if (output)
            /// TODO: to support virtual and alias columns inside MVs, we should return here the inserted block extended
            ///       with additional columns directly from storage and pass it to MVs instead of raw block.
            output->write(block);
    }

    /// Don't process materialized views if this block is duplicate
    if (!getContext()->getSettingsRef().deduplicate_blocks_in_dependent_materialized_views && replicated_output && replicated_output->lastBlockIsDuplicate())
        return;

    // Insert data into materialized views only after successful insert into main table
    const Settings & settings = getContext()->getSettingsRef();

    // Reset some block settings so that it doesn't block MV insert. e.g. in kafka scenario.
    // After checking is caller, it is safe to update the const context here
    const_cast<Settings &>(settings).allow_map_access_without_key = true;

    if (settings.parallel_view_processing && views.size() > 1)
    {
        // Push to views concurrently if enabled and more than one view is attached
        ThreadPool pool(std::min(size_t(settings.max_threads), views.size()));
        for (auto & view : views)
        {
            auto thread_group = CurrentThread::getGroup();
            pool.scheduleOrThrowOnError([=, &view, this]
            {
                setThreadName("PushingToViews");
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                process(block, view);
            });
        }
        // Wait for concurrent view processing
        pool.wait();
    }
    else
    {
        // Process sequentially
        for (auto & view : views)
        {
            process(block, view);

            if (view.exception)
                std::rethrow_exception(view.exception);
        }
    }
}

void PushingToViewsBlockOutputStream::writePrefix()
{
    if (output)
        output->writePrefix();

    for (auto & view : views)
    {
        try
        {
            view.out->writePrefix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while write prefix to view " + view.table_id.getNameForLogs());
            throw;
        }
    }
}

void PushingToViewsBlockOutputStream::writeSuffix()
{
    if (output)
        output->writeSuffix();

    std::exception_ptr first_exception;

    const Settings & settings = getContext()->getSettingsRef();
    bool parallel_processing = false;

    /// Run writeSuffix() for views in separate thread pool.
    /// In could have been done in PushingToViewsBlockOutputStream::process, however
    /// it is not good if insert into main table fail but into view succeed.
    if (settings.parallel_view_processing && views.size() > 1)
    {
        parallel_processing = true;

        // Push to views concurrently if enabled and more than one view is attached
        ThreadPool pool(std::min(size_t(settings.max_threads), views.size()));
        auto thread_group = CurrentThread::getGroup();

        for (auto & view : views)
        {
            if (view.exception)
                continue;

            pool.scheduleOrThrowOnError([thread_group, &view, this]
            {
                setThreadName("PushingToViews");
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);

                Stopwatch watch;
                try
                {
                    view.out->writeSuffix();
                }
                catch (...)
                {
                    view.exception = std::current_exception();
                }
                view.elapsed_ms += watch.elapsedMilliseconds();

                LOG_TRACE(log, "Pushing from {} to {} took {} ms.",
                    storage->getStorageID().getNameForLogs(),
                    view.table_id.getNameForLogs(),
                    view.elapsed_ms);
            });
        }
        // Wait for concurrent view processing
        pool.wait();
    }

    for (auto & view : views)
    {
        if (view.exception)
        {
            if (!first_exception)
                first_exception = view.exception;

            continue;
        }

        if (parallel_processing)
            continue;

        Stopwatch watch;
        try
        {
            view.out->writeSuffix();
        }
        catch (Exception & ex)
        {
            ex.addMessage("while write prefix to view " + view.table_id.getNameForLogs());
            throw;
        }
        view.elapsed_ms += watch.elapsedMilliseconds();

        LOG_TRACE(log, "Pushing from {} to {} took {} ms.",
            storage->getStorageID().getNameForLogs(),
            view.table_id.getNameForLogs(),
            view.elapsed_ms);
    }

    if (first_exception)
        std::rethrow_exception(first_exception);

    UInt64 milliseconds = main_watch.elapsedMilliseconds();
    if (views.size() > 1)
    {
        LOG_DEBUG(log, "Pushing from {} to {} views took {} ms.",
            storage->getStorageID().getNameForLogs(), views.size(),
            milliseconds);
    }

    /// explicit_commit_txn is true finally commit transaction after all write actions complete
    /// TODO: find better solution, this implementation is a little trick
    if (explicit_commit_txn)
    {
        auto txn = getContext()->getCurrentTransaction();
        if (dynamic_pointer_cast<CnchServerTransaction>(txn))
        {
            txn->setMainTableUUID(storage->getStorageUUID());
            txn->commitV2();
        }
    }

    /// A trick way to commit insert transaction for multi MVs
    /// Implicit commit is used in kafka consumer and insert values executed in worker
    /// TODO: it should be changed into interactive transaction when it is ready
    if (getContext()->getServerType() == ServerType::cnch_worker)
    {
        auto txn = getContext()->getCurrentTransaction();
        if (auto worker_txn = dynamic_pointer_cast<CnchWorkerTransaction>(txn);
            worker_txn && worker_txn->hasEnableExplicitCommit() &&
            worker_txn->getExplicitCommitStorageID() == storage->getStorageID())
        {
            txn->commitV2();
        }
    }
}

void PushingToViewsBlockOutputStream::flush()
{
    if (output)
        output->flush();

    for (auto & view : views)
        view.out->flush();
}

void PushingToViewsBlockOutputStream::process(const Block & block, ViewInfo & view)
{
    Stopwatch watch;

    try
    {
        BlockInputStreamPtr in;

        /// We need keep InterpreterSelectQuery, until the processing will be finished, since:
        ///
        /// - We copy Context inside InterpreterSelectQuery to support
        ///   modification of context (Settings) for subqueries
        /// - InterpreterSelectQuery lives shorter than query pipeline.
        ///   It's used just to build the query pipeline and no longer needed
        /// - ExpressionAnalyzer and then, Functions, that created in InterpreterSelectQuery,
        ///   **can** take a reference to Context from InterpreterSelectQuery
        ///   (the problem raises only when function uses context from the
        ///    execute*() method, like FunctionDictGet do)
        /// - These objects live inside query pipeline (DataStreams) and the reference become dangling.

        if (view.query)
        {
            /// We create a table with the same name as original table and the same alias columns,
            ///  but it will contain single block (that is INSERT-ed into main table).
            /// InterpreterSelectQuery will do processing of alias columns.

            auto local_context = Context::createCopy(select_context);
            local_context->addViewSource(
                StorageValues::create(storage->getStorageID(), metadata_snapshot->getColumns(), block, storage->getVirtuals()));

#if USE_RDKAFKA
            /// Set the limits for Kafka specially as Kafka stream may need long time to write a block with many map keys;
            /// If write data timeout, throw exception to trigger re-consumption
            if (auto * kafka = dynamic_cast<StorageCloudKafka *>(storage.get()))
            {
                auto settings = local_context->getSettingsRef();
                settings.max_execution_time = kafka->getSettings().max_write_execution_second;
                settings.timeout_overflow_mode = OverflowMode::THROW;
                local_context->setSettings(settings);
                LOG_TRACE(log, "Set max execution limit for reading from {} to write view with {} s",
                          storage->getStorageID().getNameForLogs(), local_context->getSettings().max_execution_time.totalSeconds());
            }
#endif

            if (auto * select = view.query->as<ASTSelectWithUnionQuery>())
            {
                InterpreterSelectWithUnionQuery interepter_select(view.query, local_context, SelectQueryOptions());
                in = std::make_shared<MaterializingBlockInputStream>(interepter_select.execute().getInputStream());
            }
            else
            {
                InterpreterSelectQuery interepter_select(view.query, local_context, SelectQueryOptions());
                in = std::make_shared<MaterializingBlockInputStream>(interepter_select.execute().getInputStream());
            }

            /// Squashing is needed here because the materialized view query can generate a lot of blocks
            /// even when only one block is inserted into the parent table (e.g. if the query is a GROUP BY
            /// and two-level aggregation is triggered).
            in = std::make_shared<SquashingBlockInputStream>(
                    in, getContext()->getSettingsRef().min_insert_block_size_rows, getContext()->getSettingsRef().min_insert_block_size_bytes);
            in = std::make_shared<ConvertingBlockInputStream>(in, view.output_header ? view.output_header : view.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Name);
        }
        else
            in = std::make_shared<OneBlockInputStream>(block);

        in->readPrefix();

        while (Block result_block = in->read())
        {
            Nested::validateArraySizes(result_block);
            view.out->write(result_block);
        }

        in->readSuffix();
    }
    catch (Exception & ex)
    {
        ex.addMessage("while pushing to view " + view.table_id.getNameForLogs());
        view.exception = std::current_exception();
    }
    catch (...)
    {
        view.exception = std::current_exception();
    }

    view.elapsed_ms += watch.elapsedMilliseconds();
}

}
