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

#include <mutex>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>

#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchDataWriter.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/CnchSystemLog.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <Parsers/ASTPartition.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <string_view>
#include <Transaction/CnchWorkerTransaction.h>
#include <WorkerTasks/ManipulationType.h>
#include <Core/SettingsEnums.h>
#include <iostream>
namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
    extern const int INCORRECT_DATA;
    extern const int INSERTION_LABEL_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int UNIQUE_KEY_STRING_SIZE_LIMIT_EXCEEDED;
}

CloudMergeTreeBlockOutputStream::CloudMergeTreeBlockOutputStream(
    MergeTreeMetaBase & storage_,
    StorageMetadataPtr metadata_snapshot_,
    ContextPtr context_,
    ASTPtr overwrite_partition_)
    : storage(storage_)
    , log(storage.getLogger())
    , metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
    , writer(storage, IStorage::StorageLocation::AUXILITY)
    , cnch_writer(storage, context, ManipulationType::Insert)
    , overwrite_partition(overwrite_partition_)
{
    checkAndInit();
}

void CloudMergeTreeBlockOutputStream::checkAndInit()
{
    if (metadata_snapshot->hasUniqueKey())
    {
        dedup_parameters.enable_staging_area = context->getSettingsRef().enable_staging_area_for_write.value || storage.getSettings()->cloud_enable_staging_area;
        dedup_parameters.enable_append_mode = context->getSettingsRef().dedup_key_mode == DedupKeyMode::APPEND;

        if (dedup_parameters.enable_staging_area)
        {
            if (context->getSettings().dedup_key_mode != DedupKeyMode::REPLACE)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only UPSERT mode can write to staging area.");
            LOG_DEBUG(log, "enable staging area for write");
        }
        else
        {
            switch (context->getSettings().dedup_key_mode) {
                case DedupKeyMode::APPEND:
                    /// case 1(unique table with async insert): commit all the temp parts as staged parts, which will be converted to visible parts later by dedup worker
                    /// case 2(unique table with append mode): just commit all the temp parts as visible parts with empty delete bitmaps. Insert is lock-free and faster than upsert due to its simplicity.
                    cnch_writer.setDedupMode(CnchDedupHelper::DedupMode::APPEND);
                    LOG_DEBUG(log, "enable append dedup key mode");
                    break;
                case DedupKeyMode::THROW:
                    /// case 3(unique table with sync insert and throw when there has same key with existing parts)
                    cnch_writer.setDedupMode(CnchDedupHelper::DedupMode::THROW);
                    LOG_DEBUG(log, "enable throw dedup key mode");
                    break;
                case DedupKeyMode::REPLACE:
                    /// case 4(unique table with sync insert): In commit stage, acquire the necessary locks to avoid write-write conflicts and then remove duplicate keys between visible parts and temp parts.
                    cnch_writer.setDedupMode(CnchDedupHelper::DedupMode::UPSERT);
                    LOG_TRACE(log, "enable upsert dedup mode");
                    break;
                case DedupKeyMode::IGNORE:
                    /// case 5(unique table with sync insert, when there has same keys, only keep the first occurrences of the row and ignore subsequent occurrences rows)
                    cnch_writer.setDedupMode(CnchDedupHelper::DedupMode::IGNORE);
                    LOG_TRACE(log, "enable insert ignore dedup mode");
                    break;
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Unsupported dedup key mode: {}", context->getSettings().dedup_key_mode.toString());
            }
        }
    }

    initOverwritePartitionPruner();
}

void CloudMergeTreeBlockOutputStream::initOverwritePartitionPruner()
{
    if (!overwrite_partition)
        return;

    if (auto * partition_list = typeid_cast<ASTExpressionList *>(overwrite_partition.get()))
    {
        /// Get overwrite partition ids from query
        for (const auto & partition : partition_list->children)
            overwrite_partition_ids.insert(storage.getPartitionIDFromQuery(partition, context));
    }
    else
    {
        overwrite_partition_ids.insert(storage.getPartitionIDFromQuery(overwrite_partition, context));
    }
}

Block CloudMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void CloudMergeTreeBlockOutputStream::writePrefix()
{
    auto max_threads = context->getSettingsRef().max_threads_for_cnch_dump;
    LOG_DEBUG(log, "dump with {} threads", max_threads);
    cnch_writer.initialize(max_threads);
}

void CloudMergeTreeBlockOutputStream::write(const Block & block)
{
    Stopwatch watch;
    LOG_DEBUG(storage.getLogger(), "Start to write new block of size: {}", block.rows());
    auto temp_parts = convertBlockIntoDataParts(block);
    /// Generate delete bitmaps, delete bitmap is valid only when using delete_flag info for unique table
    LocalDeleteBitmaps bitmaps;
    const auto & txn = context->getCurrentTransaction();

    if (metadata_snapshot->hasUniqueKey())
    {
        /// Handle delete flag and generate emtpy bitmap for unique table in APPEND mode
        for (const auto & part : temp_parts)
        {
            auto delete_bitmap = part->getDeleteBitmap(/*allow_null*/ true);
            if (delete_bitmap && delete_bitmap->cardinality())
            {
                if (dedup_parameters.enable_append_mode)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delete flag can not used in APPEND dedup key mode.");

                bitmaps.emplace_back(LocalDeleteBitmap::createBase(
                    part->info,
                    std::const_pointer_cast<Roaring>(delete_bitmap),
                    txn->getPrimaryTransactionID().toUInt64(),
                    part->bucket_number));
                part->delete_flag = true;
            }
            else if (dedup_parameters.enable_append_mode)
            {
                bitmaps.emplace_back(LocalDeleteBitmap::createBase(
                    part->info, std::make_shared<Roaring>(), txn->getPrimaryTransactionID().toUInt64(), part->bucket_number));
            }
        }
    }
    LOG_DEBUG(storage.getLogger(), "Finish converting block into parts, elapsed {} ms", watch.elapsedMilliseconds());
    watch.restart();

    IMutableMergeTreeDataPartsVector temp_staged_parts;
    if (dedup_parameters.enable_staging_area)
    {
        temp_staged_parts.swap(temp_parts);
    }

    cnch_writer.schedule(temp_parts, bitmaps, temp_staged_parts);
}

MergeTreeMutableDataPartsVector CloudMergeTreeBlockOutputStream::convertBlockIntoDataParts(const Block & block, bool use_inner_block_id)
{
    auto part_log = context->getGlobalContext()->getPartLog(storage.getDatabaseName());
    auto merge_tree_settings = storage.getSettings();
    auto settings = context->getSettingsRef();

    BlocksWithPartition part_blocks;

    /// For unique table, need to ensure that each part does not contain duplicate keys
    /// - when unique key is partition-level, split into sub-blocks first and then dedup the sub-block for each partition
    /// - when unique key is table-level
    /// -   if without version column, should dedup the input block first because split may change row order
    /// -   if use partition value as version, split first because `dedupWithUniqueKey` doesn't evaluate partition key expression
    /// -   if use explicit version, both approach work
    if (metadata_snapshot->hasUniqueKey() && !merge_tree_settings->partition_level_unique_keys
        && !storage.merging_params.partitionValueAsVersion())
    {
        FilterInfo filter_info = dedupWithUniqueKey(block);
        part_blocks = writer.splitBlockIntoParts(
            filter_info.num_filtered ? CnchDedupHelper::filterBlock(block, filter_info) : block,
            settings.max_partitions_per_insert_block,
            metadata_snapshot,
            context);
    }
    else
        part_blocks = writer.splitBlockIntoParts(block, settings.max_partitions_per_insert_block, metadata_snapshot, context);

    std::mutex parts_mutex;
    IMutableMergeTreeDataPartsVector parts;
    size_t rows_size = 0;
    LOG_DEBUG(storage.getLogger(), "Size of blocks is {} after split by partition", part_blocks.size());

    const auto & txn = context->getCurrentTransaction();
    auto primary_txn_id = txn->getPrimaryTransactionID();

    auto processBlockWithBucket = [&](BlockWithPartition & bucketed_block_with_partition) {
        Stopwatch watch;
        auto block_id = use_inner_block_id ? increment.get() : context->getTimestamp();

        MergeTreeMutableDataPartPtr temp_part
            = writer.writeTempPart(bucketed_block_with_partition, metadata_snapshot, context, block_id, primary_txn_id);

        if (txn->isSecondary())
            temp_part->secondary_txn_id = txn->getTransactionID();
        if (part_log)
            part_log->addNewPart(context, temp_part, watch.elapsed());
        LOG_DEBUG(
            storage.getLogger(),
            "Write part {}, {} rows, elapsed {} ms",
            temp_part->name,
            bucketed_block_with_partition.block.rows(),
            watch.elapsedMilliseconds());

        std::lock_guard parts_lock(parts_mutex);
        parts.push_back(std::move(temp_part));
        rows_size += bucketed_block_with_partition.block.rows();
    };

    size_t thread_num = storage.getSettings()->cnch_write_part_threads;
    bool use_thread_pool = thread_num > 1;
    /// Set queue size to unlimited to avoid dead lock
    ThreadPool write_pool(thread_num, thread_num, /*queue_size=*/ 0);
    Stopwatch write_pool_watch;
    auto thread_group = CurrentThread::getGroup();
    auto processBlockWithPartition = [&](BlockWithPartition & block_with_partition) {
        Row original_partition{block_with_partition.partition};

        /// We need to dedup in block before split block by cluster key when unique table supports cluster key because cluster key may be different with unique key. Otherwise, we will lost the insert order.
        if (metadata_snapshot->hasUniqueKey()
            && (merge_tree_settings->partition_level_unique_keys || storage.merging_params.partitionValueAsVersion()))
        {
            FilterInfo filter_info = dedupWithUniqueKey(block_with_partition.block);
            if (filter_info.num_filtered)
                block_with_partition.block = CnchDedupHelper::filterBlock(block_with_partition.block, filter_info);
        }

        auto bucketed_part_blocks = writer.splitBlockPartitionIntoPartsByClusterKey(
            block_with_partition, context->getSettingsRef().max_partitions_per_insert_block, metadata_snapshot, context);
        LOG_TRACE(storage.getLogger(), "Size of blocks is {} after split by bucket", bucketed_part_blocks.size());

        for (auto & bucketed_block_with_partition : bucketed_part_blocks)
        {
            bucketed_block_with_partition.partition = Row(original_partition);
            if (use_thread_pool)
                write_pool.scheduleOrThrowOnError([&, bucketed_block = std::move(bucketed_block_with_partition), thread_group]() mutable {
                    SCOPE_EXIT({
                        if (thread_group)
                            CurrentThread::detachQueryIfNotDetached();
                    });
                    if (thread_group)
                        CurrentThread::attachToIfDetached(thread_group);
                    setThreadName("WritePart");

                    processBlockWithBucket(bucketed_block);
                });
            else
                processBlockWithBucket(bucketed_block_with_partition);
        }
    };

    // Get all blocks of partition by expression
    for (auto & block_with_partition : part_blocks)
    {
        if (overwrite_partition)
        {
            auto partition_id = MergeTreePartition{block_with_partition.partition}.getID(storage);
            if (!overwrite_partition_ids.count(partition_id))
            {
                LOG_DEBUG(storage.getLogger(), "Ignore part block due to not match overwrite partition for partition id: {}", partition_id);
                continue;
            }
        }

        if (use_thread_pool)
            write_pool.scheduleOrThrowOnError([&, block_data = std::move(block_with_partition), thread_group]() mutable {
                SCOPE_EXIT({
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                });
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                setThreadName("WritePart");

                processBlockWithPartition(block_data);
            });
        else
            processBlockWithPartition(block_with_partition);
    }

    if (use_thread_pool)
    {
        write_pool.wait();
        LOG_DEBUG(
            storage.getLogger(),
            "Write pool totally write {} part, {} rows, pool size {}, elapsed {} ms",
            parts.size(),
            rows_size,
            thread_num,
            write_pool_watch.elapsedMilliseconds());
    }

    return parts;
}

void CloudMergeTreeBlockOutputStream::writeSuffix()
{
    cnch_writer.finalize();
    auto & dumped_data = cnch_writer.res;

    if (!dumped_data.parts.empty())
    {
        preload_parts = std::move(dumped_data.parts);
    }

    if (!dumped_data.staged_parts.empty())
    {
        std::move(std::begin(dumped_data.staged_parts), std::end(dumped_data.staged_parts), std::back_inserter(preload_parts));
    }

    try
    {
        writeSuffixImpl();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::INSERTION_LABEL_ALREADY_EXISTS)
        {
            LOG_DEBUG(storage.getLogger(), e.displayText());
            return;
        }
        throw;
    }
}

void CloudMergeTreeBlockOutputStream::writeSuffixImpl()
{
    cnch_writer.preload(preload_parts);

    auto txn = context->getCurrentTransaction();
    if (dynamic_pointer_cast<CnchServerTransaction>(txn) && !disable_transaction_commit)
    {
        txn->setMainTableUUID(storage.getCnchStorageUUID());
        txn->commitV2();
        LOG_DEBUG(storage.getLogger(), "Finishing insert values commit in cnch server.");
    }
    else if (auto worker_txn = dynamic_pointer_cast<CnchWorkerTransaction>(txn))
    {
        if (worker_txn->hasEnableExplicitCommit())
            return;

        auto kafka_table_id = txn->getKafkaTableID();
        if (!kafka_table_id.empty() && !worker_txn->hasEnableExplicitCommit())
        {
            txn->setMainTableUUID(storage.getCnchStorageUUID());
            Stopwatch watch;
            txn->commitV2();
            LOG_TRACE(
                storage.getLogger(), "Committed Kafka transaction {} elapsed {} ms", txn->getTransactionID(), watch.elapsedMilliseconds());
        }
        else if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            /// INITIAL_QUERY means the query is sent from client (and to worker directly), so commit it instantly.
            Stopwatch watch;
            txn->commitV2();
            LOG_TRACE(
                storage.getLogger(),
                "Committed transaction {} elapsed {} ms.", txn->getTransactionID(), watch.elapsedMilliseconds());
        }
        else
        {
            /// TODO: I thought the multiple branches should be unified.
            /// And a exception should be threw in the last `else` clause, otherwise there might be some potential bugs.
        }
    }
}

namespace
{
    struct BlockUniqueKeyComparator
    {
        const ColumnsWithTypeAndName & keys;
        explicit BlockUniqueKeyComparator(const ColumnsWithTypeAndName & keys_) : keys(keys_) { }

        bool operator()(size_t lhs, size_t rhs) const
        {
            for (const auto & key : keys)
                if (key.column->compareAt(lhs, rhs, *key.column, /*nan_direction_hint=*/1))
                    return false;
            return true;
        }
    };

    struct BlockUniqueKeyHasher
    {
        const ColumnsWithTypeAndName & keys;
        explicit BlockUniqueKeyHasher(const ColumnsWithTypeAndName & keys_) : keys(keys_) { }

        size_t operator()(size_t rowid) const
        {
            size_t hash_value{0};
            std::hash<std::string_view> hash_function;
            for (const auto & key : keys)
                hash_value ^= hash_function(key.column.get()->getDataAt(rowid).toView());
            return hash_value;
        }
    };
}

CloudMergeTreeBlockOutputStream::FilterInfo CloudMergeTreeBlockOutputStream::dedupWithUniqueKey(const Block & block)
{
    if (!metadata_snapshot->hasUniqueKey())
        return FilterInfo{};

    const ColumnWithTypeAndName * version_column = nullptr;
    if (metadata_snapshot->hasUniqueKey() && storage.merging_params.hasExplicitVersionColumn())
        version_column = &block.getByName(storage.merging_params.version_column);

    Block block_copy = block;
    metadata_snapshot->getUniqueKeyExpression()->execute(block_copy);

    ColumnsWithTypeAndName keys;
    ColumnsWithTypeAndName string_keys;
    for (auto & name : metadata_snapshot->getUniqueKeyColumns())
    {
        auto & col = block_copy.getByName(name);
        keys.push_back(col);
        if (col.type->getTypeId() == TypeIndex::String)
            string_keys.push_back(col);
    }

    BlockUniqueKeyComparator comparator(keys);
    BlockUniqueKeyHasher hasher(keys);
    /// first rowid of key -> rowid of the last occurrence of the same key in replace/append/throw mode;
    /// first rowid of key -> rowid of the first occurrence of the same key in insert ignore mode.
    phmap::flat_hash_map<size_t, size_t, decltype(hasher), decltype(comparator)> index(keys[0].column->size(), hasher, comparator);

    auto block_size = block_copy.rows();
    FilterInfo res;
    res.filter.assign(block_size, UInt8(1));

    ColumnWithTypeAndName delete_flag_column;
    if (block.has(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::DELETE_FLAG_COLUMN_NAME);

    auto is_delete_row = [&](int rowid) { return delete_flag_column.column && delete_flag_column.column->getBool(rowid); };

    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version
        = [&](int rowid) { return is_delete_row(rowid) && version_column && !version_column->column->getUInt(rowid); };

    /// If there are duplicated keys, only keep the last one
    for (size_t rowid = 0; rowid < block_size; ++rowid)
    {
        if (auto it = index.find(rowid); it != index.end())
        {
            /// When there is no explict version column, use rowid as version number,
            /// Otherwise use value from version column
            size_t old_pos = it->second;
            size_t new_pos = rowid;

            if (context->getSettings().dedup_key_mode == DedupKeyMode::THROW)
            {
                /// In insert throw mode, when multiple records with the same unique key are found,
                /// we will not consider the delete flag column, instead, we will immediately throw an exception.
                throw Exception("Found duplication in the block when insert with setting dedup_key_mode=DedupKeyMode::THROW", ErrorCodes::INCORRECT_DATA);
            }
            else if (context->getSettings().dedup_key_mode == DedupKeyMode::REPLACE || context->getSettings().dedup_key_mode == DedupKeyMode::APPEND)
            {
                if (version_column && !delete_ignore_version(rowid) && version_column->column->getUInt(old_pos) > version_column->column->getUInt(new_pos))
                    std::swap(old_pos, new_pos);
                res.filter[old_pos] = 0;
                it->second = new_pos;
                res.num_filtered++;
            }
            else
            {
                /// In insert ignore mode, when multiple records with the same unique key are found,
                /// we will ignore version column, and save the first row(not deleted) of duplicated keys.
                if (is_delete_row(old_pos))
                    std::swap(old_pos, new_pos);
                res.filter[new_pos] = 0;
                it->second = old_pos;
                res.num_filtered++;
            }
        }
        else
        {
            index[rowid] = rowid;
        }

        /// Check the length limit for string type.
        size_t unique_string_keys_size = 0;
        for (auto & key : string_keys)
            unique_string_keys_size += static_cast<const ColumnString &>(*key.column).getDataAt(rowid).size;
        if (unique_string_keys_size > context->getSettingsRef().max_string_size_for_unique_key)
            throw Exception("The size of unique string keys out of limit", ErrorCodes::UNIQUE_KEY_STRING_SIZE_LIMIT_EXCEEDED);
    }
    return res;
}

}
