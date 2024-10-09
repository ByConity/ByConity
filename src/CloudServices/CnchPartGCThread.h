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

#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/CnchBGThreadPartitionSelector.h>
#include <CloudServices/ICnchBGThread.h>
#include <Core/Names.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/TxnTimestamp.h>

#include <pcg_random.hpp>

namespace DB
{
struct MergeTreeSettings;

/// A thread clean up the stale stuff (like parts, deleted bitmaps, labels) for table
/// Also, mark expired parts accord to table level TTL
/// Just name PartGC thread for convenience because I couldn't get a better name
class CnchPartGCThread : public ICnchBGThread
{
public:
    CnchPartGCThread(ContextPtr context_, const StorageID & id);

    /**
     * Synchronousely perform GC by SYSTEM GC command, useful in sql tests.
     * User should run SYSTEM STOP GC before SYSTEM GC to avoid contention with the bg thread.
     */
    void executeManually(const ASTPtr & partition, ContextPtr local_context);

private:
    void stop() override;
    CnchBGThreadPtr getMergeThread();

    void runImpl() override;
    void clearData() override;

    /**
     * @brief Try select valid invisible parts/delete bitmaps and move their metadata to trash.
     *
     * @param partitions Partitions to be selected.
     * @return Return `true` if any items removed.
     */
    bool doPhaseOneGC(const StoragePtr & istorage, StorageCnchMergeTree & storage, const Strings & partitions);
    /**
     * @param items_removed A reference to a boolean variable that will be set to true if any items were removed.
     * @return total items count (that are still) in partition.
     */
    size_t doPhaseOnePartitionGC(
        const StoragePtr & istorage,
        StorageCnchMergeTree & storage,
        const String & partition_id,
        bool in_wakeup,
        TxnTimestamp gc_timestamp,
        bool & items_removed);
    void movePartsToTrash(const StoragePtr & storage, const ServerDataPartsVector & parts, bool is_staged, String log_type, size_t pool_size, size_t batch_size, bool is_zombie_with_staging_txn_id = false);
    void moveDeleteBitmapsToTrash(const StoragePtr & storage, const DeleteBitmapMetaPtrVector & bitmaps, size_t pool_size, size_t batch_size);
    void clearOldInsertionLabels(const StoragePtr & istorage, StorageCnchMergeTree & storage);

    void clearEmptyPartitions(const StoragePtr & istorage, StorageCnchMergeTree & storage, const Strings & partitions);

    /**
     * @brief Try to delete the actual data and trashed metadata for a table.
     *
     * @return total number of items deleted.
     */
    size_t doPhaseTwoGC(const StoragePtr & istorage, StorageCnchMergeTree & storage);

    /**
     * @brief Task to remove data in the trash. Executed by `data_remover`.
     *
     * If there are no parts to remove, the pace will be adaptively slowed down.
     */
    void runDataRemoveTask();

    TxnTimestamp calculateGCTimestamp(UInt64 delay_second, bool in_wakeup);
    Strings selectPartitions(const StoragePtr & istorage, std::shared_ptr<const MergeTreeSettings> & storage_settings);

    void tryMarkExpiredPartitions(StorageCnchMergeTree & storage, const ServerDataPartsVector & visible_parts);

    std::pair<ServerDataPartsVector, ServerDataPartsVector> processIntermediateParts(ServerDataPartsVector & parts, TxnTimestamp gc_timestamp);

    // void updatePartCache(const String & partition_id, Int64 part_num) override
    // {
    //     if (auto merge = getMergeThread())
    //         merge->updatePartCache(partition_id, -1 * part_num);
    // }

    /// Remove parts/delete bitmaps from remote storage and clear trash.
    size_t round_removing_no_data = 0;
    size_t phase_one_continuous_hits = 0;
    size_t phase_two_continuous_hits = 0;

    /// Delete data files in the trash state in background.
    BackgroundSchedulePool::TaskHolder data_remover;

    CnchBGThreadPartitionSelector::RoundRobinState partition_round_robin_state{};

    BackgroundSchedulePool::TaskHolder checkpoint_task;

    pcg64 rng;
    TxnTimestamp last_gc_timestamp{0};

    std::queue<IMergeTreeDataPartPtr> removing_queue;

    std::weak_ptr<ICnchBGThread> merge_thread;

    String phase_two_start_key;
    /// The total number of items got cleaned from the start key to end key.
    /// Reset when a new round start.
    /// Recovery rates can be more conservative if the value is too low.
    size_t cleaned_items_in_a_round = 0;
};


}
