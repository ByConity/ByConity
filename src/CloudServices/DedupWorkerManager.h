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

#include <CloudServices/ICnchBGThread.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <CloudServices/DedupScheduler.h>
#include <CloudServices/DedupDataChecker.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class StorageCnchMergeTree;

/**
 * Manage dedup worker of one unique table that enables staging area for writing.
 */
class DedupWorkerManager: public ICnchBGThread
{
public:

    DedupWorkerManager(ContextPtr context, const StorageID & storage_id);

    ~DedupWorkerManager() override;

    void runImpl() override;

    /**
     * Check whether target dedup worker instance is valid.
     */
    DedupWorkerHeartbeatResult reportHeartbeat(const String & worker_table_name);

    std::vector<DedupWorkerStatus> getDedupWorkerStatus();

    void dedupWithHighPriority(const ASTPtr & partition, const ContextPtr & local_context);

    struct DeduperInfo
    {
        /// Make sure worker_storage_id does not contain uuid as there may be multi dedup workers on the same Cnch-Worker side
        DeduperInfo(size_t index_, const StorageID & storage_id_):
            index(index_),
            worker_storage_id(storage_id_.getDatabaseName(), storage_id_.getTableName()) {}

        DeduperInfo(const DeduperInfo & other):
            is_running(other.is_running),
            index(other.index),
            worker_client(other.worker_client),
            worker_storage_id(other.worker_storage_id) {}

        mutable bthread::Mutex mutex;
        bool is_running{false};
        size_t index{0};
        CnchWorkerClientPtr worker_client;
        StorageID worker_storage_id;
    };
    using DeduperInfoPtr = std::shared_ptr<DeduperInfo>;

private:

    void clearData() override;

    void iterate(StoragePtr & storage, StorageCnchMergeTree & cnch_table);

    void initialize(StoragePtr & storage, StorageCnchMergeTree & cnch_table);

    void createDeduperOnWorker(StoragePtr & storage, StorageCnchMergeTree & cnch_table, DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock);

    void selectDedupWorker(StorageCnchMergeTree & cnch_table, DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock);

    void markDedupWorker(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock);

    void stopDeduperWorker(DeduperInfoPtr & info);

    bool checkDedupWorkerStatus(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock);

    static void assignHighPriorityDedupPartition(DeduperInfoPtr & info, const Names & high_priority_partition, std::unique_lock<bthread::Mutex> & info_lock);
    static void unsetWorkerClient(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock);
    static void assignRepairGran(DeduperInfoPtr & info, const DedupGran & dedup_gran, const UInt64 & max_event_time, std::unique_lock<bthread::Mutex> & info_lock);
    static String getDedupWorkerDebugInfo(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock);

    mutable bthread::Mutex deduper_infos_mutex;
    std::atomic<bool> initialized{false};
    std::vector<DeduperInfoPtr> deduper_infos;
    std::shared_ptr<DedupScheduler> dedup_scheduler;

    /// Check if data duplication in background.
    std::shared_ptr<DedupDataChecker> data_checker;
};

}
