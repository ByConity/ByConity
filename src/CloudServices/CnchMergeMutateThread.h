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
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Transaction/ICnchTransaction.h>
#include <WorkerTasks/ManipulationList.h>

#include <condition_variable>

namespace DB
{

class CnchWorkerClient;
using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;

class CnchMergeMutateThread;
struct PartMergeLogElement;

struct ManipulationTaskRecord
{
    explicit ManipulationTaskRecord(CnchMergeMutateThread & p) : parent(p) {}
    ~ManipulationTaskRecord();

    CnchMergeMutateThread & parent;

    ManipulationType type;
    std::unique_ptr<ManipulationListEntry> manipulation_entry;
    bool try_execute{false};

    bool remove_task{false};

    String task_id;
    TransactionCnchPtr transaction;

    UInt64 submit_time_ns{0};

    /// Set task_record's commit_start_time once it go into txn commit stage.
    /// There are some other operations may be conflict with merge.
    /// 1. DROP PARTITION - get the current max block id and generate a DropRange part. 
    ///    Need to cancel merge tasks before getting data parts.
    /// 2. INGEST PARTITION - generate new content based on current source parts.
    ///    Need to cancel merge tasks and suspend the merge process before INGEST PARTITION finish.
    /// 3. INSERT OVERWRITE - need to generate a DropRange part to invalid old parts.
    /// So it's possible that other threads may cancel merge tasks.
    /// As we can't control the txn state after it's committing (it's controlled by txn manager),
    /// those committing tasks must be destroyed by txn, but not others(DROP PARTITION, INGEST PARTITION).
    time_t commit_start_time{0};

    ServerDataPartsVector parts;

    /// for heartbeat
    CnchWorkerClientPtr worker;
    size_t lost_count{0};

    Strings getSourcePartNames(bool flatten = false) const;
};

struct FutureManipulationTask
{
    FutureManipulationTask(CnchMergeMutateThread & p, ManipulationType t) : parent(p), record(std::make_unique<ManipulationTaskRecord>(p))
    {
        record->type = t;
    }

    ~FutureManipulationTask();

    FutureManipulationTask & setTryExecute(bool try_execute_)
    {
        record->try_execute = this->try_execute = try_execute_;
        return *this;
    }

    FutureManipulationTask & setMutationEntry(CnchMergeTreeMutationEntry m)
    {
        mutation_entry.emplace(std::move(m));
        return *this;
    }

    TxnTimestamp calcColumnsCommitTime() const;
    FutureManipulationTask & tagSourceParts(ServerDataPartsVector && parts);
    FutureManipulationTask & prepareTransaction();
    std::unique_ptr<ManipulationTaskRecord> moveRecord();

    CnchMergeMutateThread & parent;

    bool try_execute{false};

    std::unique_ptr<ManipulationTaskRecord> record;
    ServerDataPartsVector parts;
    std::optional<CnchMergeTreeMutationEntry> mutation_entry;
};

struct MergeSelectionMetrics
{
    Stopwatch watch;
    size_t elapsed_get_data_parts = 0;
    size_t elapsed_calc_visible_parts = 0;
    size_t elapsed_calc_merge_parts = 0;
    size_t elapsed_select_parts = 0;

    /// #partition returned from partition selector
    size_t num_partitions = 0;
    /// #partition without locked partitions.
    size_t num_unlock_partitions = 0;
    /// total number of parts of above partitions.
    size_t num_source_parts = 0;
    /// #part after calcVisibleParts
    size_t num_visible_parts = 0;
    /// #part without illegal parts (merging part or big part)
    size_t num_legal_visible_parts = 0;

    size_t totalElapsed() const { return elapsed_get_data_parts + elapsed_calc_visible_parts + elapsed_calc_merge_parts + elapsed_select_parts; }

    String toDebugString() const
    {
        WriteBufferFromOwnString wb;
        wb << '{'
            << "elapsed_get_data_parts(us):" << elapsed_get_data_parts
            << ", elapsed_calc_visible_parts:" << elapsed_calc_visible_parts
            << ", elapsed_select_parts:" << elapsed_select_parts
            << "; num_partitions:" << num_partitions
            << ", num_unlock_partitions:" << num_unlock_partitions
            << ", num_source_parts:" << num_source_parts
            << ", num_visible_parts:" << num_visible_parts
            << ", num_legal_visible_parts:" << num_legal_visible_parts
            << '}';
        return wb.str();
    }
};

struct ClusterTaskProgress
{
    UInt64 progress{100}; // default is completed because all parts of a table has same table_definition_hash intially
    UInt64 start_time_seconds{0};

    String toString() const
    {
        String result = std::to_string(progress) + "%";
        if (start_time_seconds)
        {
            time_t sec_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(std::chrono::seconds(start_time_seconds)));
            char buffer[80];
            struct tm * timeinfo;
            timeinfo = localtime(&sec_time);
            strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
            std::string str_time(buffer);
            result += " start at " + str_time;
        }
        return result;
    }
};

class CnchMergeMutateThread : public ICnchBGThread
{
    using TaskRecord = ManipulationTaskRecord;
    using TaskRecordPtr = std::unique_ptr<TaskRecord>;

    friend struct ManipulationTaskRecord;
    friend struct FutureManipulationTask;

public:
    CnchMergeMutateThread(ContextPtr context, const StorageID & id);
    ~CnchMergeMutateThread() override;


    void tryRemoveTask(const String & task_id);
    void finishTask(const String & task_id, std::function<void(const Strings &, UInt64)> && precommit_parts);
    bool removeTasksOnPartitions(const std::unordered_set<String> & partitions);

    String triggerPartMerge(StoragePtr & istorage, const String & partition_id, bool final, bool try_select, bool try_execute);
    void triggerPartMutate(StoragePtr storage);

    void waitTasksFinish(const std::vector<String> & task_ids, UInt64 timeout_ms);
    void waitMutationFinish(UInt64 mutation_commit_time, UInt64 timeout_ms);
    MergeTreeMutationStatusVector getAllMutationStatuses();
    ClusterTaskProgress getReclusteringTaskProgress();
    void setTableClusterStatus();

private:
    void preStart() override;
    void clearData() override;

    void runHeartbeatTask();

    void runImpl() override;

    /// Merge
    PartMergeLogElement createPartMergeLogElement();
    void writePartMergeLogElement(StoragePtr & istorage, PartMergeLogElement & elem, const MergeSelectionMetrics & metrics);

    bool tryMergeParts(StoragePtr & istorage, StorageCnchMergeTree & storage);
    bool trySelectPartsToMerge(StoragePtr & istorage, StorageCnchMergeTree & storage, MergeSelectionMetrics & metrics);
    String submitFutureManipulationTask(const StorageCnchMergeTree & storage, FutureManipulationTask & future_task, bool maybe_sync_task = false);

    // Mutate
    void removeMutationEntryFromKV(const CnchMergeTreeMutationEntry & entry, std::lock_guard<std::mutex> &);
    void calcMutationPartitions(CnchMergeTreeMutationEntry & mutate_entry, StoragePtr & istorage, StorageCnchMergeTree & storage);
    bool tryMutateParts(StoragePtr & istorage, StorageCnchMergeTree & storage);

    void removeTaskImpl(const String & task_id, std::lock_guard<std::mutex> & lock, TaskRecordPtr * out_task_record = nullptr);

    CnchWorkerClientPtr getWorker(ManipulationType type, const ServerDataPartsVector & all_parts);

    bool needCollectExtendedMergeMetrics();

    NameSet copyCurrentlyMergingMutatingParts()
    {
        std::lock_guard lock(currently_merging_mutating_parts_mutex);
        return currently_merging_mutating_parts;
    }

    Strings removeLockedPartition(const Strings & partitions);

    std::mutex currently_merging_mutating_parts_mutex;
    NameSet currently_merging_mutating_parts;

    std::condition_variable currently_synchronous_tasks_cv; /// for waitTasksFinish function
    std::mutex currently_synchronous_tasks_mutex;
    NameSet currently_synchronous_tasks;

    std::mutex task_records_mutex;
    std::unordered_map<String, TaskRecordPtr> task_records;

    std::mutex try_merge_parts_mutex; /// protect tryMergeParts(), triggerPartMerge()
    /// protected by try_merge_parts_mutex
    std::queue<std::unique_ptr<FutureManipulationTask>> merge_pending_queue;
    /// round robin states used by partition selector, protected by try_merge_parts_mutex
    CnchBGThreadPartitionSelector::RoundRobinState partition_round_robin_state{};
    /// Partitions that postponed from merge for a while, protected by try_merge_parts_mutex
    std::unordered_map<String, time_t> postponed_partitions{};

    std::mutex try_mutate_parts_mutex; /// protect tryMutateParts(), getMutationStatus()
    /// Partitions that all parts are mutated or under mutating.
    NameSet scheduled_mutation_partitions;
    /// Partitions that all parts are mutated.
    NameSet finish_mutation_partitions;
    std::optional<CnchMergeTreeMutationEntry> current_mutate_entry;

    /// Separate quota for merge & mutation tasks
    std::atomic<int> running_merge_tasks{0};
    std::atomic<int> running_mutation_tasks{0};

    time_t last_heartbeat_time = 0;
    time_t last_time_collect_extended_merge_metrics = 0;

    /// the start time of current MergeMutateThread.
    UInt64 thread_start_time{0};
    /// the last schedule time of MergeMutateThread. Its a physical timestamp.
    UInt64 last_schedule_time{0};

    std::mutex worker_pool_mutex;
    String vw_name;
    String pick_worker_algo;
    VirtualWarehouseHandle vw_handle;

    UInt64 num_default_workers_in_vw_settings{0};

    std::atomic_bool shutdown_called{false};
};


}
