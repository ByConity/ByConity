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

#include <CloudServices/CnchMergeMutateThread.h>

#include <common/logger_useful.h>
#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/selectPartsToMerge.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchWorkerClient.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Common/Configurations.h>
#include <Common/ProfileEvents.h>
#include <Common/TestUtils.h>
#include <Interpreters/PartMergeLog.h>
#include <Interpreters/ServerPartLog.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/formatAST.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/PartCacheManager.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/ManipulationType.h>

#include <common/sleep.h>
#include <chrono>
#include <algorithm>

namespace ProfileEvents
{
    extern const Event Manipulation;
    extern const Event ManipulationSuccess;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int UNKNOWN_TABLE;
    extern const int TOO_MANY_SIMULTANEOUS_TASKS;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

namespace
{
    constexpr auto DELAY_SCHEDULE_TIME_IN_SECOND = 60ul;
    constexpr auto DELAY_SCHEDULE_RANDOM_TIME_IN_SECOND = 3ul;

    bool needMutate(const ServerDataPartPtr & part, const TxnTimestamp & commit_ts, bool change_schema, bool is_recluster, const TableDefinitionHash & table_definition_hash)
    {
        if (is_recluster)
        {
            return !table_definition_hash.match(part->part_model().table_definition_hash());
        }
        /// Some mutation commands (@see MutationCommands::changeSchema()) will not change the table schema
        /// which means it will not update columns_commit_time. To track those mutation commands,
        /// we add a new field `mutation_commit_time` in part metadata. And it's set to 0 for a new part by default.
        /// To check whether a part `p` need to do a mutation `m`.
        /// if (m change schema): check p.columns_commit_time < m.commit_time.
        /// if (m not change schema): check p.mutation_commit_time < m.commit_time and p.min_block < m.commit_time.
        /// As mutation_commit_time is 0 by default, we need use part's min_block when mutation_commit_time is 0.
        if (change_schema)
            return part->getColumnsCommitTime() < commit_ts;
        else
            return part->getMutationCommitTime() < commit_ts && static_cast<UInt64>(part->info().min_block) < commit_ts;
    }

    TxnTimestamp getFirstMutation(const ServerDataPartPtr & part, const std::vector<std::pair<TxnTimestamp, bool>> & mutations)
    {
        for (const auto & [commit_time, change_schema] : mutations)
            if (needMutate(part, commit_time, change_schema, false, {}))
                return commit_time;
        return TxnTimestamp::maxTS();
    }

    ServerCanMergeCallback getMergePred(const NameSet & merging_mutating_parts_snapshot, const std::vector<std::pair<TxnTimestamp, bool>> & mutations)
    {
        return [&](const ServerDataPartPtr & lhs, const ServerDataPartPtr & rhs) -> bool
        {
            if (!lhs)
                return !merging_mutating_parts_snapshot.count(rhs->name());

            if (merging_mutating_parts_snapshot.count(lhs->name()) || merging_mutating_parts_snapshot.count(rhs->name()))
                return false;

            auto lhs_columns_commit_time = lhs->getColumnsCommitTime();
            auto rhs_columns_commit_time = rhs->getColumnsCommitTime();
            /// We can't find the right table_schema for parts which column_commit_time = 0
            if (!lhs_columns_commit_time || !rhs_columns_commit_time)
                return false;

            /// Consider this case:
            ///     T0: commit part_1                                     | part columns_commit_time: a
            ///     T1: alter table (change columns): mutation_1          | table columns_commit_time: a -> b
            ///     T2: commit part_2                                     | part columns_commit_time: b
            ///     T3: part_1 apply mutation_1: chain a new partial part | part columns_commit_time: a -> b

            /// We can't merge part part_1 and part_2 between T2 and T3 as their columns_commit_time are different.
            /// We can merge part_1 and part_2 after T3 as their columns_commit_time both are b.
            if (lhs_columns_commit_time != rhs_columns_commit_time)
                return false;

            /// The same as above for mutation_commit_time.
            auto lhs_mutation_commit_time = lhs->getMutationCommitTime();
            auto rhs_mutation_commit_time = rhs->getMutationCommitTime();
            if (lhs_mutation_commit_time != rhs_mutation_commit_time)
                return false;

            /// Consider this case:
            ///     T0: create table                                       | table columns_commit_time: a
            ///     T1: commit part_1                                      | part_1 columns_commit_time: a, mutation_commit_time: 0
            ///     T2: alter table (not change schema): mutation_1        | not update columns_commit_time
            ///     T3: commit part_2                                      | part_2 columns_commit_time: a, mutation_commit_time: 0
            /// part_1 and part_2 have the same columns_commit_time and mutation_commit_time.
            /// But obviously we should not merge part_1 and part_2. part_1's next mutation is mutation_1 which is invisible for part_2.
            /// In a word, the first needed mutation for parts should be the same.
            auto lhs_first_mutation = getFirstMutation(lhs, mutations);
            auto rhs_first_mutation = getFirstMutation(rhs, mutations);
            return lhs_first_mutation == rhs_first_mutation;
        };
    }
}

/// We maintain merging_mutating_parts based on the merge task's lifecycle.
/// Source parts are added to merging_mutating_parts when task is created, see FutureManipulationTask::tagSourceParts.
/// And they are removed from merging_mutating_parts when task record is destroyed.
/// As the merge txn is committed in a 2-phase style, we need to hold the task record until txn phase-2 finish (success or fail).
/// * phase 1 - ::finishTask is called. We mark the task record as committing by set commit_start_time, instead of destroy the record.
/// * phase 2 - We use ITransaction::tryCleanMergeTagger (which is called after phase 2 finish) to destroy the task record.
ManipulationTaskRecord::~ManipulationTaskRecord()
{
    try
    {
        if (!try_execute && !parts.empty())
        {
            std::lock_guard lock(parent.currently_merging_mutating_parts_mutex);
            for (auto & part : parts)
            {
                parent.currently_merging_mutating_parts.erase(part->name());
                auto prev_part = part->tryGetPreviousPart();
                while(prev_part)
                {
                    parent.currently_merging_mutating_parts.erase(prev_part->name());
                    prev_part = prev_part->tryGetPreviousPart();
                }
            }
        }

        {
            std::lock_guard lock(parent.currently_synchronous_tasks_mutex);
            parent.currently_synchronous_tasks.erase(task_id);
            parent.currently_synchronous_tasks_cv.notify_all();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

Strings ManipulationTaskRecord::getSourcePartNames(bool flatten) const
{
    Strings res;
    for (const auto & part : parts)
    {
        res.emplace_back(part->name());
        if (likely(flatten))
        {
            auto prev_part = part->tryGetPreviousPart();
            while (prev_part)
            {
                res.emplace_back(prev_part->name());
                prev_part = prev_part->tryGetPreviousPart();
            }
        }
    }
    return res;
}

FutureManipulationTask::~FutureManipulationTask()
{
    try
    {
        if (!try_execute && !parts.empty())
        {
            std::lock_guard lock(parent.currently_merging_mutating_parts_mutex);
            for (auto & part : parts)
            {
                parent.currently_merging_mutating_parts.erase(part->name());
                auto prev_part = part->tryGetPreviousPart();
                while(prev_part)
                {
                    parent.currently_merging_mutating_parts.erase(prev_part->name());
                    prev_part = prev_part->tryGetPreviousPart();
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

/// Add source parts (include invisible parts) to merging_mutating_parts.
FutureManipulationTask & FutureManipulationTask::tagSourceParts(ServerDataPartsVector && parts_)
{
    auto check_and_add = [&](const auto & part_name) {
        if (parent.currently_merging_mutating_parts.count(part_name))
            throw Exception("Part '" + part_name + "' was already in other Task, cancel merge.", ErrorCodes::ABORTED);
        parent.currently_merging_mutating_parts.emplace(part_name);
    };

    if (!record->try_execute)
    {
        std::lock_guard lock(parent.currently_merging_mutating_parts_mutex);

        for (const auto & p : parts_)
        {
            check_and_add(p->name());

            auto prev_part = p->tryGetPreviousPart();
            while (prev_part)
            {
                check_and_add(prev_part->name());
                prev_part = prev_part->tryGetPreviousPart();
            }
        }
    }

    if (parent.log->trace())
    {
        WriteBufferFromOwnString wb;
        for (const auto & p : parts_)
            wb << p->name() << " ";
        LOG_TRACE(parent.log, "Added parts to merging_mutating_parts: {}", wb.str());
    }

    parts = std::move(parts_);
    return *this;
}

TxnTimestamp FutureManipulationTask::calcColumnsCommitTime() const
{
    if (parts.empty())
        throw Exception("The `parts` of manipulation task cannot be empty", ErrorCodes::LOGICAL_ERROR);

    if (mutation_entry)
        return mutation_entry->columns_commit_time;
    else
        return parts.front()->part_model().columns_commit_time();
}

FutureManipulationTask & FutureManipulationTask::prepareTransaction()
{
    if (parts.empty())
        throw Exception("The `parts` of manipulation task cannot be empty", ErrorCodes::LOGICAL_ERROR);

    if ((record->type == ManipulationType::Mutate || record->type == ManipulationType::Clustering) && !mutation_entry)
        throw Exception("The `mutation_entry` is not set for Mutation or Clustering task", ErrorCodes::LOGICAL_ERROR);

    auto & txn_coordinator = parent.getContext()->getCnchTransactionCoordinator();
    record->transaction = txn_coordinator.createTransaction(
        CreateTransactionOption().setInitiator(CnchTransactionInitiator::Merge).setPriority(CnchTransactionPriority::low));
    record->transaction->setMainTableUUID(parent.storage_id.uuid);

    return *this;
}

std::unique_ptr<ManipulationTaskRecord> FutureManipulationTask::moveRecord()
{
    if (!record->transaction)
        throw Exception("The transaction of manipulation task is not initialized", ErrorCodes::LOGICAL_ERROR);

    record->parts.clear();
    std::swap(record->parts, parts);
    return std::move(record);
}

CnchMergeMutateThread::CnchMergeMutateThread(ContextPtr context_, const StorageID & id)
    : ICnchBGThread(context_->getGlobalContext(), CnchBGThreadType::MergeMutate, id)
{
}

CnchMergeMutateThread::~CnchMergeMutateThread()
{
    shutdown_called = true;

    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void CnchMergeMutateThread::preStart()
{
    LOG_DEBUG(log, "preStarting MergeMutateThread for table {}", storage_id.getFullTableName());

    thread_start_time = getContext()->getTimestamp();
    catalog->setMergeMutateThreadStartTime(storage_id, thread_start_time);
    is_stale = false;
}

void CnchMergeMutateThread::clearData()
{
    LOG_DEBUG(log, "Dropping MergeMutate Data for table {}", storage_id.getFullTableName());

    std::lock_guard lock_merge(try_merge_parts_mutex);
    std::lock_guard lock_mutate(try_mutate_parts_mutex);

    std::unordered_set<CnchWorkerClientPtr> workers;
    {
        std::lock_guard lock(task_records_mutex);
        for (auto & [_, task_record] : task_records)
            workers.emplace(task_record->worker);
    }

    for (const auto & worker : workers)
    {
        try
        {
            worker->shutdownManipulationTasks(storage_id.uuid);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to update status of tasks on " + worker->getHostWithPorts().toDebugString());
        }
    }

    {
        decltype(merge_pending_queue) empty_for_clear;
        merge_pending_queue.swap(empty_for_clear);
    }

    {
        std::lock_guard lock(task_records_mutex);
        LOG_DEBUG(log, "Remove all {} merge tasks when shutdown MergeTask.", task_records.size());
        task_records.clear();

        running_merge_tasks = 0;
        running_mutation_tasks = 0;
    }

    {
        std::lock_guard lock(currently_synchronous_tasks_mutex);
        currently_synchronous_tasks.clear();
        currently_synchronous_tasks_cv.notify_all();
    }
}

void CnchMergeMutateThread::runHeartbeatTask()
{
    const auto & root_config = getContext()->getRootConfig();

    {
        std::lock_guard lock(task_records_mutex);
        if (task_records.empty())
            return;
    }

    auto now = time(nullptr);
    if (now - last_heartbeat_time < static_cast<time_t>(root_config.cnch_task_heartbeat_interval))
        return;
    last_heartbeat_time = now;

    std::unordered_map<CnchWorkerClientPtr, Strings> worker_with_tasks;

    {
        std::lock_guard lock(task_records_mutex);
        for (auto & [task_id, task] : task_records)
        {
            worker_with_tasks[task->worker].push_back(task->task_id);
        }
    }

    auto on_failure = [this, &root_config](std::unordered_map<String, TaskRecordPtr>::iterator it, std::lock_guard<std::mutex> & lock) {
        if (auto & task = it->second; ++task->lost_count >= root_config.cnch_task_heartbeat_max_retries)
        {
            LOG_WARNING(log, "Merge/Mutate task_id: {} is lost, remove it from MergeList.", it->first);
            if (task->type == ManipulationType::Mutate)
            {
                /// Remove the partition id from scheduled_mutation_partitions
                /// to reschedule mutation tasks of this partition
                const auto & partition_id = task->parts.front()->info().partition_id;
                scheduled_mutation_partitions.erase(partition_id);
            }

            if (time(nullptr) - task->commit_start_time > root_config.cnch_merge_txn_commit_timeout)
                removeTaskImpl(task->task_id, lock);
        }
    };

    for (auto & [worker, request_tasks] : worker_with_tasks)
    {
        try
        {
            auto response_tasks = worker->touchManipulationTasks(storage_id.uuid, request_tasks);

            std::lock_guard lock(task_records_mutex);
            for (auto & request_task : request_tasks)
            {
                if (auto iter = task_records.find(request_task); iter != task_records.end())
                {
                    if (0 == response_tasks.count(request_task)) /// Not found in response
                        on_failure(iter, lock);
                    else /// Found in response
                        iter->second->lost_count = 0;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to update status of tasks on " + worker->getRPCAddress());

            std::lock_guard lock(task_records_mutex);
            for (auto & request_task : request_tasks)
            {
                if (auto iter = task_records.find(request_task); iter != task_records.end())
                    on_failure(iter, lock);
            }
        }
    }
}

void CnchMergeMutateThread::runImpl()
{
    auto local_context = getContext();

    if (local_context->getRootConfig().debug_disable_merge_mutate_thread.safeGet())
    {
        scheduled_task->scheduleAfter(10 * 1000);
        return;
    }

    UInt64 fetched_thread_start_time = 0;
    UInt64 current_ts = 0;
    try
    {
        current_ts = local_context->getTimestamp() >> 18;
        fetched_thread_start_time = catalog->getMergeMutateThreadStartTime(storage_id);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// If failed to get current ts or fetch start time from catalog, wait for a while and try again.
        scheduled_task->scheduleAfter(1 * 1000);
        return;
    }

    /// only if the thread_start_time equals to that in catalog the MergeMutateThread can schedule background tasks and accept task result.
    if (fetched_thread_start_time != thread_start_time)
    {
        /// If thread_start_time is not equal to that in catalog. The MergeMutateThread will stop running and wait to be removed or scheduled again.
        LOG_ERROR(
            log,
            "Current MergeMutateThread start time {} does not equal to that in catalog {}. Remove all {} merge tasks and stop current BG thread.",
            thread_start_time, fetched_thread_start_time, task_records.size());

        clearData();
        return;
    }
    /// now fetched_thread_start_time == thread_start_time

    /// Delay first schedule for some time (60+s) to make sure other old duplicate threads has been killed.
    if (last_schedule_time == 0)
    {
        std::mt19937 generator(std::random_device{}());
        last_schedule_time = current_ts;
        auto delay_ms = 1000 * ((isCIEnv() ? 0 : DELAY_SCHEDULE_TIME_IN_SECOND) + generator() % DELAY_SCHEDULE_RANDOM_TIME_IN_SECOND);
        scheduled_task->scheduleAfter(delay_ms);
        LOG_DEBUG(log, "Schedule after {} ms because of last_schedule_time = 0", delay_ms);
        return;
    }
    last_schedule_time = current_ts;

    try
    {
        runHeartbeatTask();

        auto istorage = getStorageFromCatalog();
        auto & storage = checkAndGetCnchTable(istorage);
        auto storage_settings = storage.getSettings();

        if (istorage->is_dropped)
        {
            LOG_DEBUG(log, "Table was dropped, wait for removing...");
            scheduled_task->scheduleAfter(10 * 1000);
            return;
        }

        {
            std::lock_guard lock(worker_pool_mutex);
            vw_name = storage.getSettings()->cnch_vw_write;
            /// TODO: pick_worker_algo = storage.getSettings()->cnch_merge_pick_worker_algo;
            vw_handle = getContext()->getVirtualWarehousePool().get(vw_name);

            /// Update default worker number, used to set expected_parts_number
            const String default_vw_name = storage.getSettings()->cnch_vw_default;
            auto default_vw_handle = getContext()->getVirtualWarehousePool().get(default_vw_name);
            num_default_workers_in_vw_settings = default_vw_handle->getSettingsRef().num_workers;
        }

        bool merge_success = false;

        try
        {
            auto max_mutation_task_num = storage_settings->max_addition_mutation_task_num;
            if (running_mutation_tasks < max_mutation_task_num)
            {
                merge_success |= tryMutateParts(istorage, storage);
            }
            else
            {
                LOG_DEBUG(log, "Too many mutation tasks (current: {}, max: {})", running_mutation_tasks, max_mutation_task_num);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        try
        {
            /// Ensure not submit too many merge tasks even if addition_bg_task and batch_select are active.
            auto max_bg_task_num = static_cast<int>(storage_settings->max_addition_bg_task_num.value);
            if (running_merge_tasks < max_bg_task_num)
            {
                merge_success |= tryMergeParts(istorage, storage);
            }
            else
            {
                LOG_DEBUG(log, "Too many merge tasks (current: {}, max: {})", running_merge_tasks, max_bg_task_num);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (merge_success)
            scheduled_task->scheduleAfter(1 * 1000);
        else
            scheduled_task->scheduleAfter(10 * 1000);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        scheduled_task->scheduleAfter(10 * 1000);
    }
}

PartMergeLogElement CnchMergeMutateThread::createPartMergeLogElement()
{
    PartMergeLogElement elem;
    elem.database = storage_id.database_name;
    elem.table = storage_id.table_name;
    elem.uuid = storage_id.uuid;
    elem.event_type = PartMergeLogElement::MERGE_SELECT;
    elem.event_time = time(nullptr);
    return elem;
}

void CnchMergeMutateThread::writePartMergeLogElement(
    StoragePtr & istorage, PartMergeLogElement & elem, const MergeSelectionMetrics & metrics)
{
    auto local_context = getContext();

    auto part_merge_log = local_context->getPartMergeLog();
    if (!part_merge_log)
        return;

    elem.extended = needCollectExtendedMergeMetrics();
    if (elem.extended)
    {
        std::unique_lock lock(task_records_mutex);
        for (auto & [_, task_record] : task_records)
        {
            elem.future_committed_parts += 1;
            elem.future_covered_parts += task_record->parts.size();
        }
        lock.unlock();

        std::unordered_map<String, PartitionFullPtr> partition_metrics;
        if (auto cache_manager = local_context->getPartCacheManager())
            cache_manager->getPartsInfoMetrics(*istorage, partition_metrics, false);
        for (auto & [_, p_metric] : partition_metrics)
        {
            if (p_metric && p_metric->partition_info_ptr && p_metric->partition_info_ptr->metrics_ptr)
                elem.current_parts += p_metric->partition_info_ptr->metrics_ptr->read().total_parts_number;
        }
    }

    elem.duration_us = metrics.watch.elapsedMicroseconds();
    elem.get_parts_duration_us = metrics.elapsed_get_data_parts;
    elem.select_parts_duration_us = metrics.elapsed_select_parts;

    part_merge_log->add(elem);
}

bool CnchMergeMutateThread::tryMergeParts(StoragePtr & istorage, StorageCnchMergeTree & storage)
{
    std::lock_guard lock(try_merge_parts_mutex);

    LOG_TRACE(log, "Try to merge parts... pending task {} running task {}", merge_pending_queue.size(), running_merge_tasks);

    bool result = true;

    auto part_merge_log_elem = createPartMergeLogElement();
    MergeSelectionMetrics metrics;

    if (merge_pending_queue.empty())
    {
        result = trySelectPartsToMerge(istorage, storage, metrics);
    }

    auto storage_settings = storage.getSettings();
    /// When submitting tasks, we limit the max concurrency and also the tasks submitted in each round
    /// as the merger_mutator may generate hundreds tasks in one time.
    size_t num_workers = vw_handle->getNumWorkers();
    size_t tasks_per_worker = storage_settings->max_replicated_merges_in_queue.value;
    size_t max_tasks_in_total = std::max(num_workers * tasks_per_worker, storage_settings->max_addition_bg_task_num.value);
    size_t max_tasks_in_round = std::max(num_workers, storage_settings->max_partition_for_multi_select.value);

    for (size_t i = 0; i < max_tasks_in_round //
         && static_cast<size_t>(running_merge_tasks.load(std::memory_order_relaxed)) < max_tasks_in_total //
         && !merge_pending_queue.empty();
         ++i)
    {
        auto future_task = std::move(merge_pending_queue.front());
        merge_pending_queue.pop();

        part_merge_log_elem.new_tasks += 1;
        part_merge_log_elem.source_parts_in_new_tasks += future_task->record->parts.size();

        submitFutureManipulationTask(storage, *future_task);
    }

    // try
    // {
    //     writePartMergeLogElement(istorage, part_merge_log_elem, metrics);
    // }
    // catch (...)
    // {
    //     tryLogCurrentException(__PRETTY_FUNCTION__);
    // }

    return result;
}

void removeUnselectableParts(
    ServerDataPartsVector & visible_parts,
    NameSet & merging_mutating_parts_snapshot,
    std::multimap<String, UInt64> & unselectable_part_rows,
    UInt64 max_bytes,
    UInt64 max_rows)
{
    visible_parts.erase(
        std::remove_if(
            visible_parts.begin(),
            visible_parts.end(),
            [&merging_mutating_parts_snapshot, &unselectable_part_rows, max_bytes, max_rows](const auto & p) {
                if (merging_mutating_parts_snapshot.erase(p->name())
                        || p->part_model().rows_count() >= max_rows * 0.9
                        || p->part_model().size() >= max_bytes * 0.9)
                {
                    unselectable_part_rows.emplace(p->info().partition_id, p->part_model().rows_count());
                    return true;
                }
                return false;
            }),
        visible_parts.end()
    );
}

/// Merge task selection is done by multiple roles.
/// Firstly, we use PartitionSelector to select a few partitions that most recently ingested or have slowlest merge speed.
/// Then, we get all visible parts in these partitions, and use MergeSelector to get some merge(manipulation) tasks.
/// However, both PartitionSelector and MergeSelector will need some information about the historical statistics of insertion
///     and merge, which is stored in BgTaskStatistics.
/// What's more, there are some difference MergeSelector implimentations, e.g. simple, dance, and they may have different behavior.
///     So we use a extra role AdaptiveController as the interface of BgTaskStatistics. It need a input of expected parts, and
///     will controll the MergeSelector not be too aggressive.
bool CnchMergeMutateThread::trySelectPartsToMerge(StoragePtr & istorage, StorageCnchMergeTree & storage, MergeSelectionMetrics & metrics)
{
    /// 6 steps of selecting parts to merge. (the order is important)
    /// 1. copy currently_merging_mutating_parts (do it before getting parts to avoid removing parts after we get a part list).
    /// 2. get parts & calculate visible parts.
    /// 3. get mutation list (do it after getting parts to make sure we capture all mutations of those parts).
    /// 4. create merge predicate based on merging_mutating_parts_snapshot and mutation list.
    /// 5. do selecting based on merge predicate.
    /// 6. push tasks to pending queue.
    auto storage_settings = storage.getSettings();

    SCOPE_EXIT(if (metrics.totalElapsed() >= 200 * 1000) {
        LOG_DEBUG(log, "trySelectPartsToMerge digest: {}", metrics.toDebugString());
    });

    /// Step 1: copy currently_merging_mutating_parts
    /// Must do it before getting data parts so that the copy won't change during selection.
    /// Because we can accept stale data parts but cannot accept stale merging_mutating_parts
    NameSet merging_mutating_parts_snapshot = copyCurrentlyMergingMutatingParts();

    /// Step 2: get parts & calculate visible parts.
    Stopwatch watch;
    ServerDataPartsVector data_parts;

    /// Step 2.1: select partitions by bg_task_stats, round_robin_state and postponed_partitions
    auto bg_task_stats = MergeTreeBgTaskStatisticsInitializer::instance().getOrCreateTableStats(storage.getStorageID());
    auto get_all_partitions_callback = [&istorage, &storage, &bg_task_stats, this] () {
        auto partition_list = getContext()->getCnchCatalog()->getPartitionList(istorage, nullptr);
        storage.filterPartitionByTTL(partition_list, time(nullptr));
        Strings partition_ids;
        for (const auto & partition : partition_list)
            partition_ids.emplace_back(partition->getID(storage));

        /// Add all unknown partitions to bg task stats, so these partitions can be selected by partition selector next time.
        bg_task_stats->fillMissingPartitions(partition_ids);
        return partition_ids;
    };

    CnchBGThreadPartitionSelector partition_selector(storage.getStorageID(), bg_task_stats, postponed_partitions, get_all_partitions_callback);
    auto partitions = partition_selector.selectForMerge(
        /* n = */ storage_settings->max_partition_for_multi_select.value,
        /* round_robin_interval = */ storage_settings->cnch_merge_round_robin_partitions_interval.value,
        partition_round_robin_state,
        /* only_realtime_partitions = */ storage_settings->cnch_merge_only_realtime_partition);

    if (partitions.empty())
    {
        LOG_TRACE(log, "Skip empty table");
        return false;
    }

    metrics.num_partitions = partitions.size();
    partitions = removeLockedPartition(partitions);
    metrics.num_unlock_partitions = partitions.size();

    /// Step 2.2: get all visible parts in selected partitions
    if (partitions.empty())
    {
        LOG_TRACE(log, "There is no partition to merge");
        return false;
    }
    else
    {
        data_parts = catalog->getServerDataPartsInPartitions(istorage, partitions, getContext()->getTimestamp(), nullptr);
    }
    metrics.num_source_parts = data_parts.size();

    if (data_parts.empty())
    {
        time_t now = time(nullptr);
        for (const auto & p : partitions)
            postponed_partitions[p] = now;
    }

    metrics.elapsed_get_data_parts = watch.elapsedMicroseconds();
    watch.restart();

    auto visible_parts = CnchPartsHelper::calcVisibleParts(data_parts, false);
    metrics.num_visible_parts = visible_parts.size();
    metrics.elapsed_calc_visible_parts = watch.elapsedMicroseconds();
    watch.restart();

    if (visible_parts.empty())
    {
        LOG_TRACE(log, "There is no visible parts, exit merge selection.");
        return false;
    }

    if (int max_merge_task = storage_settings->max_addition_bg_task_num.value; running_merge_tasks >= max_merge_task)
    {
        LOG_DEBUG(log, "Too many concurrent merge tasks(curr: {}, max: {}. will retry later.", running_merge_tasks, max_merge_task);
        return false;
    }

    /// Step 3: get mutation list
    /// We need to find the first mutation in the mutation list for each part, and compare mutation versions in the merge predicate.
    std::map<TxnTimestamp, CnchMergeTreeMutationEntry> mutation_entries;
    std::vector<std::pair<TxnTimestamp, bool>> mutation_timestamps;
    catalog->fillMutationsByStorage(storage_id, mutation_entries);
    mutation_timestamps.reserve(mutation_entries.size());
    for (const auto & [_, mutation_entry] : mutation_entries)
        mutation_timestamps.emplace_back(mutation_entry.commit_time, mutation_entry.commands.changeSchema());

    /// Calc merge parts
    /// visible_parts = calcServerMergeParts(storage, visible_parts, partitions_from_cache);
    /// metrics.elapsed_calc_merge_parts = watch.elapsedMicroseconds();
    /// watch.restart();

    /// TODO: support checkpoints

    /// Used to calculate total rows of each partition so we can prevent generating huge merge tasks.
    std::multimap<String, UInt64> unselectable_part_rows;

    auto max_bytes = std::min(
        storage_settings->cnch_merge_max_total_bytes_to_merge.value,
        storage_settings->max_bytes_to_merge_at_max_space_in_pool.value);
    /// If selecting nonadjacent parts is allowed,
    /// we will filter out some parts before selecting:
    /// 1. all merging parts.
    /// 2. a single part that big enough (limited by rows | bytes).
    if (storage.getSettings()->cnch_merge_select_nonadjacent_parts)
    {
        size_t max_rows = storage_settings->cnch_merge_max_total_rows_to_merge;
        removeUnselectableParts(visible_parts, merging_mutating_parts_snapshot, unselectable_part_rows, max_bytes, max_rows);

        if (visible_parts.empty())
        {
            LOG_TRACE(log, "There is no visible parts, exit merge selection.");
            return false;
        }
    }
    else
    {
        for (const auto & p: visible_parts)
        {
            if (merging_mutating_parts_snapshot.count(p->name()) > 0)
                unselectable_part_rows.emplace(p->info().partition_id, p->part_model().rows_count());
        }
    }

    metrics.num_legal_visible_parts = visible_parts.size();

    /// Step 4: create merge predicate
    auto can_merge_callback = getMergePred(merging_mutating_parts_snapshot, mutation_timestamps);

    SelectPartsToMergeSettings select_settings = SelectPartsToMergeSettings {
        .max_total_size_to_merge = max_bytes,
        .num_default_workers = num_default_workers_in_vw_settings,
        .aggressive = false,
        .enable_batch_select = storage_settings->cnch_merge_enable_batch_select,
        .final = false,
        // .merge_with_ttl_allowed = false,
    };

    /// Step 5: do selecting.
    std::vector<ServerDataPartsVector> res;
    [[maybe_unused]] auto decision = selectPartsToMerge(
        storage,
        res,
        visible_parts,
        unselectable_part_rows,
        can_merge_callback,
        select_settings,
        log); /// log

    metrics.elapsed_select_parts = watch.elapsedMicroseconds();
    watch.restart();

    LOG_DEBUG(log, "Selected {} candidate groups", res.size());

    time_t now = time(nullptr);
    if (res.empty())
    {
        for (const auto & p : partitions)
            postponed_partitions[p] = now;
        return false;
    }

    /// Step 6: push tasks to pending queue
    std::unordered_map<String, UInt32> selected_tasks_by_partition{};
    for (const auto & p : partitions)
        selected_tasks_by_partition.emplace(p, 0);

    for (auto & selected_parts : res)
    {
        selected_tasks_by_partition[selected_parts.front()->info().partition_id]++;

        auto future_task = std::make_unique<FutureManipulationTask>(*this, ManipulationType::Merge);
        future_task->tagSourceParts(std::move(selected_parts));

        merge_pending_queue.push(std::move(future_task));
    }

    for (const auto & [p, t] : selected_tasks_by_partition)
    {
        if (t == 0)
            postponed_partitions[p] = now;
    }

    LOG_DEBUG(log, "Push {} tasks to pending queue.", res.size());

    return true;
}

Strings CnchMergeMutateThread::removeLockedPartition(const Strings & partitions)
{
    constexpr UInt64 slow_threshold_ms = 200;
    Stopwatch watch;
    auto & txn_coordinator = getContext()->getCnchTransactionCoordinator();
    auto transaction = txn_coordinator.createTransaction(
        CreateTransactionOption().setInitiator(CnchTransactionInitiator::MergeSelect).setPriority(CnchTransactionPriority::low));

    SCOPE_EXIT({
        try
        {
            txn_coordinator.finishTransaction(transaction);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    });

    auto txn_id = transaction->getTransactionID();
    Strings res;
    time_t now = time(nullptr);
    std::for_each(partitions.begin(), partitions.end(),
        [& res, txn_id, now, this] (const String & partition)
        {
            LockInfoPtr partition_lock = std::make_shared<LockInfo>(txn_id);
            partition_lock->setMode(LockMode::X);
            partition_lock->setUUIDAndPrefix(getStorageID().uuid, LockInfo::task_domain);
            partition_lock->setPartition(partition);

            auto cnch_lock = std::make_shared<CnchLockHolder>(getContext(), std::move(partition_lock));
            if (cnch_lock->tryLock())
            {
                LOG_TRACE(log, "partition {} is not lock", partition);
                res.push_back(partition);
            }
            else
            {
                LOG_TRACE(log, "partition {} is lock", partition);
                postponed_partitions[partition] = now;
            }
        });

    /// It's safe to use commitV2() (not abort()) to release the txn as there is no actions to do.
    /// And finishTransaction in the SCOPE_EXIT make sure the txn is clean by server but not DM.
    transaction->commitV2();
    UInt64 milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= slow_threshold_ms)
        LOG_INFO(log, "removeLockedPartition took {} ms.", milliseconds);
    return res;
}

String CnchMergeMutateThread::submitFutureManipulationTask(
    const StorageCnchMergeTree & storage,
    FutureManipulationTask & future_task,
    bool maybe_sync_task)
{
    auto local_context = getContext();

    auto & task_record = *future_task.record;
    auto type = task_record.type;

    if (maybe_sync_task)
        LOG_TRACE(log, "[SyncMergeTask] Begin to submit a sync merge task: {}", task_record.task_id);

    /// Create transaction
    future_task.prepareTransaction();
    auto transaction = task_record.transaction;
    auto transaction_id = transaction->getTransactionID();

    /// acquire lock to prevent conflict with upsert query

    LockInfoPtr partition_lock = std::make_shared<LockInfo>(transaction_id);
    partition_lock->setMode(LockMode::X);
    partition_lock->setUUIDAndPrefix(getStorageID().uuid, LockInfo::task_domain);
    partition_lock->setTimeout(storage.getSettings()->unique_acquire_write_lock_timeout.value.totalMilliseconds());

    /*
    if (type == ManipulationType::Merge)
    {
        String partition_id = future_task.parts.front()->info().partition_id;
        partition_lock->setPartition(partition_id);

        if (transaction->tryLock(partition_lock))
            LOG_DEBUG(log, "Acquired lock in successful for partition " << partition_id);
        else
        {
            throw Exception("Failed to acquire lock for partition " + partition_id, ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
        }
    }
    else if (type == ManipulationType::Mutate || type == ManipulationType::Clustering)
    {
        if (transaction->tryLock(partition_lock))
            LOG_DEBUG(log, "Acquired lock for table successful");
        else
        {
            throw Exception("Failed to acquire lock for table", ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
        }
    }

    auto cnch_lock = std::make_shared<CnchLockHolder>(getContext(), std::move(partition_lock));

    if (type == ManipulationType::Merge || type == ManipulationType::Mutate || type == ManipulationType::Clustering)
        cnch_lock->lock();

    if (maybe_sync_task)
        LOG_TRACE(log, "[SyncMergeTask] Txn locked");

    SCOPE_EXIT(if (type == ManipulationType::Merge || type == ManipulationType::Mutate || type == ManipulationType::Clustering) {
        try
        {
            cnch_lock->unlock();
            LOG_TRACE(log, "Successful call unlock on transaction");
        }
        catch (...)
        {
            LOG_WARNING(log, "Failed to call unlock() on transaction!");
        }
    });
    */

    /// get specific version storage
    auto istorage = catalog->getTableByUUID(*local_context, toString(storage_id.uuid), future_task.calcColumnsCommitTime());
    auto & cnch_table = checkAndGetCnchTable(istorage);

    /// fill task parameters
    ManipulationTaskParams params(istorage);
    params.type = type;
    params.rpc_port = local_context->getRPCPort();
    params.task_id = toString(transaction_id.toUInt64());
    params.txn_id = transaction_id.toUInt64();
    /// storage info
    params.create_table_query = cnch_table.genCreateTableQueryForWorker("");
    params.is_bucket_table = cnch_table.isBucketTable();
    /// parts
    params.assignSourceParts(future_task.parts);
    params.columns_commit_time = future_task.calcColumnsCommitTime();

    const auto & settings = local_context->getSettingsRef();
    if (settings.parts_preload_level
        && (storage.getSettings()->enable_local_disk_cache || storage.getSettings()->enable_nexus_fs))
    {
        params.parts_preload_level
            = storage.getSettings()->enable_preload_parts ? PreloadLevelSettings::AllPreload : storage.getSettings()->parts_preload_level;
    }
    /// mutation
    if (future_task.mutation_entry)
    {
        params.mutation_commit_time = future_task.mutation_entry->commit_time;
        params.mutation_commands = std::make_shared<MutationCommands>(future_task.mutation_entry->commands);
    }

    if (maybe_sync_task)
        LOG_TRACE(log, "[SyncMergeTask] RPC params constructed.");

    auto worker_client = getWorker(type, future_task.parts);

    task_record.task_id = params.task_id;
    task_record.worker = worker_client;
    task_record.manipulation_entry = local_context->getGlobalContext()->getManipulationList().insert(params, true, getContext());
    task_record.manipulation_entry->get()->related_node = worker_client->getRPCAddress();

    try
    {
        {
            std::lock_guard lock(task_records_mutex);
            task_records[params.task_id] = future_task.moveRecord();
            task_records[params.task_id]->submit_time_ns = clock_gettime_ns(CLOCK_MONOTONIC_COARSE);

            if (ManipulationType::Merge == type)
                ++running_merge_tasks;
            else
                ++running_mutation_tasks;
        }

        if (maybe_sync_task)
        {
            std::lock_guard lock(currently_synchronous_tasks_mutex);
            currently_synchronous_tasks.emplace(params.task_id);
        }

        const auto & worker_addr_str = worker_client->getHostWithPorts().toDebugString();
        if (maybe_sync_task)
            LOG_TRACE(log, "[SyncMergeTask] Submit a sync merge task to {}", worker_addr_str);

        ProfileEvents::increment(ProfileEvents::Manipulation, 1);
        worker_client->submitManipulationTask(cnch_table, params, transaction_id);
        LOG_DEBUG(log, "Submitted manipulation task to {}, {}", worker_addr_str, params.toDebugString());
    }
    catch (...)
    {
        {
            std::lock_guard lock(task_records_mutex);
            removeTaskImpl(params.task_id, lock);
        }

        throw;
    }

    return params.task_id;
}

String CnchMergeMutateThread::triggerPartMerge(
    StoragePtr & istorage, const String & partition_id, bool final, bool try_select, bool try_execute)
{
    auto local_context = getContext();

    std::lock_guard lock(try_merge_parts_mutex);

    /// Step 1: copy currently_merging_mutating_parts
    NameSet merging_mutating_parts_snapshot = copyCurrentlyMergingMutatingParts();

    if (try_select)
        LOG_TRACE(log, "merging_mutating_parts:\n{}", fmt::join(merging_mutating_parts_snapshot, "\n"));

    /// Step 2: get parts & calculate visible parts.
    ServerDataPartsVector data_parts;
    if (partition_id.empty() || partition_id == "all")
        data_parts = catalog->getAllServerDataParts(istorage, local_context->getTimestamp(), nullptr);
    else
        data_parts = catalog->getServerDataPartsInPartitions(istorage, {partition_id}, local_context->getTimestamp(), nullptr);

    auto visible_parts = CnchPartsHelper::calcVisibleParts(
        data_parts, false, (try_select ? CnchPartsHelper::EnableLogging : CnchPartsHelper::DisableLogging));
    if (visible_parts.empty())
    {
        LOG_TRACE(log, "There is no visible parts, exit merge selection.");
        return {};
    }

    /// Step 3: get mutation list
    std::map<TxnTimestamp, CnchMergeTreeMutationEntry> mutation_entries;
    std::vector<std::pair<TxnTimestamp, bool>> mutation_timestamps;
    catalog->fillMutationsByStorage(storage_id, mutation_entries);
    mutation_timestamps.reserve(mutation_entries.size());
    for (const auto & [_, mutation_entry] : mutation_entries)
        mutation_timestamps.emplace_back(mutation_entry.commit_time, mutation_entry.commands.changeSchema());
    
    /// Used to calculate total rows of each partition so we can prevent generating huge merge tasks.
    std::multimap<String, UInt64> unselectable_part_rows;

    auto & storage = checkAndGetCnchTable(istorage);
    auto storage_settings = storage.getSettings();

    /// For compatibility.
    auto max_bytes = std::min(
        storage_settings->cnch_merge_max_total_bytes_to_merge.value,
        storage_settings->max_bytes_to_merge_at_max_space_in_pool.value);

    /// If selecting nonadjacent parts is allowed,
    /// we will filter out all merging parts firstly and then only check part's columns_commit_time.
    if (storage_settings->cnch_merge_select_nonadjacent_parts)
    {
        auto max_rows = storage_settings->cnch_merge_max_total_rows_to_merge.value;
        removeUnselectableParts(visible_parts, merging_mutating_parts_snapshot, unselectable_part_rows, max_bytes, max_rows);
    }
    else
    {
        for (const auto & p: visible_parts)
        {
            if (merging_mutating_parts_snapshot.count(p->name()) > 0)
                unselectable_part_rows.emplace(p->info().partition_id, p->part_model().rows_count());
        }
    }

    /// Step 4: create merge predicate
    auto can_merge_callback = getMergePred(merging_mutating_parts_snapshot, mutation_timestamps);

    SelectPartsToMergeSettings select_settings = SelectPartsToMergeSettings {
        .max_total_size_to_merge = max_bytes,
        .num_default_workers = num_default_workers_in_vw_settings,
        .aggressive = true,
        .enable_batch_select = false,
        .final = final,
        // .merge_with_ttl_allowed = false,
    };

    /// Step 5: do selecting
    std::vector<ServerDataPartsVector> res;
    [[maybe_unused]] auto decision = selectPartsToMerge(
        storage,
        res,
        visible_parts,
        unselectable_part_rows,
        can_merge_callback,
        select_settings,
        log);

    LOG_DEBUG(log, "triggerPartMerge(): Selected {} groups from {} parts.", res.size(), visible_parts.size());

    if (!try_select && !res.empty())
    {
        {
            std::lock_guard pool_lock(worker_pool_mutex);
            vw_name = storage_settings->cnch_vw_write;
            /// pick_worker_algo = storage_settings->cnch_merge_pick_worker_algo;
            vw_handle = getContext()->getVirtualWarehousePool().get(vw_name);
        }

        return submitFutureManipulationTask(
            storage,
            FutureManipulationTask(*this, ManipulationType::Merge)
                .setTryExecute(try_execute)
                .tagSourceParts(std::move(res.front())),
            true);
    }

    return {};
}

void CnchMergeMutateThread::waitTasksFinish(const std::vector<String> & task_ids, UInt64 timeout_ms)
{
    Stopwatch watch;
    for (const auto & task_id: task_ids)
    {
        std::unique_lock lock(currently_synchronous_tasks_mutex);
        if (!timeout_ms)
        {
            currently_synchronous_tasks_cv.wait(lock, [&]() { return !shutdown_called && !currently_synchronous_tasks.count(task_id); });
        }
        else
        {
            auto escaped_time = watch.elapsedMilliseconds();
            if (escaped_time >= timeout_ms)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout waiting for task `{}` to finish", task_id);

            bool done = currently_synchronous_tasks_cv.wait_for(
                lock,
                std::chrono::milliseconds(timeout_ms - escaped_time),
                [&]() { return !shutdown_called && !currently_synchronous_tasks.count(task_id); });
            if (!done)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout waiting for task `{}` to finish", task_id);
        }
    }

    if (shutdown_called)
        throw Exception(ErrorCodes::ABORTED, "Tasks maybe canceled due to server shutdown");
}

void CnchMergeMutateThread::waitMutationFinish(UInt64 mutation_commit_time, UInt64 timeout_ms)
{
    Stopwatch watch;
    while (true)
    {
        std::map<TxnTimestamp, CnchMergeTreeMutationEntry> current_mutations_by_version;
        catalog->fillMutationsByStorage(storage_id, current_mutations_by_version);
        if (!current_mutations_by_version.count(mutation_commit_time))
            break;

        if (shutdown_called)
            throw Exception(ErrorCodes::ABORTED, "Cancel waiting because thread {} is shutting done", log->name());
        if (timeout_ms && watch.elapsedMilliseconds() >= timeout_ms)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout waiting for mutation {} to finish, current MergeMutateThread is {}",
                mutation_commit_time, toString(getThreadStatus()));
        scheduled_task->schedule();
        sleepForMilliseconds(500);
    }
}

void CnchMergeMutateThread::tryRemoveTask(const String & task_id)
{
    std::lock_guard lock(task_records_mutex);
    removeTaskImpl(task_id, lock);
}

void CnchMergeMutateThread::removeTaskImpl(const String & task_id, std::lock_guard<std::mutex> &, TaskRecordPtr * out_task_record)
{
    auto it = task_records.find(task_id);
    if (it == task_records.end())
        return;

    if (it->second->type == ManipulationType::Merge)
        --running_merge_tasks;
    else
        --running_mutation_tasks;

    if (out_task_record)
        *out_task_record = std::move(it->second);

    task_records.erase(it);
}

/// Merge txn's phase-1 commit.
void CnchMergeMutateThread::finishTask(const String & task_id, std::function<void(const Strings &, UInt64)> && precommit_parts)
{
    auto local_context = getContext();
    UInt64 current_ts = local_context->getPhysicalTimestamp();
    bool need_check_start_time = current_ts - last_schedule_time > DELAY_SCHEDULE_TIME_IN_SECOND;
    UInt64 fetched_start_time = need_check_start_time ? catalog->getMergeMutateThreadStartTime(storage_id) : 0;

    Stopwatch watch;

    /// Set task_record's commit_start_time before precommit_parts().
    /// And for a committing task, it must be destroyed from txn (commit/abort/rollback) or heartbeat check.
    /// Other threads (like INGEST PARTITION) should skip those committing tasks when cancelling merge.
    String partition_id;
    Strings source_part_names;
    bool try_execute;
    UInt64 manipulation_submit_time_ns;

    {
        std::lock_guard lock(task_records_mutex);
        auto it = task_records.find(task_id);
        if (it == task_records.end())
            throw Exception(ErrorCodes::ABORTED, "Task {} not found in this node.", task_id);
        it->second->commit_start_time = time(nullptr);

        partition_id = it->second->parts.front()->info().partition_id;
        source_part_names = it->second->getSourcePartNames(/*flatten*/true);
        try_execute = it->second->try_execute;
        manipulation_submit_time_ns = it->second->submit_time_ns;
    }

    if (local_context->getRootConfig().debug_disable_merge_commit.safeGet())
        throw Exception("Disable merge commit", ErrorCodes::ABORTED);

    if (try_execute)
    {
        LOG_DEBUG(log, "Ignored the `try_execute` task {}", task_id);
        /// Leave the task in task_records and it will be destroyed by heartbeat check.
        return;
    }

    /// Check with catalog when the last_schedule_time is not updated for some time. The task can be committed only if current
    /// thread start up time equals to that in catalog.
    if (need_check_start_time)
    {
        if (thread_start_time != fetched_start_time)
            throw Exception(
                ErrorCodes::ABORTED,
                "Task {} cannot be committed because current MergeMutateThread for {} is stale. Drop this task.",
                task_id, storage_id.getFullTableName());
    }

    precommit_parts(source_part_names, manipulation_submit_time_ns);

    if(auto gc_thread = getContext()->tryGetCnchBGThread(CnchBGThreadType::PartGC, storage_id))
        gc_thread->addCandidatePartition(partition_id);

    LOG_TRACE(log, "Finish manipulation task {}", task_id);
}

CnchWorkerClientPtr CnchMergeMutateThread::getWorker([[maybe_unused]] ManipulationType type, [[maybe_unused]] const ServerDataPartsVector & all_parts)
{
    std::lock_guard lock(worker_pool_mutex);
    return vw_handle->getWorker();
}

bool CnchMergeMutateThread::needCollectExtendedMergeMetrics()
{
    auto now = time(nullptr);
    if (now - last_time_collect_extended_merge_metrics >= 60 * 10)
    {
        last_time_collect_extended_merge_metrics = now;
        return true;
    }
    return false;
}

ClusterTaskProgress CnchMergeMutateThread::getReclusteringTaskProgress()
{
    ClusterTaskProgress cluster_task_progress;
    if (!current_mutate_entry->isReclusterMutation())
        return cluster_task_progress;

    auto istorage = getStorageFromCatalog();
    auto partition_list = catalog->getPartitionList(istorage, nullptr);
    if (partition_list.empty())
        return cluster_task_progress;

    cluster_task_progress.progress = (finish_mutation_partitions.size() / static_cast<double>(partition_list.size())) * 100;
    cluster_task_progress.start_time_seconds = current_mutate_entry->commit_time.toSecond();
    return cluster_task_progress;
}

void CnchMergeMutateThread::calcMutationPartitions(CnchMergeTreeMutationEntry & mutate_entry, StoragePtr & istorage, StorageCnchMergeTree & storage)
{
    if (mutate_entry.partition_ids.has_value())
        return;

    auto catalog = getContext()->getCnchCatalog();
    if (!storage.getInMemoryMetadataPtr()->getPartitionKeyAST())
    {
        mutate_entry.setPartitionIDs(catalog->getPartitionIDs(istorage, nullptr));
        return;
    }

    const auto & mutation_command = mutate_entry.commands.front();
    if (mutation_command.partition)
        mutate_entry.setPartitionIDs({storage.getPartitionIDFromQuery(mutation_command.partition, getContext())});
    else if (mutation_command.predicate)
        mutate_entry.setPartitionIDs(storage.getPartitionsByPredicate(mutation_command.predicate, getContext()));
    else
        mutate_entry.setPartitionIDs(catalog->getPartitionIDs(istorage, nullptr));
}


/// Mutate
/// There are 3 steps in the main schedule loop:
/// 0. get the first mutation entry from KV.
/// 1. generate tasks for the mutation entry if there are parts need to be mutated.
/// 2. check whether the mutation entry is finished and remove it from KV if finished successfully.
bool CnchMergeMutateThread::tryMutateParts(StoragePtr & istorage, StorageCnchMergeTree & storage)
{
    LOG_TRACE(log, "Try to mutate parts... running task {}", running_mutation_tasks);
    auto storage_settings = storage.getSettings();

    std::lock_guard lock(try_mutate_parts_mutex);
    auto merging_mutating_parts_snapshot = copyCurrentlyMergingMutatingParts();

    auto finish_current_mutation = [this, &lock, &storage]()
    {
        if (!current_mutate_entry)
            return;

        removeMutationEntryFromKV(*current_mutate_entry, lock);
        storage.removeMutationEntry(current_mutate_entry->commit_time);

        scheduled_mutation_partitions.clear();
        finish_mutation_partitions.clear();
        current_mutate_entry.reset();
    };

    /// Fetch mutation entries from KV.
    std::map<TxnTimestamp, CnchMergeTreeMutationEntry> current_mutations_by_version;
    auto catalog = getContext()->getCnchCatalog();
    catalog->fillMutationsByStorage(storage_id, current_mutations_by_version);
    if (current_mutations_by_version.empty())
    {
        LOG_TRACE(log, "No mutation entry from KV.");
        return false;
    }

    /// Step 0: get the first mutation entry.
    const auto & entry_from_catalog = current_mutations_by_version.begin()->second;
    if (!current_mutate_entry.has_value())
    {
        current_mutate_entry = std::make_optional<CnchMergeTreeMutationEntry>(entry_from_catalog);
    }
    else if (current_mutate_entry->commit_time != entry_from_catalog.commit_time)
    {
        /// Should not happen
        LOG_WARNING(log, "Current mutation entry missed: {}, found {}", current_mutate_entry->commit_time, entry_from_catalog.commit_time);
        scheduled_mutation_partitions.clear();
        finish_mutation_partitions.clear();
        current_mutate_entry = std::make_optional<CnchMergeTreeMutationEntry>(entry_from_catalog);
    }

    if (current_mutate_entry->isReclusterMutation())
    {
        if (!getContext()->getTableReclusterTaskStatus(storage_id))
        {
            LOG_TRACE(log, "recluster task is disabled for {}", storage_id.getNameForLogs());
            return false;
        }
        if (current_mutate_entry->columns_commit_time < storage.commit_time)
        {
            /// There is newer version storage, needs to check whether `cluster by` definition changed

            /// get specific version storage
            auto entry_istorage = catalog->getTableByUUID(*getContext(), toString(storage_id.uuid), current_mutate_entry->columns_commit_time);
            auto & entry_cnch_table = checkAndGetCnchTable(entry_istorage);

            if (entry_cnch_table.getTableHashForClusterBy() != storage.getTableHashForClusterBy())
            {
                LOG_INFO(log, "recluster task {} is canceled due to newer version cluster by", current_mutate_entry->txn_id.toString());
                finish_current_mutation();
                return false;
            }
        }
    }

    bool change_schema = current_mutate_entry->commands.changeSchema();
    bool is_recluster = current_mutate_entry->isReclusterMutation();
    auto table_definition_hash = storage.getTableHashForClusterBy();

    /// Function to generating new tasks. Return true if we can still generate new tasks.
    auto generate_tasks = [&](const ServerDataPartsVector & visible_parts, const NameSet & snapshot)
    {
        auto type = current_mutate_entry->isReclusterMutation() ? ManipulationType::Clustering : ManipulationType::Mutate;
        const auto & commit_ts= current_mutate_entry->commit_time;

        size_t curr_mutate_part_size = 0;
        ServerDataPartsVector alter_parts;
        bool remain_tasks_in_partition = false;

        for (const auto & part : visible_parts)
        {
            if (!needMutate(part, commit_ts, change_schema, is_recluster, table_definition_hash))
                continue;
            if (snapshot.count(part->name()))
            {
                remain_tasks_in_partition = true;
                continue;
            }
            remain_tasks_in_partition = true;
            alter_parts.push_back(part);
            curr_mutate_part_size += part->part_model().size();
            auto p_part = part->tryGetPreviousPart();
            while (p_part)
            {
                curr_mutate_part_size += p_part->part_model().size();
                p_part = p_part->tryGetPreviousPart();
            }
            /// Batch n parts in one task.
            if (alter_parts.size() >= storage_settings->cnch_mutate_max_parts_to_mutate
                || curr_mutate_part_size >= storage_settings->cnch_mutate_max_total_bytes_to_mutate)
            {
                submitFutureManipulationTask(
                    storage,
                    FutureManipulationTask(*this, type)
                        .setMutationEntry(*current_mutate_entry)
                        .tagSourceParts(std::move(alter_parts)));
                alter_parts.clear();
                curr_mutate_part_size = 0;
                if (running_mutation_tasks >= storage.getSettings()->max_addition_mutation_task_num)
                    return true;
            }
        }

        /// Handle the last batch if any.
        if (!alter_parts.empty())
        {
            submitFutureManipulationTask(
                storage,
                FutureManipulationTask(*this, type)
                    .setMutationEntry(*current_mutate_entry)
                    .tagSourceParts(std::move(alter_parts)));
        }

        return remain_tasks_in_partition;
    };

    /// Function to check whether parts are all mutated. Return true if all parts are mutated.
    auto check_all_done = [&](const ServerDataPartsVector & visible_parts, const ServerDataPartsVector & visible_staged_parts)
    {
        const auto & commit_ts = current_mutate_entry->commit_time;
        for (const auto & part : visible_parts)
        {
            if (needMutate(part, commit_ts, change_schema, is_recluster, table_definition_hash))
                return false;
        }
        for (const auto & part : visible_staged_parts)
        {
            if (needMutate(part, commit_ts, change_schema, is_recluster, table_definition_hash))
                return false;
        }
        return true;
    };

    /// Step 1: generate mutations tasks for the earliest mutation entry.
    bool is_finish = true;

    auto timestamp = getContext()->getTimestamp();
    if (storage.getInMemoryMetadataPtr()->getPartitionKeyAST())
    {
        calcMutationPartitions(*current_mutate_entry, istorage, storage);
        for (const auto & partition_id : current_mutate_entry->partition_ids.value())
        {
            if (running_mutation_tasks > storage.getSettings()->max_addition_mutation_task_num)
            {
                is_finish = false;
                break;
            }

            if (finish_mutation_partitions.find(partition_id) != finish_mutation_partitions.end())
                continue;

            auto parts = catalog->getServerDataPartsInPartitions(istorage, {partition_id}, timestamp, nullptr);
            auto visible_parts = CnchPartsHelper::calcVisibleParts(parts, false);

            /// Step 2: check whether parts are all mutated.

            /// All parts are scheduled (mutated or in mutating) - just check whether all parts are mutated.
            if (scheduled_mutation_partitions.contains(partition_id))
            {
                ServerDataPartsVector visible_staged_parts;
                if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
                {
                    NameSet partitions{partition_id};
                    auto staged_parts = catalog->getStagedServerDataParts(istorage, timestamp, &partitions);
                    visible_staged_parts = CnchPartsHelper::calcVisibleParts(staged_parts, false);
                }

                if (check_all_done(visible_parts, visible_staged_parts))
                    finish_mutation_partitions.emplace(partition_id);
            }
            /// Some parts are not scheduled, generate tasks for those parts
            ///  - if all tasks are generated at this round, then mark the partition as scheduled.
            else if (!generate_tasks(visible_parts, merging_mutating_parts_snapshot))
            {
                LOG_TRACE(log, "No more mutation tasks for partition {}, mutation id: {}", partition_id,  current_mutate_entry->txn_id);
                scheduled_mutation_partitions.emplace(partition_id);
            }
            ///  - if can't generate all tasks at this round, then go into next round.
            else
            {
                is_finish = false;
            }
        }
    }
    else
    {
        /// We don't maintain finish_mutation_partitions/scheduled_mutation_partitions for tables without partition key.
        auto parts = catalog->getAllServerDataParts(istorage, timestamp, nullptr);
        auto visible_parts = CnchPartsHelper::calcVisibleParts(parts, false);

        /// There is still new pending tasks, then no need to do check_all_done (just submit new tasks and go into next round).
        if (generate_tasks(visible_parts, merging_mutating_parts_snapshot))
        {
            is_finish = false;
        }
        else
        {
            /// All tasks are scheduled, now we can do check_all_done.
            auto current_parts = catalog->getAllServerDataParts(istorage, timestamp, nullptr);
            auto current_visible_parts = CnchPartsHelper::calcVisibleParts(current_parts, false);
            ServerDataPartsVector current_visible_staged_parts;
            if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
            {
                auto staged_parts = catalog->getStagedServerDataParts(istorage, timestamp);
                current_visible_staged_parts = CnchPartsHelper::calcVisibleParts(staged_parts, false);
            }

            is_finish = check_all_done(current_visible_parts, current_visible_staged_parts);
        }
    }

    if (!is_finish)
        return true;

    bool is_newest_cluster_mutation = current_mutate_entry->isReclusterMutation();
    const auto & commit_ts = current_mutate_entry->commit_time;
    /// Check whether there is any newer recluster mutation
    for (auto & mutation : current_mutations_by_version)
    {
        if (mutation.first > commit_ts && mutation.second.isReclusterMutation())
        {
            is_newest_cluster_mutation = false;
            break;
        }
    }

    if (is_newest_cluster_mutation)
        setTableClusterStatus();

    finish_current_mutation();

    return false;
}

MergeTreeMutationStatusVector CnchMergeMutateThread::getAllMutationStatuses()
{
    MergeTreeMutationStatusVector res;

    /// Fetch all mutation entries from KV.
    std::map<TxnTimestamp, CnchMergeTreeMutationEntry> current_mutations_by_version;
    auto context = getContext();
    auto catalog = context->getCnchCatalog();
    catalog->fillMutationsByStorage(storage_id, current_mutations_by_version);
    if (current_mutations_by_version.empty())
        return res;

    /// Fetch all data parts.
    TxnTimestamp curr_ts = context->tryGetTimestamp(__PRETTY_FUNCTION__);
    auto istorage = catalog->getTableByUUID(*context, UUIDHelpers::UUIDToString(storage_id.uuid), curr_ts);
    auto & storage = checkAndGetCnchTable(istorage);
    auto table_definition_hash = storage.getTableHashForClusterBy();
    auto all_parts = catalog->getAllServerDataParts(istorage, curr_ts, nullptr);
    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);

    std::lock_guard lock(try_mutate_parts_mutex);

    /// Fill each mutation entry's status.
    for (auto & [commit_ts, entry] : current_mutations_by_version)
    {
        MergeTreeMutationStatus status;
        status.id = entry.txn_id.toString();
        status.query_id = entry.query_id;

        calcMutationPartitions(entry, istorage, storage);
        auto & partitions = entry.partition_ids.value();
        bool change_schema = entry.commands.changeSchema();
        bool is_recluster = entry.isReclusterMutation();
        for (auto & part : visible_parts)
        {
            bool partition_match = std::find(partitions.begin(), partitions.end(), part->info().partition_id) != partitions.end();
            if (partition_match && needMutate(part, commit_ts, change_schema, is_recluster, table_definition_hash))
                ++status.parts_to_do;
        }

        status.create_time = commit_ts.toSecond();
        if (!status.parts_to_do)
            status.is_done = true;

        WriteBufferFromOwnString wb;
        DB::formatAST(*entry.commands.ast(), wb, true, true);
        status.command = wb.str();

        res.push_back(status);
    }

    return res;
}

void CnchMergeMutateThread::setTableClusterStatus()
{
    /// modify cluster status before removing recluster mutation entry.
    LOG_DEBUG(log, "All reclusted tasks in table {} have been executed, check for cluster status", storage_id.getNameForLogs());

    bool clustered = true;
    auto istorage = getStorageFromCatalog();
    auto & storage = checkAndGetCnchTable(istorage);
    auto table_definition_hash = storage.getTableHashForClusterBy();
    auto check_clustered = [&table_definition_hash](const ServerDataPartsVector & parts)
    {
        return std::all_of(parts.begin(), parts.end(),
            [&table_definition_hash](const ServerDataPartPtr & part) { return table_definition_hash.match(part->part_model().table_definition_hash()); });
    };
    auto partition_ids = catalog->getPartitionIDs(istorage, nullptr);
    for (const auto & partition_id : partition_ids)
    {
        auto parts = catalog->getServerDataPartsInPartitions(istorage, {partition_id}, TxnTimestamp::maxTS(), nullptr);
        auto visible_parts = CnchPartsHelper::calcVisibleParts(parts, false);
        ServerDataPartsVector visible_staged_parts;
        if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
        {
            auto staged_parts = catalog->getStagedServerDataParts(istorage, TxnTimestamp::maxTS());
            visible_staged_parts = CnchPartsHelper::calcVisibleParts(staged_parts, false);
        }
        clustered = check_clustered(visible_parts) && check_clustered(visible_staged_parts);
        if (!clustered)
            break;
    }

    catalog->setTableClusterStatus(storage.getStorageID().uuid, clustered, table_definition_hash);
}

void CnchMergeMutateThread::removeMutationEntryFromKV(const CnchMergeTreeMutationEntry & entry, std::lock_guard<std::mutex> &)
{
    const auto & commit_time = entry.commit_time;
    /// FIXME: (zuochuang.zema) buggy: we don't touch active timestamp for insertion,
    ///   so the mutation_entry may be deleted before T4.
    /// When is mutation allowed to be deleted?
    /// Consider the following case:
    ///     T1: Insert part_1(invisible, using metadata in T1).
    ///     T2: Submit an alter query (Create a mutation which commit_ts = T2).
    ///     T3: Alter query done (According current visible parts).
    ///     T4: Insert part_1 successful.
    /// We can only allow the mutation to be deleted after T4 to ensure part_1 can be modified.
    /// At T3, the MinActiveTimestamp should be T1, T1 < T2, so, the Mutation should not allow to remove.
    /// At T4, the MinActiveTimestamp should be greater than T2, so, the Mutation could be remove.
    /// TODO: should be acquired before get parts
    auto min_active_ts = calculateMinActiveTimestamp();
    if (min_active_ts <= commit_time)
    {
        LOG_TRACE(log, "min_active_ts {} is not updated to the mutation's commit_time {}.", min_active_ts, commit_time);
        return;
    }

    WriteBufferFromOwnString buf;
    entry.commands.writeText(buf);
    LOG_DEBUG(log, "Mutation {}(command: {}) has been done, will remove it from catalog.", commit_time, buf.str());
    getContext()->getCnchCatalog()->removeMutation(storage_id, entry.txn_id.toString());
}

void CnchMergeMutateThread::triggerPartMutate(StoragePtr storage)
{
    auto * cnch = typeid_cast<StorageCnchMergeTree *>(storage.get());
    if (!cnch)
        return;

    {
        std::lock_guard pool_lock(worker_pool_mutex);
        vw_name = cnch->getSettings()->cnch_vw_write;
        vw_handle = getContext()->getVirtualWarehousePool().get(vw_name);
    }

    tryMutateParts(storage, *cnch);
}

/// Cancel existing merge tasks:
/// 1. [lock guarded] collect tasks and group by worker.
/// 2. [lock guarded] remove tasks from task_records.
/// 3. send shutdown signal to workers.
/// 4. wait committing tasks finish (be aborted or committed).
bool CnchMergeMutateThread::removeTasksOnPartitions(const std::unordered_set<String> & partitions)
{
    if (partitions.empty())
        return true;
    bool remove_all = *partitions.begin() == "all";

    std::lock_guard lock_merge(try_merge_parts_mutex);
    std::lock_guard lock_mutate(try_mutate_parts_mutex);
    {
        decltype(merge_pending_queue) empty_for_clear;
        merge_pending_queue.swap(empty_for_clear);
    }

    std::vector<String> task_ids;
    std::unordered_map<CnchWorkerClientPtr, Strings> worker_with_tasks;
    NameSet committing_tasks;

    {
        std::lock_guard lock(task_records_mutex);
        for (const auto & [task_id, task] : task_records)
        {
            if (task->parts.empty())
                continue;

            if (task->commit_start_time > 0)
            {
                committing_tasks.insert(task_id);
                continue;
            }

            if (task->type == ManipulationType::Clustering || task->type == ManipulationType::Mutate || remove_all)
                task_ids.push_back(task_id);
            else if (task->type == ManipulationType::Merge && partitions.contains(task->parts.front()->info().partition_id))
                task_ids.push_back(task_id);
        }

        for (const auto & task_id : task_ids)
        {
            auto it = task_records.find(task_id);
            if (it == task_records.end())
                continue;
            worker_with_tasks[it->second->worker].push_back(task_id);

            LOG_INFO(log, "Remove task: {} ", task_id);
            removeTaskImpl(task_id, lock);
        }
    }

    for (const auto & [worker, tasks] : worker_with_tasks)
    {
        if (!worker)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find worker host for task {}", tasks[0]);
        worker->shutdownManipulationTasks(storage_id.uuid, tasks);
    }

    /// Make sure all tasks are aborted or committed, so that we can generate a DropRange safely.
    /// The final state of the task(aborted or committed) is not important.
    auto & txn_coordinator = getContext()->getCnchTransactionCoordinator();
    for (const auto & task_id : committing_tasks)
    {
        auto txn_id = std::stoull(task_id);
        try {
            txn_coordinator.getTransaction(txn_id)->abort();
        } catch(...) {}
    }

    return true;
}

}
