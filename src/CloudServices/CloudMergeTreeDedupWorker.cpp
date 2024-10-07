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

#include <Catalog/Catalog.h>
#include <CloudServices/CloudMergeTreeDedupWorker.h>
#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchServerClientPool.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <CloudServices/CnchDataWriter.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/CnchSystemLog.h>
#include <MergeTreeCommon/MergeTreeDataDeduper.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ParserPartition.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/CnchWorkerTransaction.h>

namespace ProfileEvents
{
    extern const Event ScheduledDedupTaskNumber;
}

namespace CurrentMetrics
{
    extern const Metric BackgroundDedupSchedulePoolTask;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

CloudMergeTreeDedupWorker::CloudMergeTreeDedupWorker(StorageCloudMergeTree & storage_)
    : storage(storage_)
    , context(storage.getContext())
    , log_name(storage.getLogName() + "(DedupWorker)")
    , log(getLogger(log_name))
    , interval_scheduler(storage.getSettings()->staged_part_lifetime_threshold_ms_to_block_kafka_consume)
{
    task = storage.getContext()->getUniqueTableSchedulePool().createTask(log_name, [this] { run(); });

    /// The maximum error of staged parts commit time is tso_windows, so it's necessary to reserve that time.
    UInt64 tso_windows_ms = context->getConfigRef().getInt("tso_service.tso_window_ms", 3000);
    if (interval_scheduler.staged_part_max_life_time_ms < tso_windows_ms)
        interval_scheduler.staged_part_max_life_time_ms = 0;
    else
        interval_scheduler.staged_part_max_life_time_ms -= tso_windows_ms;

    status.create_time = time(nullptr);

    {
        /// init current_deduper before iterate
        std::lock_guard lock(current_deduper_mutex);
        current_deduper = std::make_unique<MergeTreeDataDeduper>(storage, context, CnchDedupHelper::DedupMode::UPSERT);
    }

    if (storage.getSettings()->duplicate_auto_repair)
    {
        data_repairer = context->getUniqueTableSchedulePool().createTask(log_name, [this] { runDataRepairTask(); });
        data_repairer->deactivate();
    }
}

CloudMergeTreeDedupWorker::~CloudMergeTreeDedupWorker()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        std::lock_guard lock(status_mutex);
        status.last_exception = getCurrentExceptionMessage(false);
        status.last_exception_time = time(nullptr);
    }
}

void CloudMergeTreeDedupWorker::run()
{
    {
        std::lock_guard lock(status_mutex);
        status.total_schedule_cnt++;
    }

    if (!isActive())
        return;

    try
    {
        Stopwatch timer;
        interval_scheduler.has_excep_or_timeout = false;
        iterate();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        interval_scheduler.has_excep_or_timeout = true;

        std::lock_guard lock(status_mutex);
        status.last_exception = getCurrentExceptionMessage(false);
        status.last_exception_time = time(nullptr);
    }

    if (isActive())
        task->scheduleAfter(interval_scheduler.getScheduleTime());
}

void CloudMergeTreeDedupWorker::iterate()
{
    if (server_host_ports.empty())
    {
        LOG_DEBUG(log, "server host and ports haven't been set, skip");
        return;
    }

    ProfileEvents::increment(ProfileEvents::ScheduledDedupTaskNumber);
    CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundDedupSchedulePoolTask};

    DedupWorkerStatus copy_status = getDedupWorkerStatus();

    Stopwatch cost_all;
    auto catalog = context->getCnchCatalog();
    TxnTimestamp ts = context->getTimestamp();
    /// must use cnch table to construct staged parts
    auto table = catalog->tryGetTableByUUID(*context, UUIDHelpers::UUIDToString(storage.getCnchStorageUUID()), ts);
    if (!table)
    {
        LOG_INFO(log, "table have been dropped, skip");
        return;
    }
    auto cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!cnch_table)
    {
        LOG_ERROR(log, "table {} is not cnch merge tree", table->getStorageID().getNameForLogs());
        detachSelf();
        return;
    }
    if (cnch_table->getStorageID().getFullTableName() != storage.getCnchStorageID().getFullTableName())
    {
        LOG_WARNING(
            log,
            "cnch table storage id has changed, detach dedup worker. Origin storage id: {}, current storage id: {}",
            storage.getCnchStorageID().getNameForLogs(),
            cnch_table->getStorageID().getNameForLogs());
        detachSelf();
        return;
    }

    /// get all the partitions of committed staged parts
    auto pre_check_staged_parts = cnch_table->getStagedParts(ts);

    IMergeTreeDataPartsVector preload_parts{};
    /// Filter using hash with max_dedup_worker_number & index
    for (const auto & part : pre_check_staged_parts)
    {
        String hash_str = part->info.partition_id + "." + std::to_string(part->bucket_number);
        SipHash hash_state;
        hash_state.update(hash_str.data(), hash_str.size());
        if (hash_state.get64() % storage.getSettings()->max_dedup_worker_number == index)
            preload_parts.push_back(part);
    }

    if (preload_parts.empty())
    {
        LOG_TRACE(log, "no more staged parts to process, skip");
        return;
    }

    auto server_client = context->getCnchServerClientPool().get(server_host_ports);
    if (!server_client)
        throw Exception("no server client found", ErrorCodes::LOGICAL_ERROR);

    /// create dedup transaction
    auto dedup_context = Context::createCopy(context);
    dedup_context->setSessionContext(dedup_context);
    CnchWorkerTransactionPtr txn;
    try
    {
        txn = std::make_shared<CnchWorkerTransaction>(dedup_context, server_client);
        txn->setMainTableUUID(storage.getCnchStorageUUID());
        dedup_context->setCurrentTransaction(txn);
        /// we need to set context for current_deduper as partial update may need to dump parts during sub iteration
        current_deduper->setDeduperContext(dedup_context);
    }
    catch (...)
    {
        tryLogCurrentException(log, "failed to create dedup transaction on server " + server_client->getRPCAddress());
        throw;
    }

    CnchLockHolderPtr cnch_lock;
    MergeTreeDataPartsCNCHVector visible_parts, staged_parts;
    bool force_normal_dedup = false;
    Stopwatch lock_watch;
    do
    {
        CnchDedupHelper::DedupScope scope = CnchDedupHelper::getDedupScope(*cnch_table, preload_parts, force_normal_dedup);

        /// XXX: May acquire more locks as we will filter by max_staged_part_number_per_task & max_staged_part_rows_per_task
        std::vector<LockInfoPtr> locks_to_acquire = CnchDedupHelper::getLocksToAcquire(
            scope, txn->getTransactionID(), *cnch_table, storage.getSettings()->unique_acquire_write_lock_timeout.value.totalMilliseconds());
        lock_watch.restart();
        cnch_lock = std::make_shared<CnchLockHolder>(context, std::move(locks_to_acquire));
        if (!cnch_lock->tryLock())
        {
            if (auto unique_table_log = context->getCloudUniqueTableLog())
            {
                auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, cnch_table->getCnchStorageID());
                current_log.txn_id = txn->getTransactionID();
                current_log.metric = ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED;
                current_log.event_msg = "Failed to acquire lock for txn " + txn->getTransactionID().toString();
                unique_table_log->add(current_log);
            }
            throw Exception("Failed to acquire lock for txn " + txn->getTransactionID().toString(), ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
        }

        lock_watch.restart();
        ts = context->getTimestamp(); /// must get a new ts after locks are acquired
        visible_parts = CnchDedupHelper::getVisiblePartsToDedup(scope, *cnch_table, ts);
        staged_parts = CnchDedupHelper::getStagedPartsToDedup(scope, *cnch_table, ts);

        /// In some case, visible parts or staged parts doesn't have same bucket definition or not a bucket part, we need to convert bucket lock to normal lock.
        /// Otherwise, it may lead to duplicated data.
        if (scope.isBucketLock() && !cnch_table->getSettings()->enable_bucket_level_unique_keys
            && !CnchDedupHelper::checkBucketParts(*cnch_table, visible_parts, staged_parts))
        {
            force_normal_dedup = true;
            cnch_lock->unlock();
            LOG_TRACE(log, "Check bucket parts failed, switch to normal lock to dedup.");
            continue;
        }
        else
        {
            /// Filter staged parts if lock scope is bucket level
            scope.filterParts(staged_parts);
            break;
        }
    } while (true);

    if (staged_parts.empty())
    {
        LOG_INFO(log, "no more staged parts after acquired the locks, they may have been processed by other thread");
        return;
    }

    txn->appendLockHolder(cnch_lock);

    /// Sorts by commit time
    std::sort(staged_parts.begin(), staged_parts.end(), [](auto & lhs, auto & rhs) {
        return lhs->commit_time < rhs->commit_time;
    });

    MergeTreeDataPartsCNCHVector current_staged_parts;
    UInt64 min_staged_part_timestamp{UINT64_MAX};
    UInt64 current_part_number{0};
    UInt64 current_part_row_count{0};

    NameSet current_high_priority_dedup_partition;
    {
        std::lock_guard lock(high_priority_dedup_partition_mutex);
        current_high_priority_dedup_partition = high_priority_dedup_partition;
    }

    auto add_current_staged_parts = [&](MergeTreeDataPartCNCHPtr & part) -> void {
        current_part_number++;
        current_part_row_count += part->rows_count;
        min_staged_part_timestamp = std::min(min_staged_part_timestamp, part->commit_time.toUInt64());
        LOG_DEBUG(log, "Dedup staged part: {}, commit time: {} ms.", part->name, part->commit_time.toMillisecond());
        current_staged_parts.push_back(part);
    };

    bool need_second_found_filter = true;
    /// Pick current_staged_parts in two rounds
    /// First round by current_high_priority_dedup_partition & commit_time & settings
    for (auto it = staged_parts.begin(); it != staged_parts.end();)
    {
        if (current_high_priority_dedup_partition.count((*it)->get_info().partition_id))
        {
            add_current_staged_parts(*it);
            if (current_part_number >= storage.getSettings()->max_staged_part_number_per_task || current_part_row_count >= storage.getSettings()->max_staged_part_rows_per_task)
            {
                need_second_found_filter = false;
                break;
            }
            it = staged_parts.erase(it);
        }
        else
            it++;
    }
    /// Second round by commit_time & settings
    if (need_second_found_filter)
    {
        for (auto & part : staged_parts)
        {
            add_current_staged_parts(part);
            if (current_part_number >= storage.getSettings()->max_staged_part_number_per_task || current_part_row_count >= storage.getSettings()->max_staged_part_rows_per_task)
                break;
        }
    }

    Stopwatch watch;
    LocalDeleteBitmaps bitmaps_to_dump = current_deduper->dedupParts(
        txn->getTransactionID(),
        CnchPartsHelper::toIMergeTreeDataPartsVector(visible_parts),
        CnchPartsHelper::toIMergeTreeDataPartsVector(current_staged_parts));
    LOG_DEBUG(
        log,
        "Dedup took {} ms in total, processed {} staged parts with {} parts",
        watch.elapsedMilliseconds(),
        current_staged_parts.size(),
        visible_parts.size());

    copy_status.last_task_staged_part_cnt = current_staged_parts.size();
    copy_status.last_task_visible_part_cnt = visible_parts.size();
    copy_status.last_task_dedup_cost_ms = watch.elapsedMilliseconds();

    size_t staged_parts_total_rows = 0, visible_parts_total_rows = 0;
    for (auto & part : current_staged_parts)
        staged_parts_total_rows += part->rows_count;
    for (auto & part : visible_parts)
        visible_parts_total_rows += part->rows_count;
    copy_status.last_task_staged_part_total_rows = staged_parts_total_rows;
    copy_status.last_task_visible_part_total_rows = visible_parts_total_rows;

    watch.restart();
    CnchDataWriter cnch_writer(storage, dedup_context, ManipulationType::Insert);

    if (!current_deduper->isCancelled())
    {
        cnch_writer.publishStagedParts(current_staged_parts, bitmaps_to_dump);
        copy_status.last_task_publish_cost_ms = watch.elapsedMilliseconds();
        LOG_DEBUG(log, "publishing took {} ms, txn_id: {}", watch.elapsedMilliseconds(), txn->getTransactionID().toUInt64());
    }
    else
    {
        /// convent to a relative large UInt64 num which indicates that the last iteration has been cancelled
        copy_status.last_task_publish_cost_ms = -1;
        LOG_DEBUG(log, "current deduper iterate has been cancelled, commit dedup txn {} without publishStagedParts", txn->getTransactionID().toUInt64());
    }

    txn->commitV2();
    /// we need to reset context for current_deduper to release lock here
    current_deduper->setDeduperContext(context);
    LOG_INFO(log, "Committed dedup txn {} (with {} ms holding lock)", txn->getTransactionID().toUInt64(), lock_watch.elapsedMilliseconds());

    interval_scheduler.calNextScheduleTime(min_staged_part_timestamp, context->getTimestamp());

    copy_status.total_dedup_cnt++;
    copy_status.last_task_total_cost_ms = cost_all.elapsedMilliseconds();
    copy_status.last_schedule_wait_ms = interval_scheduler.getScheduleTime();

    std::lock_guard lock(status_mutex);
    status = std::move(copy_status);
}

void CloudMergeTreeDedupWorker::setServerIndexAndHostPorts(size_t deduper_index, HostWithPorts host_ports)
{
    {
        std::lock_guard lock(server_mutex);
        index = deduper_index;
        server_host_ports = std::move(host_ports);
    }

    heartbeat_task = context->getSchedulePool().createTask(log_name, [this] { heartbeat(); });
    heartbeat_task->activateAndSchedule();
}

HostWithPorts CloudMergeTreeDedupWorker::getServerHostWithPorts()
{
    std::lock_guard lock(server_mutex);
    return server_host_ports;
}

void CloudMergeTreeDedupWorker::assignHighPriorityDedupPartition(const NameSet & high_priority_dedup_partition_)
{
    std::lock_guard lock(high_priority_dedup_partition_mutex);
    high_priority_dedup_partition = std::move(high_priority_dedup_partition_);
}

void CloudMergeTreeDedupWorker::assignRepairGran(const String & partition_id, const Int64 & bucket_number, const UInt64 & max_event_time)
{
    std::lock_guard lock(repair_grans_mutex);
    DedupGran tmp_repair_gran{partition_id, bucket_number};
    /// Just return and do nothing if tmp_repair_gran last completion time + duplicate_repair_interval > max_event_time
    if (history_repair_gran_expiration_time.count(tmp_repair_gran) && history_repair_gran_expiration_time[tmp_repair_gran] > max_event_time)
        return;
    if (!count(current_repair_grans.begin(), current_repair_grans.end(), tmp_repair_gran))
        current_repair_grans.emplace_back(tmp_repair_gran);
}

DedupWorkerStatus CloudMergeTreeDedupWorker::getDedupWorkerStatus()
{
    DedupWorkerStatus res;
    {
        std::lock_guard lock(status_mutex);
        res = status;
    }
    {
        std::lock_guard lock(current_deduper_mutex);
        if (current_deduper)
            res.dedup_tasks_progress = current_deduper->getTasksProgress();
    }
    return res;
}

void CloudMergeTreeDedupWorker::cancelDedupTasks()
{
    std::lock_guard lock(current_deduper_mutex);
    if (current_deduper)
        current_deduper->setCancelled();
}

void CloudMergeTreeDedupWorker::heartbeat()
{
    if (!isActive())
        return;
    auto & server_pool = context->getCnchServerClientPool();

    try
    {
        auto server_client = server_pool.get(server_host_ports);
        UInt32 ret = server_client->reportDeduperHeartbeat(storage.getCnchStorageID(), storage.getTableName());
        if (ret == DedupWorkerHeartbeatResult::Kill)
        {
            LOG_DEBUG(log, "Deduper will shutdown as it received a kill signal.");
            detachSelf();
            return;
        }
        else if (ret == DedupWorkerHeartbeatResult::Success)
        {
            last_heartbeat_time = time(nullptr);
        }
        else
            throw Exception("Deduper buffer received invalid response", ErrorCodes::LOGICAL_ERROR);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        auto now = time(nullptr);
        LOG_DEBUG(log, "Dedup time interval not receive hb: {}", UInt64(now - last_heartbeat_time));
        {
            std::lock_guard lock(status_mutex);
            status.last_exception = getCurrentExceptionMessage(false);
            status.last_exception_time = time(nullptr);
        }
        if (UInt64(now - last_heartbeat_time) > storage.getSettings()->dedup_worker_max_heartbeat_interval)
        {
            LOG_DEBUG(log, "Deduper will shutdown as heartbeat reached time out.");
            detachSelf();
            return;
        }
    }

    if (isActive())
        heartbeat_task->scheduleAfter(storage.getContext()->getSettingsRef().dedup_worker_heartbeat_ms.totalMilliseconds());
}

void CloudMergeTreeDedupWorker::detachSelf()
{
    is_stopped = true;
    ThreadFromGlobalPool([log = this->log, s = this->storage.shared_from_this(), c = this->context] {
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->database = s->getDatabaseName();
        drop_query->table = s->getTableName();
        drop_query->kind = ASTDropQuery::Drop;
        drop_query->if_exists = true;
        LOG_DEBUG(log, "Detach self: {}", s->getStorageID().getNameForLogs());
        InterpreterDropQuery(drop_query, c).execute();
        LOG_DEBUG(log, "Detached self: {}", s->getStorageID().getNameForLogs());
    }).detach();
}

void CloudMergeTreeDedupWorker::runDataRepairTask()
{
    size_t sleep_ms = 3 * 1000;

    try
    {
        auto catalog = context->getCnchCatalog();
        TxnTimestamp ts = context->getTimestamp();
        DedupGran repair_gran;
        {
            std::lock_guard lock(repair_grans_mutex);
            /// Check and expire history_repair_gran_expiration_time
            /// Consistent with the server, need to delay expiration
            auto check_time = ts.toSecond() - storage.getSettings()->duplicate_repair_interval.totalSeconds();
            for (auto it = history_repair_gran_expiration_time.begin(); it != history_repair_gran_expiration_time.end();)
            {
                if (it->second < check_time)
                    it = history_repair_gran_expiration_time.erase(it);
                else
                    it++;
            }
            LOG_TRACE(log, "history_repair_gran_expiration_time size: {}, check time: {}", history_repair_gran_expiration_time.size(), check_time);
            if (!current_repair_grans.empty())
                repair_gran = {current_repair_grans.begin()->partition_id, current_repair_grans.begin()->bucket_number};
        }
        auto server_client = context->getCnchServerClientPool().get(server_host_ports);
        if (!repair_gran.empty() && server_client)
        {
            LOG_DEBUG(log, "Start to repair gran: {}", repair_gran.getDedupGranDebugInfo());
            ASTSystemQuery repair_query;
            ParserPartition parser_partition;
            if (repair_gran.partition_id != ALL_DEDUP_GRAN_PARTITION_ID)
            {
                auto partition = std::make_shared<ASTPartition>();
                partition->id = repair_gran.partition_id;
                repair_query.partition = partition;
            }
            repair_query.specify_bucket = repair_gran.bucket_number != -1;
            repair_query.bucket_number = repair_gran.bucket_number;

            /// must use cnch table to construct staged parts
            auto table = catalog->tryGetTableByUUID(*context, UUIDHelpers::UUIDToString(storage.getCnchStorageUUID()), ts);
            if (!table)
            {
                LOG_INFO(log, "table have been dropped, skip");
                return;
            }
            auto *cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
            if (!cnch_table)
            {
                LOG_ERROR(log, "table {} is not cnch merge tree", table->getStorageID().getNameForLogs());
                return;
            }
            /// create repair_context and set txn
            auto repair_context = Context::createCopy(context);
            repair_context->setSessionContext(repair_context);
            CnchWorkerTransactionPtr txn = std::make_shared<CnchWorkerTransaction>(repair_context, server_client);
            repair_context->setCurrentTransaction(txn);
            txn->setMainTableUUID(storage.getCnchStorageUUID());

            cnch_table->executeDedupForRepair(repair_query, repair_context);
            {
                std::lock_guard lock(repair_grans_mutex);
                TxnTimestamp complete_time = context->getTimestamp();
                history_repair_gran_expiration_time[repair_gran] = complete_time.toSecond() + storage.getSettings()->duplicate_repair_interval.totalSeconds();
                LOG_DEBUG(log, "Repair gran {} next auto repair time: {}", repair_gran.getDedupGranDebugInfo(), history_repair_gran_expiration_time[repair_gran]);
                auto it = find(current_repair_grans.begin(), current_repair_grans.end(), repair_gran);
                current_repair_grans.erase(it);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    data_repairer->scheduleAfter(sleep_ms);
}

}
