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

#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/DedupWorkerManager.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

DedupWorkerManager::DedupWorkerManager(ContextPtr context_, const StorageID & storage_id_)
    : ICnchBGThread(context_, CnchBGThreadType::DedupWorker, storage_id_)
{
}

DedupWorkerManager::~DedupWorkerManager()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void DedupWorkerManager::runImpl()
{
    try
    {
        auto istorage = getStorageFromCatalog();
        auto & cnch_table = checkAndGetCnchTable(istorage);
        iterate(istorage, cnch_table);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    const auto ITERATE_INTERVAL_MS = 3 * 1000;
    scheduled_task->scheduleAfter(ITERATE_INTERVAL_MS);
}

void DedupWorkerManager::clearData()
{
    std::lock_guard lock(deduper_infos_mutex);
    for (auto & info : deduper_infos)
        stopDeduperWorker(info);
    deduper_infos.clear();
    if (data_checker)
        data_checker->stop();
    initialized = false;
}

void DedupWorkerManager::iterate(StoragePtr & storage, StorageCnchMergeTree & cnch_table)
{
    if (!initialized)
        initialize(storage, cnch_table);

    /// If initialized failed, return and wait for next iterate
    if (!initialized)
        return;

    /// Make a copy of `deduper_infos` for each iteration to avoid hold the lock for a long time
    std::vector<DeduperInfoPtr> current_dedup_infos;
    {
        std::lock_guard lock(deduper_infos_mutex);
        current_dedup_infos = deduper_infos;
    }

    /// check if storage id is changed, such as rename table
    if (storage_id.getFullTableName() != storage->getStorageID().getFullTableName())
    {
        LOG_INFO(
            log,
            "cnch table storage id has changed, reassign dedup workers. Origin storage id: {}, current storage id: {}",
            storage_id.getNameForLogs(),
            storage->getStorageID().getNameForLogs());
        // correct storage id and reset all worker client
        const_cast<StorageID &>(storage_id) = storage->getStorageID();
        for (auto & info : current_dedup_infos)
        {
            std::unique_lock info_lock(info->mutex);
            unsetWorkerClient(info, info_lock);
        }
    }

    Names high_priority_partition = dedup_scheduler->getHighPriorityPartition();
    for (auto & info : current_dedup_infos)
    {
        std::unique_lock info_lock(info->mutex);

        if (!checkDedupWorkerStatus(info, info_lock))
            createDeduperOnWorker(storage, cnch_table, info, info_lock);

        assignHighPriorityDedupPartition(info, high_priority_partition, info_lock);
    }

    if (cnch_table.getSettings()->duplicate_auto_repair)
    {
        DedupGranTimeMap dedup_gran_time_map = data_checker->getNeedRepairGrans(cnch_table);
        for (const auto & dedup_gran_entry : dedup_gran_time_map)
        {
            auto dedup_gran = dedup_gran_entry.first;
            auto dedup_worker_index = (dedup_gran.hash() % cnch_table.getSettings()->max_dedup_worker_number) % current_dedup_infos.size();

            auto info = current_dedup_infos.at(dedup_worker_index);
            std::unique_lock info_lock(info->mutex);
            LOG_TRACE(log, "Assigning a repair gran {} to worker, max event time: {}, worker info: {}", dedup_gran.getDedupGranDebugInfo(), dedup_gran_entry.second, getDedupWorkerDebugInfo(info, info_lock));
            assignRepairGran(info, dedup_gran, dedup_gran_entry.second, info_lock);
            LOG_DEBUG(log, "Assigned a repair gran {} to worker, max event time: {}, worker info: {}", dedup_gran.getDedupGranDebugInfo(), dedup_gran_entry.second, getDedupWorkerDebugInfo(info, info_lock));
        }
    }
}

void DedupWorkerManager::initialize(StoragePtr & storage, StorageCnchMergeTree & cnch_table)
{
    std::lock_guard lock(deduper_infos_mutex);
    try {
        dedup_scheduler = std::make_shared<DedupScheduler>();
        for (size_t i = 0; i < cnch_table.getSettings()->max_dedup_worker_number; ++i)
        {
            deduper_infos.emplace_back();
            auto & info = deduper_infos.back();
            info = std::make_shared<DeduperInfo>(i, storage->getStorageID());
        }
        if (cnch_table.getSettings()->check_duplicate_key || cnch_table.getSettings()->duplicate_auto_repair)
        {
            data_checker = std::make_shared<DedupDataChecker>(getContext(), storage->getCnchStorageID().getNameForLogs() + "(data_checker)", cnch_table);
            if (cnch_table.getSettings()->check_duplicate_key)
                data_checker->start();
        }
        initialized = true;
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to init deduper_infos due to {}", getCurrentExceptionMessage(false));
        deduper_infos.clear();
        if (data_checker)
            data_checker->stop();
        initialized = false;
    }
}

void DedupWorkerManager::createDeduperOnWorker(StoragePtr & storage, StorageCnchMergeTree & cnch_table, DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock)
{
    if (info->worker_client)
        return;
    try
    {
        auto cnch_storage_id = storage->getStorageID();
        info->worker_storage_id = {cnch_storage_id.getDatabaseName(), cnch_storage_id.getTableName()};
        selectDedupWorker(cnch_table, info, info_lock);

        /// create a unique table suffix
        String deduper_table_suffix = '_' + toString(std::chrono::system_clock::now().time_since_epoch().count()) + '_' + toString(info->index);
        info->worker_storage_id.table_name = storage_id.table_name + deduper_table_suffix;

        auto create_ast = getASTCreateQueryFromStorage(*storage, getContext());
        auto & create = *create_ast;
        create.table =  info->worker_storage_id.table_name;
        replaceCnchWithCloud(create.storage, cnch_storage_id.getDatabaseName(), cnch_storage_id.getTableName());
        modifyOrAddSetting(create, "cloud_enable_dedup_worker", Field(UInt64(1)));
        modifyOrAddSetting(create, "allow_nullable_key", Field(UInt64(1)));
        /// Set cnch uuid for CloudMergeTree to commit data on worker side
        modifyOrAddSetting(create, "cnch_table_uuid", Field(static_cast<String>(UUIDHelpers::UUIDToString(create_ast->uuid))));
        /// It's not allowed to create multi tables with same uuid on Cnch-Worker side now
        create.uuid = UUIDHelpers::Nil;
        String create_query = getTableDefinitionFromCreateQuery(static_pointer_cast<IAST>(create_ast), false);
        LOG_TRACE(log, "Create table query of dedup worker: {}", create_query);

        LOG_DEBUG(log, "Assigning a new dedup worker: {}", getDedupWorkerDebugInfo(info, info_lock));
        info->worker_client->createDedupWorker(info->worker_storage_id, create_query, getContext()->getHostWithPorts(), info->index);
        markDedupWorker(info, info_lock);
        LOG_DEBUG(log, "Assigned a new dedup worker: {}", getDedupWorkerDebugInfo(info, info_lock));
    }
    catch (...)
    {
        LOG_ERROR(log, "Fail to assign a new dedup worker due to {}", getCurrentExceptionMessage(false));
        unsetWorkerClient(info, info_lock);
    }
}

void DedupWorkerManager::selectDedupWorker(StorageCnchMergeTree & cnch_table, DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> &  /*info_lock*/)
{
    auto vw_handle = getContext()->getVirtualWarehousePool().get(cnch_table.getSettings()->cnch_vw_write);
    HostWithPorts history_dedup_worker = dedup_scheduler->tryPickWorker(info->index);
    try{
        if (!history_dedup_worker.empty())
            info->worker_client = vw_handle->getWorkerByHostWithPorts(history_dedup_worker);
        else
            info->worker_client = vw_handle->getWorker();
    }
    catch (...)
    {
        info->worker_client = vw_handle->getWorker();
    }
}

void DedupWorkerManager::markDedupWorker(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> &  /*info_lock*/)
{
    dedup_scheduler->markIndexDedupWorker(info->index, info->worker_client->getHostWithPorts());
    info->is_running = true;
}

void DedupWorkerManager::assignHighPriorityDedupPartition(DeduperInfoPtr & info, const Names & high_priority_partition, std::unique_lock<bthread::Mutex> & /*info_lock*/)
{
    if (!info->worker_client)
        return;

    info->worker_client->assignHighPriorityDedupPartition(info->worker_storage_id, high_priority_partition);
}

void DedupWorkerManager::unsetWorkerClient(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> &  /*info_lock*/)
{
    info->worker_client = nullptr;
    info->is_running = false;
}

void DedupWorkerManager::assignRepairGran(DeduperInfoPtr & info, const DedupGran & dedup_gran, const UInt64 & max_event_time, std::unique_lock<bthread::Mutex> & /*info_lock*/)
{
    if (!info->worker_client)
        return;

    info->worker_client->assignRepairGran(info->worker_storage_id, dedup_gran.partition_id, dedup_gran.bucket_number, max_event_time);
}

void DedupWorkerManager::stopDeduperWorker(DeduperInfoPtr & info)
{
    std::unique_lock info_lock(info->mutex);

    if (!info->worker_client)
        return;
    try
    {
        LOG_DEBUG(log, "Try to stop deduper: {}", getDedupWorkerDebugInfo(info, info_lock));
        info->worker_client->dropDedupWorker(info->worker_storage_id);
        LOG_DEBUG(log, "Stop deduper successfully: {}", getDedupWorkerDebugInfo(info, info_lock));
    }
    catch (...)
    {
        LOG_ERROR(log, "Fail to stop deduper due to {}", getCurrentExceptionMessage(false));
        // In this case, it's not necessary to repeat the stop action, just set worker_client to nullptr is enough
        // because dedup worker will check its validity via heartbeat and it'll stop itself soon.
    }
    unsetWorkerClient(info, info_lock);
}

String DedupWorkerManager::getDedupWorkerDebugInfo(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> &  /*info_lock*/)
{
    if (!info->worker_client)
        return "dedup worker is not assigned.";
    return "worker table is " + info->worker_storage_id.getNameForLogs() + ", host_with_port is "
        + info->worker_client->getHostWithPorts().toDebugString();
}

bool DedupWorkerManager::checkDedupWorkerStatus(DeduperInfoPtr & info, std::unique_lock<bthread::Mutex> & info_lock)
{
    if (!info->worker_client)
        return false;
    try
    {
        DedupWorkerStatus status = info->worker_client->getDedupWorkerStatus(info->worker_storage_id);
        if (!status.is_active)
        {
            LOG_WARNING(log, "Deduper is inactive, try to assign a new one. Old deduper info: {}", getDedupWorkerDebugInfo(info, info_lock));
            unsetWorkerClient(info, info_lock);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        /// just assign a new dedup worker as deduper will check validity.
        LOG_ERROR(log, "Failed to get deduper status of {}, try to assign a new one.", info->worker_storage_id.getNameForLogs());
        unsetWorkerClient(info, info_lock);
    }
    return info->worker_client != nullptr;
}

std::vector<DedupWorkerStatus> DedupWorkerManager::getDedupWorkerStatus()
{
    std::vector<DedupWorkerStatus> ret;
    std::lock_guard lock(deduper_infos_mutex);

    for (auto & info : deduper_infos)
    {
        std::lock_guard info_lock(info->mutex);
        DedupWorkerStatus status;
        if (!info->worker_client)
            status.is_active = false;
        else
        {
            status = info->worker_client->getDedupWorkerStatus(info->worker_storage_id);
            auto worker_host_ports = info->worker_client->getHostWithPorts();
            status.worker_rpc_address = worker_host_ports.getRPCAddress();
            status.worker_tcp_address = worker_host_ports.getTCPAddress();
        }
        ret.emplace_back(status);
    }
    return ret;
}

void DedupWorkerManager::dedupWithHighPriority(const ASTPtr & partition, const ContextPtr & local_context)
{
    std::lock_guard lock(deduper_infos_mutex);
    if (!initialized)
        throw Exception("Dedup manager has not been initialized", ErrorCodes::LOGICAL_ERROR);

    auto istorage = getStorageFromCatalog();
    auto & cnch_table = checkAndGetCnchTable(istorage);
    if (!cnch_table.getSettings()->partition_level_unique_keys)
        throw Exception("Unique table with partition_level_unique_keys=0 does not allow dedupWithHighPriority", ErrorCodes::BAD_ARGUMENTS);

    if (partition)
    {
        auto partition_id = cnch_table.getPartitionIDFromQuery(partition, local_context);
        LOG_DEBUG(log, "Deduper partition {} with high priority", partition_id);
        dedup_scheduler->dedupWithHighPriority(partition_id);
    }
}

DedupWorkerHeartbeatResult DedupWorkerManager::reportHeartbeat(const String & worker_table_name)
{
    LOG_TRACE(log, "Report heartbeat of dedup worker: worker table name is {}", worker_table_name);
    std::lock_guard lock(deduper_infos_mutex);
    for (const auto & info : deduper_infos)
    {
        std::lock_guard info_lock(info->mutex);
        if (info->worker_client != nullptr && worker_table_name == info->worker_storage_id.table_name)
            return DedupWorkerHeartbeatResult::Success;
    }
    return DedupWorkerHeartbeatResult::Kill;
}

}
