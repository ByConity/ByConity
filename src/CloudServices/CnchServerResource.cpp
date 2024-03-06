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

#include <unordered_map>
#include <Catalog/CatalogUtils.h>
#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchServerResource.h>
#include <CloudServices/CnchWorkerResource.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/WorkerStatusManager.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <Storages/Hive/HiveFile/IHiveFile.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <brpc/controller.h>
#include "Common/ProfileEvents.h"
#include "common/logger_useful.h"
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

#include <Storages/RemoteFile/StorageCnchHDFS.h>
#include <Storages/RemoteFile/StorageCnchS3.h>
#include <Storages/StorageCnchMergeTree.h>

namespace ProfileEvents
{
    extern const Event CnchPartAllocationSplits;
    extern const Event CnchSendResourceRpcCallElapsedMilliseconds;
    extern const Event CnchSendResourceElapsedMilliseconds;
}

namespace DB
{
AssignedResource::AssignedResource(const StoragePtr & storage_) : storage(storage_)
{
}

AssignedResource::AssignedResource(AssignedResource && resource)
{
    storage = resource.storage;
    worker_table_name = resource.worker_table_name;
    create_table_query = resource.create_table_query;
    sent_create_query = resource.sent_create_query;
    bucket_numbers = resource.bucket_numbers;
    replicated = resource.replicated;

    server_parts = std::move(resource.server_parts);
    hive_parts = std::move(resource.hive_parts);
    file_parts = std::move(resource.file_parts);
    part_names = resource.part_names; // don't call move here

    resource.sent_create_query = true;
    object_columns = resource.object_columns;
}

void AssignedResource::addDataParts(const ServerDataPartsVector & parts)
{
    for (const auto & part : parts)
    {
        if (!part_names.contains(part->name()))
        {
            part_names.emplace(part->name());
            server_parts.emplace_back(part);
        }
    }
}

void AssignedResource::addDataParts(const HiveFiles & parts)
{
    for (const auto & part : parts)
    {
        auto [it, insert] = part_names.emplace(part->file_path);
        if (!insert) {
            // what to do here? if we addede duplicated file path
            // self join case may trigger the exception
            // throw Exception(ErrorCodes::BAD_ARGUMENTS, "Find duplicated part name '{}'", part->file_path);
        }
        else {
            hive_parts.emplace_back(part);
        }
    }
}

void AssignedResource::addDataParts(const FileDataPartsCNCHVector & parts)
{
    for (const auto & file : parts)
    {
        if (!part_names.count(file->info.getBasicPartName()))
        {
            part_names.emplace(file->info.getBasicPartName());
            file_parts.emplace_back(file);
        }
    }
}

void ResourceStageInfo::filterResource(std::optional<ResourceOption> resource_option)
{
    if (resource_option)
    {
        for (auto iter = resource_option->table_ids.begin(); iter != resource_option->table_ids.end();)
        {
            if (sent_resource.find(*iter) != sent_resource.end())
            {
                iter = resource_option->table_ids.erase(iter);
            }
            else
            {
                sent_resource.insert(*iter);
                iter++;
            }
        }
    }

}

void CnchServerResource::cleanResource()
{
    {
        auto lock = getLock();
        assigned_table_resource.clear();
        assigned_worker_resource.clear();
        // assigned_storage_workers.clear();
    }

    cleanResourceInWorker();
}

void CnchServerResource::cleanResourceInWorker()
{
    if (!worker_group || skip_clean_worker)
        return;

    auto worker_clients = worker_group->getWorkerClients();

    for (auto & worker_client : worker_clients)
    {
        try
        {
            worker_client->removeWorkerResource(txn_id);
        }
        catch (...)
        {
            tryLogCurrentException(
                log, "Error occurs when remove WorkerResource{" + txn_id.toString() + "} in worker " + worker_client->getRPCAddress());
        }
    }
}


CnchServerResource::~CnchServerResource()
{
    try
    {
        cleanResourceInWorker();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void CnchServerResource::addCreateQuery(
    const ContextPtr & context,
    const StoragePtr & storage,
    const String & create_query,
    const String & worker_table_name,
    bool create_local_table)
{
    /// table should exists in SelectStreamFactory::createForShard
    /// so we create table in worker in advance
    if (context->getServerType() == ServerType::cnch_worker && create_local_table)
    {
        auto temp_context = Context::createCopy(context);
        auto worker_resource = context->getCnchWorkerResource();
        worker_resource->executeCreateQuery(temp_context, create_query, /* skip_if_exists */ true);
    }

    auto lock = getLock();

    auto it = assigned_table_resource.find(storage->getStorageUUID());
    if(storage->getStorageUUID() == UUIDHelpers::Nil)
    {
        // throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "UUID for resources should not be nil");
        LOG_DEBUG(log, "UUID for resources should not be nil, query: {}, table: {}", create_query, worker_table_name);
    }
    if (it == assigned_table_resource.end())
        it = assigned_table_resource.emplace(storage->getStorageUUID(), AssignedResource{storage}).first;

    it->second.create_table_query = create_query;
    it->second.worker_table_name = worker_table_name;
}

void CnchServerResource::sendResource(const ContextPtr & context, const HostWithPorts & worker)
{
    Stopwatch watch;
    /**
     * send_lock:
     * For union query, it may send resources to a worker multiple times,
     * If it is sent concurrently, the data may not be ready when one of the sub-queries is executed
     * So we need to avoid this situation by taking the send_lock.
     */
    auto send_lock = getLockForSend(worker.getRPCAddress());

    std::vector<AssignedResource> resources_to_send;
    {
        auto lock = getLock();
        allocateResource(context, lock);

        auto it = assigned_worker_resource.find(worker);
        if (it == assigned_worker_resource.end())
            return;

        resources_to_send = std::move(it->second);
        assigned_worker_resource.erase(it);
    }

    auto handler = std::make_shared<ExceptionHandlerWithFailedInfo>();
    auto worker_client = worker_group->getWorkerClient(worker);
    auto full_worker_id = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), worker.id);
    auto call_id = worker_client->sendResources(context, resources_to_send, handler, full_worker_id, send_mutations);
    ProfileEvents::increment(ProfileEvents::CnchSendResourceRpcCallElapsedMilliseconds, watch.elapsedMilliseconds());
    brpc::Join(call_id);
    handler->throwIfException();
    ProfileEvents::increment(ProfileEvents::CnchSendResourceElapsedMilliseconds, watch.elapsedMilliseconds());
}

void CnchServerResource::sendResources(const ContextPtr & context, std::optional<ResourceOption> resource_option)
{
    Stopwatch watch;
    auto send_lock = getLockForSend("ALL_WORKER");

    // filter resource for stage send resource
    resource_stage_info.filterResource(resource_option);
    std::unordered_map<HostWithPorts, std::vector<AssignedResource>> all_resources;
    {
        auto lock = getLock();
        allocateResource(context, lock, resource_option);

        if (!worker_group)
            return;

        std::swap(all_resources, assigned_worker_resource);
    }

    if (all_resources.empty())
        return;
    computeResourceSize(resource_option, all_resources);
    auto handler = std::make_shared<ExceptionHandlerWithFailedInfo>();
    std::vector<brpc::CallId> call_ids;
    call_ids.resize(all_resources.size());
    auto worker_send_resources = [&](const HostWithPorts & host_ports, const std::vector<AssignedResource> & resources_to_send, size_t i)
    {
        auto worker_client = worker_group->getWorkerClient(host_ports);
        auto full_worker_id = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), host_ports.id);
        call_ids[i] = worker_client->sendResources(context, resources_to_send, handler, full_worker_id, send_mutations);
    };

    size_t max_threads = Catalog::getMaxThreads();
    size_t i = 0;
    if (max_threads < 2 || all_resources.size() < 2)
    {
        for (auto & [host_ports_, resources_] : all_resources)
        {
            worker_send_resources(host_ports_, resources_, i);
            i++;
        }
    }
    else
    {
        max_threads = std::min(max_threads, all_resources.size());
        ExceptionHandler exception_handler;
        ThreadPool thread_pool(max_threads);
        for (auto it = all_resources.begin(); it != all_resources.end(); it++)
        {
            thread_pool.scheduleOrThrowOnError(createExceptionHandledJob(
                [&, i, it]()
                    {
                        worker_send_resources(it->first, it->second, i);
                    },
                exception_handler));
            i++;
        }
        thread_pool.wait();
        exception_handler.throwIfException();
    }

    ProfileEvents::increment(ProfileEvents::CnchSendResourceRpcCallElapsedMilliseconds, watch.elapsedMilliseconds());

    for (auto & call_id : call_ids)
        brpc::Join(call_id);

    auto worker_group_status = context->getWorkerGroupStatusPtr();
    if (worker_group_status)
    {
        auto rpc_infos = handler->getFailedRpcInfo();
        for (const auto & [worker_id, error_code] : rpc_infos)
            context->getWorkerStatusManager()->setWorkerNodeDead(worker_id, error_code);

        for (const auto & worker_id : worker_group_status->getHalfOpenWorkers())
        {
            if (rpc_infos.count(worker_id) == 0)
                context->getWorkerStatusManager()->CloseCircuitBreaker(worker_id);
        }
        worker_group_status->clearHalfOpenWorkers();
    }

    handler->throwIfException();

    ProfileEvents::increment(ProfileEvents::CnchSendResourceElapsedMilliseconds, watch.elapsedMilliseconds());
}

void CnchServerResource::sendResources(const ContextPtr & context, WorkerAction act)
{
    auto handler = std::make_shared<ExceptionHandler>();
    std::unordered_map<HostWithPorts, std::vector<AssignedResource>> all_resources;
    {
        auto lock = getLock();
        allocateResource(context, lock);

        if (!worker_group)
            return;

        std::swap(all_resources, assigned_worker_resource);
    }

    std::vector<brpc::CallId> call_ids;
    for (auto & [host_ports, resource] : all_resources)
    {
        auto worker_client = worker_group->getWorkerClient(host_ports);
        auto ids = act(worker_client, resource, handler);
        call_ids.insert(call_ids.end(), ids.begin(), ids.end());
    }

    for (auto & call_id : call_ids)
        brpc::Join(call_id);

    handler->throwIfException();
}

void CnchServerResource::allocateResource(
    const ContextPtr & context, std::lock_guard<std::mutex> &, std::optional<ResourceOption> resource_option)
{
    std::vector<AssignedResource> resource_to_allocate;

    for (auto & [table_id, resource] : assigned_table_resource)
    {
        if (resource.empty())
            continue;

        if (resource_option && !(*resource_option).table_ids.count(table_id))
            continue;

        resource_to_allocate.emplace_back(std::move(resource));
    }

    if (resource_to_allocate.empty())
        return;

    if (!worker_group)
        worker_group = context->tryGetCurrentWorkerGroup();

    if (worker_group)
    {
        const auto & host_ports_vec = worker_group->getHostWithPortsVec();

        for (auto & resource : resource_to_allocate)
        {
            const auto & storage = resource.storage;
            const auto & server_parts = resource.server_parts;
            const auto & required_bucket_numbers = resource.bucket_numbers;
            ServerAssignmentMap assigned_map;
            HivePartsAssignMap assigned_hive_map;
            FilePartsAssignMap assigned_file_map;
            ServerDataPartsVector bucket_parts;
            ServerDataPartsVector leftover_server_parts;

            if (dynamic_cast<StorageCnchMergeTree *>(storage.get()))
            {
                // NOTE: server_parts maybe moved due to splitCnchParts and cannot be used again
                std::tie(bucket_parts, leftover_server_parts) = splitCnchParts(context, *storage, server_parts);
                if (!bucket_parts.empty() && !leftover_server_parts.empty())
                {
                    LOG_TRACE(
                        log,
                        "Cnch part allocation has been split. Bucket parts size = [{}], Server parts size = [{}]",
                        bucket_parts.size(),
                        leftover_server_parts.size());
                    ProfileEvents::increment(ProfileEvents::CnchPartAllocationSplits);
                }
                assigned_map = assignCnchParts(worker_group, leftover_server_parts);
                moveBucketTablePartsToAssignedParts(assigned_map, bucket_parts, worker_group->getWorkerIDVec(), required_bucket_numbers);
            }
            else if (auto * cnchhive = dynamic_cast<StorageCnchHive *>(storage.get()))
            {
                assigned_hive_map = assignCnchHiveParts(worker_group, resource.hive_parts);
            }
            else if (auto * cnch_file = dynamic_cast<IStorageCnchFile *>(storage.get()))
            {
                String file_storage = "unknown";
                if (auto * cnch_hdfs = dynamic_cast<StorageCnchHDFS *>(storage.get()))
                    file_storage = cnch_hdfs->getName();
                else if (auto * cnch_s3 = dynamic_cast<StorageCnchS3 *>(storage.get()))
                    file_storage = cnch_s3->getName();

                if (cnch_file->settings.resourcesAssignType() == StorageResourcesAssignType::SERVER_PUSH)
                {
                    bool use_simple_hash = cnch_file->settings.simple_hash_resources;
                    LOG_TRACE(log, "{} assignCnchFileParts use server push and use_simple_hash =  {}", file_storage, use_simple_hash);
                    assigned_file_map = assignCnchFileParts(worker_group, resource.file_parts);
                }
                else
                {
                    LOG_TRACE(log, "{} assignCnchFileParts use server local", file_storage);
                }
            }

            for (const auto & host_ports : host_ports_vec)
            {
                ServerDataPartsVector assigned_parts;
                FileDataPartsCNCHVector assigned_file_parts;
                HiveFiles assigned_hive_parts;
                if (auto it = assigned_map.find(host_ports.id); it != assigned_map.end())
                {
                    assigned_parts = std::move(it->second);
                    CnchPartsHelper::flattenPartsVector(assigned_parts);
                }

                if (auto it = assigned_hive_map.find(host_ports.id); it != assigned_hive_map.end())
                {
                    assigned_hive_parts = std::move(it->second);
                }


                if (auto it = assigned_file_map.find(host_ports.id); it != assigned_file_map.end())
                {
                    assigned_file_parts = std::move(it->second);
                    LOG_TRACE(
                        log,
                        "assign {}.{} file data parts to works {}, size = {}",
                        storage->getDatabaseName(),
                        storage->getTableName(),
                        host_ports.toDebugString(),
                        assigned_file_parts.size());
                }

                auto it = assigned_worker_resource.find(host_ports);
                if (it == assigned_worker_resource.end())
                {
                    it = assigned_worker_resource.emplace(host_ports, std::vector<AssignedResource>{}).first;
                }

                it->second.emplace_back(storage);
                auto & worker_resource = it->second.back();

                worker_resource.addDataParts(assigned_parts);
                worker_resource.addDataParts(assigned_hive_parts);
                worker_resource.addDataParts(assigned_file_parts);
                worker_resource.sent_create_query = resource.sent_create_query;
                worker_resource.create_table_query = resource.create_table_query;
                worker_resource.worker_table_name = resource.worker_table_name;
                worker_resource.object_columns = resource.object_columns;
            }
        }
    }
}

void CnchServerResource::computeResourceSize(
    std::optional<ResourceOption> & resource_option, std::unordered_map<HostWithPorts, std::vector<AssignedResource>> & all_resources)
{
    if (resource_option)
    {
        for (const auto & [host_ports, assinged_resource] : all_resources)
        {
            std::unordered_map<UUID, size_t> table_size;
            for (const auto & r : assinged_resource)
            {
                size_t s = std::accumulate(r.server_parts.cbegin(), r.server_parts.cend(), 0, [](size_t sum, ServerDataPartPtr p) {
                    return sum + p->rowsCount();
                });
                auto & t_size = table_size[r.storage->getStorageID().uuid];
                if (t_size)
                    t_size += s;
                else
                    t_size = s;
            }
            for (const auto & [t, s] : table_size)
            {
                assigned_resources_size[t][host_ports] = s;
            }
        }
    }
}

std::unordered_map<HostWithPorts, size_t> & CnchServerResource::getResourceSizeMap(UUID & table_id)
{
    return assigned_resources_size[table_id];
}
}
