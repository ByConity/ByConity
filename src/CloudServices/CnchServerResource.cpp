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

#include <CloudServices/CnchServerResource.h>

#include <Catalog/DataModelPartWrapper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchWorkerResource.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <brpc/controller.h>
#include <Common/Exception.h>
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/Hive/StorageCnchHive.h"

#include <Storages/StorageCnchMergeTree.h>

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

    server_parts = std::move(resource.server_parts);
    hive_parts = std::move(resource.hive_parts);
    part_names = resource.part_names; // don't call move here

    resource.sent_create_query = true;
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
        if (!part_names.count(part->file_path))
        {
            part_names.emplace(part->file_path);
            hive_parts.emplace_back(part);
        }
    }
}

CnchServerResource::~CnchServerResource()
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

void CnchServerResource::addCreateQuery(
    const ContextPtr & context, const StoragePtr & storage, const String & create_query, const String & worker_table_name)
{
    /// table should exists in SelectStreamFactory::createForShard
    /// so we create table in worker in advance
    if (context->getServerType() == ServerType::cnch_worker)
    {
        auto temp_context = Context::createCopy(context);
        auto worker_resource = context->getCnchWorkerResource();
        worker_resource->executeCreateQuery(temp_context, create_query, /* skip_if_exists */ true);
    }

    auto lock = getLock();

    auto it = assigned_table_resource.find(storage->getStorageUUID());
    if (it == assigned_table_resource.end())
        it = assigned_table_resource.emplace(storage->getStorageUUID(), AssignedResource{storage}).first;

    it->second.create_table_query = create_query;
    it->second.worker_table_name = worker_table_name;
}

void CnchServerResource::sendResource(const ContextPtr & context, const HostWithPorts & worker)
{
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
    auto call_id = worker_client->sendResources(context, resources_to_send, handler, full_worker_id);
    brpc::Join(call_id);
    handler->throwIfException();
}

void CnchServerResource::sendResources(const ContextPtr & context)
{
    auto send_lock = getLockForSend("ALL_WORKER");

    std::unordered_map<HostWithPorts, std::vector<AssignedResource>> all_resources;
    std::vector<brpc::CallId> call_ids;
    {
        auto lock = getLock();
        allocateResource(context, lock);

        if (!worker_group)
            return;

        std::swap(all_resources, assigned_worker_resource);
    }

    auto handler = std::make_shared<ExceptionHandlerWithFailedInfo>();

    for (const auto & [host_ports, resources_to_send] : all_resources)
    {
        auto worker_client = worker_group->getWorkerClient(host_ports);
        auto full_worker_id = WorkerStatusManager::getWorkerId(worker_group->getVWName(), worker_group->getID(), host_ports.id);
        call_ids.emplace_back(worker_client->sendResources(context, resources_to_send, handler, full_worker_id));
    }

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
}

void CnchServerResource::sendResources(const ContextPtr & context, WorkerAction act)
{
    auto handler = std::make_shared<ExceptionHandler>();
    std::unordered_map<HostWithPorts, std::vector<AssignedResource>> all_resources;
    std::vector<brpc::CallId> call_ids;
    {
        auto lock = getLock();
        allocateResource(context, lock);

        if (!worker_group)
            return;

        std::swap(all_resources, assigned_worker_resource);
    }

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

void CnchServerResource::allocateResource(const ContextPtr & context, std::lock_guard<std::mutex> &)
{
    std::vector<AssignedResource> resource_to_allocate;

    for (auto & [table_id, resource] : assigned_table_resource)
    {
        if (resource.empty())
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
            BucketNumbersAssignmentMap assigned_bucket_numbers_map;
            if (dynamic_cast<StorageCnchMergeTree *>(storage.get()))
            {
                if (isCnchBucketTable(context, *storage, server_parts))
                {
                    auto assignment = assignCnchPartsForBucketTable(server_parts, worker_group->getWorkerIDVec(), required_bucket_numbers);
                    assigned_map = assignment.parts_assignment_map;
                    assigned_bucket_numbers_map = assignment.bucket_number_assignment_map;
                }
                else
                    assigned_map = assignCnchParts(worker_group, server_parts);
            }
            else if (auto * cnchhive = dynamic_cast<StorageCnchHive *>(storage.get()))
            {
                assigned_hive_map = assignCnchHiveParts(worker_group, resource.hive_parts);
            }

            for (const auto & host_ports : host_ports_vec)
            {
                ServerDataPartsVector assigned_parts;
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

                std::set<Int64> assigned_bucket_numbers;
                if (auto it = assigned_bucket_numbers_map.find(host_ports.id); it != assigned_bucket_numbers_map.end())
                {
                    assigned_bucket_numbers = std::move(it->second);
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
                worker_resource.sent_create_query = resource.sent_create_query;
                worker_resource.create_table_query = resource.create_table_query;
                worker_resource.worker_table_name = resource.worker_table_name;
                worker_resource.bucket_numbers = assigned_bucket_numbers;
            }
        }
    }
}

}
