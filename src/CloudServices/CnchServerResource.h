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
#include <Common/Logger.h>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DataPart_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <Poco/Logger.h>
#include <Common/HostWithPorts.h>
#include <common/types.h>


namespace DB
{
class ServerResourceLockManager
{
public:
    void remove(const String & address)
    {
        std::lock_guard lock(mutex);
        hosts.erase(address);
        cv.notify_one();
    }

    void add(const String & address)
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&]() { return !hosts.count(address); });
        hosts.emplace(address);
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_set<std::string> hosts;
};

struct SendLock
{
    SendLock(const std::string & address_, ServerResourceLockManager & manager_) : address(address_), manager(manager_)
    {
        manager.add(address);
    }

    ~SendLock() { manager.remove(address); }

    std::string address;
    ServerResourceLockManager & manager;
};

struct TableDefinitionResource
{
    /// if cacheable == 0, it's the rewrited table definition for worker;
    /// otherwise, it's the original definition for cnch table
    String definition;
    String local_table_name;
    bool cacheable = false;
    WorkerEngineType engine_type = WorkerEngineType::CLOUD;
    String underlying_dictionary_tables; // local dictionary table names for bitengine
};

struct AssignedResource
{
    explicit AssignedResource(const StoragePtr & storage);

    AssignedResource(AssignedResource & resource);
    AssignedResource(AssignedResource && resource);

    StoragePtr storage;
    UInt64 table_version{0};  //send table version instead of parts if set
    TableDefinitionResource table_definition;
    bool sent_create_query{false};
    bool replicated{false};

    /// parts info
    ServerDataPartsVector server_parts;
    ServerVirtualPartVector virtual_parts;
    LakeScanInfos lake_scan_info_parts;
    FileDataPartsCNCHVector file_parts;
    std::set<Int64> bucket_numbers;

    std::unordered_set<String> part_names;

    ColumnsDescription object_columns;

    void addDataParts(const ServerDataPartsVector & parts);
    void addDataParts(ServerVirtualPartVector parts);
    void addDataParts(const LakeScanInfos & parts);
    void addDataParts(const FileDataPartsCNCHVector & parts);

    bool empty() const
    {
        return sent_create_query
            && table_version == 0
            && server_parts.empty()
            && virtual_parts.empty()
            && lake_scan_info_parts.empty()
            && file_parts.empty();
    }
};

// Send resources separately by UUID
struct ResourceOption
{
    std::unordered_set<UUID> table_ids;
    // resend the resources have been already sent to workers.
    bool resend = false;
};

struct ResourceStageInfo
{
    std::unordered_set<UUID> sent_resource;
    void filterResource(std::optional<ResourceOption> & resource_option);
};
class CnchServerResource
{
public:
    explicit CnchServerResource(TxnTimestamp curr_txn_id)
        : txn_id(curr_txn_id), log(getLogger("ServerResource"))
    {
    }

    ~CnchServerResource();

    using WorkerInfoSet = std::unordered_set<HostWithPorts, std::hash<HostWithPorts>, HostWithPorts::IsSameEndpoint>;

    void addCreateQuery(
        const ContextPtr & context,
        const StoragePtr & storage,
        const String & create_query,
        const String & worker_table_name,
        bool create_local_table = true);

    void addCacheableCreateQuery(
        const StoragePtr & storage,
        const String & worker_table_name,
        WorkerEngineType engine_type,
        String underlying_dictionary_tables);

    void setTableVersion(
        const UUID & storage_uuid,
        UInt64 table_version,
        const std::set<Int64> & required_bucket_numbers);

    void setAggregateWorker(HostWithPorts aggregate_worker_) { aggregate_worker = std::move(aggregate_worker_); }

    void setWorkerGroup(WorkerGroupHandle worker_group_)
    {
        if (!worker_group)
            worker_group = std::move(worker_group_);
    }

    void skipCleanWorker() { skip_clean_worker = true; }

    template <typename T>
    void addDataParts(
        const UUID & storage_id,
        const std::vector<T> & data_parts,
        const std::set<Int64> & required_bucket_numbers = {},
        bool replicated = false)
    {
        std::lock_guard lock(mutex);
        auto & assigned_resource = assigned_table_resource.at(storage_id);

        /// accumulate resources for a table
        assigned_resource.addDataParts(data_parts);
        assigned_resource.replicated = assigned_resource.replicated | replicated;
        assigned_resource.bucket_numbers.insert(required_bucket_numbers.begin(), required_bucket_numbers.end());
    }

    const WorkerInfoSet & getAssignedWorkers(const UUID & storage_uuid)
    {
        return assigned_storage_workers[storage_uuid];
    }

    void setResourceReplicated(const UUID & storage_id, bool replicated)
    {
        std::lock_guard lock(mutex);
        auto & assigned_resource = assigned_table_resource.at(storage_id);
        assigned_resource.replicated = replicated;
    }

    /// Send resource to worker
    /// NOTE: Only used when optimizer is disabled.
    void sendResource(const ContextPtr & context, const HostWithPorts & worker);
    /// Resend resource to worker, used in bsp retry.
    void resendResource(const ContextPtr & context, const HostWithPorts & worker);
    /// allocate and send resource to worker_group
    void sendResources(const ContextPtr & context, std::optional<ResourceOption> resource_option = std::nullopt);
    // prepare query resource buf for batch send plan segments
    void prepareQueryResourceBuf(std::unordered_map<WorkerId, butil::IOBuf, WorkerIdHash> & resource_buf_map, const ContextPtr & context);

    /// WorkerAction should not throw
    using WorkerAction
        = std::function<std::vector<brpc::CallId>(CnchWorkerClientPtr, const std::vector<AssignedResource> &, const ExceptionHandlerPtr &)>;
    /// Submit custom tasks (like "preload", "drop disk cache") to workers with allocated resources.
    void submitCustomTasks(const ContextPtr & context, WorkerAction act);
    void cleanResource();

    void addDynamicObjectSchema(const UUID & storage_id, const ColumnsDescription & object_columns_)
    {
        std::lock_guard lock(mutex);
        auto & assigned_resource = assigned_table_resource.at(storage_id);

        assigned_resource.object_columns = object_columns_;
    }

    void setSendMutations(bool send_mutations_) { send_mutations = send_mutations_; }

    const std::unordered_map<UUID, std::unordered_map<AddressInfo, SourceTaskPayload, AddressInfo::Hash>> & getSourceTaskPayload() const
    {
        return source_task_payload;
    }
    std::unordered_map<UUID, std::unordered_map<AddressInfo, SourceTaskPayload, AddressInfo::Hash>> & getSourceTaskPayloadRef()
    {
        return source_task_payload;
    }

private:
    auto getLock() const
    {
        return std::lock_guard(mutex);
    }
    auto getLockForSend(const String & address) const { return SendLock{address, lock_manager}; }
    void cleanTaskInWorker(bool clean_resource = false) const;

    void cleanResourceInWorker();

    /// move resource from assigned_table_resource to assigned_worker_resource
    void allocateResource(
        const ContextPtr & context,
        std::lock_guard<std::mutex> &,
        std::optional<ResourceOption> resource_option = std::nullopt);

    void
    initSourceTaskPayload(const ContextPtr & context, std::unordered_map<HostWithPorts, std::vector<AssignedResource>> & all_resources);

    void sendCreateQueries(const ContextPtr & context);
    void sendDataParts(const ContextPtr & context);
    void sendOffloadingInfo(const ContextPtr & context);

    brpc::CallId doAsyncSend(
        const ContextPtr & context,
        const HostWithPorts & worker,
        const std::vector<AssignedResource> & resources,
        const ExceptionHandlerWithFailedInfoPtr & handler);

    TxnTimestamp txn_id;
    mutable std::mutex mutex; /// mutex for manager resource

    WorkerGroupHandle worker_group;
    HostWithPorts aggregate_worker;

    /// storage_uuid, assigned_resource
    std::unordered_map<UUID, AssignedResource> assigned_table_resource;
    std::unordered_map<UUID, AssignedResource> table_resources_saved_for_retry;
    std::unordered_map<HostWithPorts, std::vector<AssignedResource>> assigned_worker_resource;
    std::unordered_map<UUID, WorkerInfoSet> assigned_storage_workers;
    /// all workers that we've sent resource to
    WorkerInfoSet requested_workers;

    std::unordered_map<UUID, std::unordered_map<AddressInfo, SourceTaskPayload, AddressInfo::Hash>> source_task_payload;
    ResourceStageInfo resource_stage_info;

    bool skip_clean_worker{false};
    LoggerPtr log;
    mutable ServerResourceLockManager lock_manager;

    bool send_mutations{false};
};

}
