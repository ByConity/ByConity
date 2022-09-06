#pragma once
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Common/HostWithPorts.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <Poco/Logger.h>


namespace DB
{

struct AssignedResource
{
    explicit AssignedResource(const StoragePtr & storage);

    StoragePtr storage;
    String worker_table_name;
    String create_table_query;
    bool sent_create_query{false};

    /// offloading info
    HostWithPortsVec buffer_workers;

    /// parts info
    ServerDataPartsVector server_parts;
    // HiveDataPartsCNCHVector hive_parts;
    std::set<Int64> bucket_numbers;

    std::unordered_set<String> part_names;

    void addDataParts(const ServerDataPartsVector & parts);
    // void addDataParts(const HiveDataPartsCNCHVector & parts);

    bool empty() const { return sent_create_query && server_parts.empty(); }
};

class CnchServerResource
{
public:
    explicit CnchServerResource(TxnTimestamp curr_txn_id):
        txn_id(curr_txn_id),
        log(&Poco::Logger::get("SessionResource(" + txn_id.toString() + ")"))
    {}

    ~CnchServerResource();

    void addCreateQuery(const ContextPtr & context, const StoragePtr & storage, const String & create_query, const String & worker_table_name);
    void setAggregateWorker(HostWithPorts aggregate_worker_)
    {
        aggregate_worker = std::move(aggregate_worker_);
    }

    void setWorkerGroup(WorkerGroupHandle worker_group_)
    {
        if (!worker_group)
            worker_group = std::move(worker_group_);
    }

    template <typename T>
    void addDataParts(const UUID & storage_id, const std::vector<T> & data_parts, const std::set<Int64> & bucket_numbers = {})
    {
        std::lock_guard lock(mutex);
        auto & assigned_resource = assigned_table_resource.at(storage_id);

        assigned_resource.addDataParts(data_parts);
        if (assigned_resource.bucket_numbers.empty() && !bucket_numbers.empty())
            assigned_resource.bucket_numbers = bucket_numbers;
    }

    void addBufferWorkers(const UUID & storage_id, const HostWithPortsVec & buffer_workers);

    /// Send resource to worker
    void sendResource(const ContextPtr & context, const HostWithPorts & worker);
    /// allocate and send resource to worker_group
    void sendResource(const ContextPtr & context);

    /// remove all resource in server
    void removeAll();

private:
    auto getLock() const { return std::lock_guard(mutex); }
    void cleanTaskInWorker(bool clean_resource = false) const;

    /// move resource from assigned_table_resource to assigned_worker_resource
    void allocateResource(const ContextPtr & context, std::lock_guard<std::mutex> &);

    void sendCreateQueries(const ContextPtr & context);
    void sendDataParts(const ContextPtr & context);
    void sendOffloadingInfo(const ContextPtr & context);

    TxnTimestamp txn_id;
    mutable std::mutex mutex; /// mutex for manager resource

    WorkerGroupHandle worker_group;
    HostWithPorts aggregate_worker;

    /// storage_uuid, assigned_resource
    std::unordered_map<UUID, AssignedResource> assigned_table_resource;
    std::unordered_map<HostWithPorts, std::vector<AssignedResource>> assigned_worker_resource;

    Poco::Logger * log;
};

}
