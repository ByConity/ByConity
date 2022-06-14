#pragma once

#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/PhysicalWorkerGroup.h>
#include <ResourceManagement/QueryScheduler.h>
#include <ResourceManagement/SharedWorkerGroup.h>
#include <Common/Config/ConfigProcessor.h>

#include <vector>
#include <map>
#include <shared_mutex>

namespace DB
{

namespace Catalog
{
class Catalog;
using CatalogPtr = std::shared_ptr<Catalog>;
}

namespace ResourceManagement
{
using VirtualWarehousePtr = std::shared_ptr<VirtualWarehouse>;

/// VirtualWarehouse consists of WorkerGroups
class VirtualWarehouse : private boost::noncopyable, public std::enable_shared_from_this<VirtualWarehouse>
{
    using ReadLock = std::shared_lock<std::shared_mutex>;
    using WriteLock = std::unique_lock<std::shared_mutex>;
    friend class QueryScheduler;
    friend class VirtualWarehouseFactory;

    /// NOTE: Using VirtualWarehouseFactory::create(...).
    VirtualWarehouse(String n, UUID u, const VirtualWarehouseSettings & s = {});

public:
    auto & getName() const { return name; }
    auto getUUID() const { return uuid; }
    auto & getQueryScheduler() { return *query_scheduler; }

    auto getReadLock() const { return ReadLock(state_mutex); }
    auto getWriteLock() const { return WriteLock(state_mutex); }

    VirtualWarehouseSettings getSettings() const
    {
        auto rlock = getReadLock();
        return settings;
    }

    auto getExpectedNumWorkers() const
    {
        auto rlock = getReadLock();
        return settings.num_workers;
    }

    void applySettings(const VirtualWarehouseAlterSettings & new_settings, const Catalog::CatalogPtr & catalog);

    VirtualWarehouseData getData() const;
    std::vector<WorkerGroupPtr> getAllWorkerGroups() const;

    /// Worker group operations
    size_t getNumGroups() const;

    void addWorkerGroup(const WorkerGroupPtr & group);
    void loadGroup(const WorkerGroupPtr & group);
    void removeGroup(const String & id);

    WorkerGroupPtr getWorkerGroup(const String & id);
    WorkerGroupPtr getWorkerGroup(const size_t & index);

    /// Worker Node operations
    void registerNode(const WorkerNodePtr & node);
    void registerNodes(const std::vector<WorkerNodePtr> & node);
    void removeNode(const String & worker_group_id, const String & worker_id);

    size_t getNumWorkers() const;

    void updateQueueInfo(const String & server_id, const QueryQueueInfo & server_query_queue_info);
    QueryQueueInfo getAggQueueInfo();

private:
    const WorkerGroupPtr & getWorkerGroupImpl(const String & id, ReadLock & rlock);
    const WorkerGroupPtr & getWorkerGroupExclusiveImpl(const String & id, WriteLock & wlock);

    void registerNodeImpl(const WorkerNodePtr & node, WriteLock & wlock);

    size_t getNumWorkersImpl(ReadLock & lock) const;

    const WorkerGroupPtr & randomWorkerGroup() const;

    const String name;
    const UUID uuid;

    mutable std::shared_mutex state_mutex;

    mutable std::mutex queue_map_mutex;
    ServerQueryQueueMap server_query_queue_map;

    VirtualWarehouseSettings settings;

    std::map<String, WorkerGroupPtr> groups;

    std::unique_ptr<QueryScheduler> query_scheduler;

    void cleanupQueryQueueMap();
};

class VirtualWarehouseFactory
{
public:
    /// Creating VW by this factory method to ensure the query scheduler is prepared.
    static VirtualWarehousePtr create(String n, UUID u, const VirtualWarehouseSettings & s)
    {
        return VirtualWarehousePtr(new VirtualWarehouse(std::move(n), u, s));
    }
};

}
}
