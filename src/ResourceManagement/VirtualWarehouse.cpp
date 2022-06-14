#include <Catalog/Catalog.h>
#include <Common/Exception.h>
#include <ResourceManagement/VirtualWarehouse.h>

#include <chrono>

namespace DB::ErrorCodes
{
    extern const int RESOURCE_MANAGER_ERROR;
    extern const int RESOURCE_MANAGER_INCORRECT_SETTING;
    extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER_GROUP;
}

namespace DB::ResourceManagement
{

VirtualWarehouse::VirtualWarehouse(String n, UUID u, const VirtualWarehouseSettings & s)
    : name(std::move(n)), uuid(u), settings(s)
{
    query_scheduler = std::make_unique<QueryScheduler>(*this);
}

void VirtualWarehouse::applySettings(const VirtualWarehouseAlterSettings & new_settings, const Catalog::CatalogPtr & catalog)
{
    catalog->alterVirtualWarehouse(name, getData());
    {
        auto wlock = getWriteLock();
        if (new_settings.type)
            settings.type = *new_settings.type;
        if (new_settings.max_worker_groups)
        {
            if ((new_settings.min_worker_groups
                 && new_settings.min_worker_groups.value() > new_settings.max_worker_groups.value())
                || (!new_settings.min_worker_groups && settings.min_worker_groups > new_settings.max_worker_groups.value()))
                throw Exception("min_worker_groups cannot be less than max_worker_groups", ErrorCodes::RESOURCE_MANAGER_INCORRECT_SETTING);
            settings.max_worker_groups = *new_settings.max_worker_groups;
        }
        if (new_settings.min_worker_groups)
        {
            settings.min_worker_groups = *new_settings.min_worker_groups;
        }
        if (new_settings.num_workers)
        {
            settings.num_workers = *new_settings.num_workers;
        }
        if (new_settings.auto_suspend)
            settings.auto_suspend = *new_settings.auto_suspend;
        if (new_settings.auto_resume)
            settings.auto_resume = *new_settings.auto_resume;
        if (new_settings.max_concurrent_queries)
            settings.max_concurrent_queries = *new_settings.max_concurrent_queries;
        if (new_settings.max_queued_queries)
            settings.max_queued_queries = *new_settings.max_queued_queries;
        if (new_settings.max_queued_waiting_ms)
            settings.max_queued_waiting_ms = *new_settings.max_queued_waiting_ms;
        if (new_settings.vw_schedule_algo)
            settings.vw_schedule_algo = *new_settings.vw_schedule_algo;
    }
}

VirtualWarehouseData VirtualWarehouse::getData() const
{
    VirtualWarehouseData data;
    data.name = name;
    data.uuid = uuid;

    auto rlock = getReadLock();
    data.settings = settings;
    data.num_worker_groups = groups.size();
    data.num_workers = getNumWorkersImpl(rlock);

    return data;
}

std::vector<WorkerGroupPtr> VirtualWarehouse::getAllWorkerGroups() const
{
    std::vector<WorkerGroupPtr> res;

    auto rlock = getReadLock();

    for (const auto & [_, group]: groups)
        res.emplace_back(group);

    return res;
}

size_t VirtualWarehouse::getNumGroups() const
{
    auto rlock = getReadLock();
    return groups.size();
}

void VirtualWarehouse::addWorkerGroup(const WorkerGroupPtr & group)
{
    auto wlock = getWriteLock();
    auto res = groups.try_emplace(group->getID(), group).second;
    if (!res)
        throw Exception("Worker group " + group->getID() + " already exists in VW " + getName(), ErrorCodes::LOGICAL_ERROR);
}

void VirtualWarehouse::loadGroup(const WorkerGroupPtr & group)
{
    addWorkerGroup(group);
}

void VirtualWarehouse::removeGroup(const String & id)
{
    auto wlock = getWriteLock();
    size_t num = groups.erase(id);
    if (num == 0)
        throw Exception("Cannot remove a nonexistent worker group " + id + " in VW " + getName(), ErrorCodes::LOGICAL_ERROR);
}

WorkerGroupPtr VirtualWarehouse::getWorkerGroup(const String & id)
{
    auto rlock = getReadLock();
    return getWorkerGroupImpl(id, rlock);
}

WorkerGroupPtr VirtualWarehouse::getWorkerGroup(const size_t & index)
{
    auto rlock = getReadLock();
    return std::next(groups.begin(), index)->second;
}

const WorkerGroupPtr & VirtualWarehouse::getWorkerGroupImpl(const String & id, ReadLock & /*rlock*/)
{
    auto it = groups.find(id);
    if (it == groups.end())
        throw Exception("Worker group " + id + " not found in VW " + getName(), ErrorCodes::LOGICAL_ERROR);
    return it->second;
}

const WorkerGroupPtr & VirtualWarehouse::getWorkerGroupExclusiveImpl(const String & id, WriteLock & /*wlock*/)
{
    auto it = groups.find(id);
    if (it == groups.end())
        throw Exception("Worker group " + id + " not found in VW " + getName(), ErrorCodes::LOGICAL_ERROR);
    return it->second;
}

void VirtualWarehouse::registerNodeImpl(const WorkerNodePtr & node, WriteLock & wlock)
{
    if (node->worker_group_id.empty())
        throw Exception("Group ID cannot be empty", ErrorCodes::RESOURCE_MANAGER_ERROR);
    auto & group = getWorkerGroupExclusiveImpl(node->worker_group_id, wlock);
    group->registerNode(node);
}

void VirtualWarehouse::registerNode(const WorkerNodePtr & node)
{
    auto wlock = getWriteLock();
    registerNodeImpl(node, wlock);
}

void VirtualWarehouse::registerNodes(const std::vector<WorkerNodePtr> & nodes)
{
    auto wlock = getWriteLock();
    for (const auto & node : nodes)
        registerNodeImpl(node, wlock);
}

void VirtualWarehouse::removeNode(const String & worker_group_id, const String & worker_id)
{
    auto wlock = getWriteLock();
    auto & group = getWorkerGroupExclusiveImpl(worker_group_id, wlock);
    group->removeNode(worker_id);
}

/// The final fallback strategy for picking a worker group from this vw.
const WorkerGroupPtr & VirtualWarehouse::randomWorkerGroup() const
{
    std::uniform_int_distribution dist;

    {
        auto rlock = getReadLock();
        if (auto size = groups.size())
        {
            auto begin = dist(thread_local_rng) % size;
            for (size_t i = 0; i < size; i++)
            {
                auto index = (begin + i) % size;
                auto & group = std::next(groups.begin(), index)->second;
                if (!group->empty())
                    return group;
            }
        }
    }

    throw Exception("No available worker group for " + name, ErrorCodes::RESOURCE_MANAGER_NO_AVAILABLE_WORKER_GROUP);
}

size_t VirtualWarehouse::getNumWorkersImpl(ReadLock & /*rlock*/) const
{
    size_t res{0};
    for (auto & [_, group] : groups)
        res += group->getNumWorkers();
    return res;
}

size_t VirtualWarehouse::getNumWorkers() const
{
    auto rlock = getReadLock();
    return getNumWorkersImpl(rlock);
}

void VirtualWarehouse::updateQueueInfo(const String & server_id, const QueryQueueInfo & server_query_queue_info)
{
    std::lock_guard lock(queue_map_mutex);

    server_query_queue_map[server_id] = server_query_queue_info;
}

QueryQueueInfo VirtualWarehouse::getAggQueueInfo()
{
    std::lock_guard lock(queue_map_mutex);

    UInt64 time_now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()
                                                                            .time_since_epoch()).count();
    // TODO: Use setting vw_queue_server_sync_expiry_seconds
    auto timeout = 15;
    auto timeout_threshold = time_now - timeout * 1000;

    QueryQueueInfo res;

    auto it = server_query_queue_map.begin();

    while (it != server_query_queue_map.end())
    {
        //Remove outdated entries
        if (it->second.last_sync < timeout_threshold)
        {
            LOG_DEBUG(&Poco::Logger::get("VirtualWarehouse"), 
                        "Removing outdated server sync from {}, last synced {}",
                        it->first, std::to_string(it->second.last_sync));
            it = server_query_queue_map.erase(it);
        }
        else
        {
            res.queued_query_count += it->second.queued_query_count;
            res.running_query_count += it->second.running_query_count;
            ++it;
        }
    }

    return res;
}

}
