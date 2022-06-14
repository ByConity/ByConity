#include <ResourceManagement/SharedWorkerGroup.h>

#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::ResourceManagement
{

size_t SharedWorkerGroup::getNumWorkers() const
{
    std::lock_guard lock(state_mutex);
    return linked_group->getNumWorkers();
}

std::map<String, WorkerNodePtr> SharedWorkerGroup::getWorkers() const
{
    std::lock_guard lock(state_mutex);
    return getWorkersImpl(lock);
}

std::map<String, WorkerNodePtr> SharedWorkerGroup::getWorkersImpl(std::lock_guard<std::mutex> & /*lock*/) const
{
    return linked_group->getWorkers();
}

WorkerGroupData SharedWorkerGroup::getData(bool with_metrics) const
{
    WorkerGroupData data;
    data.id = getID();
    data.type = WorkerGroupType::Shared;
    data.vw_uuid = getVWUUID();
    data.vw_name = getVWName();

    {
        std::lock_guard lock(state_mutex);

        if (linked_group)
            data.linked_id = linked_group->getID();
        for (const auto & [_, worker] : getWorkersImpl(lock))
            data.host_ports_vec.push_back(worker->host);
    }

    data.num_workers = data.host_ports_vec.size();

    if (with_metrics)
        data.metrics = getAggregatedMetrics();
    return data;
}

WorkerGroupMetrics SharedWorkerGroup::getAggregatedMetrics() const
{
    std::lock_guard lock(state_mutex);
    return linked_group->getAggregatedMetrics();
}

void SharedWorkerGroup::registerNode(const WorkerNodePtr &)
{
    throw Exception("Cannot register node to SharedWorkerGroup", ErrorCodes::LOGICAL_ERROR);
}

void SharedWorkerGroup::removeNode(const String &)
{
    throw Exception("Cannot remove node from SharedWorkerGroup", ErrorCodes::LOGICAL_ERROR);
}

void SharedWorkerGroup::setLinkedGroup(WorkerGroupPtr group)
{
    /// TODO: (zuochuang.zema) maybe in future we can remove this lock for SharedWorkerGroup.
    std::lock_guard lock(state_mutex);
    linked_group = std::move(group);
}

}
