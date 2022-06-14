#include <ResourceManagement/QueryScheduler.h>
#include <ResourceManagement/VirtualWarehouse.h>

#include <Common/Exception.h>
#include <Common/thread_local_rng.h>

namespace DB::ErrorCodes
{
    extern const int RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO;
}

namespace DB::ResourceManagement
{

static inline bool cmp_group_cpu(const WorkerGroupAndMetrics & a, const WorkerGroupAndMetrics & b)
{
    return a.second.avg_cpu_usage < b.second.avg_cpu_usage;
}

static inline bool cmp_group_mem(const WorkerGroupAndMetrics & a, const WorkerGroupAndMetrics & b)
{
    return a.second.avg_mem_usage < b.second.avg_mem_usage;
}

static inline bool cmp_worker_cpu(const WorkerNodePtr & a, const WorkerNodePtr & b)
{
    return a->cpu_usage < b->cpu_usage;
}

static inline bool cmp_worker_mem(const WorkerNodePtr & a, const WorkerNodePtr & b)
{
    return a->memory_usage < b->memory_usage;
}

static inline bool cmp_worker_disk(const WorkerNodePtr & a, const WorkerNodePtr & b)
{
    return a->disk_space > b->disk_space;
}

QueryScheduler::QueryScheduler(VirtualWarehouse & vw_) : vw(vw_)
{
    log = &Poco::Logger::get(vw.getName() + " (QueryScheduler)");
}

/// pickWorkerGroups stage 1: filter groups by requirement.
void QueryScheduler::filterGroup(const Requirement & requirement, std::vector<WorkerGroupAndMetrics> & res) const
{
    auto rlock = vw.getReadLock();
    for (auto const & [_, group] : vw.groups)
    {
        if (auto metrics = group->getAggregatedMetrics(); metrics.available(requirement))
            res.emplace_back(group, metrics);
    }
}

/// pickWorkerGroups stage 2: select a group order by algo.
WorkerGroupPtr QueryScheduler::selectGroup(const VWScheduleAlgo & algo, const std::vector<WorkerGroupAndMetrics> & available_groups)
{
    if (available_groups.size() == 1)
        return available_groups[0].first;

    auto comparator = cmp_group_cpu;
    switch (algo)
    {
        case VWScheduleAlgo::Random:
        {
            std::uniform_int_distribution dist;
            auto index = dist(thread_local_rng) % available_groups.size();
            return available_groups[index].first;
        }

        case VWScheduleAlgo::GlobalRoundRobin:
        {
            size_t index = pick_group_sequence.fetch_add(1, std::memory_order_relaxed) % available_groups.size();
            return available_groups[index].first;
        }

        case VWScheduleAlgo::GlobalLowCpu:
            break;

        case VWScheduleAlgo::GlobalLowMem:
            comparator = cmp_group_mem;
            break;

        default:
            throw Exception("Wrong vw_schedule_algo for query scheduler: " + std::string(toString(algo)),
                            ErrorCodes::RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO);
    }

    auto it = std::min_element(available_groups.begin(), available_groups.end(), comparator);
    return it->first;
}

/// Picking a worker group from the virtual warehouse. Two stages:
/// - 1. filter worker groups by resource requirement. @see filterGroup
/// - 2. select one from filtered groups by algo. @see selectGroup
WorkerGroupPtr QueryScheduler::pickWorkerGroup(const VWScheduleAlgo & algo, const Requirement & requirement)
{
    std::vector<WorkerGroupAndMetrics> available_groups;
    filterGroup(requirement, available_groups);
    if (available_groups.empty())
    {
        LOG_WARNING(log, "No available worker group for requirement: {}, choose on randomly.", requirement.toDebugString());
        return vw.randomWorkerGroup();
    }
    return selectGroup(algo, available_groups);
}


/// pickWorker stage 1: filter workers by requirement.
void QueryScheduler::filterWorker(const Requirement & requirement, std::vector<WorkerNodePtr> & res)
{
    auto rlock = vw.getReadLock();

    /// Scan specified worker group's workers
    if (!requirement.worker_group.empty())
    {
        auto required_group = vw.getWorkerGroup(requirement.worker_group);
        for (auto const & [_, worker] : required_group->getWorkers())
        {
            if (worker->available(requirement))
            {
                res.emplace_back(worker);
            }
        }
        return;
    }

    /// Otherwise, scan all.
    for (auto const & [_, group] : vw.groups)
    {
        for (auto const & [_, worker] : group->getWorkers())
        {
            if (worker->available(requirement))
            {
                res.emplace_back(worker);
            }
        }
    }
}

/// pickWorker stage 2: select a worker order by algo.
HostWithPorts QueryScheduler::selectWorker(const VWScheduleAlgo & algo, const std::vector<WorkerNodePtr> & available_workers)
{
    if (available_workers.size() == 1)
        return available_workers[0]->host;

    auto comparator = cmp_worker_mem;
    switch (algo)
    {
        case VWScheduleAlgo::GlobalRoundRobin:
        {
            size_t index = pick_worker_sequence.fetch_add(1, std::memory_order_relaxed) % available_workers.size();
            return available_workers[index]->host;
        }

        case VWScheduleAlgo::GlobalLowMem:
            break;

        case VWScheduleAlgo::GlobalLowCpu:
            comparator = cmp_worker_cpu;
            break;

        case VWScheduleAlgo::GlobalLowDisk:
            comparator = cmp_worker_disk;
            break;

        default:
            throw Exception("Wrong vw_schedule_algo for query scheduler: " + std::string(toString(algo)), ErrorCodes::RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO);
    }

    auto it = std::min_element(available_workers.begin(), available_workers.end(), comparator);
    return (*it)->host;
}

/// Picking a worker from the virtual warehouse. Two stages:
/// - 1. filter workers by resource requirement. @see filterWorker
/// - 2. select one from filtered workers by algo. @see selectWorker
HostWithPorts QueryScheduler::pickWorker(const VWScheduleAlgo & algo, const Requirement & requirement)
{
    std::vector<WorkerNodePtr> available_workers;
    filterWorker(requirement, available_workers);
    if (available_workers.empty())
    {
        LOG_ERROR(log, "No available worker for requirement: {}, choose one randomly.", requirement.toDebugString());
        auto group = requirement.worker_group.empty() ? vw.randomWorkerGroup() : vw.getWorkerGroup(requirement.worker_group);
        return group->randomWorker()->host;
    }
    return selectWorker(algo, available_workers);
}

}
