#include <ResourceManagement/PhysicalWorkerGroup.h>
#include <ResourceManagement/CommonData.h>


namespace DB::ErrorCodes
{
    extern const int RESOURCE_MANAGER_NO_AVAILABLE_WORKER;
}

namespace DB::ResourceManagement
{
size_t PhysicalWorkerGroup::getNumWorkers() const
{
    std::lock_guard lock(state_mutex);
    return workers.size();
}

std::map<String, WorkerNodePtr> PhysicalWorkerGroup::getWorkers() const
{
    std::lock_guard lock(state_mutex);
    return getWorkersImpl(lock);
}

std::map<String, WorkerNodePtr> PhysicalWorkerGroup::getWorkersImpl(std::lock_guard<std::mutex> & /*lock*/) const
{
    return workers;
}

WorkerGroupData PhysicalWorkerGroup::getData(bool with_metrics) const
{
    WorkerGroupData data;
    data.id = getID();
    data.type = WorkerGroupType::Physical;
    data.vw_uuid = getVWUUID();
    data.vw_name = getVWName();
    data.psm = psm;
    for (const auto & [_, worker] : getWorkers())
        data.host_ports_vec.push_back(worker->host);
    data.num_workers = data.host_ports_vec.size();

    if (with_metrics)
        data.metrics = getAggregatedMetrics();
    return data;
}

/// A background task refreshing the worker group's aggregated metrics.
void PhysicalWorkerGroup::refreshAggregatedMetrics()
{
    {
        std::lock_guard lock(state_mutex);
        aggregated_metrics.reset();

        auto workers_ = getWorkersImpl(lock);
        auto count = workers_.size();
        for (auto const & [_, worker] : workers_)
        {
            auto worker_cpu_usage = worker->cpu_usage.load(std::memory_order_relaxed);
            if (worker_cpu_usage < aggregated_metrics.min_cpu_usage)
                aggregated_metrics.min_cpu_usage = worker_cpu_usage;
            if (worker_cpu_usage > aggregated_metrics.max_cpu_usage)
                aggregated_metrics.max_cpu_usage = worker_cpu_usage;
            aggregated_metrics.avg_cpu_usage += worker_cpu_usage;

            auto worker_mem_usage = worker->memory_usage.load(std::memory_order_relaxed);
            if (worker_mem_usage < aggregated_metrics.min_mem_usage)
                aggregated_metrics.min_mem_usage = worker_mem_usage;
            if (worker_mem_usage > aggregated_metrics.max_mem_usage)
                aggregated_metrics.max_mem_usage = worker_mem_usage;
            aggregated_metrics.avg_mem_usage += worker_mem_usage;

            auto worker_mem_available = worker->memory_available.load(std::memory_order::relaxed);
            if (worker_mem_available < aggregated_metrics.min_mem_available)
                aggregated_metrics.min_mem_available = worker_mem_available;

            /// For now, we do not count in a worker's disk space.

            aggregated_metrics.total_queries += worker->query_num.load(std::memory_order_relaxed);
        }
        aggregated_metrics.avg_cpu_usage /= count;
        aggregated_metrics.avg_mem_usage /= count;
        aggregated_metrics.num_workers = count;
    }

    refresh_metrics_task->scheduleAfter(1 * 1000);
}

WorkerGroupMetrics PhysicalWorkerGroup::getAggregatedMetrics() const
{
    std::lock_guard lock(state_mutex);
    return aggregated_metrics;
}

void PhysicalWorkerGroup::registerNode(const WorkerNodePtr & node)
{
    std::lock_guard lock(state_mutex);
    /// replace the old Node if exists.
    workers[node->getID()] = node;
}

void PhysicalWorkerGroup::removeNode(const String & worker_id)
{
    std::lock_guard lock(state_mutex);
    workers.erase(worker_id);
}

WorkerNodePtr PhysicalWorkerGroup::randomWorker() const
{
    {
        std::lock_guard lock(state_mutex);
        if (!workers.empty())
        {
            std::uniform_int_distribution dist;
            auto index = dist(thread_local_rng) % workers.size();
            return std::next(workers.begin(), index)->second;
        }
    }

    throw Exception("No available worker for " + id, ErrorCodes::RESOURCE_MANAGER_NO_AVAILABLE_WORKER);
}

}
