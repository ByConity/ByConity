#include <atomic>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>
namespace ProfileEvents
{
extern const Event AllWorkerSize;
extern const Event HealthWorkerSize;
extern const Event UnhealthWorkerSize;
extern const Event OpenWorkerSize;
extern const Event UnknownWorkerSize;
extern const Event HalfSelfCheckWorkerSize;
extern const Event HalfOtherCheckWorkerSize;
}

namespace DB
{
String ResourceStatus::toDebugString() const
{
    return fmt::format("status {} score {} update_time {}", scheduler_status, scheduler_score, last_status_create_time);
}

WorkerGroupStatus::~WorkerGroupStatus()
{
    for (const auto & half_open_id : half_open_workers)
    {
        status_manager->restoreWorkerNode(half_open_id);
        LOG_DEBUG(getLogger("WorkerStatusManager"), "restore half open worker {}", half_open_id.toString());
    }
}

void WorkerGroupStatus::calculateStatus()
{
    if (getAvaibleSize() == 0)
        status = GroupHealthType::Critical;

    if (!half_open_workers.empty())
        need_check.store(true, std::memory_order_relaxed);

    ProfileEvents::increment(ProfileEvents::AllWorkerSize, total_count);
    ProfileEvents::increment(ProfileEvents::UnhealthWorkerSize, unhealth_count);
    ProfileEvents::increment(ProfileEvents::HealthWorkerSize, health_count);
    ProfileEvents::increment(ProfileEvents::OpenWorkerSize, open_count);
    ProfileEvents::increment(ProfileEvents::UnknownWorkerSize, unknown_count);
    ProfileEvents::increment(ProfileEvents::HalfSelfCheckWorkerSize, half_open_checking_count);
    ProfileEvents::increment(ProfileEvents::HalfOtherCheckWorkerSize, half_open_checking_by_other_count);
    LOG_DEBUG(
        getLogger("WorkerStatusManager"),
        "all: {}  health: {}  unhealth: {} unknown: {}  other_checking: {}  checking: {} open: {}",
        total_count,
        health_count,
        unhealth_count,
        unknown_count,
        half_open_checking_by_other_count,
        half_open_checking_count,
        open_count);
}

std::optional<std::vector<size_t>> WorkerGroupStatus::selectHealthNode(const HostWithPortsVec & host_ports_vec)
{
    if (filter_indices.size() == host_ports_vec.size())
        return std::nullopt;
    return filter_indices;
}

void WorkerStatusManager::updateWorkerNode(const Protos::WorkerNodeResourceData & resource_info, UpdateSource source)
{
    ResourceStatus resource_status(
        resource_info, recommended_concurrent_query_limit.load(std::memory_order_relaxed), health_worker_cpu_usage_threshold.load(std::memory_order_relaxed));
    auto id = getWorkerId(resource_info);
    auto now = std::chrono::system_clock::now();
    LOG_TRACE(log, "update worker id {} : {}", id.toString(), resource_info.ShortDebugString());
    worker_status_map.updateEmplaceIfNotExist(
        id,
        [id, this, &now, &resource_status, source](WorkerStatus & val) {
            // Worker has restarted. We must put it ahead of status update.
            if (resource_status.register_time > val.resource_status.register_time)
            {
                auto context_ptr = context.lock();
                if (context_ptr)
                {
                    context_ptr->getCnchWorkerClientPools().getWorker(resource_status.host_ports, /*refresh=*/true);
                    RpcChannelPool::getInstance().getClient(
                        resource_status.host_ports.getRPCAddress(), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, /*refresh=*/true);
                    context_ptr->getSegmentScheduler()->workerRestarted(id, resource_status.host_ports, resource_status.register_time);
                }
                
            }
            val.server_last_update_time = now;
            if (val.resource_status.last_status_create_time < resource_status.last_status_create_time)
                val.resource_status = resource_status;

            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open
                && std::chrono::duration_cast<std::chrono::seconds>(now - val.circuit_break.open_time).count() > circuit_breaker_open_to_halfopen_wait_seconds)
            {
                LOG_DEBUG(log, "worker: {} is back, set circuit breaker to half open.", id.toString());
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::HalfOpen;
                val.circuit_break.fail_count = 0;
            }
            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen && source == UpdateSource::ComeFromWorker)
            {
                LOG_DEBUG(log, "worker: {} is back, close circuit breaker.", id.toString());
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Close;
                val.circuit_break.fail_count = 0;
                val.circuit_break.is_checking = false;
            }
        },
        resource_status,
        now);
}

void WorkerStatusManager::setWorkerNodeDead(const WorkerId & key, int error_code)
{
    LOG_TRACE(log, "set worker: {} dead", key.toString());
    auto now = std::chrono::system_clock::now();
    worker_status_map.update(key, [&key, this, error_code, &now](WorkerStatus & val) {
        if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open)
        {
            LOG_TRACE(log, "worker: {}'s circuit break is open, wait RM to restart this worker.", key.toString());
            return;
        }
        size_t error_weight = 1;
        switch (error_code)
        {
            case EHOSTDOWN:
                error_weight = circuit_breaker_open_error_threshold.load(std::memory_order_relaxed) + 1;
                break;
            case ETIMEDOUT:
            case ECONNREFUSED:
                error_weight = 1;
                break;
            default:
                break;
        }
        val.circuit_break.fail_count += error_weight;
        if (val.circuit_break.fail_count > circuit_breaker_open_error_threshold.load(std::memory_order_relaxed)
            || val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
        {
            LOG_TRACE(log, "worker: {}'s fail_count {} open circuit break.", key.toString(), val.circuit_break.fail_count);
            val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Open;
            val.circuit_break.fail_count = 0;
            val.circuit_break.is_checking = false;
            val.circuit_break.open_time = now;
            LOG_TRACE(log, "add unhealth worker {}", key.toString());
        }
    });
}

void WorkerStatusManager::restoreWorkerNode(const WorkerId & key)
{
    worker_status_map.update(key, [&](WorkerStatus & val) {
        if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
        {
            val.circuit_break.is_checking = false;
        }
    });
}

std::shared_ptr<WorkerGroupStatus>
WorkerStatusManager::getWorkerGroupStatus(const std::vector<WorkerId> & worker_ids, SchedulerMode mode)
{
    auto worker_group_status = std::make_shared<WorkerGroupStatus>(shared_from_this());
    worker_group_status->total_count = worker_ids.size();
    worker_group_status->filter_indices.reserve(worker_ids.size());
    worker_group_status->workers_resource_status.reserve(worker_ids.size());
    auto now = std::chrono::system_clock::now();

    for (size_t idx = 0; idx < worker_ids.size(); ++idx)
    {
        const auto & id = worker_ids[idx];
        bool exist = false;
        worker_status_map.update(id, [&](WorkerStatus & val) {
            exist = true;
            LOG_TRACE(log, "get worker group status : {}", val.toDebugString());
            if (likely(val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Close))
            {
                if (val.resource_status.getStatus() == ScheduleType::Unhealth)
                    worker_group_status->unhealth_count++;
                else
                    worker_group_status->health_count++;

                if (mode == SchedulerMode::SKIP_SLOW_NODE)
                {
                    if (val.resource_status.getStatus() == ScheduleType::Unhealth
                        && std::chrono::duration_cast<std::chrono::seconds>(now - val.server_last_update_time).count()
                            < unhealth_worker_recheck_wait_seconds)
                        return;
                }

                worker_group_status->filter_indices.emplace_back(idx);
                worker_group_status->workers_resource_status.emplace_back(val.resource_status);
            }
            else if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
            {
                if (val.circuit_break.is_checking)
                {
                    LOG_DEBUG(log, "half open worker {} is checking", id.toString());
                    worker_group_status->half_open_checking_by_other_count++;
                }
                else
                {
                    LOG_DEBUG(log, "check half open worker {}", id.toString());
                    val.circuit_break.is_checking = true;
                    worker_group_status->half_open_checking_count++;
                    worker_group_status->half_open_workers.emplace(id);
                    worker_group_status->workers_resource_status.emplace_back(val.resource_status);
                    worker_group_status->filter_indices.emplace_back(idx);
                }
            }
            else if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open)
            {
                worker_group_status->open_count++;
                if (std::chrono::duration_cast<std::chrono::seconds>(now - val.circuit_break.open_time).count() > circuit_breaker_open_to_halfopen_wait_seconds)
                {
                    LOG_TRACE(log, "worker: {} is timeout, set circuit breaker to half open.", id.toString());
                    val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::HalfOpen;
                    val.circuit_break.fail_count = 0;
                }
            }
        });
        if (!exist)
        {
            worker_group_status->unknown_count++;
            worker_group_status->filter_indices.emplace_back(idx);
            worker_group_status->workers_resource_status.emplace_back(std::nullopt);
            LOG_DEBUG(log, "can't find worker node : {}'s status", id.toString());
        }
    }
    worker_group_status->calculateStatus();
    return worker_group_status;
}


}
