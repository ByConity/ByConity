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
namespace CurrentMetrics
{
extern const Metric BackgroundRMHeartbeatSchedulePoolTask;
}
namespace DB
{
String WorkerStatus::toDebugString() const
{
    Protos::WorkerNodeResourceData pb_data;
    fillProto(pb_data);
    return pb_data.ShortDebugString();
}

WorkerGroupStatus::~WorkerGroupStatus()
{
    if (global_context)
    {
        for (const auto & half_open_id : half_open_workers)
        {
            global_context->getWorkerStatusManager()->restoreWorkerNode(half_open_id);
            LOG_DEBUG(getLogger("WorkerStatusManager"), "restore half open worker {}", half_open_id.ToString());
        }
    }
}

void WorkerGroupStatus::calculateStatus()
{
    total_worker_size = workers_status.size() + unknown_worker_size;
    for (auto & [_, worker_status] : workers_status)
    {
        switch (worker_status->scheduler_status)
        {
            case WorkerSchedulerStatus::Health:
                health_worker_size++;
                break;
            case WorkerSchedulerStatus::OnlySource:
                only_source_worker_size++;
                break;
            case WorkerSchedulerStatus::HeavyLoad:
                heavy_load_worker_size++;
                break;
            case WorkerSchedulerStatus::Unhealth:
                unhealth_worker_size++;
                break;
            default:
                break;
        }
    }

    if (getAvaiableComputeWorkerSize() == 0)
        status = WorkerGroupHealthStatus::Critical;

    LOG_DEBUG(
        getLogger("WorkerStatusManager"),
        "allWorkerSize: {}  healthWorkerSize: {}  unhealthWorkerSize: {} \
    HeavyLoadSize: {} onlySourceSize: {} unknowWorkerSize: {} notConnectedWorkerSize: {} halfOpenChecking: {}  halfOpen: {}",
        total_worker_size,
        health_worker_size,
        unhealth_worker_size,
        heavy_load_worker_size,
        only_source_worker_size,
        unknown_worker_size,
        not_connected_worker_size,
        half_open_workers_checking_size,
        half_open_workers_size);
}

std::optional<std::vector<size_t>> WorkerGroupStatus::selectHealthNode(const HostWithPortsVec & host_ports_vec)
{
    if (filter_indices.size() == host_ports_vec.size())
        return std::nullopt;
    return filter_indices;
}

WorkerStatusManager::WorkerStatusManager(ContextWeakMutablePtr context_)
    : WithContext(context_), log(getLogger("WorkerStatusManager"))
{
    schedule_pool.emplace(1, CurrentMetrics::BackgroundRMHeartbeatSchedulePoolTask, "RMHeart");
    startHeartbeat(*schedule_pool);
}

void WorkerStatusManager::shutdown()
{
    stop();
    if (schedule_pool)
        schedule_pool.reset();
}

WorkerStatusManager::~WorkerStatusManager()
{
    shutdown();
}

void WorkerStatusManager::heartbeat()
{
    LOG_DEBUG(log, "update worker status from rm heartbeat");
    try
    {
        auto rm_client = getContext()->getResourceManagerClient();
        if (!rm_client)
        {
            LOG_WARNING(log, "The client of ResourceManagement is not initialized");
        }
        else
        {
            std::vector<WorkerNodeResourceData> data;
            rm_client->getAllWorkers(data);
            if (data.empty())
            {
                LOG_WARNING(log, "No worker group found from RM");
            }
            else
            {
                auto cannot_update_workers = getWorkersCannotUpdateFromRM();
                for (const auto & group_data : data)
                {
                    auto worker_id = WorkerStatusManager::getWorkerId(group_data.vw_name, group_data.worker_group_id, group_data.id);
                    if (!cannot_update_workers.count(worker_id))
                    {
                        LOG_TRACE(log, "resource_data : " + group_data.toDebugString());
                        LOG_TRACE(log, "update worker {} from rm", worker_id.ToString());
                        Protos::WorkerNodeResourceData resource_info;
                        group_data.fillProto(resource_info);
                        updateWorkerNode(resource_info, WorkerStatusManager::UpdateSource::ComeFromRM);
                    }
                }
            }
        }
    }
    catch (...)
    {
        tryLogDebugCurrentException(__PRETTY_FUNCTION__);
    }
    task->scheduleAfter(heartbeat_interval.load());
}

void WorkerStatusManager::updateWorkerNode(const Protos::WorkerNodeResourceData & resource_info, UpdateSource source)
{
    auto worker_status = std::make_shared<WorkerStatus>(resource_info);
    worker_status->setSchedulerInfo(adaptive_scheduler_config);
    auto id = getWorkerId(resource_info);
    WorkerSchedulerStatus old_status{WorkerSchedulerStatus::Unknown};
    auto new_status = worker_status->getStatus();
    auto now = std::chrono::system_clock::now();
    bool need_callback = true;
    global_extra_workers_status.updateEmplaceIfNotExist(
        id,
        [new_status, &old_status, id, this, &now, &worker_status, &need_callback, &source](WorkerStatusExtra & val) {
            // Worker has restarted. We must put it ahead of status update.
            // TODO(wangtao.vip): support source from worker.
            if (worker_status->register_time > val.worker_status->register_time && source == UpdateSource::ComeFromRM)
            {
                getContext()->getCnchWorkerClientPools().getWorker(worker_status->host_ports, /*refresh=*/true);
                RpcChannelPool::getInstance().getClient(
                    worker_status->host_ports.getRPCAddress(), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, /*refresh=*/true);
                getContext()->getSegmentScheduler()->workerRestarted(id, worker_status->host_ports, val.worker_status->register_time);
            }
            if (worker_status->last_status_create_time > val.worker_status->last_status_create_time)
            {
                old_status = val.worker_status->getStatus();
                val.worker_status = worker_status;
                val.server_last_update_time = now;

                LOG_TRACE(log, "worker {} status changed : {}", worker_status->toDebugString(), (old_status != new_status));
                if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open)
                {
                    LOG_TRACE(log, "worker: {} is back, set circuit breaker to half open.", id.ToString());
                    val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::HalfOpen;
                    val.circuit_break.fail_count = 0;
                }
            }
            else
                need_callback = false;
        },
        [this, new_status, source, &id, &now, &need_callback]() {
            if (need_callback)
            {
                auto is_new_status_unhealth = new_status == WorkerSchedulerStatus::Unhealth;
                if (is_new_status_unhealth)
                {
                    LOG_TRACE(log, "add unhealth worker {} ", id.ToString());
                    unhealth_workers_status.set(id, UnhealthWorkerStatus{WorkerSchedulerStatus::Unhealth, now});
                }
                else if (source == UpdateSource::ComeFromRM)
                {
                    unhealth_workers_status.erase(id);
                    LOG_TRACE(log, "remove unhealth worker {}", id.ToString());
                }
            }
        },
        worker_status,
        now);
}

void WorkerStatusManager::setWorkerNodeDead(const WorkerId & key, int error_code)
{
    LOG_TRACE(log, "set worker: {} dead", key.ToString());
    auto now = std::chrono::system_clock::now();
    global_extra_workers_status.updateCallbackIfNotExist(
        key,
        [&key, this, error_code, &now](WorkerStatusExtra & val) {
            if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Open)
            {
                LOG_TRACE(log, "worker: {}'s circuit break is open, wait RM to restart this worker.", key.ToString());
                return;
            }
            size_t error_weight = 1;
            switch (error_code)
            {
                case EHOSTDOWN:
                    error_weight = 20;
                    break;
                case ETIMEDOUT:
                case ECONNREFUSED:
                    error_weight = 5;
                    break;
                default:
                    break;
            }
            val.circuit_break.fail_count += error_weight;
            if (val.circuit_break.fail_count > CIRCUIT_BREAKER_THRESHOLD
                || val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
            {
                LOG_TRACE(log, "worker: {}'s fail_count {} open circuit break.", key.ToString(), val.circuit_break.fail_count);
                val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Open;
                val.worker_status = std::make_shared<WorkerStatus>();
                val.worker_status->scheduler_status = WorkerSchedulerStatus::NotConnected;
                val.circuit_break.fail_count = 0;
                val.circuit_break.is_checking = false;
                unhealth_workers_status.set(key, UnhealthWorkerStatus{WorkerSchedulerStatus::NotConnected, now});
                LOG_TRACE(log, "add unhealth worker ", key.ToString());
            }
        },
        [key](WorkerStatusExtra & new_val) {
            new_val.worker_status = std::make_shared<WorkerStatus>();
            new_val.worker_status->scheduler_status = WorkerSchedulerStatus::NotConnected;
        });
}

UnhealthWorkerStatusMap WorkerStatusManager::getWorkersCannotUpdateFromRM()
{
    UnhealthWorkerStatusMap ret;
    auto now = std::chrono::system_clock::now();
    unhealth_workers_status.traverse([&ret, &now, this](const WorkerId & key, const UnhealthWorkerStatus & worker_info) {
        if (worker_info.status == WorkerSchedulerStatus::NotConnected)
        {
            if (std::chrono::duration_cast<std::chrono::seconds>(now - worker_info.update_time).count()
                < adaptive_scheduler_config.UNHEALTH_RECHECK_SECONDS)
                ret.emplace(key, worker_info);
        }
    });

    return ret;
}

void WorkerStatusManager::restoreWorkerNode(const WorkerId & key)
{
    global_extra_workers_status.eraseWithCallback(key, [&key, this]() { unhealth_workers_status.erase(key); });
}

void WorkerStatusManager::CloseCircuitBreaker(const WorkerId & key)
{
    global_extra_workers_status.update(key, [this, &key](WorkerStatusExtra & val) {
        if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
        {
            LOG_DEBUG(log, "worker: {} is back close circuit break.", key.ToString());
            val.circuit_break.breaker_status = WorkerCircuitBreakerStatus::Close;
            val.circuit_break.is_checking = false;
            val.circuit_break.fail_count = 0;
        }
    });
}

std::shared_ptr<WorkerGroupStatus> WorkerStatusManager::getWorkerGroupStatus(const String & vw_name, const String & wg_name)
{
    auto worker_id_vec = vw_worker_list_map.get(vw_name + "." + wg_name);
    if (!worker_id_vec)
        return nullptr;
    return getWorkerGroupStatus(
        nullptr, **worker_id_vec, vw_name, wg_name, [](const String &, const String &, const WorkerId & id) { return id; }, false);
}

std::optional<WorkerStatusExtra> WorkerStatusManager::getWorkerStatus(const WorkerId & worker_id)
{
    return global_extra_workers_status.get(worker_id);
}

void WorkerStatusManager::updateConfig(const ASConfiguration & as_config)
{
    LOG_DEBUG(log, "update WorkerStatusManager config.");
    adaptive_scheduler_config.MEM_WEIGHT = as_config.mem_weight.safeGet();
    adaptive_scheduler_config.QUERY_NUM_WEIGHT = as_config.query_num_weight.safeGet();
    adaptive_scheduler_config.MAX_PLAN_SEGMENT_SIZE = as_config.max_plan_segment_size.safeGet();
    adaptive_scheduler_config.UNHEALTH_SEGMENT_SIZE = as_config.unhealth_segment_size.safeGet();
    adaptive_scheduler_config.HEAVY_LOAD_THRESHOLD = as_config.heavy_load_threshold.safeGet();
    adaptive_scheduler_config.ONLY_SOURCE_THRESHOLD = as_config.only_source_threshold.safeGet();
    adaptive_scheduler_config.UNHEALTH_THRESHOLD = as_config.unhealth_threshold.safeGet();
    adaptive_scheduler_config.NEED_RESET_SECONDS = as_config.need_reset_seconds.safeGet();
    adaptive_scheduler_config.UNHEALTH_RECHECK_SECONDS = as_config.unhealth_recheck_seconds.safeGet();
    heartbeat_interval = as_config.heartbeat_interval.safeGet();
}

}
