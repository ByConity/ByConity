#pragma once

#include <chrono>
#include <optional>
#include <unordered_set>
#include <vector>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Protos/data_models.pb.h>
#include <ResourceManagement/CommonData.h>
#include <boost/noncopyable.hpp>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Common/Configurations.h>
#include <Common/HostWithPorts.h>
#include <Common/Stopwatch.h>
namespace DB
{
template <typename Config, class AtomicType>
void configReload(
    const Config & config,
    const String & config_name,
    typename AtomicType::value_type (Config::*get)(const String & key) const,
    AtomicType & old_value)
{
    if (config.has(config_name))
    {
        auto new_value = (config.*get)(config_name);
        if (new_value != old_value.load(std::memory_order_relaxed))
            old_value.store(new_value, std::memory_order_relaxed);
    }
}
struct AdaptiveSchedulerConfig
{
    std::atomic<size_t> MEM_WEIGHT{2};
    std::atomic<size_t> QUERY_NUM_WEIGHT{4};
    std::atomic<size_t> MAX_PLAN_SEGMENT_SIZE{500};
    std::atomic<size_t> UNHEALTH_SEGMENT_SIZE{480};
    std::atomic<double> HEAVY_LOAD_THRESHOLD{0.75};
    std::atomic<double> ONLY_SOURCE_THRESHOLD{0.90};
    std::atomic<double> UNHEALTH_THRESHOLD{0.95};
    std::atomic<int64_t> NEED_RESET_SECONDS{300};
    std::atomic<int64_t> UNHEALTH_RECHECK_SECONDS{10};
};

using WorkerGroupData = ResourceManagement::WorkerGroupData;

enum class WorkerSchedulerStatus : uint8_t
{
    Health = 1,
    Unknown = 2,
    HeavyLoad = 3,
    OnlySource = 4,
    Unhealth = 5,
    NotConnected = 6
};

enum class WorkerGroupHealthStatus : uint8_t
{
    Health = 1,
    Unhealth = 2,
    Critical = 3
};

enum class WorkerCircuitBreakerStatus : uint8_t
{
    Close = 1,
    HalfOpen = 2,
    Open = 3
};

struct UnhealthWorkerStatus
{
    WorkerSchedulerStatus status{WorkerSchedulerStatus::Unknown};
    std::chrono::system_clock::time_point update_time{};
};

struct WorkerCircuitBreaker
{
    size_t fail_count{0};
    WorkerCircuitBreakerStatus breaker_status{WorkerCircuitBreakerStatus::Close};
    bool is_checking{false};
    String toDebugString() const
    {
        return fmt::format("breaker_status {} fail_count {} is_checking {}", breaker_status, fail_count, is_checking);
    }
};

template <typename KEY, typename VALUE, typename HASH = std::hash<KEY>, typename EQUAL = std::equal_to<KEY>, uint32_t MAP_COUNT = 23>
class ThreadSafeMap
{
    static_assert(MAP_COUNT > 0, "Invalid MAP_COUNT parameters.");

public:
    void set(const KEY & key, const VALUE & value)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        _map[idx][key] = value;
    }

    void erase(const KEY & key)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        _map[idx].erase(key);
    }

    template <class Func>
    void eraseWithCallback(const KEY & key, Func && func)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        _map[idx].erase(key);
        func();
    }

    std::optional<VALUE> get(const KEY & key)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        if (_map[idx].count(key) == 0)
            return std::nullopt;
        return _map[idx][key];
    }

    template <class... Args, class Func>
    void updateEmplaceIfNotExist(const KEY & key, const std::function<void(VALUE & value)> & call, Func && func, Args &&... args)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter != _map[idx].end())
            call(iter->second);
        else
            _map[idx].emplace(std::make_pair(key, VALUE(args...)));
        func();
    }

    void updateCallbackIfNotExist(
        const KEY & key, const std::function<void(VALUE & value)> & call, const std::function<void(VALUE & value)> & init_call)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter != _map[idx].end())
            call(iter->second);
        else
        {
            auto [new_iter, _] = _map[idx].try_emplace(key);
            init_call(new_iter->second);
        }
    }

    template <class Func>
    void update(const KEY & key, Func && call)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
        auto iter = _map[idx].find(key);
        if (iter != _map[idx].end())
            call(iter->second);
    }

    void traverse(const std::function<void(const KEY & k, const VALUE & v)> & call)
    {
        for (uint32_t idx = 0; idx < MAP_COUNT; ++idx)
        {
            std::unique_lock<bthread::Mutex> lock(_mutex[idx]);
            for (const auto & [k, v] : _map[idx])
                call(k, v);
        }
    }

private:
    uint32_t mapIdx(const KEY & key) { return HASH{}(key) % MAP_COUNT; }

private:
    std::unordered_map<KEY, VALUE, HASH, EQUAL> _map[MAP_COUNT];
    bthread::Mutex _mutex[MAP_COUNT];
};

struct WorkerStatus : public DB::WorkerNodeResourceData
{
    WorkerStatus() = default;
    WorkerStatus(const Protos::WorkerNodeResourceData & resource_info) : DB::WorkerNodeResourceData(resource_info) { }
    void setSchedulerInfo(AdaptiveSchedulerConfig & config)
    {
        //scheduler score, less is better
        scheduler_score
            = config.QUERY_NUM_WEIGHT * (100.0 * query_num / config.MAX_PLAN_SEGMENT_SIZE) + cpu_usage + config.MEM_WEIGHT * memory_usage;
        if (query_num > config.UNHEALTH_THRESHOLD * config.MAX_PLAN_SEGMENT_SIZE || memory_usage > 100 * config.UNHEALTH_THRESHOLD)
            scheduler_status = WorkerSchedulerStatus::Unhealth;
        else if (
            query_num > config.ONLY_SOURCE_THRESHOLD * config.MAX_PLAN_SEGMENT_SIZE || memory_usage > 100 * config.ONLY_SOURCE_THRESHOLD)
            scheduler_status = WorkerSchedulerStatus::OnlySource;
        else if (
            query_num > config.HEAVY_LOAD_THRESHOLD * config.MAX_PLAN_SEGMENT_SIZE || memory_usage > 100 * config.HEAVY_LOAD_THRESHOLD
            || cpu_usage > 100 * config.UNHEALTH_THRESHOLD)
            scheduler_status = WorkerSchedulerStatus::HeavyLoad;
        else
            scheduler_status = WorkerSchedulerStatus::Health;
    }

    bool compare(const WorkerStatus & rhs)
    {
        if (scheduler_status == rhs.scheduler_status)
            return scheduler_score < rhs.scheduler_score;
        else
            return scheduler_status < rhs.scheduler_status;
    }

    WorkerSchedulerStatus getStatus() const { return scheduler_status; }
    String toDebugString() const;

    WorkerSchedulerStatus scheduler_status{WorkerSchedulerStatus::Unknown};
    double scheduler_score{0};
};

using WorkerNodeSet = std::unordered_set<WorkerId, WorkerIdHash>;
using WorkerStatusPtr = std::shared_ptr<WorkerStatus>;
using WorkerNodeStatusContainer = std::unordered_map<WorkerId, WorkerStatusPtr, WorkerIdHash>;

struct WorkerStatusExtra
{
    WorkerStatusExtra() = default;
    WorkerStatusExtra(WorkerStatusPtr ptr, std::chrono::system_clock::time_point time) : worker_status(ptr), server_last_update_time(time)
    {
    }
    WorkerStatusPtr worker_status;
    std::chrono::system_clock::time_point server_last_update_time;
    WorkerCircuitBreaker circuit_break;
    String toDebugString() const { return worker_status->toDebugString() + circuit_break.toDebugString(); }
};
class WorkerGroupStatus
{
public:
    friend class WorkerStatusManager;
    WorkerGroupStatus(Context * context) : global_context(context) { }
    WorkerGroupStatus() = default;
    ~WorkerGroupStatus();
    void calculateStatus();

    std::optional<std::vector<size_t>> selectHealthNode(const HostWithPortsVec & host_ports);

    WorkerGroupHealthStatus getWorkerGroupHealth() const { return status; }

    size_t getHealthWorkerSize() const { return health_worker_size; }
    size_t getUnknownWorkerSize() const { return unknown_worker_size; }
    size_t getHeavyLoadWorkerSize() const { return heavy_load_worker_size; }
    size_t getOnlySourceWorkerSize() const { return only_source_worker_size; }
    size_t getAllWorkerSize() const { return total_worker_size; }
    size_t getAvaiableComputeWorkerSize() const
    {
        size_t health_size = health_worker_size + unknown_worker_size;
        return health_size > 0 ? health_size : health_size + heavy_load_worker_size;
    }
    size_t getUnhealthWorkerSize() const { return unhealth_worker_size; }
    size_t getNotConnectedWorkerSize() const { return not_connected_worker_size; }
    const WorkerNodeSet & getHalfOpenWorkers() { return half_open_workers; }
    void clearHalfOpenWorkers() { half_open_workers.clear(); }

    bool hasAvaibleWorker() const { return health_worker_size + only_source_worker_size + unknown_worker_size > 0; }

    WorkerNodeStatusContainer & getWorkersStatus() { return workers_status; }

private:
    WorkerNodeStatusContainer workers_status;
    WorkerGroupHealthStatus status{WorkerGroupHealthStatus::Health};
    WorkerNodeSet half_open_workers;
    size_t health_worker_size{0};
    size_t unhealth_worker_size{0};
    size_t not_connected_worker_size{0};
    size_t total_worker_size{0};
    size_t unknown_worker_size{0};
    size_t only_source_worker_size{0};
    size_t half_open_workers_checking_size{0};
    size_t half_open_workers_size{0};
    size_t heavy_load_worker_size{0};
    std::vector<size_t> filter_indices;
    Context * global_context{nullptr};
};

using WorkerGroupStatusPtr = std::shared_ptr<WorkerGroupStatus>;
using UnhealthWorkerStatusMap = std::unordered_map<WorkerId, UnhealthWorkerStatus, WorkerIdHash>;

class WorkerStatusManager : public WithContext
{
public:
    static inline const String prefix{"worker_status_manager"};
    enum class UpdateSource : uint8_t
    {
        ComeFromRM = 1,
        ComeFromWorker = 2
    };

    // vw_name.wg_name
    using VWType = String;
    using WorkerList = std::vector<WorkerId>;
    using WorkerListPtr = std::shared_ptr<WorkerList>;

    constexpr static const size_t CIRCUIT_BREAKER_THRESHOLD = 20;

    explicit WorkerStatusManager(ContextWeakMutablePtr context_);

    ~WorkerStatusManager();

    void updateWorkerNode(const Protos::WorkerNodeResourceData & resource_info, UpdateSource source);

    void setWorkerNodeDead(const WorkerId & key, int error_code);

    void restoreWorkerNode(const WorkerId & key);

    void CloseCircuitBreaker(const WorkerId & key);

    UnhealthWorkerStatusMap getWorkersCannotUpdateFromRM();

    template <class WorkersVecType, class GetWorkerFunc>
    std::shared_ptr<WorkerGroupStatus> getWorkerGroupStatus(
        Context * global_context,
        const WorkersVecType & host_ports,
        const String & vw_name,
        const String & wg_name,
        GetWorkerFunc && func,
        bool can_check = true);

    std::shared_ptr<WorkerGroupStatus> getWorkerGroupStatus(const String & vw_name, const String & wg_name);

    void updateConfig(const ASConfiguration & as_config);

    static WorkerId getWorkerId(const String & vw_name, const String & group_id, const String & id)
    {
        return WorkerId{vw_name, group_id, id};
    }

    static WorkerId getWorkerId(const Protos::WorkerNodeResourceData & resource_info)
    {
        return WorkerId{resource_info.vw_name(), resource_info.worker_group_id(), resource_info.id()};
    }

    template <class WorkerVecType>
    void updateVWWorkerList(const WorkerVecType & host_ports, const String & vw_name, const String & wg_name)
    {
        if constexpr (std::is_same_v<WorkerVecType, HostWithPortsVec>)
        {
            String vw_wg_name = vw_name + '.' + wg_name;
            LOG_TRACE(log, "update {} worker list.", vw_wg_name);
            auto worker_list = std::make_shared<WorkerList>();
            for (const auto & host : host_ports)
            {
                worker_list->emplace_back(vw_name, wg_name, host.id);
            }
            vw_worker_list_map.set(vw_wg_name, worker_list);
        }
    }

    void startHeartbeat(BackgroundSchedulePool & pool_)
    {
        task = pool_.createTask(prefix, [&] { heartbeat(); });
        task->activateAndSchedule();
    }
    void stop() { task->deactivate(); }
    void heartbeat();
    void shutdown();

private:
    ThreadSafeMap<WorkerId, WorkerStatusExtra, WorkerIdHash> global_extra_workers_status;
    ThreadSafeMap<WorkerId, UnhealthWorkerStatus, WorkerIdHash> unhealth_workers_status;
    ThreadSafeMap<VWType, WorkerListPtr> vw_worker_list_map;

    AdaptiveSchedulerConfig adaptive_scheduler_config;
    mutable bthread::Mutex map_mutex;
    Poco::Logger * log;
    // rm heartbeat
    mutable std::optional<BackgroundSchedulePool> schedule_pool;
    std::atomic<UInt64> heartbeat_interval{10000}; /// in ms;
    BackgroundSchedulePool::TaskHolder task;
};

template <class WorkersVecType, class GetWorkerFunc>
std::shared_ptr<WorkerGroupStatus> WorkerStatusManager::getWorkerGroupStatus(
    Context * global_context,
    const WorkersVecType & host_ports,
    const String & vw_name,
    const String & wg_name,
    GetWorkerFunc && func,
    bool can_check)
{
    auto worker_group_status = std::make_unique<WorkerGroupStatus>(global_context);
    auto now = std::chrono::system_clock::now();
    WorkerNodeSet need_reset_workers;
    worker_group_status->filter_indices.reserve(host_ports.size());

    updateVWWorkerList(host_ports, vw_name, wg_name);
    for (size_t idx = 0; idx < host_ports.size(); ++idx)
    {
        const auto & host = host_ports[idx];
        auto id = func(vw_name, wg_name, host);
        bool exist = false;
        global_extra_workers_status.update(
            id, [idx, &need_reset_workers, &now, this, id, &exist, &worker_group_status, can_check](WorkerStatusExtra & val) {
                exist = true;
                auto worker_scheduler_status = val.worker_status->getStatus();
                if ((worker_scheduler_status == WorkerSchedulerStatus::Unhealth
                     || worker_scheduler_status == WorkerSchedulerStatus::NotConnected)
                    && std::chrono::duration_cast<std::chrono::seconds>(now - val.server_last_update_time).count()
                        > adaptive_scheduler_config.NEED_RESET_SECONDS)
                    need_reset_workers.emplace(id);

                LOG_TRACE(log, val.toDebugString());
                if (likely(val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::Close))
                {
                    if (val.worker_status->getStatus() <= WorkerSchedulerStatus::OnlySource)
                    {
                        worker_group_status->filter_indices.emplace_back(idx);
                        worker_group_status->workers_status.emplace(id, val.worker_status);
                    }
                    else
                        worker_group_status->unhealth_worker_size++;
                }
                else if (val.circuit_break.breaker_status == WorkerCircuitBreakerStatus::HalfOpen)
                {
                    if (val.circuit_break.is_checking)
                    {
                        LOG_TRACE(log, "half open worker {} is checking", id.ToString());
                        worker_group_status->half_open_workers_checking_size++;
                    }
                    else
                    {
                        if (can_check)
                        {
                            LOG_TRACE(log, "check half open worker ", id.ToString());
                            val.circuit_break.is_checking = true;
                            worker_group_status->half_open_workers_size++;
                            worker_group_status->half_open_workers.emplace(id);
                            worker_group_status->workers_status.emplace(id, val.worker_status);
                            worker_group_status->filter_indices.emplace_back(idx);
                        }
                        else
                        {
                            LOG_TRACE(log, "half open worker {} is checking", id.ToString());
                            worker_group_status->half_open_workers_checking_size++;
                        }
                    }
                }
            });
        if (!exist)
        {
            worker_group_status->unknown_worker_size++;
            worker_group_status->filter_indices.emplace_back(idx);
            LOG_DEBUG(log, "can't find worker node : {}'s status", id.ToString());
        }
    }
    worker_group_status->calculateStatus();
    for (const auto & id : need_reset_workers)
    {
        LOG_DEBUG(log, "restore worker ", id.ToString());
        restoreWorkerNode(id);
    }

    return worker_group_status;
}
}
