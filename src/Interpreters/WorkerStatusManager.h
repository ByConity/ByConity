#pragma once

#include <chrono>
#include <optional>
#include <unordered_set>
#include <vector>
#include <Interpreters/Cluster.h>
#include <Protos/data_models.pb.h>
#include <ResourceManagement/CommonData.h>
#include <boost/noncopyable.hpp>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
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

template <typename KEY, typename VALUE, typename HASH, typename EQUAL, uint32_t MAP_COUNT = 23>
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

using WorkerNodeSet = std::unordered_set<WorkerId, WorkerIdHash, WorkerIdEqual>;
using WorkerStatusPtr = std::shared_ptr<WorkerStatus>;
using WorkerNodeStatusContainer = std::unordered_map<WorkerId, WorkerStatusPtr, WorkerIdHash, WorkerIdEqual>;

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
    WorkerGroupStatus(Context * context) : global_context(*context) { }
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
    WorkerGroupHealthStatus status;
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
    Context & global_context;
};

using WorkerGroupStatusPtr = std::shared_ptr<WorkerGroupStatus>;
using UnhealthWorkerStatusMap = std::unordered_map<WorkerId, UnhealthWorkerStatus, WorkerIdHash, WorkerIdEqual>;

class WorkerStatusManager
{
public:
    enum class UpdateSource : uint8_t
    {
        ComeFromRM = 1,
        ComeFromWorker = 2
    };

    constexpr static const size_t CIRCUIT_BREAKER_THRESHOLD = 20;

    WorkerStatusManager() : log(&Poco::Logger::get("WorkerStatusManager")) { }

    void updateWorkerNode(
        const Protos::WorkerNodeResourceData & resource_info,
        UpdateSource source,
        WorkerSchedulerStatus update_for_status = WorkerSchedulerStatus::Unknown);

    void setWorkerNodeDead(const WorkerId & key, int error_code);

    void restoreWorkerNode(const WorkerId & key);

    void CloseCircuitBreaker(const WorkerId & key);

    UnhealthWorkerStatusMap getWorkersNeedUpdateFromRM();

    std::shared_ptr<WorkerGroupStatus>
    getWorkerGroupStatus(Context * global_context, const HostWithPortsVec & host_ports, const String & vw_name, const String & wg_name);

    void updateConfig(const Poco::Util::AbstractConfiguration & config);

    static WorkerId getWorkerId(const String & vw_name, const String & group_id, const String & id)
    {
        return WorkerId{vw_name, group_id, id};
    }

    static WorkerId getWorkerId(const Protos::WorkerNodeResourceData & resource_info)
    {
        return WorkerId{resource_info.vw_name(), resource_info.worker_group_id(), resource_info.id()};
    }

private:
    ThreadSafeMap<WorkerId, WorkerStatusExtra, WorkerIdHash, WorkerIdEqual> global_extra_workers_status;
    ThreadSafeMap<WorkerId, UnhealthWorkerStatus, WorkerIdHash, WorkerIdEqual> unhealth_workers_status;

    AdaptiveSchedulerConfig adaptive_scheduler_config;
    mutable bthread::Mutex map_mutex;
    Poco::Logger * log;
};
}
