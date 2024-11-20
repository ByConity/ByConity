#pragma once

#include <Common/Logger.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
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
#include <Core/SettingsEnums.h>
#include <Interpreters/Context.h>
#include <Protos/RPCHelpers.h>
namespace DB
{
class VirtualWarehouseHandleImpl;
using WorkerGroupData = ResourceManagement::WorkerGroupData;

class WorkerStatusManager;
using WorkerStatusManagerPtr = std::shared_ptr<WorkerStatusManager>;

enum class ScheduleType : uint8_t
{
    Health = 1,
    Unknown = 2,
    Unhealth = 5,
};

enum class GroupHealthType : uint8_t
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
    ScheduleType status{ScheduleType::Unknown};
    std::chrono::system_clock::time_point update_time{};
};

struct WorkerCircuitBreaker
{
    int64_t fail_count{0};
    WorkerCircuitBreakerStatus breaker_status{WorkerCircuitBreakerStatus::Close};
    bool is_checking{false};
    std::chrono::system_clock::time_point open_time;
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
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        map[idx][key] = value;
    }

    std::optional<VALUE> get(const KEY & key)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        if (map[idx].count(key) == 0)
            return std::nullopt;
        return map[idx][key];
    }

    template <class... Args>
    void updateEmplaceIfNotExist(const KEY & key, const std::function<void(VALUE & value)> & call, Args &&... args)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        auto iter = map[idx].find(key);
        if (iter != map[idx].end())
            call(iter->second);
        else
            map[idx].emplace(std::make_pair(key, VALUE(args...)));
    }

    template <class Func>
    void update(const KEY & key, Func && call)
    {
        uint32_t idx = mapIdx(key);
        std::unique_lock<bthread::Mutex> lock(mutex[idx]);
        auto iter = map[idx].find(key);
        if (iter != map[idx].end())
            call(iter->second);
    }

private:
    uint32_t mapIdx(const KEY & key) { return HASH{}(key) % MAP_COUNT; }

    std::unordered_map<KEY, VALUE, HASH, EQUAL> map[MAP_COUNT];
    bthread::Mutex mutex[MAP_COUNT];
};

struct ResourceStatus
{
    ResourceStatus() = default;
    explicit ResourceStatus(const Protos::WorkerNodeResourceData & info, size_t recommended_concurrent_query_limit, double health_worker_cpu_usage_threshold)
    {
        if (info.query_num() > recommended_concurrent_query_limit || info.cpu_usage_1min() > 100 * health_worker_cpu_usage_threshold)
            scheduler_status = ScheduleType::Unhealth;
        else
            scheduler_status = ScheduleType::Health;

        last_status_create_time = info.last_status_create_time();
        //scheduler score, less is better
        scheduler_score = 100.0 * info.query_num() / recommended_concurrent_query_limit + info.cpu_usage_1min();
        register_time = info.register_time();
        host_ports = RPCHelpers::createHostWithPorts(info.host_ports());
    }

    bool compare(const ResourceStatus & rhs) const
    {
        if (scheduler_status == rhs.scheduler_status)
            return scheduler_score < rhs.scheduler_score;
        else
            return scheduler_status < rhs.scheduler_status;
    }

    ScheduleType getStatus() const { return scheduler_status; }
    String toDebugString() const;

    ScheduleType scheduler_status{ScheduleType::Unknown};
    double scheduler_score{0};
    UInt64 last_status_create_time{0};
    UInt32 register_time{0};
    HostWithPorts host_ports;
};

using WorkerNodeStatusContainer = std::unordered_map<WorkerId, ResourceStatus, WorkerIdHash>;

struct WorkerStatus
{
    WorkerStatus() = default;
    WorkerStatus(ResourceStatus status, std::chrono::system_clock::time_point time) : resource_status(status), server_last_update_time(time)
    {
    }
    ResourceStatus resource_status;
    std::chrono::system_clock::time_point server_last_update_time;
    WorkerCircuitBreaker circuit_break;
    String toDebugString() const { return resource_status.toDebugString() + "\t" + circuit_break.toDebugString(); }
};

class WorkerGroupStatus
{
public:
    friend class WorkerStatusManager;
    explicit WorkerGroupStatus(WorkerStatusManagerPtr status_manager_) : status_manager(status_manager_) { }
    ~WorkerGroupStatus();
    void calculateStatus();

    std::optional<std::vector<size_t>> selectHealthNode(const HostWithPortsVec & host_ports);

    GroupHealthType getWorkerGroupHealth() const { return status; }

    size_t getAllWorkerSize() const { return total_count; }
    size_t getAvaibleSize() const { return filter_indices.size(); }
    size_t getHealthWorkerSize() const { return health_count; }

    bool needCheckHalfOpenWorker() const { return need_check; }
    void removeHalfOpenWorker(const WorkerId & worker_id)
    {
        std::lock_guard<bthread::Mutex> lock(mutex);
        half_open_workers.erase(worker_id);
    }

    const std::vector<std::optional<ResourceStatus>> & getWorkersResourceStatus() const { return workers_resource_status; }

private:
    size_t open_count{0};
    size_t health_count{0};
    size_t unhealth_count{0};
    size_t total_count{0};
    size_t unknown_count{0};
    size_t half_open_checking_by_other_count{0};
    size_t half_open_checking_count{0};
    GroupHealthType status{GroupHealthType::Health};
    std::vector<size_t> filter_indices;
    std::vector<std::optional<ResourceStatus>> workers_resource_status;
    WorkerStatusManagerPtr status_manager;

    bthread::Mutex mutex;
    WorkerNodeSet half_open_workers;
    std::atomic<bool> need_check{false};
};

using WorkerGroupStatusPtr = std::shared_ptr<WorkerGroupStatus>;
using UnhealthWorkerStatusMap = std::unordered_map<WorkerId, UnhealthWorkerStatus, WorkerIdHash>;


class WorkerStatusManager : public std::enable_shared_from_this<WorkerStatusManager>, WithContext
{
public:
    friend class VirtualWarehouseHandleImpl;
    enum class UpdateSource : uint8_t
    {
        ComeFromRM = 1,
        ComeFromWorker = 2
    };

    // vw_name.wg_name
    using VWType = String;
    using WorkerVec = std::vector<WorkerId>;
    using WorkerVecPtr = std::shared_ptr<WorkerVec>;

    virtual ~WorkerStatusManager() = default;
    WorkerStatusManager() = default;
    explicit WorkerStatusManager(const ContextPtr global_context_) : WithContext(global_context_), log(getLogger("WorkerStatusManager")) { }

    void updateWorkerNode(const Protos::WorkerNodeResourceData & resource_info, UpdateSource source);

    void setWorkerNodeDead(const WorkerId & key, int error_code);

    void restoreWorkerNode(const WorkerId & key);

    virtual std::optional<WorkerStatus> getWorkerStatus(const WorkerId & worker_id)
    {
        return worker_status_map.get(worker_id);
    }

    std::shared_ptr<WorkerGroupStatus>
    getWorkerGroupStatus(const std::vector<WorkerId> & worker_ids, SchedulerMode mode);

    static WorkerId getWorkerId(const String & vw_name, const String & group_id, const String & id)
    {
        return WorkerId{vw_name, group_id, id};
    }

    static WorkerId getWorkerId(const Protos::WorkerNodeResourceData & resource_info)
    {
        return WorkerId{resource_info.vw_name(), resource_info.worker_group_id(), resource_info.id()};
    }

private:
    ThreadSafeMap<WorkerId, WorkerStatus, WorkerIdHash> worker_status_map;

    std::atomic<size_t> recommended_concurrent_query_limit{480};
    std::atomic<double> health_worker_cpu_usage_threshold{0.95};
    std::atomic<int64_t> circuit_breaker_open_to_halfopen_wait_seconds{60};
    std::atomic<int64_t> unhealth_worker_recheck_wait_seconds{10};
    std::atomic<int64_t> circuit_breaker_open_error_threshold{10};
    mutable bthread::Mutex map_mutex;
    LoggerPtr log;
};

}
