#pragma once

#include <unordered_map>
#include <unordered_set>
#include <memory>

#include <Core/UUID.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <ResourceManagement/VWScheduleAlgo.h>
#include <ResourceManagement/WorkerGroupType.h>
#include <Common/HostWithPorts.h>

namespace DB
{
    class Context;
}

namespace DB::Protos
{
    class ResourceRequirement;
    class VirtualWarehouseSettings;
    class VirtualWarehouseAlterSettings;
    class VirtualWarehouseData;
    class WorkerNodeData;
    class WorkerNodeResourceData;
    class WorkerGroupData;
    class WorkerGroupMetrics;
}

namespace DB::ResourceManagement
{

struct VirtualWarehouseSettings
{
    VirtualWarehouseType type{VirtualWarehouseType::Unknown};
    size_t num_workers{0}; /// per group
    size_t min_worker_groups{0};
    size_t max_worker_groups{0};
    size_t max_concurrent_queries{0};
    size_t auto_suspend{0};
    size_t auto_resume{1};
    VWScheduleAlgo vw_schedule_algo{VWScheduleAlgo::Random};

    void fillProto(Protos::VirtualWarehouseSettings & pb_settings) const;
    void parseFromProto(const Protos::VirtualWarehouseSettings & pb_settings);

    static inline auto createFromProto(const Protos::VirtualWarehouseSettings & pb_settings)
    {
        VirtualWarehouseSettings vw_settings;
        vw_settings.parseFromProto(pb_settings);
        return vw_settings;
    }
};

struct VirtualWarehouseAlterSettings
{
    std::optional<VirtualWarehouseType> type;
    std::optional<size_t> num_workers; /// per group
    std::optional<size_t> min_worker_groups;
    std::optional<size_t> max_worker_groups;
    std::optional<size_t> max_concurrent_queries;
    std::optional<size_t> auto_suspend;
    std::optional<size_t> auto_resume;
    std::optional<VWScheduleAlgo> vw_schedule_algo;

    void fillProto(Protos::VirtualWarehouseAlterSettings & pb_settings) const;
    void parseFromProto(const Protos::VirtualWarehouseAlterSettings & pb_settings);

    static inline auto createFromProto(const Protos::VirtualWarehouseAlterSettings & pb_settings)
    {
        VirtualWarehouseAlterSettings vw_settings;
        vw_settings.parseFromProto(pb_settings);
        return vw_settings;
    }
};

struct VirtualWarehouseData
{
    /// constants
    std::string name;
    UUID uuid;

    VirtualWarehouseSettings settings;

    ///  runtime information
    size_t num_worker_groups{0};
    size_t num_workers{0};

    void fillProto(Protos::VirtualWarehouseData & pb_data) const;
    void parseFromProto(const Protos::VirtualWarehouseData & pb_data);
    static inline auto createFromProto(const Protos::VirtualWarehouseData & pb_data)
    {
        VirtualWarehouseData res;
        res.parseFromProto(pb_data);
        return res;
    }

    std::string serializeAsString() const;
    void parseFromString(const std::string & s);

    static inline auto createFromString(const std::string & s)
    {
        VirtualWarehouseData res;
        res.parseFromString(s);
        return res;
    }
};

struct WorkerNodeCatalogData
{
    std::string id;
    std::string worker_group_id;
    HostWithPorts host_ports;

    std::string serializeAsString() const;
    void parseFromString(const std::string & s);

    static inline auto createFromString(const std::string & s)
    {
        WorkerNodeCatalogData res;
        res.parseFromString(s);
        return res;
    }

    static WorkerNodeCatalogData createFromProto(const Protos::WorkerNodeData & worker_data);
};

struct WorkerNodeResourceData
{
    HostWithPorts host_ports;
    std::string id;
    std::string vw_name;
    std::string worker_group_id;

    double cpu_usage;
    double memory_usage;
    UInt64 memory_available;
    UInt64 disk_space;
    UInt32 query_num;

    UInt64 cpu_limit;
    UInt64 memory_limit;

    UInt32 last_update_time;
    std::string serializeAsString() const;
    void parseFromString(const std::string & s);

    static inline auto createFromString(const std::string & s)
    {
        WorkerNodeCatalogData res;
        res.parseFromString(s);
        return res;
    }

    void fillProto(Protos::WorkerNodeResourceData & resource_info) const;
    static WorkerNodeResourceData createFromProto(const Protos::WorkerNodeResourceData & resource_info);
};

/// Based on VWScheduleAlgo, we can pass some additional requirements.
/// We can queue the current query/task if there is no worker/wg satisfy the requirements.
/// Examples:
///  1. Require at least 20G disk space for a merge task.
///  2. Require about 5G memory for a insert query.
///  3. Require the avg. cpu of the worker/wg not higher than 80%.
struct ResourceRequirement
{
    /// the target worker/wg's max/avg cpu must be lower than this limit.
    double limit_cpu{0};

    /// request memory in bytes, eg: a insert query requests 5000 MB memory.
    size_t request_mem_bytes{0};

    /// request disk in bytes, eg: a merge task requests 10 GB disk.
    size_t request_disk_bytes{0};

    uint32_t expected_workers{0};

    String worker_group{};

    void fillProto(Protos::ResourceRequirement & proto) const;
    void parseFromProto(const Protos::ResourceRequirement & proto);

    static inline auto createFromProto(const Protos::ResourceRequirement & pb_data)
    {
        ResourceRequirement requirement{};
        requirement.parseFromProto(pb_data);
        return requirement;
    }

};

inline std::ostream & operator<<(std::ostream & os, const ResourceRequirement & requirement)
{
    return os << "{limit_cpu:" << requirement.limit_cpu
              << ", request_mem_bytes:" << requirement.request_mem_bytes
              << ", request_disk_bytes:" << requirement.request_disk_bytes
              << ", expected_workers:" << requirement.expected_workers
              << "}";
}

/// Worker Group's aggregated metrics.
struct WorkerGroupMetrics
{
    String id;
    uint32_t num_workers; /// 0 means metrics (and the worker group) is unavailable.

    /// CPU state.
    double min_cpu_usage;
    double max_cpu_usage;
    double avg_cpu_usage;

    /// MEM state.
    double min_mem_usage;
    double max_mem_usage;
    double avg_mem_usage;
    uint64_t min_mem_available;

    /// Query State.
    uint32_t total_queries;

    WorkerGroupMetrics(const String & _id = "") : id(_id)
    {
        min_cpu_usage = std::numeric_limits<double>::max();
        min_mem_usage = std::numeric_limits<double>::max();
        min_mem_available = std::numeric_limits<uint64_t>::max();
    }

    void reset();
    void fillProto(Protos::WorkerGroupMetrics & proto) const;
    void parseFromProto(const Protos::WorkerGroupMetrics & proto);
    static inline auto createFromProto(const Protos::WorkerGroupMetrics & proto)
    {
        WorkerGroupMetrics metrics;
        metrics.parseFromProto(proto);
        return metrics;
    }

    bool available(const ResourceRequirement & requirement) const
    {
        /// Worker group is unavailable if there are no enough active workers.
        if (num_workers < requirement.expected_workers)
            return false;
        /// Worker group is unavailable if any worker's cpu usage is higher than limit.
        if (requirement.limit_cpu && max_cpu_usage > requirement.limit_cpu)
            return false;
        /// Worker group is unavailable if any worker's available memory space is less than request.
        if (requirement.request_mem_bytes && min_mem_available < requirement.request_mem_bytes * 1000)
            return false;
        return true;
    }
};

inline std::ostream & operator<<(std::ostream & os, const WorkerGroupMetrics & metrics)
{
    return os << metrics.id << ":"
               << metrics.max_cpu_usage << "|" << metrics.min_cpu_usage << "|" << metrics.avg_cpu_usage << "|"
               << metrics.max_mem_usage << "|" << metrics.min_mem_usage << "|" << metrics.avg_mem_usage << "|" << metrics.min_mem_available
               << "|" << metrics.total_queries;
}

struct WorkerGroupData
{
    std::string id;
    WorkerGroupType type {WorkerGroupType::Physical};
    UUID vw_uuid {};
    std::string vw_name;

    std::string psm; /// For physical group, XXX: remove me
    std::string linked_id; /// For shared group

    std::vector<HostWithPorts> host_ports_vec;
    size_t num_workers{};

    /// For query scheduler
    WorkerGroupMetrics metrics;

    std::string serializeAsString() const;
    void parseFromString(const std::string & s);

    void fillProto(Protos::WorkerGroupData & data, const bool with_host_ports, const bool with_metrics) const;
    void parseFromProto(const Protos::WorkerGroupData & data);

    static inline auto createFromProto(const Protos::WorkerGroupData & pb_data)
    {
        WorkerGroupData group_data;
        group_data.parseFromProto(pb_data);
        return group_data;
    }
};

}

namespace DB
{
namespace RM = ResourceManagement;
using VirtualWarehouseSettings = RM::VirtualWarehouseSettings;
using VirtualWarehouseAlterSettings = RM::VirtualWarehouseAlterSettings;
using VirtualWarehouseType = RM::VirtualWarehouseType;
using VirtualWarehouseData = RM::VirtualWarehouseData;
using WorkerNodeData = RM::WorkerNodeCatalogData;
using WorkerNodeResourceData = RM::WorkerNodeResourceData;
using WorkerGroupData = RM::WorkerGroupData;
using WorkerGroupType = RM::WorkerGroupType;
using ResourceRequirement = RM::ResourceRequirement;
using VWScheduleAlgo = RM::VWScheduleAlgo;
}
