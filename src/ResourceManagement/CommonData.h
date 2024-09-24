/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include <Common/formatReadable.h>
#include <Common/HostWithPorts.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Core/SettingsEnums.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <ResourceManagement/VWScheduleAlgo.h>
#include <ResourceManagement/WorkerGroupType.h>
#include <Protos/data_models.pb.h>

namespace DB
{
    class Context;
}

namespace DB::Protos
{
    class ResourceRequirement;
    class QueryQueueInfo;
    class VirtualWarehouseSettings;
    class VirtualWarehouseAlterSettings;
    class VirtualWarehouseData;
    class WorkerNodeData;
    class WorkerNodeResourceData;
    class WorkerGroupData;
    class WorkerGroupMetrics;
    class WorkerMetrics;
}

namespace DB::ResourceManagement
{

struct QueueRule
{
    std::string rule_name;
    std::vector<std::string> databases;
    std::vector<std::string> tables;
    std::string query_id;
    std::string user;
    std::string ip;
    std::string fingerprint;

    void fillProto(Protos::QueueRule & queue_rule) const;
    void parseFromProto(const Protos::QueueRule & queue_rule);
};

struct QueueData
{
    std::string queue_name;
    size_t max_concurrency{100};
    size_t query_queue_size{200};
    std::vector<QueueRule> queue_rules;

    void fillProto(Protos::QueueData & queue_data) const;
    void parseFromProto(const Protos::QueueData & queue_data);
};

struct VirtualWarehouseSettings
{
    /// basic information ///
    VirtualWarehouseType type{VirtualWarehouseType::Unknown};
    size_t min_worker_groups{0}; // include shared groups
    size_t max_worker_groups{0}; // include shared groups
    size_t num_workers{0}; /// #worker per group
    size_t auto_suspend{0};
    size_t auto_resume{1};

    /// vw queue (resource group) ///
    size_t max_concurrent_queries{0};
    size_t max_queued_queries{0};
    size_t max_queued_waiting_ms{5000};
    VWScheduleAlgo vw_schedule_algo{VWScheduleAlgo::Random};

    /// resource coordinator (auto-sharing & auto-scaling) ///
    size_t max_auto_borrow_links{0};
    size_t max_auto_lend_links{0};

    // vw is allowed to create a new shared wg link to other wg(in other vw) if metric > threshold
    size_t cpu_busy_threshold{100};
    size_t mem_busy_threshold{100};

    // vw is allowed to share its wg to other vws if metric < threshold.
    size_t cpu_idle_threshold{0};
    size_t mem_idle_threshold{0};

    // if vw has lent wgs and metrc > threshold, recall the lent wg (by drop the shared wg).
    size_t cpu_threshold_for_recall{100};
    size_t mem_threshold_for_recall{100};

    size_t cooldown_seconds_after_scaleup{300};
    size_t cooldown_seconds_after_scaledown{300};

    std::vector<QueueData> queue_datas;

    //adaptive scheduler
    size_t recommended_concurrent_query_limit{480};
    double health_worker_cpu_usage_threshold{0.95};
    int64_t circuit_breaker_open_to_halfopen_wait_seconds{60};
    int64_t unhealth_worker_recheck_wait_seconds{30};
    int64_t circuit_breaker_open_error_threshold{8};

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
    std::optional<size_t> max_queued_queries;
    std::optional<size_t> max_queued_waiting_ms;
    std::optional<size_t> auto_suspend;
    std::optional<size_t> auto_resume;
    std::optional<VWScheduleAlgo> vw_schedule_algo;
    std::optional<size_t> max_auto_borrow_links;
    std::optional<size_t> max_auto_lend_links;
    std::optional<size_t> cpu_busy_threshold;
    std::optional<size_t> mem_busy_threshold;
    std::optional<size_t> cpu_idle_threshold;
    std::optional<size_t> mem_idle_threshold;
    std::optional<size_t> cpu_threshold_for_recall;
    std::optional<size_t> mem_threshold_for_recall;
    std::optional<size_t> cooldown_seconds_after_scaleup;
    std::optional<size_t> cooldown_seconds_after_scaledown;

    Protos::QueueAlterType queue_alter_type {Protos::QueueAlterType::UNKNOWN};
    std::optional<size_t> max_concurrency;
    std::optional<size_t> query_queue_size;
    std::optional<String> query_id;
    std::optional<String> user;
    std::optional<String> ip;
    std::optional<String> rule_name;
    std::optional<String> fingerprint;
    std::optional<String> queue_name;
    bool has_table {false};
    bool has_database {false};
    std::vector<String> tables;
    std::vector<String> databases;

    std::optional<QueueData> queue_data;

    //adaptive scheduler
    std::optional<size_t> recommended_concurrent_query_limit;
    std::optional<double> health_worker_cpu_usage_threshold;
    std::optional<int64_t> circuit_breaker_open_to_halfopen_wait_seconds;
    std::optional<int64_t> unhealth_worker_recheck_wait_seconds;
    std::optional<int64_t> circuit_breaker_open_error_threshold;

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
    size_t running_query_count;
    size_t queued_query_count;
    size_t num_borrowed_worker_groups{0};
    size_t num_lent_worker_groups{0};
    uint64_t last_borrow_timestamp{0};
    uint64_t last_lend_timestamp{0};

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

enum class WorkerState
{
    Registering,
    Running,
    Stopped, /// We don't have a Stopping state as we regard the worker as stopped immediately when RM receive a unregister request.
};

struct WorkerNodeResourceData
{
    HostWithPorts host_ports;
    std::string id;
    std::string vw_name;
    std::string worker_group_id;

    double cpu_usage;
    double cpu_usage_1min;
    double cpu_usage_10sec;
    double memory_usage;
    double memory_usage_1min;
    UInt64 memory_available;
    UInt64 disk_space;
    UInt32 query_num;
    UInt32 manipulation_num;
    UInt32 consumer_num;

    UInt64 cpu_limit;
    UInt64 memory_limit;

    // TODO: consider using UInt64 here to avoid Year 2038 problem
    UInt32 register_time;
    // TODO: consider using UInt64 here to avoid Year 2038 problem
    UInt32 last_update_time;

    UInt64 reserved_memory_bytes;
    UInt32 reserved_cpu_cores;
    WorkerState state;
    UInt64 last_status_create_time {0};

    std::string serializeAsString() const;
    void parseFromString(const std::string & s);

    static inline auto createFromString(const std::string & s)
    {
        WorkerNodeCatalogData res;
        res.parseFromString(s);
        return res;
    }
    WorkerNodeResourceData() = default;
    WorkerNodeResourceData(const Protos::WorkerNodeResourceData & resource_info);

    void fillProto(Protos::WorkerNodeResourceData & resource_info) const;
    static WorkerNodeResourceData createFromProto(const Protos::WorkerNodeResourceData & resource_info);
    inline String toDebugString() const
    {
        std::stringstream ss;
        ss << "{ vw:" << vw_name << ", worker_group:" << worker_group_id << ", id:" << id << ", register_time: " << register_time
           << ", cpu_usage:" << cpu_usage << ", memory_usage:" << memory_usage
           << ", memory_available:" << formatReadableSizeWithDecimalSuffix(memory_available) << ", query_num:" << query_num << " }";
        return ss.str();
    }
};

/// Based on VWScheduleAlgo, we can pass some additional requirements.
/// We can queue the current query/task if there is no worker/wg satisfy the requirements.
/// Examples:
///  1. Require at least 20G disk space for a merge task.
///  2. Require about 5G memory for a insert query.
///  3. Require the avg. cpu of the worker/wg not higher than 80%.
struct ResourceRequirement
{
    size_t request_mem_bytes{0};
    size_t request_disk_bytes{0};
    uint32_t expected_workers{0};
    String worker_group{};
    uint32_t request_cpu_cores{0};
    double cpu_usage_max_threshold{0};
    uint32_t task_cold_startup_sec{0};
    std::unordered_set<String> blocklist;
    bool forbid_random_result{false};
    bool no_repeat{false};

    void fillProto(Protos::ResourceRequirement & proto) const;
    void parseFromProto(const Protos::ResourceRequirement & proto);

    static inline auto createFromProto(const Protos::ResourceRequirement & pb_data)
    {
        ResourceRequirement requirement{};
        requirement.parseFromProto(pb_data);
        return requirement;
    }

    inline String toDebugString() const
    {
        std::stringstream ss;
        ss << "{request_cpu_cores:" << request_cpu_cores
           << ", cpu_usage_max_threshold:" << cpu_usage_max_threshold
           << ", request_mem_bytes:" << request_mem_bytes
           << ", request_disk_bytes:" << request_disk_bytes
           << ", expected_workers:" << expected_workers
           << ", worker_group:" << worker_group
           << ", task_cold_startup_sec:" << task_cold_startup_sec
           << ", blocklist size: " << blocklist.size()
           << "}";
        return ss.str();
    }

};

struct WorkerMetrics
{
    String id;

    double cpu_1min{0};
    double mem_1min{0};
    uint32_t num_queries{0};
    uint32_t num_manipulations{0};
    uint32_t num_consumers{0};
    uint32_t num_dedups{0};

    void fillProto(Protos::WorkerMetrics & data) const;
    void parseFromProto(const Protos::WorkerMetrics & data);
    static inline auto createFromProto(const Protos::WorkerMetrics & pb_data)
    {
        WorkerMetrics metrics;
        metrics.parseFromProto(pb_data);
        return metrics;
    }

    inline auto numTasks() const
    {
        return num_manipulations + num_consumers + num_dedups;
    }
};

/// Worker Group's metrics.
struct WorkerGroupMetrics
{
    String id;

    std::vector<WorkerMetrics> worker_metrics_vec;

    WorkerGroupMetrics(const String & _id = "") : id(_id) { }

    void reset();
    void fillProto(Protos::WorkerGroupMetrics & proto) const;
    void parseFromProto(const Protos::WorkerGroupMetrics & proto);
    static inline auto createFromProto(const Protos::WorkerGroupMetrics & proto)
    {
        WorkerGroupMetrics metrics;
        metrics.parseFromProto(proto);
        return metrics;
    }

    bool available([[maybe_unused]] const ResourceRequirement & requirement) const
    {
        /// TODO: TBC
        return true;
    }

    double avg_cpu_1min() const
    {
        if (worker_metrics_vec.empty())
            return 0;

        double sum = 0;
        for (const auto & metrics : worker_metrics_vec)
            sum += metrics.cpu_1min;
        return sum / worker_metrics_vec.size();
    }

    double avg_mem_1min() const
    {
        if (worker_metrics_vec.empty())
            return 0;

        double sum = 0;
        for (const auto & metrics : worker_metrics_vec)
            sum += metrics.mem_1min;
        return sum / worker_metrics_vec.size();
    }
};



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

    std::vector<WorkerNodeResourceData> worker_node_resource_vec;
    /// For query scheduler
    WorkerGroupMetrics metrics;

    bool is_auto_linked {false};
    std::string linked_vw_name;
    Int64 priority {0};

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

// Struct used for synchronising Query Queue information with Resource Manager
struct QueryQueueInfo
{
    UInt32 queued_query_count {0};
    UInt32 running_query_count {0};
    UInt64 last_sync {0};

    void fillProto(Protos::QueryQueueInfo & data) const;
    void parseFromProto(const Protos::QueryQueueInfo & data);

    static inline auto createFromProto(const Protos::QueryQueueInfo & pb_data)
    {
        QueryQueueInfo group_data;
        group_data.parseFromProto(pb_data);
        return group_data;
    }
};

}

namespace DB
{
namespace RM = ResourceManagement;
using ServerQueryQueueMap = std::unordered_map<String, RM::QueryQueueInfo>;
using VWQueryQueueMap = std::unordered_map<String, RM::QueryQueueInfo>;
using AggQueryQueueMap = std::unordered_map<String, RM::QueryQueueInfo>;
using QueryQueueInfo = RM::QueryQueueInfo;
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
using WorkerMetrics = RM::WorkerMetrics;
}
