#pragma once

#include <Core/Types.h>
#include <Common/HostWithPorts.h>
#include <ResourceManagement/CommonData.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <iostream>
#include <cmath>


namespace DB::Protos
{
class WorkerNodeData;
class WorkerNodeResourceData;
}

namespace DB::ResourceManagement
{
struct WorkerNodeResourceData;
struct ResourceRequirement;

enum DeductionType
{
    Cpu,
    Mem,
};

struct DeductionEntry
{
    DeductionType type;
    UInt64 value;
    time_t delete_time;

    DeductionEntry(DeductionType type_, UInt64 value_, time_t delete_time_) 
    : type(type_), value(value_), delete_time(delete_time_) {}
};

struct WorkerNode
{
    WorkerNode(const WorkerNodeResourceData & data, const bool set_running) { init(data, set_running); }
    WorkerNode(String id_, const HostWithPorts & host_, String vw_name_, String worker_group_id_)
        : id(std::move(id_)), host(host_), vw_name(std::move(vw_name_)), worker_group_id(std::move(worker_group_id_))
    {
    }

    std::string id;
    HostWithPorts host;

    std::string vw_name;
    std::string worker_group_id;

    const auto & getID() const { return id; }

    /// metrics
    std::atomic<double> cpu_usage;
    std::atomic<double> memory_usage;
    std::atomic<UInt64> memory_available;
    std::atomic<UInt64> disk_space;
    std::atomic<UInt32> query_num;
    std::atomic<WorkerState> state{WorkerState::Registering};

    UInt32 cpu_limit = 0;
    UInt32 memory_limit = 0;

    time_t register_time = 0;
    time_t last_update_time = 0;

    bool assigned = false;

    mutable std::mutex deduction_mutex;
    std::atomic<UInt64> reserved_memory_bytes = 0;
    std::atomic<UInt32> reserved_cpu_cores = 0;
    std::vector<DeductionEntry> deductions{};

    String toDebugString() const;
    void init(const WorkerNodeResourceData & data, const bool set_running);
    void update(const WorkerNodeResourceData & data, const size_t register_granularity = 5);
    WorkerNodeResourceData getResourceData() const;
    void fillProto(Protos::WorkerNodeData & entry) const;
    void fillProto(Protos::WorkerNodeResourceData & entry) const;

    inline double convertCoresToPercents() const
    {
        return convertCoresToPercents(reserved_cpu_cores.load(std::memory_order_relaxed));
    }

    inline double convertCoresToPercents(UInt32 num_cores) const
    {
        return 100 * num_cores / static_cast<double>(cpu_limit);
    }

    bool available(UInt64 part_bytes = 0) const;
    bool available(const ResourceRequirement & requirement) const;
    Int32 score(UInt64 part_bytes, double usage = 0) const;

    void reserveResourceQuotas(const ResourceRequirement & requirement, const uint32_t n = 1);
};

using WorkerNodePtr = std::shared_ptr<WorkerNode>;

}
