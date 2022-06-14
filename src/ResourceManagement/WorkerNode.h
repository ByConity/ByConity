#pragma once

#include <Core/Types.h>
#include <Common/HostWithPorts.h>

#include <atomic>
#include <memory>
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

struct WorkerNode
{
    WorkerNode(const WorkerNodeResourceData & data) { init(data); }
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

    UInt32 cpu_limit;
    UInt32 memory_limit;

    time_t last_update_time = 0;

    bool assigned = false;

    String toDebugString() const;
    void init(const WorkerNodeResourceData & data);
    void update(const WorkerNodeResourceData & data);
    WorkerNodeResourceData getResourceData() const;
    void fillProto(Protos::WorkerNodeData & entry) const;
    void fillProto(Protos::WorkerNodeResourceData & entry) const;

    bool available(UInt64 part_bytes = 0) const;
    bool available(const ResourceRequirement & requirement) const;
    Int32 score(UInt64 part_bytes, double usage = 0) const;

    // xxx
    static constexpr size_t cpu_weight = 50;
    static constexpr size_t mem_weight = 50;
};

using WorkerNodePtr = std::shared_ptr<WorkerNode>;

}
