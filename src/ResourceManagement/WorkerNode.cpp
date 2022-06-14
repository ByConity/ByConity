#include <ResourceManagement/WorkerNode.h>

#include <ResourceManagement/CommonData.h>
#include <Protos/data_models.pb.h>
#include <Protos/RPCHelpers.h>
#include <common/logger_useful.h>

namespace DB::ResourceManagement
{

bool WorkerNode::available(UInt64 part_bytes) const
{
    // 10 seconds
    if (!last_update_time || time(nullptr) - last_update_time >= 10)
        return false;

    if (part_bytes && disk_space <= part_bytes * 1.5)
        return false;

    return true;
}

bool WorkerNode::available(const ResourceRequirement & requirement) const
{
    if (requirement.limit_cpu > 0.01 && cpu_usage > requirement.limit_cpu)
        return false;

    if (requirement.request_mem_bytes && memory_available < requirement.request_mem_bytes)
        return false;
    if (requirement.request_disk_bytes && disk_space < requirement.request_disk_bytes)
        return false;
    return true;
}

Int32 WorkerNode::score(UInt64 part_bytes, double usage) const
{
    if (disk_space && disk_space <= part_bytes * 1.5)  // Disk space not enough.
        return 0;

    if (fabs(usage) >= 1e-5)  // usage != 0
        return memory_usage <= usage && cpu_usage <= usage;

    Int32 res = 0;  // TODO
    res += cpu_weight * (1 - cpu_usage);
    res += mem_weight * (1 - memory_usage);

    return res;
}

String WorkerNode::toDebugString() const
{
    std::stringstream ss;
    ss << "Current Resource Usage in Worker ID: " << id << "\n";
    ss << "\tWorker Group: " << worker_group_id << "\n";
    ss << "\tVW: " << vw_name << "\n";
    ss << "\tHost: " << host.toDebugString() << "\n";
    ss << "\tCPU limit: " << cpu_limit << "\tCurrent CPU usage: " << cpu_usage << "\n";
    ss << "\tMemory limit: " << memory_limit << "\tCurrent Memory usage: " << memory_usage << "\n";
    ss << "\tMemory available: " << memory_available << "\n";
    ss << "\tAvailable Disk Space: " << disk_space << "\n";
    ss << "\tCurrent executing query: " << query_num << "\n";
    return ss.str();
}

void WorkerNode::update(const WorkerNodeResourceData & data)
{
    cpu_usage.store(data.cpu_usage, std::memory_order_relaxed);
    memory_usage.store(data.memory_usage, std::memory_order_relaxed);
    memory_available.store(data.memory_available, std::memory_order_relaxed);
    disk_space.store(data.disk_space, std::memory_order_relaxed);
    query_num.store(data.query_num, std::memory_order_relaxed);
    last_update_time = time(nullptr);
}

WorkerNodeResourceData WorkerNode::getResourceData() const
{
    WorkerNodeResourceData res;
    res.host_ports = host;
    res.vw_name = vw_name;
    res.worker_group_id = worker_group_id;

    res.cpu_usage = cpu_usage.load(std::memory_order_relaxed);
    res.memory_usage = memory_usage.load(std::memory_order_relaxed);
    res.memory_available = memory_available.load(std::memory_order_relaxed);
    res.disk_space = disk_space.load(std::memory_order_relaxed);
    res.query_num = query_num.load(std::memory_order_relaxed);

    res.cpu_limit = cpu_limit;
    res.memory_limit = memory_limit;

    res.last_update_time = last_update_time;

    return res;
}

void WorkerNode::init(const WorkerNodeResourceData & data)
{
    update(data);

    host = data.host_ports;
    cpu_limit = data.cpu_limit;
    memory_limit = data.memory_limit;

    id = data.id;
    vw_name = data.vw_name;
    worker_group_id = data.worker_group_id;
}

void WorkerNode::fillProto(Protos::WorkerNodeData & entry) const
{
    entry.set_id(id);
    entry.set_worker_group_id(worker_group_id);
    RPCHelpers::fillHostWithPorts(host, *entry.mutable_host_ports());
}

void WorkerNode::fillProto(Protos::WorkerNodeResourceData & entry) const
{
    RPCHelpers::fillHostWithPorts(host, *entry.mutable_host_ports());

    entry.set_query_num(query_num.load(std::memory_order_relaxed));
    entry.set_cpu_usage(cpu_usage.load(std::memory_order_relaxed));
    entry.set_memory_usage(memory_usage.load(std::memory_order_relaxed));
    entry.set_disk_space(disk_space.load(std::memory_order_relaxed));
    entry.set_memory_available(memory_available.load(std::memory_order_relaxed));

    entry.set_id(id);
    entry.set_vw_name(vw_name);
    entry.set_worker_group_id(worker_group_id);
    entry.set_last_update_time(static_cast<UInt32>(last_update_time));
}

}
