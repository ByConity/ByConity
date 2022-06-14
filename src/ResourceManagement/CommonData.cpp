#include <ResourceManagement/CommonData.h>

#include <Interpreters/Context.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/RPCHelpers.h>
#include <Protos/data_models.pb.h>

namespace DB::ResourceManagement
{

void VirtualWarehouseSettings::fillProto(Protos::VirtualWarehouseSettings & pb_settings) const
{
    pb_settings.set_type(int(type));
    if (min_worker_groups)
        pb_settings.set_min_worker_groups(min_worker_groups);
    if (max_worker_groups)
        pb_settings.set_max_worker_groups(max_worker_groups);
    pb_settings.set_num_workers(num_workers);
    if (auto_suspend)
        pb_settings.set_auto_suspend(auto_suspend);
    if (auto_resume)
        pb_settings.set_auto_resume(auto_resume);
    if (max_concurrent_queries)
        pb_settings.set_max_concurrent_queries(max_concurrent_queries);
    if (max_queued_queries)
        pb_settings.set_max_queued_queries(max_queued_queries);
    if (max_queued_waiting_ms)
        pb_settings.set_max_queued_waiting_ms(max_queued_waiting_ms);
    pb_settings.set_vw_schedule_algo(int(vw_schedule_algo));
}

void VirtualWarehouseSettings::parseFromProto(const Protos::VirtualWarehouseSettings & pb_settings)
{
    type = VirtualWarehouseType(pb_settings.type());
    min_worker_groups = pb_settings.has_min_worker_groups() ? pb_settings.min_worker_groups() : 0;
    max_worker_groups = pb_settings.has_max_worker_groups() ? pb_settings.max_worker_groups() : 0;
    num_workers = pb_settings.num_workers();
    auto_suspend = pb_settings.auto_suspend();
    auto_resume = pb_settings.auto_resume();
    max_concurrent_queries = pb_settings.max_concurrent_queries();
    max_queued_queries = pb_settings.max_queued_queries();
    max_queued_waiting_ms = pb_settings.max_queued_waiting_ms();
    vw_schedule_algo = VWScheduleAlgo(pb_settings.vw_schedule_algo());
}

void VirtualWarehouseAlterSettings::fillProto(Protos::VirtualWarehouseAlterSettings & pb_settings) const
{
    if (type)
        pb_settings.set_type(int(*type));
    if (min_worker_groups)
        pb_settings.set_min_worker_groups(*min_worker_groups);
    if (max_worker_groups)
        pb_settings.set_max_worker_groups(*max_worker_groups);
    if (num_workers)
        pb_settings.set_num_workers(*num_workers);
    if (auto_suspend)
        pb_settings.set_auto_suspend(*auto_suspend);
    if (auto_resume)
        pb_settings.set_auto_resume(*auto_resume);
    if (max_concurrent_queries)
        pb_settings.set_max_concurrent_queries(*max_concurrent_queries);
    if (max_queued_queries)
        pb_settings.set_max_queued_queries(*max_queued_queries);
    if (max_queued_waiting_ms)
        pb_settings.set_max_queued_waiting_ms(*max_queued_waiting_ms);
    if (vw_schedule_algo)
        pb_settings.set_vw_schedule_algo(int(*vw_schedule_algo));
}

void VirtualWarehouseAlterSettings::parseFromProto(const Protos::VirtualWarehouseAlterSettings & pb_settings)
{
    if (pb_settings.has_type())
        type = VirtualWarehouseType(pb_settings.type());
    if (pb_settings.has_min_worker_groups())
        min_worker_groups = pb_settings.min_worker_groups();
    if (pb_settings.has_max_worker_groups())
        max_worker_groups = pb_settings.max_worker_groups();
    if (pb_settings.has_num_workers())
        num_workers = pb_settings.num_workers();
    if (pb_settings.has_auto_suspend())
        auto_suspend = pb_settings.auto_suspend();
    if (pb_settings.has_auto_resume())
        auto_resume = pb_settings.auto_resume();
    if (pb_settings.has_max_concurrent_queries())
        max_concurrent_queries = pb_settings.max_concurrent_queries();
    if (pb_settings.has_max_queued_queries() )
        max_queued_queries = pb_settings.max_queued_queries();
    if (pb_settings.has_max_queued_waiting_ms() )
        max_queued_waiting_ms = pb_settings.max_queued_waiting_ms();
    if (pb_settings.has_vw_schedule_algo())
        vw_schedule_algo = VWScheduleAlgo(pb_settings.vw_schedule_algo());
}

void VirtualWarehouseData::fillProto(Protos::VirtualWarehouseData & pb_data) const
{
    pb_data.set_name(name);
    RPCHelpers::fillUUID(uuid, *pb_data.mutable_uuid());
    settings.fillProto(*pb_data.mutable_settings());
    pb_data.set_num_worker_groups(num_worker_groups);
    pb_data.set_num_workers(num_workers);
}

void VirtualWarehouseData::parseFromProto(const Protos::VirtualWarehouseData & pb_data)
{
    name = pb_data.name();
    uuid = RPCHelpers::createUUID(pb_data.uuid());
    settings.parseFromProto(pb_data.settings());
    num_worker_groups = pb_data.num_worker_groups();
    num_workers = pb_data.num_workers();
}

std::string VirtualWarehouseData::serializeAsString() const
{
    Protos::VirtualWarehouseData pb_data;
    fillProto(pb_data);
    return pb_data.SerializeAsString();
}

void VirtualWarehouseData::parseFromString(const std::string & s)
{
    Protos::VirtualWarehouseData pb_data;
    pb_data.ParseFromString(s);
    parseFromProto(pb_data);
}

std::string WorkerNodeCatalogData::serializeAsString() const
{
    Protos::WorkerNodeData pb_data;

    pb_data.set_id(id);
    pb_data.set_worker_group_id(worker_group_id);
    RPCHelpers::fillHostWithPorts(host_ports, *pb_data.mutable_host_ports());

    return pb_data.SerializeAsString();
}

void WorkerNodeCatalogData::parseFromString(const std::string & s)
{
    Protos::WorkerNodeData pb_data;
    pb_data.ParseFromString(s);

    id = pb_data.id();
    worker_group_id = pb_data.worker_group_id();
    host_ports = RPCHelpers::createHostWithPorts(pb_data.host_ports());
}

WorkerNodeCatalogData WorkerNodeCatalogData::createFromProto(const Protos::WorkerNodeData & worker_data)
{
    WorkerNodeCatalogData res;
    res.id = worker_data.id();
    res.worker_group_id = worker_data.worker_group_id();
    res.host_ports = RPCHelpers::createHostWithPorts(worker_data.host_ports());

    return res;
}

std::string WorkerNodeResourceData::serializeAsString() const
{
    Protos::WorkerNodeResourceData pb_data;

    fillProto(pb_data);

    return pb_data.SerializeAsString();
}

void WorkerNodeResourceData::parseFromString(const std::string & s)
{
    Protos::WorkerNodeResourceData pb_data;
    pb_data.ParseFromString(s);

    id = pb_data.id();
    host_ports = RPCHelpers::createHostWithPorts(pb_data.host_ports());
    vw_name = pb_data.vw_name();
    worker_group_id = pb_data.worker_group_id();

    cpu_usage = pb_data.cpu_usage();
    memory_usage = pb_data.memory_usage();
    memory_available = pb_data.memory_available();
    disk_space = pb_data.disk_space();
    query_num = pb_data.query_num();
    cpu_limit = pb_data.cpu_limit();
    memory_limit = pb_data.memory_limit();
    last_update_time = pb_data.last_update_time();


}

void WorkerNodeResourceData::fillProto(Protos::WorkerNodeResourceData & resource_info) const
{
    resource_info.set_id(id);
    RPCHelpers::fillHostWithPorts(host_ports, *resource_info.mutable_host_ports());

    resource_info.set_cpu_usage(cpu_usage);
    resource_info.set_memory_usage(memory_usage);
    resource_info.set_memory_available(memory_available);
    resource_info.set_disk_space(disk_space);
    resource_info.set_query_num(query_num);

    if (cpu_limit && memory_limit)
    {
        resource_info.set_cpu_limit(cpu_limit);
        resource_info.set_memory_limit(memory_limit);
    }

    if (!vw_name.empty())
        resource_info.set_vw_name(vw_name);

    if (!worker_group_id.empty())
        resource_info.set_worker_group_id(worker_group_id);

    if (last_update_time)
        resource_info.set_last_update_time(last_update_time);
}

WorkerNodeResourceData WorkerNodeResourceData::createFromProto(const Protos::WorkerNodeResourceData & resource_info)
{
    WorkerNodeResourceData res;
    res.id = resource_info.id();
    res.host_ports = RPCHelpers::createHostWithPorts(resource_info.host_ports());

    res.cpu_usage = resource_info.cpu_usage();
    res.memory_usage = resource_info.memory_usage();
    res.memory_available = resource_info.memory_available();
    res.disk_space = resource_info.disk_space();
    res.query_num = resource_info.query_num();

    if (resource_info.has_cpu_limit())
        res.cpu_limit = resource_info.cpu_limit();
    if (resource_info.has_memory_limit())
        res.memory_limit = resource_info.memory_limit();

    if (resource_info.has_vw_name())
        res.vw_name = resource_info.vw_name();

    if (resource_info.has_worker_group_id())
        res.worker_group_id = resource_info.worker_group_id();

    if (resource_info.has_last_update_time())
        res.last_update_time = resource_info.last_update_time();

    return res;
}

void ResourceRequirement::fillProto(Protos::ResourceRequirement & proto) const
{
    proto.set_limit_cpu(limit_cpu);
    proto.set_request_mem_bytes(request_mem_bytes);
    proto.set_request_disk_bytes(request_disk_bytes);
    proto.set_expected_workers(expected_workers);
    proto.set_worker_group(worker_group);
}

void ResourceRequirement::parseFromProto(const Protos::ResourceRequirement & proto)
{
    limit_cpu = proto.limit_cpu();
    request_mem_bytes = proto.request_mem_bytes();
    request_disk_bytes = proto.request_disk_bytes();
    expected_workers = proto.expected_workers();
    worker_group = proto.worker_group();
}

void WorkerGroupMetrics::reset()
{
    num_workers = 0;
    max_cpu_usage = 0;
    avg_cpu_usage = 0;
    min_cpu_usage = std::numeric_limits<double>::max();

    max_mem_usage = 0;
    avg_mem_usage = 0;
    min_mem_usage = std::numeric_limits<double>::max();
    min_mem_available = std::numeric_limits<uint64_t>::max();

    total_queries = 0;
}

void WorkerGroupMetrics::fillProto(Protos::WorkerGroupMetrics & proto) const
{
    proto.set_id(id);
    proto.set_num_workers(num_workers);

    proto.set_max_cpu_usage(max_cpu_usage);
    proto.set_avg_cpu_usage(avg_cpu_usage);
    proto.set_min_cpu_usage(min_cpu_usage);

    proto.set_max_mem_usage(max_mem_usage);
    proto.set_avg_mem_usage(avg_mem_usage);
    proto.set_min_mem_usage(min_mem_usage);
    proto.set_min_mem_available(min_mem_available);

    proto.set_total_queries(total_queries);
}

void WorkerGroupMetrics::parseFromProto(const Protos::WorkerGroupMetrics & proto)
{
    id = proto.id();
    num_workers = proto.num_workers();
    max_cpu_usage = proto.max_cpu_usage();
    min_cpu_usage = proto.min_cpu_usage();
    avg_cpu_usage = proto.avg_cpu_usage();

    max_mem_usage = proto.max_mem_usage();
    min_mem_usage = proto.min_mem_usage();
    avg_mem_usage = proto.avg_mem_usage();
    min_mem_available = proto.min_mem_available();

    total_queries = proto.total_queries();
}

std::string WorkerGroupData::serializeAsString() const
{
    Protos::WorkerGroupData pb_data;
    fillProto(pb_data, false, false);
    return pb_data.SerializeAsString();
}

void WorkerGroupData::parseFromString(const std::string & s)
{
    Protos::WorkerGroupData pb_data;
    pb_data.ParseFromString(s);
    parseFromProto(pb_data);
}

void WorkerGroupData::fillProto(Protos::WorkerGroupData & pb_data, const bool with_host_ports, const bool with_metrics) const
{
    pb_data.set_id(id);
    pb_data.set_type(uint32_t(type));
    RPCHelpers::fillUUID(vw_uuid, *pb_data.mutable_vw_uuid());
    if (!vw_name.empty())
        pb_data.set_vw_name(vw_name);
    if (!psm.empty())
        pb_data.set_psm(psm);
    if (!linked_id.empty())
        pb_data.set_linked_id(linked_id);

    if (with_host_ports)
    {
        for (auto & host_ports : host_ports_vec)
            RPCHelpers::fillHostWithPorts(host_ports, *pb_data.add_host_ports_vec());
    }

    pb_data.set_num_workers(num_workers);

    if (with_metrics)
        metrics.fillProto(*pb_data.mutable_metrics());
}

void WorkerGroupData::parseFromProto(const Protos::WorkerGroupData & pb_data)
{
    id = pb_data.id();
    type = WorkerGroupType(pb_data.type());
    vw_uuid = RPCHelpers::createUUID(pb_data.vw_uuid());
    if (pb_data.has_vw_name())
        vw_name = pb_data.vw_name();
    if (pb_data.has_psm())
        psm = pb_data.psm();
    if (pb_data.has_linked_id())
        linked_id = pb_data.linked_id();

    for (auto & host_ports : pb_data.host_ports_vec())
        host_ports_vec.push_back(RPCHelpers::createHostWithPorts(host_ports));

    if (pb_data.has_num_workers())
        num_workers = pb_data.num_workers();

    if (pb_data.has_metrics())
        metrics.parseFromProto(pb_data.metrics());
}

void QueryQueueInfo::fillProto(Protos::QueryQueueInfo & pb_data) const
{
    pb_data.set_queued_query_count(queued_query_count);
    pb_data.set_running_query_count(running_query_count);
}

void QueryQueueInfo::parseFromProto(const Protos::QueryQueueInfo & pb_data)
{
    queued_query_count = pb_data.queued_query_count();
    running_query_count = pb_data.running_query_count();
}

}
