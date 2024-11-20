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

#include <optional>
#include <ResourceManagement/CommonData.h>

#include <Interpreters/Context.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/RPCHelpers.h>
#include <Protos/data_models.pb.h>

namespace DB::ResourceManagement
{

void QueueRule::fillProto(Protos::QueueRule & queue_rule) const
{
    queue_rule.set_rule_name(rule_name);
    for (const auto & database : databases)
        queue_rule.add_databases(database);
    for (const auto & table : tables)
        queue_rule.add_tables(table);
    queue_rule.set_query_id(query_id);
    queue_rule.set_user(user);
    queue_rule.set_ip(ip);
    queue_rule.set_fingerprint(fingerprint);
}

void QueueRule::parseFromProto(const Protos::QueueRule & queue_rule)
{
    rule_name = queue_rule.rule_name();
    for (const auto & database : queue_rule.databases())
        databases.push_back(database);
    for (const auto & table : queue_rule.tables())
        tables.push_back(table);
    query_id = queue_rule.query_id();
    user = queue_rule.user();
    ip = queue_rule.ip();
    fingerprint = queue_rule.fingerprint();
}

void QueueData::fillProto(Protos::QueueData & queue_data) const
{
    queue_data.set_queue_name(queue_name);
    queue_data.set_max_concurrency(max_concurrency);
    queue_data.set_query_queue_size(query_queue_size);
    for (const auto & queue_rule : queue_rules)
    {
        auto queue_rule_iter = queue_data.add_queue_rules();
        queue_rule.fillProto(*queue_rule_iter);
    }
}

void QueueData::parseFromProto(const Protos::QueueData & queue_data)
{
    queue_name = queue_data.queue_name();
    max_concurrency = queue_data.max_concurrency();
    query_queue_size = queue_data.query_queue_size();
    queue_rules.reserve(queue_data.queue_rules().size());
    for (const auto & queue_rule : queue_data.queue_rules())
    {
        queue_rules.resize(queue_rules.size() + 1);
        queue_rules.back().parseFromProto(queue_rule);
    }
}

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
    pb_settings.set_vw_schedule_algo(static_cast<int>(vw_schedule_algo));
    if (max_auto_borrow_links)
        pb_settings.set_max_auto_borrow_links(max_auto_borrow_links);
    if (max_auto_lend_links)
        pb_settings.set_max_auto_lend_links(max_auto_lend_links);
    if (cpu_busy_threshold)
        pb_settings.set_cpu_busy_threshold(cpu_busy_threshold);
    if (mem_busy_threshold)
        pb_settings.set_mem_busy_threshold(mem_busy_threshold);
    if (cpu_idle_threshold)
        pb_settings.set_cpu_idle_threshold(cpu_idle_threshold);
    if (mem_idle_threshold)
        pb_settings.set_mem_idle_threshold(mem_idle_threshold);
    if (cpu_threshold_for_recall)
        pb_settings.set_cpu_threshold_for_recall(cpu_threshold_for_recall);
    if (mem_threshold_for_recall)
        pb_settings.set_mem_threshold_for_recall(mem_threshold_for_recall);
    if (cooldown_seconds_after_scaleup)
        pb_settings.set_cooldown_seconds_after_scaleup(cooldown_seconds_after_scaleup);
    if (cooldown_seconds_after_scaledown)
        pb_settings.set_cooldown_seconds_after_scaledown(cooldown_seconds_after_scaledown);

    for (const auto & queue_data : queue_datas)
    {
        queue_data.fillProto(*pb_settings.add_queue_datas());
    }

    pb_settings.set_unhealth_worker_recheck_wait_seconds(unhealth_worker_recheck_wait_seconds);
    pb_settings.set_circuit_breaker_open_error_threshold(circuit_breaker_open_error_threshold);
    pb_settings.set_recommended_concurrent_query_limit(recommended_concurrent_query_limit);
    pb_settings.set_health_worker_cpu_usage_threshold(health_worker_cpu_usage_threshold);
    pb_settings.set_circuit_breaker_open_to_halfopen_wait_seconds(circuit_breaker_open_to_halfopen_wait_seconds);
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
    max_auto_borrow_links = pb_settings.max_auto_borrow_links();
    max_auto_lend_links = pb_settings.max_auto_lend_links();
    cpu_busy_threshold = pb_settings.cpu_busy_threshold();
    mem_busy_threshold = pb_settings.mem_busy_threshold();
    cpu_idle_threshold = pb_settings.cpu_idle_threshold();
    mem_idle_threshold = pb_settings.mem_idle_threshold();
    cpu_threshold_for_recall = pb_settings.cpu_threshold_for_recall();
    mem_threshold_for_recall = pb_settings.mem_threshold_for_recall();
    cooldown_seconds_after_scaleup = pb_settings.cooldown_seconds_after_scaleup();
    cooldown_seconds_after_scaledown = pb_settings.cooldown_seconds_after_scaledown();
    queue_datas.reserve(pb_settings.queue_datas_size());
    for (const auto & pb_queue_data : pb_settings.queue_datas())
    {
        queue_datas.resize(queue_datas.size() + 1);
        queue_datas.back().parseFromProto(pb_queue_data);
    }
    if (pb_settings.has_unhealth_worker_recheck_wait_seconds())
        unhealth_worker_recheck_wait_seconds = pb_settings.unhealth_worker_recheck_wait_seconds();
    if (pb_settings.has_recommended_concurrent_query_limit())
        recommended_concurrent_query_limit = pb_settings.recommended_concurrent_query_limit();
    if (pb_settings.has_health_worker_cpu_usage_threshold())
        health_worker_cpu_usage_threshold = pb_settings.health_worker_cpu_usage_threshold();
    if (pb_settings.has_circuit_breaker_open_to_halfopen_wait_seconds())
        circuit_breaker_open_to_halfopen_wait_seconds = pb_settings.circuit_breaker_open_to_halfopen_wait_seconds();
    if (pb_settings.has_circuit_breaker_open_error_threshold())
        circuit_breaker_open_error_threshold = pb_settings.circuit_breaker_open_error_threshold();
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
    if (max_auto_borrow_links)
        pb_settings.set_max_auto_borrow_links(*max_auto_borrow_links);
    if (max_auto_lend_links)
        pb_settings.set_max_auto_lend_links(*max_auto_lend_links);
    if (cpu_busy_threshold)
        pb_settings.set_cpu_busy_threshold(*cpu_busy_threshold);
    if (mem_busy_threshold)
        pb_settings.set_mem_busy_threshold(*mem_busy_threshold);
    if (cpu_idle_threshold)
        pb_settings.set_cpu_idle_threshold(*cpu_idle_threshold);
    if (mem_idle_threshold)
        pb_settings.set_mem_idle_threshold(*mem_idle_threshold);
    if (cpu_threshold_for_recall)
        pb_settings.set_cpu_threshold_for_recall(*cpu_threshold_for_recall);
    if (mem_threshold_for_recall)
        pb_settings.set_mem_threshold_for_recall(*mem_threshold_for_recall);
    if (cooldown_seconds_after_scaleup)
        pb_settings.set_cooldown_seconds_after_scaleup(*cooldown_seconds_after_scaleup);
    if (cooldown_seconds_after_scaledown)
        pb_settings.set_cooldown_seconds_after_scaledown(*cooldown_seconds_after_scaledown);
    if (max_concurrency)
        pb_settings.set_max_concurrency(*max_concurrency);
    if (query_queue_size)
        pb_settings.set_query_queue_size(*query_queue_size);
    if (query_id)
        pb_settings.set_query_id(*query_id);
    if (user)
        pb_settings.set_user(*user);
    if (ip)
        pb_settings.set_ip(*ip);
    if (rule_name)
        pb_settings.set_rule_name(*rule_name);
    if (fingerprint)
        pb_settings.set_fingerprint(*fingerprint);
    if (queue_name)
        pb_settings.set_queue_name(*queue_name);
    if (has_table)
    {
        pb_settings.set_has_table(true);
        for (const auto & table : tables)
            pb_settings.add_tables(table);
    }
    if (has_database)
    {
        pb_settings.set_has_database(true);
        for (const auto & db : databases)
            pb_settings.add_databases(db);
    }

    pb_settings.set_queue_alter_type(queue_alter_type);
    if (queue_data)
        queue_data->fillProto(*pb_settings.mutable_queue_data());
    if (unhealth_worker_recheck_wait_seconds)
        pb_settings.set_unhealth_worker_recheck_wait_seconds(*unhealth_worker_recheck_wait_seconds);
    if (recommended_concurrent_query_limit)
        pb_settings.set_recommended_concurrent_query_limit(*recommended_concurrent_query_limit);
    if (health_worker_cpu_usage_threshold)
        pb_settings.set_health_worker_cpu_usage_threshold(*health_worker_cpu_usage_threshold);
    if (circuit_breaker_open_to_halfopen_wait_seconds)
        pb_settings.set_circuit_breaker_open_to_halfopen_wait_seconds(*circuit_breaker_open_to_halfopen_wait_seconds);
    if (circuit_breaker_open_error_threshold)
        pb_settings.set_circuit_breaker_open_error_threshold(*circuit_breaker_open_error_threshold);
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
    if (pb_settings.has_max_auto_borrow_links())
        max_auto_borrow_links = pb_settings.max_auto_borrow_links();
    if (pb_settings.has_max_auto_lend_links())
        max_auto_lend_links = pb_settings.max_auto_lend_links();
    if (pb_settings.has_cpu_busy_threshold())
        cpu_busy_threshold = pb_settings.cpu_busy_threshold();
    if (pb_settings.has_mem_busy_threshold())
        mem_busy_threshold = pb_settings.mem_busy_threshold();
    if (pb_settings.has_cpu_idle_threshold())
        cpu_idle_threshold = pb_settings.cpu_idle_threshold();
    if (pb_settings.has_mem_idle_threshold())
        mem_idle_threshold = pb_settings.mem_idle_threshold();
    if (pb_settings.has_cpu_threshold_for_recall())
        cpu_threshold_for_recall = pb_settings.cpu_threshold_for_recall();
    if (pb_settings.has_mem_threshold_for_recall())
        mem_threshold_for_recall = pb_settings.mem_threshold_for_recall();
    if (pb_settings.has_cooldown_seconds_after_scaleup())
        cooldown_seconds_after_scaleup = pb_settings.cooldown_seconds_after_scaleup();
    if (pb_settings.has_cooldown_seconds_after_scaledown())
        cooldown_seconds_after_scaledown = pb_settings.cooldown_seconds_after_scaledown();
    if (pb_settings.has_max_concurrency())
        max_concurrency = pb_settings.max_concurrency();
    if (pb_settings.has_query_queue_size())
        query_queue_size = pb_settings.query_queue_size();
    if (pb_settings.has_query_id())
        query_id = pb_settings.query_id();
    if (pb_settings.has_user())
        user = pb_settings.user();
    if (pb_settings.has_ip())
        ip = pb_settings.ip();
    if (pb_settings.has_rule_name())
        rule_name = pb_settings.rule_name();
    if (pb_settings.has_fingerprint())
        fingerprint = pb_settings.fingerprint();
    if (pb_settings.has_table())
    {
        has_table = true;
        for (const auto & table : pb_settings.tables())
            tables.push_back(table);
    }
    if (pb_settings.has_database())
    {
        has_database = true;
        for (const auto & db : pb_settings.databases())
            databases.push_back(db);
    }
    if (pb_settings.has_queue_name())
        queue_name = pb_settings.queue_name();

    queue_alter_type = pb_settings.queue_alter_type();
    if (pb_settings.has_queue_data())
    {
        queue_data = std::make_optional<QueueData>();
        queue_data->parseFromProto(pb_settings.queue_data());
    }
    if (pb_settings.has_recommended_concurrent_query_limit())
        recommended_concurrent_query_limit = pb_settings.recommended_concurrent_query_limit();
    if (pb_settings.has_unhealth_worker_recheck_wait_seconds())
        unhealth_worker_recheck_wait_seconds = pb_settings.unhealth_worker_recheck_wait_seconds();
    if (pb_settings.has_health_worker_cpu_usage_threshold())
        health_worker_cpu_usage_threshold = pb_settings.health_worker_cpu_usage_threshold();
    if (pb_settings.has_circuit_breaker_open_to_halfopen_wait_seconds())
        circuit_breaker_open_to_halfopen_wait_seconds = pb_settings.circuit_breaker_open_to_halfopen_wait_seconds();
    if (pb_settings.has_circuit_breaker_open_error_threshold())
        circuit_breaker_open_error_threshold = pb_settings.circuit_breaker_open_error_threshold();
}

void VirtualWarehouseData::fillProto(Protos::VirtualWarehouseData & pb_data) const
{
    pb_data.set_name(name);
    RPCHelpers::fillUUID(uuid, *pb_data.mutable_uuid());
    settings.fillProto(*pb_data.mutable_settings());
    pb_data.set_num_worker_groups(num_worker_groups);
    pb_data.set_num_workers(num_workers);
    pb_data.set_num_borrowed_worker_groups(num_borrowed_worker_groups);
    pb_data.set_num_lent_worker_groups(num_lent_worker_groups);
    pb_data.set_last_borrow_timestamp(last_borrow_timestamp);
    pb_data.set_last_lend_timestamp(last_lend_timestamp);
}

void VirtualWarehouseData::parseFromProto(const Protos::VirtualWarehouseData & pb_data)
{
    name = pb_data.name();
    uuid = RPCHelpers::createUUID(pb_data.uuid());
    settings.parseFromProto(pb_data.settings());
    num_worker_groups = pb_data.num_worker_groups();
    num_workers = pb_data.num_workers();
    num_borrowed_worker_groups = pb_data.num_borrowed_worker_groups();
    num_lent_worker_groups = pb_data.num_lent_worker_groups();
    last_borrow_timestamp = pb_data.last_borrow_timestamp();
    last_lend_timestamp = pb_data.last_lend_timestamp();
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
    cpu_usage_1min = pb_data.cpu_usage_1min();
    memory_usage = pb_data.memory_usage();
    memory_usage_1min = pb_data.memory_usage_1min();
    memory_available = pb_data.memory_available();
    disk_space = pb_data.disk_space();
    query_num = pb_data.query_num();
    manipulation_num = pb_data.manipulation_num();
    consumer_num = pb_data.consumer_num();
    cpu_limit = pb_data.cpu_limit();
    memory_limit = pb_data.memory_limit();
    last_update_time = pb_data.last_update_time();

    reserved_memory_bytes = pb_data.reserved_memory_bytes();
    reserved_cpu_cores = pb_data.reserved_cpu_cores();
    register_time = pb_data.register_time();
    state = WorkerState(pb_data.state());
    last_status_create_time = pb_data.last_status_create_time();
    cpu_usage_10sec = pb_data.cpu_usage_10sec();
}

void WorkerNodeResourceData::fillProto(Protos::WorkerNodeResourceData & resource_info) const
{
    resource_info.set_id(id);
    RPCHelpers::fillHostWithPorts(host_ports, *resource_info.mutable_host_ports());

    resource_info.set_cpu_usage(cpu_usage);
    resource_info.set_cpu_usage_1min(cpu_usage_1min);
    resource_info.set_memory_usage(memory_usage);
    resource_info.set_memory_usage_1min(memory_usage_1min);
    resource_info.set_memory_available(memory_available);
    resource_info.set_disk_space(disk_space);
    resource_info.set_query_num(query_num);
    resource_info.set_manipulation_num(manipulation_num);
    resource_info.set_consumer_num(consumer_num);

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

    if (reserved_memory_bytes)
        resource_info.set_reserved_memory_bytes(reserved_memory_bytes);
    if (reserved_cpu_cores)
        resource_info.set_reserved_cpu_cores(reserved_cpu_cores);
    if (register_time)
        resource_info.set_register_time(register_time);
    resource_info.set_state(static_cast<uint32_t>(state));
    if (last_status_create_time)
        resource_info.set_last_status_create_time(last_status_create_time);
    resource_info.set_cpu_usage_10sec(cpu_usage_10sec);
}

WorkerNodeResourceData::WorkerNodeResourceData(const Protos::WorkerNodeResourceData & resource_info)
{
    id = resource_info.id();
    host_ports = RPCHelpers::createHostWithPorts(resource_info.host_ports());

    cpu_usage = resource_info.cpu_usage();
    cpu_usage_1min = resource_info.cpu_usage_1min();
    cpu_usage_10sec = resource_info.cpu_usage_10sec();
    memory_usage = resource_info.memory_usage();
    memory_usage_1min = resource_info.memory_usage_1min();
    memory_available = resource_info.memory_available();
    disk_space = resource_info.disk_space();
    query_num = resource_info.query_num();

    if (resource_info.has_cpu_limit())
        cpu_limit = resource_info.cpu_limit();
    if (resource_info.has_memory_limit())
        memory_limit = resource_info.memory_limit();

    if (resource_info.has_vw_name())
        vw_name = resource_info.vw_name();

    if (resource_info.has_worker_group_id())
        worker_group_id = resource_info.worker_group_id();

    if (resource_info.has_last_update_time())
        last_update_time = resource_info.last_update_time();

    if (resource_info.has_register_time())
        register_time = resource_info.register_time();

    if (resource_info.has_last_status_create_time())
        last_status_create_time = resource_info.last_status_create_time();
    if (resource_info.has_state())
        state = WorkerState(resource_info.state());
}

WorkerNodeResourceData WorkerNodeResourceData::createFromProto(const Protos::WorkerNodeResourceData & resource_info)
{
    WorkerNodeResourceData res;
    res.id = resource_info.id();
    res.host_ports = RPCHelpers::createHostWithPorts(resource_info.host_ports());

    res.cpu_usage = resource_info.cpu_usage();
    res.cpu_usage_1min = resource_info.cpu_usage_1min();
    res.cpu_usage_10sec = resource_info.cpu_usage_10sec();
    res.memory_usage = resource_info.memory_usage();
    res.memory_usage_1min = resource_info.memory_usage_1min();
    res.memory_available = resource_info.memory_available();
    res.disk_space = resource_info.disk_space();
    res.query_num = resource_info.query_num();
    res.manipulation_num = resource_info.manipulation_num();
    res.consumer_num = resource_info.consumer_num();

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

    if (resource_info.has_reserved_memory_bytes())
        res.reserved_memory_bytes = resource_info.reserved_memory_bytes();
    if (resource_info.has_reserved_cpu_cores())
        res.reserved_cpu_cores = resource_info.reserved_cpu_cores();
    if (resource_info.has_register_time())
        res.register_time = resource_info.register_time();
    res.state = WorkerState(resource_info.state());

    return res;
}

void ResourceRequirement::fillProto(Protos::ResourceRequirement & proto) const
{
    proto.set_request_cpu_cores(request_cpu_cores);
    proto.set_cpu_usage_max_threshold(cpu_usage_max_threshold);
    proto.set_request_mem_bytes(request_mem_bytes);
    proto.set_request_disk_bytes(request_disk_bytes);
    proto.set_expected_workers(expected_workers);
    proto.set_worker_group(worker_group);
    proto.set_task_cold_startup_sec(task_cold_startup_sec);

    for (const auto & id : blocklist)
        proto.add_blocklist(id);
    proto.set_forbid_random_result(forbid_random_result);
    proto.set_no_repeat(no_repeat);
}

void ResourceRequirement::parseFromProto(const Protos::ResourceRequirement & proto)
{
    request_cpu_cores = proto.request_cpu_cores();
    cpu_usage_max_threshold = proto.cpu_usage_max_threshold();
    request_mem_bytes = proto.request_mem_bytes();
    request_disk_bytes = proto.request_disk_bytes();
    expected_workers = proto.expected_workers();
    worker_group = proto.worker_group();
    task_cold_startup_sec = proto.task_cold_startup_sec();

    for (const auto & id : proto.blocklist())
        blocklist.insert(std::move(id));
    forbid_random_result = proto.forbid_random_result();
    no_repeat = proto.no_repeat();
}

void WorkerMetrics::fillProto(Protos::WorkerMetrics & proto) const
{
    proto.set_id(id);

    proto.set_cpu_1min(cpu_1min);
    proto.set_mem_1min(mem_1min);
    proto.set_num_queries(num_queries);
    proto.set_num_manipulations(num_manipulations);
    proto.set_num_consumers(num_consumers);
    proto.set_num_dedups(num_dedups);
}

void WorkerMetrics::parseFromProto(const Protos::WorkerMetrics & proto)
{
    id = proto.id();

    cpu_1min = proto.cpu_1min();
    mem_1min = proto.mem_1min();
    num_queries = proto.num_queries();
    num_manipulations = proto.num_manipulations();
    num_consumers = proto.num_consumers();
    num_dedups = proto.num_dedups();
}


void WorkerGroupMetrics::reset()
{
    worker_metrics_vec.clear();
}

void WorkerGroupMetrics::fillProto(Protos::WorkerGroupMetrics & proto) const
{
    proto.set_id(id);
    for (const auto & worker_metrics : worker_metrics_vec)
        worker_metrics.fillProto(*proto.add_worker_metrics_vec());
}

void WorkerGroupMetrics::parseFromProto(const Protos::WorkerGroupMetrics & proto)
{
    id = proto.id();
    for (const auto & pb_worker_metrics : proto.worker_metrics_vec())
        worker_metrics_vec.push_back(WorkerMetrics::createFromProto(pb_worker_metrics));
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

    pb_data.set_is_auto_linked(is_auto_linked);
    if (!linked_vw_name.empty())
        pb_data.set_linked_vw_name(linked_vw_name);
    for (const auto & worker_source : worker_node_resource_vec)
        worker_source.fillProto(*pb_data.add_worker_node_resource_vec());
    pb_data.set_priority(priority);
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

    if (pb_data.has_is_auto_linked())
        is_auto_linked = pb_data.is_auto_linked();

    if (pb_data.has_linked_vw_name())
        linked_vw_name = pb_data.linked_vw_name();
    for (auto & worker_source_pb : pb_data.worker_node_resource_vec())
        worker_node_resource_vec.emplace_back(worker_source_pb);
    if (pb_data.has_priority())
        priority = pb_data.priority();
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
