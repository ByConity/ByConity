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
#include <unordered_map>
#include <Storages/System/StorageSystemWorkers.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/WorkerNode.h>
#include <Interpreters/WorkerStatusManager.h>
#include <ResourceManagement/CommonData.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_MANAGER_ERROR;
}

NamesAndTypesList StorageSystemWorkers::getNamesAndTypes()
{
    return {
        {"worker_id", std::make_shared<DataTypeString>()},
        {"host", std::make_shared<DataTypeString>()},
        {"tcp_port", std::make_shared<DataTypeUInt16>()},
        {"rpc_port", std::make_shared<DataTypeUInt16>()},
        {"http_port", std::make_shared<DataTypeUInt16>()},
        {"exchange_port", std::make_shared<DataTypeUInt16>()},
        {"exchange_status_port", std::make_shared<DataTypeUInt16>()},
        {"vw_name", std::make_shared<DataTypeString>()},
        {"worker_group_id", std::make_shared<DataTypeString>()},
        {"query_num", std::make_shared<DataTypeUInt32>()},
        {"cpu_usage", std::make_shared<DataTypeFloat64>()},
        {"reserved_cpu_cores", std::make_shared<DataTypeUInt32>()},
        {"memory_usage", std::make_shared<DataTypeFloat64>()},
        {"disk_space", std::make_shared<DataTypeUInt64>()},
        {"memory_available", std::make_shared<DataTypeUInt64>()},
        {"reserved_memory_bytes", std::make_shared<DataTypeUInt64>()},
        {"register_time", std::make_shared<DataTypeDateTime>()},
        {"last_update_time", std::make_shared<DataTypeDateTime>()},
        {"state", std::make_shared<DataTypeUInt8>()},
        {"resource_status", std::make_shared<DataTypeInt32>()},
        {"breaker_status", std::make_shared<DataTypeInt32>()},
        {"fail_count", std::make_shared<DataTypeInt32>()},
        {"is_checking", std::make_shared<DataTypeInt32>()},
    };
}

struct StatusResource
{
    const WorkerNodeResourceData * resource_data;
    std::optional<WorkerStatus> status;
};

void StorageSystemWorkers::fillData(MutableColumns & res_columns, const ContextPtr context, const SelectQueryInfo & /*query_info*/) const
{
    // TODO: support filter by vw_name.
    std::vector<WorkerNodeResourceData> res;

    try
    {
        auto client = context->getResourceManagerClient();
        if (client)
            client->getAllWorkers(res);
        else
            throw Exception("Resource Manager unavailable", ErrorCodes::RESOURCE_MANAGER_ERROR);
    }
    catch (const Exception & e)
    {
        throw Exception(
            "Unable to get worker data through Resource Manager at this moment due to: " + e.displayText(),
            ErrorCodes::RESOURCE_MANAGER_ERROR);
    }
    std::unordered_map<String, std::unordered_map<String, std::vector<StatusResource>>> data;
    for (auto & node : res)
    {
        data[node.vw_name][node.worker_group_id].push_back({.resource_data = &node, .status = std::nullopt});
    }

    auto & pool = context->getVirtualWarehousePool();
    for (auto & [vw_name, vw_info] : data)
    {
        auto vw_handle = pool.get(vw_name);
        if (vw_handle)
        {
            for (auto & [wg_name, wg_info] : vw_info)
            {
                auto worker_status_manager = vw_handle->getWorkerStatusManager(wg_name);
                if (worker_status_manager)
                {
                    for (auto & status_resource : wg_info)
                    {
                        status_resource.status
                            = worker_status_manager->getWorkerStatus(WorkerId(vw_name, wg_name, status_resource.resource_data->id));
                    }
                }
            }
        }
    }
    for (auto & [vw_name, vw_info] : data)
    {
        for (auto & [wg_name, wg_info] : vw_info)
        {
            for (auto & resource_status : wg_info)
            {
                auto & node = *resource_status.resource_data;
                size_t i = 0;
                res_columns[i++]->insert(node.id);
                res_columns[i++]->insert(node.host_ports.getHost());
                res_columns[i++]->insert(node.host_ports.tcp_port);
                res_columns[i++]->insert(node.host_ports.rpc_port);
                res_columns[i++]->insert(node.host_ports.http_port);
                res_columns[i++]->insert(node.host_ports.exchange_port);
                res_columns[i++]->insert(node.host_ports.exchange_status_port);
                res_columns[i++]->insert(node.vw_name);
                res_columns[i++]->insert(node.worker_group_id);
                res_columns[i++]->insert(node.query_num);
                res_columns[i++]->insert(node.cpu_usage);
                res_columns[i++]->insert(node.reserved_cpu_cores);
                res_columns[i++]->insert(node.memory_usage);
                res_columns[i++]->insert(node.disk_space);
                res_columns[i++]->insert(node.memory_available);
                res_columns[i++]->insert(node.reserved_memory_bytes);
                res_columns[i++]->insert(node.register_time);
                res_columns[i++]->insert(node.last_update_time);
                res_columns[i++]->insert(static_cast<UInt8>(node.state));
                if (resource_status.status)
                {
                    res_columns[i++]->insert(static_cast<Int32>(resource_status.status->resource_status.scheduler_status));
                    res_columns[i++]->insert(static_cast<Int32>(resource_status.status->circuit_break.breaker_status));
                    res_columns[i++]->insert(static_cast<Int32>(resource_status.status->circuit_break.fail_count));
                    res_columns[i++]->insert(static_cast<Int32>(resource_status.status->circuit_break.is_checking));
                }
                else
                {
                    res_columns[i++]->insert(static_cast<Int32>(-1));
                    res_columns[i++]->insert(static_cast<Int32>(-1));
                    res_columns[i++]->insert(static_cast<Int32>(-1));
                    res_columns[i++]->insert(static_cast<Int32>(-1));
                }
            }
        }
    }
}

}
