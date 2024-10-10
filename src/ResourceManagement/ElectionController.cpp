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

#include <ResourceManagement/ElectionController.h>
#include <Catalog/Catalog.h>
#include <Common/Configurations.h>
#include <Common/HostWithPorts.h>
#include <common/getFQDNOrHostName.h>
#include <ResourceManagement/ResourceTracker.h>
#include <ResourceManagement/VirtualWarehouseManager.h>
#include <ResourceManagement/WorkerGroupManager.h>

#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Storages/PartCacheManager.h>

namespace DB::ErrorCodes
{
extern const int RESOURCE_MANAGER_INIT_ERROR;
}

namespace DB::ResourceManagement
{

ElectionController::ElectionController(ResourceManagerController & rm_controller_)
    : WithContext(rm_controller_.getContext())
    , rm_controller(rm_controller_)
{
    const auto & config = getContext()->getRootConfig();
    auto prefix = config.service_discovery_kv.election_prefix.value;
    auto election_path = prefix + config.service_discovery_kv.resource_manager_host_path.value;
    auto refresh_interval_ms = config.service_discovery_kv.resource_manager_refresh_interval_ms.value;
    auto expired_interval_ms =  config.service_discovery_kv.resource_manager_expired_interval_ms.value;
    auto host = getContext()->getHostWithPorts();

    auto metastore_ptr = getContext()->getCnchCatalog()->getMetastore();
    elector = std::make_shared<StorageElector>(
        std::make_unique<ResourceManagerKvStorage>(metastore_ptr),
        refresh_interval_ms,
        expired_interval_ms,
        host,
        election_path,
        [&](const HostWithPorts *) { return onLeader(); },
        [&](const HostWithPorts *) { return onFollower(); }
    );
}

ElectionController::~ElectionController()
{
    try
    {
        LOG_DEBUG(log, "Exit leader election");
        elector.reset();

        shutDown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool ElectionController::isLeader() const
{
    return elector->isLeader();
}

/// Callback by StorageElector. Need to gurantee no exception thrown in this method.
bool ElectionController::onLeader()
{
    auto port = getContext()->getRootConfig().resource_manager.port.value;
    const std::string & rm_host = getHostIPFromEnv();
    auto current_address = createHostPortString(rm_host, port);
    LOG_INFO(log, "Starting leader callback for {}", current_address);

    /// Make sure get all needed metadata from KV store before becoming leader.
    if (!pullState())
    {
        LOG_WARNING(log, "Failed to pull state from KV store.");
        return false;
    }

    LOG_INFO(log, "Current RM node {} has become leader.", current_address);
    return true;
}

bool ElectionController::onFollower()
{
    shutDown();
    return true;
}

bool ElectionController::pullState()
{
    auto retry_count = 3;
    auto success = false;
    do
    {
        try
        {
            // Clear outdated data
            auto & vw_manager = rm_controller.getVirtualWarehouseManager();
            vw_manager.clearVirtualWarehouses();
            auto & group_manager = rm_controller.getWorkerGroupManager();
            group_manager.clearWorkerGroups();
            auto & resource_tracker = rm_controller.getResourceTracker();
            resource_tracker.clearWorkers();
            rm_controller.initialize();
            success = true;
        }
        catch (...)
        {
            tryLogCurrentException(log);
            --retry_count;
        }
    } while (!success && retry_count > 0);

    return success;
}

void ElectionController::shutDown()
{
}

}
