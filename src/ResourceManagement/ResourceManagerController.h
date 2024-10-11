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

#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/ResourceScheduler.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/HostWithPorts.h>
#include <Common/Logger.h>

#include <memory>

#include <boost/noncopyable.hpp>
#include <bthread/mutex.h>
#include <common/logger_useful.h>

namespace DB
{
class Context;
namespace Catalog
{
    class Catalog;
    using CatalogPtr = std::shared_ptr<Catalog>;
}
}

namespace DB::ResourceManagement
{
class VirtualWarehouseManager;
class WorkerGroupManager;
class ResourceTracker;
class ElectionController;
class IWorkerGroup;
using WorkerGroupPtr = std::shared_ptr<IWorkerGroup>;
struct ResourceCoordinateDecision;
using CoordinateDecisions = std::vector<ResourceCoordinateDecision>;

class ResourceManagerController : public WithContext, private boost::noncopyable
{
public:
    explicit ResourceManagerController(ContextPtr global_context_);
    ~ResourceManagerController();

    Catalog::CatalogPtr getCnchCatalog();
    void createVWsFromConfig();
    void createWorkerGroupsFromConfig(const String & prefix, const String & vw_name);

    void initialize();

    auto & getResourceScheduler()
    {
        return *resource_scheduler;
    }
    auto & getResourceTracker() { return *resource_tracker; }
    auto & getVirtualWarehouseManager() { return *vw_manager; }
    auto & getWorkerGroupManager() { return *group_manager; }
    auto & getElectionController() { return *election_controller; }

    void registerWorkerNode(const WorkerNodeResourceData & data); // RPC
    void removeWorkerNode(const std::string & worker_id, const std::string & vw_name, const std::string & group_id);

    WorkerGroupPtr createWorkerGroup(
        const std::string & group_id,
        const std::string & vw_name,
        const WorkerGroupData & data,
        std::lock_guard<bthread::Mutex> * vw_lock = nullptr,
        std::lock_guard<bthread::Mutex> * wg_lock = nullptr);

    void dropWorkerGroup(
        const std::string & group_id,
        bool if_exists,
        std::lock_guard<bthread::Mutex> * vw_lock = nullptr,
        std::lock_guard<bthread::Mutex> * wg_lock = nullptr);

private:
    LoggerPtr log{nullptr};

    std::unique_ptr<ResourceScheduler> resource_scheduler;
    std::unique_ptr<ResourceTracker> resource_tracker;
    std::unique_ptr<VirtualWarehouseManager> vw_manager;
    std::unique_ptr<WorkerGroupManager> group_manager;
    std::unique_ptr<ElectionController> election_controller;
};

}
