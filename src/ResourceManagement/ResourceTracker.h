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

#include <Common/Logger.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <ResourceManagement/WorkerNode.h>

#include <boost/noncopyable.hpp>
#include <bthread/mutex.h>
#include <common/logger_useful.h>

#include <unordered_map>

namespace DB::ResourceManagement
{
struct WorkerNodeCatalogData;
struct WorkerNodeResourceData;
class ResourceManagerController;

class ResourceTracker : public boost::noncopyable
{
public:
    explicit ResourceTracker(ResourceManagerController & rm_controller_);
    ~ResourceTracker();

    std::vector<WorkerNodePtr> loadWorkerNode(const String & vw_name, const std::vector<WorkerNodeCatalogData> & data);

    std::pair<bool, WorkerNodePtr> registerNode(const WorkerNodeResourceData & data);
    bool updateNode(const WorkerNodeResourceData & data);
    void removeNode(const String & worker_id);
    void clearWorkers();

    std::unordered_map<std::string, WorkerNodePtr> getAllWorkers();

private:
    ContextPtr getContext() const;
    std::pair<bool, WorkerNodePtr> registerNodeImpl(const WorkerNodeResourceData & data, std::lock_guard<bthread::Mutex> &);

    void clearLostWorkers();

    ResourceManagerController & rm_controller;
    LoggerPtr log;
    /// Use bthread::Mutex but not std::mutex to avoid deadlock issue as this lock may lock other rpc API (catalog) in the lock scope.
    bthread::Mutex node_mutex;
    std::unordered_map<std::string, WorkerNodePtr> worker_nodes;
    BackgroundSchedulePool::TaskHolder background_task;
    size_t register_granularity_sec;
};

using ResourceTrackerPtr = std::unique_ptr<ResourceTracker>;

}
