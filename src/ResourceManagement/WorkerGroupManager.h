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
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/IWorkerGroup.h>
#include <Common/ConcurrentMapForCreating.h>

#include <bthread/mutex.h>
#include <common/logger_useful.h>

namespace DB::ResourceManagement
{
class ResourceManagerController;


class WorkerGroupManager : protected ConcurrentMapForCreating<std::string, IWorkerGroup>, private boost::noncopyable
{
public:
    WorkerGroupManager(ResourceManagerController & rm_controller_);

    void loadWorkerGroups();
    void clearWorkerGroups();

    WorkerGroupPtr tryGetWorkerGroup(const std::string & group_id, std::lock_guard<bthread::Mutex> * vw_lock = nullptr);
    WorkerGroupPtr getWorkerGroup(const std::string & group_id, std::lock_guard<bthread::Mutex> * vw_lock = nullptr);

    std::unordered_map<String, WorkerGroupPtr> getAllWorkerGroups();

private:
    WorkerGroupPtr createWorkerGroupObject(const WorkerGroupData & data, std::lock_guard<std::mutex> * lock = nullptr);

    ResourceManagerController & rm_controller;
    LoggerPtr log{nullptr};
    std::atomic_bool need_sync_with_catalog{false};
    /// Use bthread::Mutex but not std::mutex to avoid deadlock issue as we call other rpc API (catalog) in the lock scope.
    mutable bthread::Mutex wg_mgr_mutex;

    auto & getMutex() const
    {
        return wg_mgr_mutex;
    }

    auto getLock() const
    {
        return std::lock_guard<bthread::Mutex>(wg_mgr_mutex);
    }

    void loadWorkerGroupsImpl(std::lock_guard<bthread::Mutex> * wg_lock);
    void clearWorkerGroupsImpl(std::lock_guard<bthread::Mutex> * wg_lock);

    WorkerGroupPtr tryGetWorkerGroupImpl(const std::string & group_id, std::lock_guard<bthread::Mutex> * vw_lock, std::lock_guard<bthread::Mutex> * wg_lock);
    WorkerGroupPtr getWorkerGroupImpl(const std::string & group_id, std::lock_guard<bthread::Mutex> * vw_lock, std::lock_guard<bthread::Mutex> * wg_lock);

    std::unordered_map<String, WorkerGroupPtr> getAllWorkerGroupsImpl(std::lock_guard<bthread::Mutex> * wg_lock);

    // Creation and deletion of worker groups should be done via ResourceManagerController
    WorkerGroupPtr createWorkerGroupImpl(
        const std::string & group_id, const std::string & vw_name, WorkerGroupData data, std::lock_guard<bthread::Mutex> * vw_lock, std::lock_guard<bthread::Mutex> * wg_lock);
    void dropWorkerGroup(const std::string & group_id);
    void dropWorkerGroupImpl(const std::string & group_id, std::lock_guard<bthread::Mutex> * wg_lock);

    friend class ResourceManagerController;
};

}
