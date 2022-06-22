#pragma once

#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/IWorkerGroup.h>
#include <Common/ConcurrentMapForCreating.h>

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

    WorkerGroupPtr tryGetWorkerGroup(const std::string & group_id, std::lock_guard<std::mutex> * vw_lock = nullptr);
    WorkerGroupPtr getWorkerGroup(const std::string & group_id, std::lock_guard<std::mutex> * vw_lock = nullptr);

    std::unordered_map<String, WorkerGroupPtr> getAllWorkerGroups();

private:
    WorkerGroupPtr createWorkerGroupObject(const WorkerGroupData & data, std::lock_guard<std::mutex> * lock = nullptr);

    ResourceManagerController & rm_controller;
    Poco::Logger * log{nullptr};
    std::atomic_bool need_sync_with_catalog{false};
    mutable std::mutex wg_mgr_mutex;

    auto & getMutex() const
    {
        return wg_mgr_mutex;
    }

    auto getLock() const
    {
        return std::lock_guard<std::mutex>(wg_mgr_mutex);
    }

    void loadWorkerGroupsImpl(std::lock_guard<std::mutex> * wg_lock);
    void clearWorkerGroupsImpl(std::lock_guard<std::mutex> * wg_lock);

    WorkerGroupPtr tryGetWorkerGroupImpl(const std::string & group_id, std::lock_guard<std::mutex> * vw_lock, std::lock_guard<std::mutex> * wg_lock);
    WorkerGroupPtr getWorkerGroupImpl(const std::string & group_id, std::lock_guard<std::mutex> * vw_lock, std::lock_guard<std::mutex> * wg_lock);

    std::unordered_map<String, WorkerGroupPtr> getAllWorkerGroupsImpl(std::lock_guard<std::mutex> * wg_lock);

    // Creation and deletion of worker groups should be done via ResourceManagerController
    WorkerGroupPtr createWorkerGroup(
        const std::string & group_id, bool if_not_exists, const std::string & vw_name, WorkerGroupData data, std::lock_guard<std::mutex> * vw_lock = nullptr);
    WorkerGroupPtr createWorkerGroupImpl(
        const std::string & group_id, bool if_not_exists, const std::string & vw_name, WorkerGroupData data, std::lock_guard<std::mutex> * vw_lock, std::lock_guard<std::mutex> * wg_lock);
    void dropWorkerGroup(const std::string & group_id);
    void dropWorkerGroupImpl(const std::string & group_id, std::lock_guard<std::mutex> * wg_lock);

    friend class ResourceManagerController;
    friend class WorkerGroupResourceCoordinator;
};

}
