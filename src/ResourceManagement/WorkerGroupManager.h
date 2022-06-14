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

    WorkerGroupPtr createWorkerGroup(
        const std::string & group_id, bool if_not_exists, const std::string & vw_name, WorkerGroupData data);
    WorkerGroupPtr tryGetWorkerGroup(const std::string & group_id);
    WorkerGroupPtr getWorkerGroup(const std::string & group_id);
    void dropWorkerGroup(const std::string & group_id, bool if_exists);

    auto getAllWorkerGroups() { return getAll(); }

private:
    WorkerGroupPtr createWorkerGroupObject(const WorkerGroupData & data, std::lock_guard<std::mutex> * lock = nullptr);

    ResourceManagerController & rm_controller;
    Poco::Logger * log{nullptr};
    std::atomic_bool need_sync_with_catalog{false};
};

}
