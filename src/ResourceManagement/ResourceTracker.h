#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <ResourceManagement/WorkerNode.h>

#include <boost/noncopyable.hpp>
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
    ResourceTracker(ResourceManagerController & rm_controller_);
    ~ResourceTracker();

    std::vector<WorkerNodePtr> loadWorkerNode(const String & vw_name, const std::vector<WorkerNodeCatalogData> & data);

    std::pair<bool, WorkerNodePtr> registerNode(const WorkerNodeResourceData & data);
    bool updateNode(const WorkerNodeResourceData & data);
    void removeNode(const String & worker_id);

    std::unordered_map<std::string, WorkerNodePtr> getAllWorkers();

private:
    ContextPtr getContext() const;
    std::pair<bool, WorkerNodePtr> registerNodeImpl(const WorkerNodeResourceData & data, std::lock_guard<std::mutex> &);

    void clearLostWorkers();

    ResourceManagerController & rm_controller;
    Poco::Logger * log;
    std::mutex node_mutex;
    std::unordered_map<std::string, WorkerNodePtr> worker_nodes;
    BackgroundSchedulePool::TaskHolder background_task;
    size_t register_granularity_sec;
};

using ResourceTrackerPtr = std::unique_ptr<ResourceTracker>;

}
