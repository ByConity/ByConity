#pragma once

#include <Common/HostWithPorts.h>
#include <Common/Config/ConfigProcessor.h>
#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/CommonData.h>

#include <memory>

#include <boost/noncopyable.hpp>
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
class WorkerGroupResourceCoordinator;
class IWorkerGroup;
using WorkerGroupPtr = std::shared_ptr<IWorkerGroup>;

class ResourceManagerController : public WithContext, private boost::noncopyable
{
public:
    ResourceManagerController(ContextPtr global_context_);
    ~ResourceManagerController();

    Catalog::CatalogPtr getCnchCatalog();
    void createVWsFromConfig();
    void createWorkerGroupsFromConfig(const String & prefix, const String & vw_name);

    void initialize();

    auto & getResourceTracker() { return *resource_tracker; }
    auto & getVirtualWarehouseManager() { return *vw_manager; }
    auto & getWorkerGroupManager() { return *group_manager; }
    auto & getElectionController() { return *election_controller; }
    auto & getWorkerGroupResourceCoordinator() { return *wg_resource_coordinator; }

    void registerWorkerNode(const WorkerNodeResourceData & data); // RPC
    void removeWorkerNode(const std::string & worker_id, const std::string & vw_name, const std::string & group_id);

    WorkerGroupPtr createWorkerGroup(
        const std::string & group_id,
        bool if_not_exists,
        const std::string & vw_name,
        WorkerGroupData data,
        std::lock_guard<std::mutex> * vw_lock = nullptr,
        std::lock_guard<std::mutex> * wg_lock = nullptr);

    void dropWorkerGroup(
        const std::string & group_id,
        bool if_exists,
        std::lock_guard<std::mutex> * vw_lock = nullptr,
        std::lock_guard<std::mutex> * wg_lock = nullptr);

private:
    Poco::Logger * log{nullptr};

    std::unique_ptr<ResourceTracker> resource_tracker;
    std::unique_ptr<VirtualWarehouseManager> vw_manager;
    std::unique_ptr<WorkerGroupManager> group_manager;
    std::unique_ptr<WorkerGroupResourceCoordinator> wg_resource_coordinator;
    std::unique_ptr<ElectionController> election_controller;
};

}
