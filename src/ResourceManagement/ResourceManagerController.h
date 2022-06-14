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

class ResourceManagerController : public WithContext, private boost::noncopyable
{
public:
    ResourceManagerController(ContextPtr global_context_);
    ~ResourceManagerController();

    Catalog::CatalogPtr getCnchCatalog();
    void createVWsFromConfig();
    void createWorkerGroupsFromConfig(const String & prefix, const String & vw_name);

    void initialize();

    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    auto & getResourceTracker() { return *resource_tracker; }
    auto & getVirtualWarehouseManager() { return *vw_manager; }
    auto & getWorkerGroupManager() { return *group_manager; }
    auto & getElectionController() { return *election_controller; }

    void registerWorkerNode(const WorkerNodeResourceData & data); // RPC
    void removeWorkerNode(const std::string & worker_id, const std::string & vw_name, const std::string & group_id);

private:
    ConfigurationPtr config;
    Poco::Logger * log{nullptr};

    std::unique_ptr<ResourceTracker> resource_tracker;
    std::unique_ptr<VirtualWarehouseManager> vw_manager;
    std::unique_ptr<WorkerGroupManager> group_manager;
    std::unique_ptr<ElectionController> election_controller;
};

}
