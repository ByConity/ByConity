#include <ResourceManagement/ElectionController.h>
#include <Catalog/Catalog.h>
#include <Common/Configurations.h>
#include <Coordination/Defines.h>
#include <Coordination/KeeperDispatcher.h>
#include <common/getFQDNOrHostName.h>
#include <ResourceManagement/ResourceTracker.h>
#include <ResourceManagement/VirtualWarehouseManager.h>
#include <ResourceManagement/WorkerGroupManager.h>
#include <ResourceManagement/WorkerGroupResourceCoordinator.h>

#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Storages/PartCacheManager.h>

namespace DB::ResourceManagement
{

ElectionController::ElectionController(ResourceManagerController & rm_controller_) 
    : WithContext(rm_controller_.getContext())
    , LeaderElectionBase(getContext()->getRootConfig().resource_manager.check_leader_info_interval_ms)
    , rm_controller(rm_controller_)
{
    enterLeaderElection();
}

ElectionController::~ElectionController() 
{
    shutDown();
}

void ElectionController::enterLeaderElection()
{
    try
    {
        auto current_address = getContext()->getHostWithPorts().getRPCAddress();
        auto election_path = getContext()->getRootConfig().resource_manager.election_path.value;

        current_zookeeper = getContext()->getZooKeeper();
        current_zookeeper->createAncestors(election_path + "/");

        leader_election = std::make_unique<zkutil::LeaderElection>(
            getContext()->getSchedulePool(),
            election_path,
            *current_zookeeper,
            [&]() { onLeader(); },
            current_address,
            false
        );
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ElectionController::onLeader()
{
    auto current_address = getContext()->getHostWithPorts().getRPCAddress();
    LOG_INFO(log, "Starting leader callback for " + current_address);

    /// Sleep to prevent multiple leaders from appearing at the same time
    std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));

    /// Make sure get all needed metadata from KV store before becoming leader.
    if (!pullState())
    {
        is_leader = false;
        exitLeaderElection();
    }
    else
    {
        LOG_INFO(log, "Current RM node " + current_address + " has become leader.");
        if (getContext()->getRootConfig().resource_manager.enable_auto_resource_sharing)
        {
            rm_controller.getWorkerGroupResourceCoordinator().start();
        }
        is_leader = true;
    }
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

void ElectionController::exitLeaderElection()
{
    shutDown();
}

void ElectionController::shutDown()
{
    is_leader = false;
    leader_election.reset();
    current_zookeeper.reset();
    LOG_DEBUG(log, "Exit leader election");
}

}
