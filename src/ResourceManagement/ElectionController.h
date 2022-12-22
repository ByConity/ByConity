#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/ResourceManagerController.h>
#include <Coordination/LeaderElectionBase.h>

namespace zkutil
{
    class LeaderElection;
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB::ResourceManagement
{

/** ElectionController is used for Resource Manager leader election (handled by ZooKeeper)
  * It contains a background thread to check for leader information, and ensures that a newly elected
  * leader retrieves its state from KV store.
  */
class ElectionController : public WithContext, public LeaderElectionBase
{

public:
    ElectionController(ResourceManagerController & rm_controller_);
    ~ElectionController() override;

private:
    void shutDown();

    void enterLeaderElection() override;
    void onLeader() override;
    void exitLeaderElection() override;

    // Pulls logical VW and worker group info from KV store.
    bool pullState();

    Poco::Logger * log = &Poco::Logger::get("ElectionController");
    ResourceManagerController & rm_controller;
    bool enable_leader_election{false};
    std::atomic_bool is_leader{false};
    
};

using ElectionControllerPtr = std::shared_ptr<ElectionController>;

}

