#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/ResourceManagerController.h>
#if BYTEJOURNAL_AVAILABLE
#include <bytejournal/sdk/leader_election_runner.h>
#endif

namespace DB::ResourceManagement
{

/** ElectionController is used for Resource Manager leader election (handled by ByteJournal)
  * It contains a background thread to check for leader information, and ensures that a newly elected
  * leader retrieves its state from ByteKV
  */
class ElectionController
{

struct LeaderElectionResult
{
    String leader_host_port {};
    bool is_leader{false};
};

public:
    ElectionController(ResourceManagerController & rm_controller_);

    ~ElectionController();

    LeaderElectionResult getLeaderElectionResult();

    void shutDown();

private:
    ContextPtr getContext() const;
    void startLeaderElection();

    void initByteJournalClient();
    void onLeader(const String & leader_addr);
    void onFollower(const String & leader_addr);
    void setLeaderElectionResult(const String & leader_addr, const bool is_leader);
    // Pulls logical VW and worker group info from ByteKV
    bool pullState();
    void checkLeaderInfo(const UInt64 & check_interval);
    bool waitFor(const UInt64 & period);

    Poco::Logger * log = &Poco::Logger::get("ElectionController");
    ResourceManagerController & rm_controller;
    
    mutable std::mutex leader_election_mutex;
    BackgroundSchedulePool::TaskHolder leader_info_checker;
    LeaderElectionResult leader_election_result;
    const String config_prefix {"resource_manager"};
    String election_ns;
    String election_point;

    #if BYTEJOURNAL_AVAILABLE
    ByteJournalClientPtr bj_client;
    std::unique_ptr<::bytejournal::sdk::LeaderElectionRunner> leader_runner;
    #endif
};

using ElectionControllerPtr = std::shared_ptr<ElectionController>;

}

