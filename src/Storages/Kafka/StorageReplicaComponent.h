#pragma once

#include <atomic>
#include <mutex>
#include <Core/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/MergeTree/LeaderElection.h>

namespace DB
{

class Context;

class StorageReplicaComponent : public WithContext
{
public:
    virtual ~StorageReplicaComponent();

    struct Status
    {
        bool is_leader;
        bool is_readonly;
        bool is_session_expired;
        String zookeeper_path;
        String replica_name;
        String replica_path;
    };

    void getStatus(Status & res);

protected:

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    std::mutex current_zookeeper_mutex;            /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper();
    zkutil::ZooKeeperPtr getZooKeeper();
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

    /// If true, the table is offline and can not be written to it.
    std::atomic_bool is_readonly {false};

    String zookeeper_path;
    String replica_name;
    String replica_path;

    /// `/replicas/me/is_active`
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;

    /// Is this replica "leading". The leader replica selects the parts to merge.
    std::mutex election_mutex;
    std::atomic<bool> is_leader {false};
    zkutil::LeaderElectionPtr leader_election;

    /// When activated, replica is initialized and startup() method could exit
    Poco::Event startup_event;

    /// Do I need to complete background threads (except restarting_thread)?
    std::atomic<bool> partial_shutdown_called {false};

    /// Event that is signalled (and is reset) by the restarting_thread when the ZooKeeper session expires.
    Poco::Event partial_shutdown_event {false};     /// Poco::Event::EVENT_MANUALRESET

    ///Poco::Logger * log;

    /// Postcondition:
    /// either leader_election is fully initialized (node in ZK is created and the watching thread is launched)
    /// or an exception is thrown and leader_election is destroyed.
    virtual void enterLeaderElection() = 0;

    /// Postcondition:
    /// is_leader is false, merge_selecting_thread is stopped, leader_election is nullptr.
    /// leader_election node in ZK is either deleted, or the session is marked expired.
    virtual void exitLeaderElection() = 0;

    StorageReplicaComponent(
            const String & zookeeper_path_,
            const String & replica_name_,
            bool attach_,
            const String & database_name_,
            const String & table_name_,
            ContextPtr context_);
};

} // end of namespace DB
