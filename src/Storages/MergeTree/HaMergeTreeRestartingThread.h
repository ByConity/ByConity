#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <atomic>
#include <thread>
#include <Poco/Event.h>
#include <common/logger_useful.h>


namespace DB
{

class StorageHaMergeTree;


/** Initializes ZK session.
  * Exposes ephemeral nodes. It sets the node values that are required for replica detection.
  * Starts participation in the leader selection. Starts all background threads.
  * Then monitors whether the session has expired. And if it expired, it will reinitialize it.
  */
class HaMergeTreeRestartingThread
{
public:
    HaMergeTreeRestartingThread(StorageHaMergeTree & storage_);

    void start() { task->activateAndSchedule(); }
    void wakeup() { task->schedule(); }
    void shutdown();

private:
    StorageHaMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    std::atomic<bool> need_stop {false};

    // We need it besides `storage.is_readonly`, because `shutdown()` may be called many times, that way `storage.is_readonly` will not change.
    bool readonly_mode_was_set = false;

    /// The random data we wrote into `/replicas/me/is_active`.
    String active_node_identifier;

    BackgroundSchedulePool::TaskHolder task;
    Int64 check_period_ms;                  /// The frequency of checking expiration of session in ZK.
    bool first_time = true;                 /// Activate replica for the first time.
    bool extra_startup = false; // in fast_create model, some intialization is not invoked, and need extra logic
    time_t prev_time_of_check_delay = 0;
    bool startup_completed = false;

    void run();

    void markSelfLost(const zkutil::ZooKeeperPtr & zookeeper);

    bool isSelfLost(const zkutil::ZooKeeperPtr & zookeeper);

    /// Start or stop background threads. Used for partial reinitialization when re-creating a session in ZooKeeper.
    bool tryStartup(); /// Returns false if ZooKeeper is not available.

    /// Note in ZooKeeper that this replica is currently active.
    void activateReplica();

    void partialShutdown();

    /// Set readonly mode for table
    void setReadonly();
};


}
