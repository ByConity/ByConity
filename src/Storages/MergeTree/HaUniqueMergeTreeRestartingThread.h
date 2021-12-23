#pragma once

#include <Poco/Event.h>
#include <common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <thread>
#include <atomic>

namespace DB
{

class StorageHaUniqueMergeTree;

/**
  * Perform periodical checks to
  * - reinitialize ZK when the current session has expired
  * - repair table when the table is lost or broken
  * - start-up the table (e.g., starts all background threads) when the state becomes normal
  */
class HaUniqueMergeTreeRestartingThread
{
public:
    explicit HaUniqueMergeTreeRestartingThread(StorageHaUniqueMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void shutdown();

private:
    StorageHaUniqueMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    std::atomic<bool> need_stop {false};

    /// The random data we wrote into `/replicas/me/is_active`.
    String active_node_identifier;

    BackgroundSchedulePool::TaskHolder task;
    Int64 check_period_ms;                  /// The frequency of checking expiration of session in ZK.
    bool first_time = true;                 /// Activate replica for the first time.
    bool need_retry = false;

    void run();

    /// Start or stop background threads. Used for partial reinitialization when re-creating a session in ZooKeeper.
    bool tryStartup(); /// Returns false if ZooKeeper is not available.

    /// Note in ZooKeeper that this replica is currently active.
    void activateReplica();

    void partialShutdown();
};

}
