#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/logger_useful.h>

#include <pcg_random.hpp>

namespace DB
{
class StorageHaMergeTree;


/** Removes obsolete data from a table of type ReplicatedMergeTree.
  */
class HaMergeTreeCleanupThread
{
public:
    HaMergeTreeCleanupThread(StorageHaMergeTree & storage_);

    void start()
    {
        task->activateAndSchedule();
        local_task->activateAndSchedule();
    }

    void wakeup()
    {
        task->schedule();
        local_task->schedule();
    }

    void stop()
    {
        task->deactivate();
        local_task->deactivate();
    }

private:
    StorageHaMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    BackgroundSchedulePool::TaskHolder task;
    BackgroundSchedulePool::TaskHolder local_task;
    pcg64 rng;

    void run();
    void localRun();
    void iterate();

    /// Remove old records from ZooKeeper.
    void clearOldLogs();

    /// Remove emphemeral sequential # node from ZK, This is done by the leader replica.
    void clearOldBlockAndLSNs();

    /// Remove old mutations that are done from ZooKeeper. This is done by the leader replica.
    void clearOldMutations();

    /// Check whether insert quorum is timeout
    void checkQuorumTimeOut() {}

    /// Delete the parts for which the quorum has failed
    void removeFailedQuorumParts() {}
};


} // end of namespace DB
