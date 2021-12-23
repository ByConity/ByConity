#pragma once

#include <Core/Types.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>

#include <pcg_random.hpp>

namespace DB
{

class StorageHaUniqueMergeTree;

/**
 * Remove obsolete data from HaUniqueMergeTree table.
 */
class HaUniqueMergeTreeCleanupThread
{
public:
    explicit HaUniqueMergeTreeCleanupThread(StorageHaUniqueMergeTree & storage_);

    void start() { task->activateAndSchedule(); }
    void wakeup() { task->schedule(); }
    void stop() { task->deactivate(); }

private:
    StorageHaUniqueMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    BackgroundSchedulePool::TaskHolder task;
    pcg64 rng;

    void run();
    void iterate();
    /// Remove ephemeral sequential # node from ZK, This is done by the leader replica.
    void clearOldBlockAndLSNs();
};

}
