#pragma once

#include <thread>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <common/logger_useful.h>


namespace DB
{
class StorageHaUniqueMergeTree;


/** For unique merge tree, manifest stores the latest schema, use this thread to
  * make a comparison with its local part schema, and alter parts if found differences.
  */
class HaUniqueMergeTreeAlterThread
{
public:
    HaUniqueMergeTreeAlterThread(StorageHaUniqueMergeTree & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void stop() { task->deactivate(); }

public:
    /// The latest part schema this thread knows.
    String part_metadata;
    String part_columns;

private:
    void run();

    StorageHaUniqueMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    BackgroundSchedulePool::TaskHolder task;
    bool force_recheck_parts = true;
};

}
