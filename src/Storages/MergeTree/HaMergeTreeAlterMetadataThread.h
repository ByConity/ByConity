#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <common/logger_useful.h>

namespace DB
{
class StorageHaMergeTree;

class HaMergeTreeAlterMetadataThread
{
public:
    HaMergeTreeAlterMetadataThread(StorageHaMergeTree & storage_);

    void start()
    {
        task->activateAndSchedule();
    }

    void stop()
    {
        task->deactivate();
    }

private:
    void run();

    StorageHaMergeTree & storage;
    String log_name;
    Poco::Logger * log;
    BackgroundSchedulePool::TaskHolder task;

    String last_processed;
};

}
