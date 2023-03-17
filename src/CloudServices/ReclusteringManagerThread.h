#pragma once

#include <CloudServices/ICnchBGThread.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class StorageCnchMergeTree;

/**
 * Manage recluster task for a table
 */
class ReclusteringManagerThread: public ICnchBackgroundThread
{
public:

    ReclusteringManagerThread(Context & context, const StorageID & storage_id);

    ~ReclusteringManagerThread() override;

    void runImpl() override;

    bool getTableReclusterStatus();

private:
    void clearData() override;

    // Pause/resume status of reclustering task for table with this thread's UUID. true = running, false = paused.
    std::atomic<bool> recluster_task_status{false};

};

}
