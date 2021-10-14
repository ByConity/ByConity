#include <Storages/MergeTree/HaMergeTreeAlterMetadataThread.h>

#include <Storages/StorageHaMergeTree.h>

namespace DB
{
static const auto ALTER_SLEEP_MS = 60 * 1000;
static const auto ALTER_ERROR_SLEEP_MS = 10 * 1000;

HaMergeTreeAlterMetadataThread::HaMergeTreeAlterMetadataThread(StorageHaMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getNameForLogs() + " (HaMergeTreeAlterMetadataThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
}


void HaMergeTreeAlterMetadataThread::run()
{
    try
    {
        LOG_TRACE(log, "Try select and execute alter metadata...");

        bool need_finalize = false;
        while (true)
        {
            auto entry = storage.queue.selectAlterMetadataToProcess(last_processed);
            if (!entry)
                break;

            storage.executeMetadataAlter(*entry);
            storage.queue.finishMetadataAlter(entry);

            last_processed = entry->znode_name;
            need_finalize = true;
        }

        if (need_finalize)
            storage.mutations_finalizing_task->schedule();

        /// Also schedule this thread after:
        ///   1. A new alter metadata genedated by other replica is added;
        ///   2. A alter data mutations finishes;
        task->scheduleAfter(ALTER_SLEEP_MS);
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;

        task->scheduleAfter(ALTER_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        task->scheduleAfter(ALTER_ERROR_SLEEP_MS);
    }
}

}
