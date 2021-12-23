#include <Storages/MergeTree/HaUniqueMergeTreeCleanupThread.h>

#include <Storages/StorageHaUniqueMergeTree.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
}

HaUniqueMergeTreeCleanupThread::HaUniqueMergeTreeCleanupThread(StorageHaUniqueMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getLogName() + " (HaUniqueMergeTreeCleanUpThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage.getContext()->getUniqueTableSchedulePool().createTask(log_name, [this]{ run(); });
}

void HaUniqueMergeTreeCleanupThread::run()
{
//     //@fix aeolus slow startup, Storage::context is global_context in startup
//     if (!storage.getContext()->isListenPortsReady())
//     {
//         task->scheduleAfter(30000);
//         return;
//     }

    auto settings = storage.getSettings();
    const auto CLEANUP_SLEEP_MS = settings->cleanup_delay_period * 1000
                                  + std::uniform_int_distribution<UInt64>(0, settings->cleanup_delay_period_random_add * 1000)(rng);

    try
    {
        iterate();
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(CLEANUP_SLEEP_MS);
}

void HaUniqueMergeTreeCleanupThread::iterate()
{
    LOG_DEBUG(log, "Begin to cleanup unused resources");
    Stopwatch overall_timer;
    Stopwatch timer;

    auto zookeeper = storage.getZooKeeper();
    storage.refreshClusterCommitVersion(zookeeper);
    UInt64 refresh_cluster_millis = timer.elapsedMilliseconds();

    timer.restart();
    storage.clearOldPartsFromFilesystem();
    UInt64 clear_parts_millis = timer.elapsedMilliseconds();

    timer.restart();
    storage.performUniqueIndexGc();
    UInt64 unique_index_gc_millis = timer.elapsedMilliseconds();

    timer.restart();
    storage.clearOldDeleteFilesFromFilesystem();
    UInt64 clear_delete_files_millis = timer.elapsedMilliseconds();

    timer.restart();
    storage.clearOldTemporaryDirectories();
    UInt64 clear_temp_dir_millis = timer.elapsedMilliseconds();

    timer.restart();
    UInt64 clear_znode_millis = 0;
    if (storage.is_leader)
    {
        clearOldBlockAndLSNs();
        clear_znode_millis = timer.elapsedMilliseconds();
    }

    UInt64 total_millis = overall_timer.elapsedMilliseconds();
    if (total_millis > 10000)
        LOG_WARNING(
            log,
            "Cleanup iteration took {}ms (refresh_cluster={}ms, clear_parts={}ms, unique_index_gc={}ms, clear_delete_files={}ms, "
            "clear_temp_dir={}ms, clear_znode={}ms)",
            total_millis,
            refresh_cluster_millis,
            clear_parts_millis,
            unique_index_gc_millis,
            clear_delete_files_millis,
            clear_temp_dir_millis,
            clear_znode_millis);
}

void HaUniqueMergeTreeCleanupThread::clearOldBlockAndLSNs()
{
    auto zookeeper = storage.getZooKeeper();

    // FIXME (UNIQUE KEY): check later
    auto removeEmpSeqZK = [&](String node_path)
    {
        Strings children;
        Coordination::Stat stat;
        auto code = zookeeper->tryGetChildren(node_path, children, &stat);
        if (code == Coordination::Error::ZNONODE)
            throw Exception(node_path + " doesn't exist", ErrorCodes::NOT_FOUND_NODE);

        zkutil::AsyncResponses<Coordination::RemoveResponse> try_remove_futures;
        for (auto & child: children)
        {
            String path = node_path + "/" + child;
            try_remove_futures.emplace_back(path, zookeeper->asyncTryRemove(path));
        }

        for (auto & pair : try_remove_futures)
        {
            const String & path = pair.first;
            auto rc = pair.second.get().error;
            if (rc != Coordination::Error::ZOK)
                LOG_WARNING(log, "Error while deleting ZooKeeper path `{}`: {}, ignoring.", path, Coordination::errorMessage(rc));
        }
    };
    removeEmpSeqZK(storage.zookeeper_path + "/block_numbers");
    removeEmpSeqZK(storage.zookeeper_path + "/latest_lsn");
}

}
