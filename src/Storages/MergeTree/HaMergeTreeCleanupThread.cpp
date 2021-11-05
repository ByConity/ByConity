#include <Storages/MergeTree/HaMergeTreeCleanupThread.h>

#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/HaMergeTreeLogManager.h>
/// #include <Storages/MergeTree/HaMergeTreeQuorumEntry.h>
#include <Storages/StorageHaMergeTree.h>
#include <boost/algorithm/string.hpp>
#include <Poco/Timestamp.h>
#include <Common/setThreadName.h>

#include <random>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int REPLICA_STATUS_CHANGED;
}

/// Defined in ReplicatedMergeTreeQueue.cpp
String padIndex(Int64 index);

HaMergeTreeCleanupThread::HaMergeTreeCleanupThread(StorageHaMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getNameForLogs() + " (HaMergeTreeCleanupThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
    local_task = storage.getContext()->getLocalSchedulePool().createTask(log_name, [this] { localRun(); });
}

void HaMergeTreeCleanupThread::run()
{
    //@fix aeolus slow startup, Storage::context is global_context in startup
    /// if (!storage.getContext()->isListenPortsReady())
    /// {
    ///     task->scheduleAfter(30000);
    ///     return;
    /// }

    auto storage_settings = storage.getSettings();

    const auto CLEANUP_SLEEP_MS = storage_settings->cleanup_delay_period * 1000
        + std::uniform_int_distribution<UInt64>(0, storage_settings->cleanup_delay_period_random_add * 1000)(rng);

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

void HaMergeTreeCleanupThread::localRun()
{
    auto storage_settings = storage.getSettings();

    const auto CLEANUP_SLEEP_MS = storage_settings->cleanup_delay_period * 1000;

    try
    {
        /// Because there is no part information on ZK, it's OK to clear the parts on FS simply
        storage.clearOldPartsFromFilesystem();

        {
            auto lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage_settings->lock_acquire_timeout_for_background_operations);
            storage.clearOldTemporaryDirectories();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    local_task->scheduleAfter(CLEANUP_SLEEP_MS);
}


void HaMergeTreeCleanupThread::iterate()
{
    clearOldLogs();
    storage.clearEmptyParts();

    if (storage.is_leader)
    {
        clearOldBlockAndLSNs();
        clearOldMutations();
    }

    // checkQuorumTimeOut();
    // removeFailedQuorumParts();
}

#if 0
/// When insert quorum replica is down other replica can make insert quorum partition status timeout
void HaMergeTreeCleanupThread::checkQuorumTimeOut()
{
    auto zookeeper = storage.getZooKeeper();

    String quorum_status = storage.zookeeper_path + "/quorum/status";
    String fail_quorum_path = storage.zookeeper_path + "/quorum/failed_parts";

    Strings insert_quorum_parts;
    if (zookeeper->tryGetChildren(quorum_status, insert_quorum_parts) != Coordination::ZOK)
        return;

    for (const auto & part_name : insert_quorum_parts)
    {
        String value;
        Coordination::Stat stat;
        if (!zookeeper->tryGet(quorum_status + "/" + part_name, value, &stat))
            return;
        HaMergeTreeQuorumEntry quorum_entry;
        quorum_entry.fromString(value);

        time_t current_time = time(nullptr);
        size_t quorum_timeout = storage.getContext()->getSettings().insert_quorum_timeout.totalMilliseconds() / 1000;
        if (quorum_entry.replicas.size() < quorum_entry.required_number_of_replicas
            && current_time - quorum_entry.create_time > static_cast<Int64>(quorum_timeout))
        {
            /// Mark this insert as failed partition
            auto code = zookeeper->tryCreate(fail_quorum_path + "/" + part_name, "", zkutil::CreateMode::Persistent);
            if (code == Coordination::ZOK)
            {
                LOG_INFO(log, "Wait for insert quorum partition-" + part_name + " time out mark this partition as failed");
            }
            else if (code == Coordination::ZNODEEXISTS)
            {
                LOG_INFO(log, "Wait for insert quorum partition-" + part_name + " time out mark this partition failure already exist");
            }
        }
    }
}

void HaMergeTreeCleanupThread::removeFailedQuorumParts()
{
    auto zookeeper = storage.getZooKeeper();

    String fail_quorum_path = storage.zookeeper_path + "/quorum/failed_parts";
    String quorum_status = storage.zookeeper_path + "/quorum/status";

    Strings failed_parts;
    if (zookeeper->tryGetChildren(fail_quorum_path, failed_parts) != Coordination::ZOK)
        return;

    for (const auto & part_name : failed_parts)
    {
        String value;
        Coordination::Stat stat;
        if (!zookeeper->tryGet(quorum_status + "/" + part_name, value, &stat))
            continue;

        HaMergeTreeQuorumEntry quorum_entry;
        quorum_entry.fromString(value);

        {
            LOG_INFO(log, "Found part " << part_name << " with failed quorum. Remove this partition.");
            auto data_parts_lock = storage.lockParts();
            MergeTreePartInfo part_info = MergeTreePartInfo::fromPartName(part_name, storage.getDataFormatVersion());
            storage.removePartsInRangeFromWorkingSet(part_info, true, true, data_parts_lock);

            /// Remove replica item from quorum entry list
            quorum_entry.replicas.erase(storage.replica_name);
        }

        /// When replica is replaced by other node remove this replica in quorum list
        for (const auto & replica : quorum_entry.replicas)
        {
            if (!zookeeper->exists(storage.zookeeper_path + "/replicas/" + replica))
            {
                quorum_entry.replicas.erase(replica);
            }
        }

        Coordination::Requests ops;
        Coordination::Responses responses;

        /// If the list of replicas which has this partition is empty delete failed partition and status zk path
        if (quorum_entry.replicas.empty())
        {
            ops.emplace_back(zkutil::makeRemoveRequest(quorum_status + "/" + part_name, -1));
            ops.emplace_back(zkutil::makeRemoveRequest(fail_quorum_path + "/" + part_name, -1));
            auto code = zookeeper->tryMulti(ops, responses);
            if (code == Coordination::ZOK)
            {
                LOG_INFO(log, "Remove failed partition-" + part_name + " on zk success.");
            }
            else if (code == Coordination::ZNONODE)
            {
                LOG_WARNING(log, "Maybe fail partition-" + part_name + " zk path already deleted by other replica.");
            }
            else
            {
                LOG_WARNING(log, "Remove failed partition-" + part_name + " failed. code-" + std::to_string(code));
            }
        }
        else
        {
            auto code = zookeeper->trySet(quorum_status + "/" + part_name, quorum_entry.toString(), stat.version);
            if (code == Coordination::ZOK)
            {
                LOG_INFO(log, "Remove local partition data and update quorum status partition-" + part_name + " on zk success.");
            }
            else if (code == Coordination::ZNONODE)
            {
                LOG_WARNING(log, "Maybe fail partition-" + part_name + " zk path already deleted by other replica.");
            }
            else
            {
                LOG_WARNING(
                    log,
                    "Remove local partition data and update quorum status partition-" + part_name + " failed. code-"
                        + std::to_string(code));
            }
        }
    }
}
#endif

void HaMergeTreeCleanupThread::clearOldBlockAndLSNs()
{
    auto zookeeper = storage.getZooKeeper();

    auto removeEmpSeqZK = [&](String node_path, String node_prefix = "") {
        Strings children;
        Coordination::Stat stat;
        if (Coordination::Error::ZOK != zookeeper->tryGetChildren(node_path, children, &stat))
            throw Exception(node_path + " doesn't exist", ErrorCodes::NOT_FOUND_NODE);

        zkutil::AsyncResponses<Coordination::RemoveResponse> try_remove_futures;
        for (auto & child : children)
        {
            if (!node_prefix.empty() && !boost::istarts_with(child, node_prefix))
                continue;
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
    removeEmpSeqZK("/clickhouse", "xid-");
}

void HaMergeTreeCleanupThread::clearOldMutations()
{
    UInt64 finished_mutations_to_keep = storage.getSettings()->finished_mutations_to_keep;
    if (!finished_mutations_to_keep)
        return;

    if (storage.queue.countFinishedMutations(/*include_alter_metadata=*/true) <= finished_mutations_to_keep)
    {
        /// Not strictly necessary, but helps to avoid unnecessary ZooKeeper requests.
        /// If even this replica hasn't finished enough mutations yet, then we don't need to clean anything.
        return;
    }

    auto zookeeper = storage.getZooKeeper();

    Coordination::Stat replicas_stat;
    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &replicas_stat);

    UInt64 min_pointer = MergeTreePartInfo::MAX_MUTATION;
    for (const String & replica : replicas)
    {
        /// skip lost and offline replica
        if (storage.isReplicaLostOrOffline(zookeeper, replica))
            continue;

        String pointer;
        zookeeper->tryGet(storage.zookeeper_path + "/replicas/" + replica + "/mutation_pointer", pointer);
        if (pointer.empty())
            return; /// One replica hasn't done anything yet so we can't delete any mutations.
        min_pointer = std::min(parse<UInt64>(pointer), min_pointer);
    }
    if (min_pointer == MergeTreePartInfo::MAX_MUTATION)
    {
        LOG_WARNING(log, "Skip cleaning mutations because all replicas are lost or offline");
        return; /// do not update mutation pointer to max in case all replicas are lost
    }

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/mutations");
    std::sort(entries.begin(), entries.end());

    /// Do not remove entries that are greater than `min_pointer` (they are not done yet).
    entries.erase(std::upper_bound(entries.begin(), entries.end(), padIndex(min_pointer)), entries.end());
    /// Do not remove last `storage_settings->finished_mutations_to_keep` entries.
    if (entries.size() <= finished_mutations_to_keep)
        return;
    entries.erase(entries.end() - finished_mutations_to_keep, entries.end());

    if (entries.empty())
        return;

    Coordination::Requests ops;
    size_t batch_start_i = 0;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        LOG_DEBUG(log, "Removing old mutation entry {} from ZK", entries[i]);
        ops.emplace_back(zkutil::makeRemoveRequest(storage.zookeeper_path + "/mutations/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas", replicas_stat.version));
            try
            {
                zookeeper->multi(ops);
            }
            catch (const zkutil::KeeperMultiException & e)
            {
                /// Another replica already deleted the same node concurrently.
                if (e.code == Coordination::Error::ZNONODE)
                    break;

                throw;
            }
            LOG_INFO(log, "Removed {} old mutations entries: {} - {}", (i + 1 - batch_start_i), entries[batch_start_i], entries[i]);
            batch_start_i = i + 1;
            ops.clear();
        }
    }
}

void HaMergeTreeCleanupThread::clearOldLogs()
{
    auto lsn_status = storage.log_manager->getLSNStatus();

    auto storage_settings = storage.getSettings();

    if (lsn_status.count < storage_settings->ha_logs_to_keep * 11 / 10)
        return;

    storage.log_manager->discardTo(lsn_status.committed_lsn);
    LOG_DEBUG(log, "Discard log entries in disk to LSN {} ", lsn_status.committed_lsn);
}


}
