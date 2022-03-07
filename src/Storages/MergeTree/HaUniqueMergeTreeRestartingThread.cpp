#include <Storages/MergeTree/HaUniqueMergeTreeRestartingThread.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/randomSeed.h>
#include <IO/Operators.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event ReplicaYieldLeadership;
    extern const Event ReplicaPartialShutdown;
}

namespace CurrentMetrics
{
    extern const Metric ReadonlyReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int REPLICA_IS_ALREADY_ACTIVE;
}

namespace
{
    // constexpr auto retry_period_ms = 10 * 1000;
}

/// Used to check whether it's us who set node `is_active`, or not.
static String generateActiveNodeIdentifier()
{
    return "pid: " + std::to_string(getpid()) + ", random: " + std::to_string(randomSeed());
}

HaUniqueMergeTreeRestartingThread::HaUniqueMergeTreeRestartingThread(StorageHaUniqueMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getLogName() + " (HaUniqueMergeTreeRestartingThread)")
    , log(&Poco::Logger::get(log_name))
    , active_node_identifier(generateActiveNodeIdentifier())
{
    auto settings = storage.getSettings();
    check_period_ms = settings->zookeeper_session_expiration_check_period.totalSeconds() * 1000;
    task = storage.getContext()->getUniqueTableSchedulePool().createTask(log_name, [this]{ run(); });
}

void HaUniqueMergeTreeRestartingThread::run()
{
    if (need_stop)
        return;

    LOG_DEBUG(log, "Restarting thread is running.");
    try
    {
        if (first_time || need_retry || storage.getUniqueTableState() != UniqueTableState::NORMAL || storage.getZooKeeper()->expired())
        {
            need_retry = false;

            if (first_time)
                LOG_DEBUG(log, "Activating replica.");
            else
                partialShutdown();

            if (auto zk = storage.tryGetZooKeeper(); !zk || zk->expired())
            {
                LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");
                bool old_val = false;
                if (storage.is_readonly.compare_exchange_strong(old_val, true))
                    CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);

            }

            /// start new ZK session when current session has been expired
            storage.setZooKeeper();

            /// enter or exit repair mode based on is_lost and table state
            while (!need_stop)
            {
                auto state = storage.getUniqueTableState();
                switch (state)
                {
                    case UniqueTableState::INIT:
                        if (storage.isSelfLost())
                            /// when adding a replica to an existing table
                            storage.setUniqueTableState(UniqueTableState::REPAIR_MANIFEST);
                        else
                            /// when starting a normal replica
                            storage.setUniqueTableState(UniqueTableState::NORMAL);
                        break;
                    case UniqueTableState::BROKEN:
                        /// when starting a replica under repair or when the replica is marked broken somehow
                        storage.markSelfLost();
                        storage.setUniqueTableState(UniqueTableState::REPAIR_MANIFEST);
                        break;
                    case UniqueTableState::REPAIR_MANIFEST:
                        storage.enterRepairMode();
                        break;
                    case UniqueTableState::REPAIR_DATA:
                        /// re-create repair_lsn in case session expired during repair
                        storage.saveRepairVersionToZk(storage.current_repair_state->checkpoint_version);
                        break;
                    case UniqueTableState::REPAIRED:
                        /// after there is nothing to repair
                        storage.verifyTableStructureAlterIfNeed();
                        storage.calculateColumnSizesImpl();
                        storage.current_repair_state = nullptr;
                        storage.removeRepairVersionFromZk();
                        storage.markSelfNonLost();
                        storage.setUniqueTableState(UniqueTableState::NORMAL);
                        break;
                    case UniqueTableState::NORMAL:
                        break; /// no-op
                }
                /// retry when table state changed
                if (storage.getUniqueTableState() == state)
                    break;
            }

            /// activate replica and restart all bg threads if state is normal
            if (!need_stop && storage.getUniqueTableState() == UniqueTableState::NORMAL && tryStartup())
            {
                bool old_val = true;
                if (storage.is_readonly.compare_exchange_strong(old_val, false))
                    CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
                if (first_time)
                    storage.startup_event.set();
                first_time = false;
                LOG_DEBUG(log, "Successfully startup replica");
            }
        }
    }
    catch (...)
    {
        need_retry = true;
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (first_time)
        storage.startup_event.set();
    task->scheduleAfter(check_period_ms);
}

/// pre-condition: table state must be NORMAL
bool HaUniqueMergeTreeRestartingThread::tryStartup()
{
    try
    {
        activateReplica();

        // const auto & zookeeper = storage.getZooKeeper();

        auto settings = storage.getSettings();
        if (settings->replicated_can_become_leader)
            storage.enterLeaderElection();
        else
            LOG_INFO(log, "Will not enter leader election because replicated_can_become_leader=0");

        /// Anything above can throw a KeeperException if something is wrong with ZK.
        /// Anything below should not throw exceptions.

        storage.partial_shutdown_called = false;
        storage.partial_shutdown_event.reset();

        if (!storage.is_leader)
        {
            storage.update_log_task->activateAndSchedule();
            storage.replay_log_task->activateAndSchedule();
        }
        storage.cleanup_thread.start();
        storage.alter_thread.start();

        return true;
    }
    catch (...)
    {
        storage.replica_is_active_node  = nullptr;

        try
        {
            throw;
        }
        catch (const zkutil::KeeperException & e)
        {
            LOG_ERROR(log, "Couldn't start replication (table will be in readonly mode): {}. {}", e.what(), DB::getCurrentExceptionMessage(true));
            return false;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::REPLICA_IS_ALREADY_ACTIVE)
                throw;

            LOG_ERROR(log, "Couldn't start replication (table will be in readonly mode): {}. {}", e.what(), DB::getCurrentExceptionMessage(true));
            return false;
        }
    }
}

void HaUniqueMergeTreeRestartingThread::activateReplica()
{
    Stopwatch stopwatch;

    auto zookeeper = storage.getZooKeeper();

    /// How other replicas can access this one.
    auto address = storage.getHaUniqueMergeTreeAddress();

    String is_active_path = storage.replica_path + "/is_active";

    /** If the node is marked as active, but the mark is made in the same instance, delete it.
      * This is possible only when session in ZooKeeper expires.
      */
    String data;
    Coordination::Stat stat;
    bool has_is_active = zookeeper->tryGet(is_active_path, data, &stat);
    if (has_is_active && data == active_node_identifier)
    {
        auto code = zookeeper->tryRemove(is_active_path, stat.version);

        if (code == Coordination::Error::ZBADVERSION)
            throw Exception("Another instance of replica " + storage.replica_path + " was created just now."
                            " You shouldn't run multiple instances of same replica. You need to check configuration files.",
                            ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);

        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
            throw zkutil::KeeperException(code, is_active_path);
    }

    /// Simultaneously declare that this replica is active, and update the host.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(is_active_path, active_node_identifier, zkutil::CreateMode::Ephemeral));
    ops.emplace_back(zkutil::makeSetRequest(storage.replica_path + "/host", address.toString(), -1));

    try
    {
        zookeeper->multi(ops);
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
            throw Exception("Replica " + storage.replica_path + " appears to be already active. If you're sure it's not, "
                            "try again in a minute or remove znode " + storage.replica_path + "/is_active manually", ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);
        throw;
    }

    /// `current_zookeeper` lives for the lifetime of `replica_is_active_node`,
    ///  since before changing `current_zookeeper`, `replica_is_active_node` object is destroyed in `partialShutdown` method.
    storage.replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *storage.current_zookeeper);

    LOG_DEBUG(log, "activateReplica() finished; cost {} ms.", stopwatch.elapsedMicroseconds() / 1000.0);
}

void HaUniqueMergeTreeRestartingThread::partialShutdown()
{
    ProfileEvents::increment(ProfileEvents::ReplicaPartialShutdown);

    storage.partial_shutdown_called = true;
    storage.partial_shutdown_event.set();
    storage.replica_is_active_node = nullptr;

    LOG_TRACE(log, "Waiting for threads to finish");

    storage.exitLeaderElection();

    storage.update_log_task->deactivate();
    storage.replay_log_task->deactivate();

    storage.cleanup_thread.stop();
    storage.alter_thread.stop();

    LOG_TRACE(log, "Threads finished");
}

void HaUniqueMergeTreeRestartingThread::shutdown()
{
    /// Stop restarting_thread before stopping other tasks - so that it won't restart them again.
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "Restarting thread finished");

    /// Stop other tasks.
    partialShutdown();
}
}
