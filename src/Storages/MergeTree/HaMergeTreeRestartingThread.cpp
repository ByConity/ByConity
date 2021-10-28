#include <Storages/MergeTree/HaMergeTreeRestartingThread.h>

#include <IO/Operators.h>
#include <Storages/MergeTree/HaMergeTreeLogManager.h>
#include <Storages/StorageHaMergeTree.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>


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
    extern const int REPLICA_STATUS_CHANGED;
}

namespace
{
    constexpr auto retry_period_ms = 10 * 1000;
}

/// Used to check whether it's us who set node `is_active`, or not.
static String generateActiveNodeIdentifier()
{
    return "pid: " + toString(getpid()) + ", random: " + toString(randomSeed());
}

HaMergeTreeRestartingThread::HaMergeTreeRestartingThread(StorageHaMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (HaMergeTreeRestartingThread)")
    , log(&Poco::Logger::get(log_name))
    , active_node_identifier(generateActiveNodeIdentifier())
{
    const auto storage_settings = storage.getSettings();
    check_period_ms = storage_settings->zookeeper_session_expiration_check_period.totalSeconds() * 1000;

    task = storage.getContext()->getRestartSchedulePool().createTask(log_name, [this] { run(); });
}

void HaMergeTreeRestartingThread::run()
{
    if (need_stop)
        return;

    try
    {
        if (first_time || readonly_mode_was_set || storage.getZooKeeper()->expired() || extra_startup)
        {
            startup_completed = false;

            if (first_time)
            {
                LOG_DEBUG(log, "Activating replica.");
            }
            else
            {
                if (storage.getZooKeeper()->expired())
                {
                    LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");
                    setReadonly();
                }
                else if (readonly_mode_was_set)
                {
                    LOG_WARNING(log, "Table was in readonly mode. Will try to activate it.");
                }
                partialShutdown();
            }

            // Question: this check seem useless as it is always false in this code path
            if (!startup_completed)
            {
                try
                {
                    storage.setZooKeeper();
                }
                catch (const zkutil::KeeperException &)
                {
                    /// The exception when you try to zookeeper_init usually happens if DNS does not work. We will try to do it again.
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);

                    if (first_time)
                        storage.startup_event.set();
                    task->scheduleAfter(retry_period_ms);
                    return;
                }

                /* If ha table in fast_create model, tryStartup doesn't clone replica, and this replica might
                 * still in lost status, TODO: need check if this is ok */
                if (!need_stop && !tryStartup())
                {
                    /// We couldn't startup replication. Table must be readonly.
                    /// Otherwise it can have partially initialized queue and other
                    /// strange parts of state.
                    setReadonly();

                    if (first_time)
                        storage.startup_event.set();

                    task->scheduleAfter(retry_period_ms);
                    return;
                }

                if (first_time)
                    storage.startup_event.set();

                startup_completed = true;
            }

            if (need_stop)
                return;

            bool old_val = true;
            if (storage.is_readonly.compare_exchange_strong(old_val, false))
            {
                readonly_mode_was_set = false;
                CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
            }

            first_time = false;
        }

        /* TODO:
        time_t current_time = time(nullptr);
        if (current_time >= prev_time_of_check_delay + static_cast<time_t>(storage.settings.check_delay_period))
        {
            /// Find out lag of replicas.
            time_t absolute_delay = 0;
            time_t relative_delay = 0;

            storage.getReplicaDelays(absolute_delay, relative_delay);

            if (absolute_delay)
                LOG_DEBUG(log, "Absolute delay: " << absolute_delay << ". Relative delay: " << relative_delay << ".");

            prev_time_of_check_delay = current_time;

            /// We give up leadership if the relative lag is greater than threshold.
            if (storage.is_leader
                && relative_delay > static_cast<time_t>(storage.settings.min_relative_delay_to_yield_leadership))
            {
                LOG_INFO(log, "Relative replica delay (" << relative_delay << " seconds) is bigger than threshold ("
                    << storage.settings.min_relative_delay_to_yield_leadership << "). Will yield leadership.");

                ProfileEvents::increment(ProfileEvents::ReplicaYieldLeadership);

                storage.exitLeaderElection();
                /// NOTE: enterLeaderElection() can throw if node creation in ZK fails.
                /// This is bad because we can end up without a leader on any replica.
                /// In this case we rely on the fact that the session will expire and we will reconnect.
                storage.enterLeaderElection();
            }
        }
        */
    }
    catch (...)
    {
        /// We couldn't activate table let's set it into readonly mode
        setReadonly();
        storage.startup_event.set();
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(check_period_ms);
}

bool HaMergeTreeRestartingThread::isSelfLost(const zkutil::ZooKeeperPtr & zookeeper)
{
    return "1" == zookeeper->get(storage.replica_path + "/is_lost");
}

void HaMergeTreeRestartingThread::markSelfLost(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// Maybe someone has already marked it.
    if ("1" == zookeeper->get(storage.replica_path + "/is_lost"))
        return;

    std::map<String, Coordination::Stat> alive_is_lost_stats;
    bool has_alive_replica = false;

    for (const String & replica_name : zookeeper->getChildren(storage.zookeeper_path + "/replicas"))
    {
        if (replica_name == storage.replica_name) continue;

        String replica_path = storage.zookeeper_path + "/replicas/" + replica_name;
        Coordination::Stat is_lost_stat;
        String replica_is_lost_value = zookeeper->get(replica_path + "/is_lost", &is_lost_stat);
        if ("0" == replica_is_lost_value)
        {
            alive_is_lost_stats[replica_name] = is_lost_stat;
            has_alive_replica = true;
        }
    }

    if (!has_alive_replica)
    {
        LOG_WARNING(log, "This is the last alive replica, cannot mark self lost");
        return;
    }

    Coordination::Stat host_stat;
    zookeeper->get(storage.replica_path + "/host", &host_stat);

    Coordination::Requests ops;
    for (auto & [replica_name, is_lost_stat] : alive_is_lost_stats)
        ops.push_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas/" + replica_name + "/is_lost", is_lost_stat.version));
    ops.push_back(zkutil::makeCheckRequest(storage.replica_path + "/host", host_stat.version));
    ops.push_back(zkutil::makeSetRequest(storage.replica_path + "/is_lost", "1", -1));

    Coordination::Responses resps;
    auto code = zookeeper->tryMulti(ops, resps);
    if (code == Coordination::Error::ZBADVERSION)
        throw Exception(storage.replica_name + " became active when we marked self lost.", ErrorCodes::REPLICA_STATUS_CHANGED);
    else
        zkutil::KeeperMultiException::check(code, ops, resps);

    LOG_DEBUG(log, "Mark {} as lost replica.", storage.replica_name);
}

bool HaMergeTreeRestartingThread::tryStartup()
{
    try
    {
        activateReplica();

        const auto & zookeeper = storage.getZooKeeper();
        const auto storage_settings = storage.getSettings();

        if (storage.log_manager->exists())
        {
            storage.log_manager->load();
        }
        else
        {
            LOG_WARNING(log, "Log files have been removed (maybe by hand), try recovering...");
            storage.log_manager->create();
            markSelfLost(zookeeper);
        }

        bool isLost = isSelfLost(zookeeper); // e.g. new replica
        if (!first_time || !storage_settings->ha_fast_create_table || !isLost)
        {
            if (extra_startup)
                LOG_DEBUG(log, "ExtraStartup trigger cloneReplicaIfNeeded");
            storage.cloneReplicaIfNeeded(zookeeper);

            /// pullLogsToQueue() after we mark replica 'is_active' (and after we repair if it was lost);
            storage.queue.load(zookeeper);
            storage.queue.pullLogsToQueue(zookeeper);

            extra_startup = false; // reset it
        }
        else
        {
            setReadonly();

            extra_startup = true; // need extra_startup
        }
        /// TODO: storage.recoverActionOnTransaction();

        if (storage_settings->replicated_can_become_leader)
            storage.enterLeaderElection();

        /// Anything above can throw a KeeperException if something is wrong with ZK.
        /// Anything below should not throw exceptions.

        storage.partial_shutdown_called = false;
        storage.partial_shutdown_event.reset();

        /// delay queue task as it doesn't recover from lost status
        if (!extra_startup)
        {
            /// Start queue processing
            storage.background_executor.start();

            storage.queue_updating_task->activateAndSchedule();
            storage.mutations_finalizing_task->activateAndSchedule();
            storage.cleanup_thread.start();
            /// storage.part_check_thread.start();
        }

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

void HaMergeTreeRestartingThread::activateReplica()
{
    Stopwatch stopwatch;

    auto zookeeper = storage.getZooKeeper();

    /// How other replicas can access this one.
    auto address = storage.getHaMergeTreeAddress();

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
            throw Coordination::Exception(code, is_active_path);
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


void HaMergeTreeRestartingThread::partialShutdown()
{
    ProfileEvents::increment(ProfileEvents::ReplicaPartialShutdown);

    storage.partial_shutdown_called = true;
    storage.partial_shutdown_event.set();
    storage.replica_is_active_node = nullptr;

    LOG_TRACE(log, "Waiting for threads to finish");

    storage.exitLeaderElection();

    storage.queue_updating_task->deactivate();
    storage.mutations_finalizing_task->deactivate();

    storage.cleanup_thread.stop();
    /// TODO: storage.part_check_thread.stop();

    /// Stop queue processing
    {
        auto fetch_lock = storage.fetcher.blocker.cancel();
        auto merge_lock = storage.merger_mutator.merges_blocker.cancel();
        auto move_lock = storage.parts_mover.moves_blocker.cancel();
        storage.background_executor.finish();
    }

    LOG_TRACE(log, "Threads finished");
}

void HaMergeTreeRestartingThread::shutdown()
{
    /// Stop restarting_thread before stopping other tasks - so that it won't restart them again.
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "Restarting thread finished");

    /// For detach table query, we should reset the ReadonlyReplica metric.
    if (readonly_mode_was_set)
    {
        CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
        readonly_mode_was_set = false;
    }

    /// Stop other tasks.
    partialShutdown();

}

void HaMergeTreeRestartingThread::setReadonly()
{
    bool old_val = false;
    if (storage.is_readonly.compare_exchange_strong(old_val, true))
    {
        readonly_mode_was_set = true;
        CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
    }
}


}
