#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/HaKafkaRestartingThread.h>

#include <IO/Operators.h>
#include <Storages/Kafka/StorageHaKafka.h>
#include <Storages/Kafka/HaKafkaAddress.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/randomSeed.h>

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
    constexpr auto retry_period_ms = 10 * 1000;

    /// Used to check whether it's us who set node `is_active`, or not.
    String generateActiveNodeIdentifier()
    {
        return "pid: " + toString(getpid()) + ", random: " + toString(randomSeed());
    }
}


HaKafkaRestartingThread::HaKafkaRestartingThread(StorageHaKafka & storage_)
    : storage(storage_)
    , log_name(storage.database_name + "." + storage.table_name + " (HaKafkaRestartingThread)")
    , log(&Poco::Logger::get(log_name))
      , active_node_identifier(generateActiveNodeIdentifier())
{
    /// TODO: avoid magic number
    check_period_ms = 60 * 1000;
    task = storage.getContext()->getRestartSchedulePool().createTask(log_name, [this] { run(); });
}

void HaKafkaRestartingThread::run()
{
    if (need_stop)
        return;

    try
    {
        if (first_time || need_retry || storage.getZooKeeper()->expired())
        {
            need_retry = false;
            startup_completed = false;

            if (first_time)
            {
                LOG_DEBUG(log, "Activating replica.");
            }
            else
            {
                LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");

                bool old_val = false;
                if (storage.is_readonly.compare_exchange_strong(old_val, true))
                    CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);

                partialShutdown();
            }

            if (!startup_completed)
            {
                try
                {
                    storage.setZooKeeper(storage.getContext()->getZooKeeper());
                }
                catch (const Coordination::Exception &)
                {
                    /// The exception when you try to zookeeper_init usually happens if DNS does not work. We will try to do it again.
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);

                    if (first_time)
                        storage.startup_event.set();
                    task->scheduleAfter(retry_period_ms);
                    return;
                }

                if (!need_stop && !tryStartup())
                {
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
                CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);

            first_time = false;
        }

        static constexpr UInt64 check_delay_period = 60;

        time_t current_time = time(nullptr);
        if (current_time >= prev_time_of_check_delay + static_cast<time_t>(check_delay_period))
        {
            prev_time_of_check_delay = current_time;

            storage.updateLeaderPriority();
            storage.updateAbsoluteDelayOfDependencies();

            /// We give up leadership if the relative lag is greater than threshold.
            if (storage.is_leader && storage.checkYieldLeaderShip())
            {
                LOG_INFO(log, "Will yield leadership.");

                ProfileEvents::increment(ProfileEvents::ReplicaYieldLeadership);

                storage.exitLeaderElection();
                /// NOTE: enterLeaderElection() can throw if node creation in ZK fails.
                /// This is bad because we can end up without a leader on any replica.
                /// In this case we rely on the fact that the session will expire and we will reconnect.
                if (!storage.stop_consume)
                    storage.enterLeaderElection();
            }
        }
    }
    catch (...)
    {
        need_retry = true;
        storage.startup_event.set();
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(check_period_ms);
}

bool HaKafkaRestartingThread::tryStartup()
{
    try
    {
        activateReplica();

        if (!storage.stop_consume)
            storage.enterLeaderElection();

        storage.partial_shutdown_called = false;
        storage.partial_shutdown_event.reset();
        return true;
    }
    catch (...)
    {
        storage.replica_is_active_node  = nullptr;
        storage.absolute_delay_node = nullptr;

        try
        {
            throw;
        }
        catch (const Coordination::Exception & e)
        {
            LOG_ERROR(log, "Couldn't start replication: {}, {}, stack trace:\n {}", e.what(), e.displayText(), e.getStackTraceString());
            return false;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::REPLICA_IS_ALREADY_ACTIVE)
                throw;

            LOG_ERROR(log, "Couldn't start replication: {}, {}, stack trace:\n {}", e.what(), e.displayText(), e.getStackTraceString());
            return false;
        }
    }
}

void HaKafkaRestartingThread::activateReplica()
{
    auto zookeeper = storage.getZooKeeper();

    auto address = storage.getHaKafkaAddress();

    String is_active_path = storage.replica_path + "/is_active";

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

        if (code != Coordination::Error::ZNONODE)
            throw Coordination::Exception(code, is_active_path);
    }

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(is_active_path, active_node_identifier, zkutil::CreateMode::Ephemeral));
    ops.emplace_back(zkutil::makeSetRequest(storage.replica_path + "/host", address.toString(), -1));

    try
    {
        zookeeper->multi(ops);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
            throw Exception("Replica " + storage.replica_path + " appears to be already active. If you're sure it's not, "
                    "try again in a minute or remove znode " + storage.replica_path + "/is_active manually", ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);
        throw;
    }

    /// `current_zookeeper` lives for the lifetime of `replica_is_active_node`,
    ///  since before changing `current_zookeeper`, `replica_is_active_node` object is destroyed in `partialShutdown` method.
    storage.replica_is_active_node = zkutil::EphemeralNodeHolder::existing(is_active_path, *storage.current_zookeeper);
}

void HaKafkaRestartingThread::partialShutdown()
{
    ProfileEvents::increment(ProfileEvents::ReplicaPartialShutdown);

    storage.partial_shutdown_called = true;
    storage.partial_shutdown_event.set();
    storage.replica_is_active_node = nullptr;
    storage.absolute_delay_node = nullptr;

    LOG_TRACE(log, "Waiting for threads to finish");

    storage.exitLeaderElection();

    LOG_TRACE(log, "Threads finished");
}

void HaKafkaRestartingThread::shutdown()
{
    /// Stop restarting_thread before stopping other tasks - so that it won't restart them again.
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "Restarting thread finished");

    /// Stop other tasks.
    partialShutdown();

    /// shut down memory table
   /// storage.shutdownMemoryTable();
}

} // end of namespace DB

#endif
