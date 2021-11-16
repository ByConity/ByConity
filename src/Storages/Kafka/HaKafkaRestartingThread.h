#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <atomic>
#include <common/logger_useful.h>
#include <Core/Types.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

class StorageHaKafka;

class HaKafkaRestartingThread
{
public:
    HaKafkaRestartingThread(StorageHaKafka & storage_);

    void start() { task->activateAndSchedule(); }

    void wakeup() { task->schedule(); }

    void wakeupForCheckLeadership()
    {
        prev_time_of_check_delay = 0;
        wakeup();
    }

    void shutdown();

private:
    StorageHaKafka & storage;
    String log_name;
    Poco::Logger * log;
    std::atomic<bool> need_stop {false};

    /// The random data we wrote into `/replicas/me/is_active`.
    String active_node_identifier;

    BackgroundSchedulePool::TaskHolder task;
    Int64 check_period_ms;                  /// The frequency of checking expiration of session in ZK.
    bool first_time = true;                 /// Activate replica for the first time.
    bool need_retry = false;
    time_t prev_time_of_check_delay = 0;
    bool startup_completed = false;

    void run();

    /// Start or stop background threads. Used for partial reinitialization when re-creating a session in ZooKeeper.
    bool tryStartup(); /// Returns false if ZooKeeper is not available.

    /// Note in ZooKeeper that this replica is currently active.
    void activateReplica();

    void partialShutdown();
};

} // end of namespace DB

#endif
