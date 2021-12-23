#pragma once

#include <utility>
#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>

#include <Core/BackgroundSchedulePool.h>
#include <Storages/MergeTree/HaConnectionMessages.h>
#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <Storages/MergeTree/LSNStatus.h>
#include <Storages/MergeTree/ManifestStore.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
class StorageHaUniqueMergeTree;
class HaReplicaEndpointHolder;

class HaMergeTreeReplicaClient;
using HaMergeTreeReplicaClientPtr = std::shared_ptr<HaMergeTreeReplicaClient>;

class HaUniqueMergeTreeLogExchangerBase : boost::noncopyable
{
public:
    explicit HaUniqueMergeTreeLogExchangerBase(StorageHaUniqueMergeTree & storage_, const String & log_name_);
    virtual ~HaUniqueMergeTreeLogExchangerBase() = default;

    /// note that replica status is updated periodically, the following methods
    /// returns cached states, not necessarily the latest
    bool isActiveReplica(const String & replica) const;
    bool isLostReplica(const String & replica) const;
    Strings getActiveReplicas() const;
    Strings getLostOrFailedReplicas() const;
    /// reload in-memory replica status from zookeeper and clear existing connection
    void resetReplicaStatusAndConnection();

    void sendPing();

protected:
    bool tryMarkLostReplica(zkutil::ZooKeeperPtr & zookeeper, const String & replica);
    void updateReplicaUnlocked(zkutil::ZooKeeperPtr & zookeeper, const String & replica, std::lock_guard<std::mutex> &);
    void updateAllReplicasUnlocked(bool force, std::lock_guard<std::mutex> &);
    HaMergeTreeReplicaClientPtr & connectUnlocked(const String & replica, std::lock_guard<std::mutex> &);
    void checkReplicaIsActive(const String & replica_name, std::lock_guard<std::mutex> &) const;

    /// execute the given action on all non-lost replicas.
    /// - update_replicas: whether to update replica statuses based on ZK before request
    /// - all_success: if not nullptr, *all_success is set to whether the action succeeded on all non-lost replicas
    void requestUnlocked(
        std::lock_guard<std::mutex> & client_lock,
        bool update_replicas,
        bool * all_success,
        std::function<void(HaMergeTreeReplicaClient &)> && action);

    /// execute the given action on the specified replica.
    /// throws exception if any error happens
    void requestReplicaUnlocked(
        std::lock_guard<std::mutex> & client_lock, const String & replica_name, std::function<void(HaMergeTreeReplicaClient &)> && action);

    struct ReplicaStat
    {
        enum Flag
        {
            ACTIVE,
            FAILED,
            LOST
        };
        time_t first_failed_time{0};
        Flag status{ACTIVE};

        static const char * toString(Flag s)
        {
            switch (s)
            {
                case ACTIVE:
                    return "ACTIVE";
                case FAILED:
                    return "FAILED";
                case LOST:
                    return "LOST";
            }
        }
    };

    StorageHaUniqueMergeTree & storage;
    String log_name;
    Poco::Logger * log{nullptr};

    mutable std::mutex client_mutex;
    std::map<String, HaMergeTreeReplicaClientPtr> connections;
    std::map<String, ReplicaStat> replica_stats;
    time_t last_replica_stats_update_time{0};
};

class HaUniqueMergeTreeLogExchanger : public HaUniqueMergeTreeLogExchangerBase
{
public:
    HaUniqueMergeTreeLogExchanger(StorageHaUniqueMergeTree & storage_);

    ManifestStore::LogEntries getLogEntries(const String & replica_name, UInt64 from, UInt64 limit);
    ManifestStore::Snapshot getManifestSnapshot(const String & replica_name, UInt64 version);
    std::map<String, ManifestStatus> getAllManifestStatus();
};

} // end of namespace DB
