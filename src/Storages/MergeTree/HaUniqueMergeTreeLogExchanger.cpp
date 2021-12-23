#include <Storages/MergeTree/HaUniqueMergeTreeLogExchanger.h>

#include <IO/ConnectionTimeoutsContext.h>
#include <Storages/MergeTree/HaMergeTreeReplicaClient.h>
#include <Storages/StorageHaUniqueMergeTree.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int REPLICA_STATUS_CHANGED;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
}

HaUniqueMergeTreeLogExchangerBase::HaUniqueMergeTreeLogExchangerBase(StorageHaUniqueMergeTree & storage_, const String & log_name_)
    : storage(storage_), log_name(storage.getStorageID().getFullTableName() + log_name_), log(&Poco::Logger::get(log_name))
{
}

void HaUniqueMergeTreeLogExchangerBase::sendPing()
{
    std::lock_guard lock(client_mutex);
    requestUnlocked(lock, false, nullptr, [](HaMergeTreeReplicaClient & connection) { connection.ping(); });
}

bool HaUniqueMergeTreeLogExchangerBase::tryMarkLostReplica(zkutil::ZooKeeperPtr & zookeeper, const String & replica)
{
    try
    {
        Coordination::Stat self_is_lost_stat;
        auto self_is_lost = zookeeper->get(storage.replica_path + "/is_lost", &self_is_lost_stat);
        if (self_is_lost == "1")
        {
            LOG_INFO(log, "Replica self is lost, cannot mark other replica lost.");
            return false;
        }

        Coordination::Stat host_stat;
        zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/host", &host_stat);
        auto is_lost = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/is_lost");

        /// Maybe someone has already marked it.
        if (is_lost == "1")
            return true;

        Coordination::Requests ops;
        ops.push_back(zkutil::makeCheckRequest(storage.replica_path + "/is_lost", self_is_lost_stat.version));
        ops.push_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas/" + replica + "/host", host_stat.version));
        ops.push_back(zkutil::makeSetRequest(storage.zookeeper_path + "/replicas/" + replica + "/is_lost", "1", -1));

        Coordination::Responses resps;
        auto code = zookeeper->tryMulti(ops, resps);
        if (code == Coordination::Error::ZBADVERSION)
            throw Exception(
                "Replica self became lost or " + replica + " became active while marking lost replica.",
                ErrorCodes::REPLICA_STATUS_CHANGED);
        else
            zkutil::KeeperMultiException::check(code, ops, resps);

        LOG_DEBUG(log, "Mark {} as lost replica.", replica);
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        return false;
    }
}

void HaUniqueMergeTreeLogExchangerBase::updateReplicaUnlocked(
    zkutil::ZooKeeperPtr & zookeeper, const String & replica, std::lock_guard<std::mutex> &)
{
    auto & replica_stat = replica_stats[replica];
    auto old_replica_status = replica_stat.status;

    if (zookeeper->exists(storage.zookeeper_path + "/replicas/" + replica + "/is_active"))
    {
        /// Active replica
        replica_stat.status = ReplicaStat::ACTIVE;
    }
    else
    {
        if (ReplicaStat::LOST != replica_stat.status)
        {
            /// Set lost replica status directly
            if ("1" == zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/is_lost"))
                replica_stat.status = ReplicaStat::LOST;
        }

        auto current_time = time(nullptr);
        if (ReplicaStat::ACTIVE == replica_stat.status)
        {
            /// Failed for the first time
            replica_stat.status = ReplicaStat::FAILED;
            replica_stat.first_failed_time = current_time;
        }
        else if (ReplicaStat::FAILED == replica_stat.status)
        {
            /// Check lost timeout
            if (static_cast<UInt64>(current_time - replica_stat.first_failed_time) > storage.getSettings()->ha_mark_lost_replica_timeout)
            {
                if (tryMarkLostReplica(zookeeper, replica))
                    replica_stat.status = ReplicaStat::LOST;
            }
        }
    }

    if (old_replica_status != replica_stat.status)
    {
        LOG_DEBUG(
            log,
            "Replica {} status changed: {} -> {}.",
            replica,
            ReplicaStat::toString(old_replica_status),
            ReplicaStat::toString(replica_stat.status));
    }
}

void HaUniqueMergeTreeLogExchangerBase::resetReplicaStatusAndConnection()
{
    std::lock_guard lock(client_mutex);
    /// make sure the following requestUnlocked(..) call won't fail with "DB::Exception: Attempt to read after eof"
    /// due to dead connections
    connections.clear();
    last_replica_stats_update_time = 0;
    updateAllReplicasUnlocked(true, lock);
}

void HaUniqueMergeTreeLogExchangerBase::updateAllReplicasUnlocked(bool force, std::lock_guard<std::mutex> & lock)
{
    auto storage_settings = storage.getSettings();

    time_t current_time = time(nullptr);
    auto interval = force ? storage_settings->ha_update_replica_stats_min_period : storage_settings->ha_update_replica_stats_period;
    if (static_cast<UInt64>(current_time - last_replica_stats_update_time) < interval)
        return;
    last_replica_stats_update_time = current_time;

    try
    {
        auto zookeeper = storage.getZooKeeper();

        auto replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");
        for (auto & replica : replicas)
        {
            if (replica == storage.replica_name)
                continue;
            updateReplicaUnlocked(zookeeper, replica, lock);
        }

        for (auto iter = replica_stats.begin(); iter != replica_stats.end();)
        {
            if (replicas.end() == std::find(replicas.begin(), replicas.end(), iter->first))
            {
                LOG_INFO(log, "Replica: {} no longer exists, remove it from request list.", iter->first);
                replica_stats.erase(iter++);
            }
            else
                ++iter;
        }
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;

        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

HaMergeTreeReplicaClientPtr & HaUniqueMergeTreeLogExchangerBase::connectUnlocked(const String & replica, std::lock_guard<std::mutex> &)
{
    if (auto iter = connections.find(replica); iter != connections.end())
    {
        return iter->second;
    }

    auto target_replica_path = storage.zookeeper_path + "/replicas/" + replica;
    HaMergeTreeAddress address(storage.getZooKeeper()->get(target_replica_path + "/host"));

    auto & settings = storage.getContext()->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings);
    auto connection = std::make_shared<HaMergeTreeReplicaClient>(
        address.host, address.ha_port, replica, storage.zookeeper_path + "/replicas/" + replica, storage.replica_name, "", "", timeouts);
    connection->connect();
    return connections.emplace(replica, connection).first->second;
}

void HaUniqueMergeTreeLogExchangerBase::requestReplicaUnlocked(
    std::lock_guard<std::mutex> & client_lock, const String & replica_name, std::function<void(HaMergeTreeReplicaClient &)> && action)
{
    updateAllReplicasUnlocked(true, client_lock);

    auto it = replica_stats.find(replica_name);
    if (it == replica_stats.end())
        throw Exception("Replica " + replica_name + " doesn't exist", ErrorCodes::REPLICA_STATUS_CHANGED);

    if (it->second.status != ReplicaStat::ACTIVE)
        throw Exception("Replica " + replica_name + " is not active", ErrorCodes::REPLICA_STATUS_CHANGED);

    try
    {
        auto connection = connectUnlocked(replica_name, client_lock);
        action(*connection);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        /// LOG_ERROR(log, e.displayText());
        /// Packet may not have been fully send/received to/from the socket,
        /// therefore we must erase the connection in order to get a clean connection next time.
        connections.erase(replica_name);
        throw;
    }
}

void HaUniqueMergeTreeLogExchangerBase::requestUnlocked(
    std::lock_guard<std::mutex> & lock,
    bool update_replicas,
    bool * out_all_success,
    std::function<void(HaMergeTreeReplicaClient &)> && action)
{
    updateAllReplicasUnlocked(update_replicas, lock);

    bool all_success{true};
    for (auto & [replica, stat] : replica_stats)
    {
        if (stat.status == ReplicaStat::FAILED)
            all_success = false;

        if (stat.status != ReplicaStat::ACTIVE)
            continue;

        try
        {
            auto connection = connectUnlocked(replica, lock);
            action(*connection);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, e.displayText());

            all_success = false;
            /// Although the replica may not be in `connections`
            connections.erase(replica);
        }
    }
    if (!all_success)
        updateAllReplicasUnlocked(true, lock);

    if (out_all_success)
        *out_all_success = all_success;
}

void HaUniqueMergeTreeLogExchangerBase::checkReplicaIsActive(const String & replica_name, std::lock_guard<std::mutex> &) const
{
    auto it = replica_stats.find(replica_name);
    if (it == replica_stats.end())
        throw Exception("Replica " + replica_name + " not found", ErrorCodes::NO_ACTIVE_REPLICAS);

    if (it->second.status != ReplicaStat::ACTIVE)
        throw Exception(
            "Replica " + replica_name + " is not active, but " + ReplicaStat::toString(it->second.status), ErrorCodes::NO_ACTIVE_REPLICAS);
}

bool HaUniqueMergeTreeLogExchangerBase::isActiveReplica(const String & replica) const
{
    std::lock_guard lock(client_mutex);
    auto it = replica_stats.find(replica);
    return it != replica_stats.end() && it->second.status == ReplicaStat::ACTIVE;
}

bool HaUniqueMergeTreeLogExchangerBase::isLostReplica(const String & replica) const
{
    std::lock_guard lock(client_mutex);
    auto it = replica_stats.find(replica);
    return it == replica_stats.end() || it->second.status == ReplicaStat::LOST;
}

Strings HaUniqueMergeTreeLogExchangerBase::getActiveReplicas() const
{
    Strings active_replicas;
    std::lock_guard lock(client_mutex);
    for (auto it = replica_stats.begin(); it != replica_stats.end(); ++it)
        if (it->second.status == ReplicaStat::ACTIVE)
            active_replicas.push_back(it->first);
    return active_replicas;
}

Strings HaUniqueMergeTreeLogExchangerBase::getLostOrFailedReplicas() const
{
    Strings lost_replicas;
    std::lock_guard lock(client_mutex);
    for (auto it = replica_stats.begin(); it != replica_stats.end(); ++it)
        if (it->second.status != ReplicaStat::ACTIVE)
            lost_replicas.push_back(it->first);
    return lost_replicas;
}

HaUniqueMergeTreeLogExchanger::HaUniqueMergeTreeLogExchanger(StorageHaUniqueMergeTree & storage_)
    : HaUniqueMergeTreeLogExchangerBase(storage_, " (HaUniqueMergeTreeLogExchanger)")
{
}

ManifestStore::LogEntries HaUniqueMergeTreeLogExchanger::getLogEntries(const String & replica_name, UInt64 from, UInt64 limit)
{
    std::lock_guard lock(client_mutex);
    updateAllReplicasUnlocked(true, lock);
    checkReplicaIsActive(replica_name, lock);
    return connectUnlocked(replica_name, lock)->fetchManifestLogs(from, limit);
}

ManifestStore::Snapshot HaUniqueMergeTreeLogExchanger::getManifestSnapshot(const String & replica_name, UInt64 version)
{
    std::lock_guard lock(client_mutex);
    updateAllReplicasUnlocked(true, lock);
    checkReplicaIsActive(replica_name, lock);
    return connectUnlocked(replica_name, lock)->getManifestSnapshot(version);
}

std::map<String, ManifestStatus> HaUniqueMergeTreeLogExchanger::getAllManifestStatus()
{
    std::lock_guard lock(client_mutex);
    std::map<String, ManifestStatus> res;
    requestUnlocked(lock, true, nullptr, [&res](HaMergeTreeReplicaClient & connection) {
        auto status = connection.getManifestStatus();
        res.emplace(connection.getRemoteReplica(), status);
    });
    return res;
}

} // end of namespace DB
