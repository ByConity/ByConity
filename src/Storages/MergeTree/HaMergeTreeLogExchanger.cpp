#include <Storages/MergeTree/HaMergeTreeLogExchanger.h>

#include <Storages/MergeTree/HaMergeTreeReplicaClient.h>
#include <Storages/StorageHaMergeTree.h>
#include <IO/ConnectionTimeoutsContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int REPLICA_STATUS_CHANGED;
extern const int NO_ACTIVE_REPLICAS;
extern const int NO_SUCH_INTERSERVER_IO_ENDPOINT;
}

/* ------------------------------------------------------------------------- */

HaMergeTreeLogExchangerBase::HaMergeTreeLogExchangerBase(StorageHaMergeTree & storage_, const String & log_name_)
    : storage(storage_), log_name(storage.getStorageID().getFullTableName() + log_name_), log(&Poco::Logger::get(log_name))
{
}

/// HaLogExchanger

HaMergeTreeLogExchanger::HaMergeTreeLogExchanger(StorageHaMergeTree & storage_)
    : HaMergeTreeLogExchangerBase(storage_, " (HaMergeTreeLogExchanger)"), queue(storage_.getSettings()->ha_log_exchanger_queue_max_size)
{
    task = storage.global_context.getSchedulePool().createTask(log_name, [this] { requestTask(); });
    /// TODO: task = storage.global_context.getHaLogSchedulePool().createTask(log_name, [this] { requestTask(); });
    task->schedule();
}

void HaMergeTreeLogExchangerBase::sendPing()
{
    std::lock_guard lock(client_mutex);
    requestUnlocked(lock, false, nullptr, [](HaMergeTreeReplicaClient & connection) { connection.ping(); });
}

void HaMergeTreeLogExchanger::sendLogEntries(const std::vector<LogEntryPtr> & entries)
{
    std::lock_guard lock(client_mutex);
    requestUnlocked(lock, true, nullptr, [&entries](HaMergeTreeReplicaClient & connection) { connection.putLogEntries(entries); });
}

void HaMergeTreeLogExchanger::requestTask()
{
    /// TODO: add a setting in the future
    static constexpr auto POOL_MS = 500;

    auto size = queue.size();
    if (size > 0)
    {
        LogEntryPtr entry;
        std::vector<LogEntryPtr> entries;
        for (size_t i = 0; i < size; ++i)
        {
            queue.pop(entry);
            if (entry)
                entries.push_back(entry);
        }

        if (0 == entries.size())
        {
            sendPing();
        }
        else
        {
            sendLogEntries(entries);
            LOG_TRACE(log, "Sent {} entries", entries.size());
        }
    }

    task->scheduleAfter(POOL_MS);
}

bool HaMergeTreeLogExchangerBase::tryMarkLostReplica(zkutil::ZooKeeperPtr & zookeeper, const String & replica)
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
            throw Exception("Replica self became lost or " + replica + " became active while marking lost replica.", ErrorCodes::REPLICA_STATUS_CHANGED);
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

void HaMergeTreeLogExchangerBase::updateReplicaUnlocked(
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
        LOG_DEBUG(log, "Replica {} status changed: {} -> {}.", replica, ReplicaStat::toString(old_replica_status), ReplicaStat::toString(replica_stat.status));
    }
}

void HaMergeTreeLogExchangerBase::resetReplicaStatusAndConnection()
{
    std::lock_guard lock(client_mutex);
    /// make sure the following requestUnlocked(..) call won't fail with "DB::Exception: Attempt to read after eof"
    /// due to dead connections
    connections.clear();
    last_replica_stats_update_time = 0;
    updateAllReplicasUnlocked(true, lock);
}

void HaMergeTreeLogExchangerBase::updateAllReplicasUnlocked(bool force, std::lock_guard<std::mutex> & lock)
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

HaMergeTreeReplicaClientPtr & HaMergeTreeLogExchangerBase::connectUnlocked(const String & replica, std::lock_guard<std::mutex> &)
{
    if (auto iter = connections.find(replica); iter != connections.end())
    {
        return iter->second;
    }

    auto target_replica_path = storage.zookeeper_path + "/replicas/" + replica;
    HaMergeTreeAddress address(storage.getZooKeeper()->get(target_replica_path + "/host"));

    auto & settings = storage.global_context.getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings);
    // ConnectionTimeouts timeouts(
    //     settings.connect_timeout, /// default 10s
    //     SettingSeconds(60), /// send_timeout
    //     SettingSeconds(60), /// receive_timeout
    //     settings.tcp_keep_alive_timeout);

    auto connection = std::make_shared<HaMergeTreeReplicaClient>(
        address.host,
        address.ha_port,
        replica,
        storage.zookeeper_path + "/replicas/" + replica,
        storage.replica_name,
        "",
        "",
        timeouts);
    connection->connect();
    return connections.emplace(replica, connection).first->second;
}

void HaMergeTreeLogExchangerBase::requestReplicaUnlocked(
    std::lock_guard<std::mutex> & client_lock,
    const String & replica_name,
    std::function<void(HaMergeTreeReplicaClient &)> && action)
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
    catch (const Exception & )
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        /// LOG_ERROR(log, e.displayText());
        /// Packet may not have been fully send/received to/from the socket,
        /// therefore we must erase the connection in order to get a clean connection next time.
        connections.erase(replica_name);
        throw;
    }
}

void HaMergeTreeLogExchangerBase::requestUnlocked(
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

void HaMergeTreeLogExchangerBase::checkReplicaIsActive(const String & replica_name, std::lock_guard<std::mutex> &) const
{
    auto it = replica_stats.find(replica_name);
    if (it == replica_stats.end())
        throw Exception("Replica " + replica_name + " not found", ErrorCodes::NO_ACTIVE_REPLICAS);

    if (it->second.status != ReplicaStat::ACTIVE)
        throw Exception("Replica " + replica_name + " is not active, but " + ReplicaStat::toString(it->second.status), ErrorCodes::NO_ACTIVE_REPLICAS);
}

void HaMergeTreeLogExchanger::ping()
{
    queue.emplace(nullptr);
}

void HaMergeTreeLogExchanger::putLogEntry(const HaMergeTreeLogEntry & e)
{
    queue.push({std::make_shared<HaMergeTreeLogEntry>(e)});
}

void HaMergeTreeLogExchanger::putLogEntry(const HaMergeTreeLogEntryPtr & entry)
{
    queue.push(entry);
}

void HaMergeTreeLogExchanger::putLogEntries(const std::vector<HaMergeTreeLogEntryPtr> & entries)
{
    for (auto & entry : entries)
        queue.push(entry);
}

std::vector<HaMergeTreeLogEntryPtr> HaMergeTreeLogExchanger::fetchLogEntries(const std::vector<UInt64> & lsns, bool & all_success)
{
    std::lock_guard lock(client_mutex);
    std::vector<HaMergeTreeLogEntryPtr> total;

    requestUnlocked(lock, true, &all_success, [&total, &lsns](HaMergeTreeReplicaClient & connection) {
        auto res = connection.fetchLogEntries(lsns);
        total.insert(total.end(), res.begin(), res.end());
    });

    std::sort(total.begin(), total.end(), HaMergeTreeLogEntry::lsn_less_compare);
    total.erase(std::unique(total.begin(), total.end(), HaMergeTreeLogEntry::lsn_equal_compare), total.end());
    return total;
}

std::vector<std::pair<String, Int64>> HaMergeTreeLogExchanger::getDelays()
{
    std::lock_guard lock(client_mutex);

    std::vector<std::pair<String, Int64>> delays;
    requestUnlocked(lock, true, nullptr, [&delays](auto & connection) {
        auto res = connection.getDelay();
        delays.emplace_back(connection.getRemoteReplica(), res);
    });

    return delays;
}

std::vector<std::pair<String, UInt64>> HaMergeTreeLogExchanger::checkPartExist(const String & part_name)
{
    std::lock_guard lock(client_mutex);

    std::vector<std::pair<String, UInt64>> results;
    requestUnlocked(lock, true, nullptr, [&](auto & connection) {
        UInt64 remote_num_send = 0;
        if (connection.checkPartExist(part_name, remote_num_send))
            results.emplace_back(connection.getRemoteReplica(), remote_num_send);
    });
    return results;
}

std::vector<HaMergeTreeLogExchanger::ReplicaWithPartAndPayload> HaMergeTreeLogExchanger::findActiveContainingPart(const String & part_name, bool & all_success)
{
    std::lock_guard lock(client_mutex);

    std::vector<ReplicaWithPartAndPayload> results;
    requestUnlocked(lock, true, &all_success, [&](auto & connection) {
        ReplicaWithPartAndPayload res;
        auto parts = connection.findActiveContainingPart({part_name}, res.payload);
        if (parts.size() == 1 && !parts[0].empty()) /// replica has containing part
        {
            res.containing_part = parts[0];
            res.replica = connection.getRemoteReplica();
            results.push_back(res);
        }
    });
    return results;
}

Strings HaMergeTreeLogExchanger::findActiveContainingParts(const String & replica_name, const Strings & parts)
{
    std::lock_guard lock(client_mutex);
    Strings res;
    requestReplicaUnlocked(lock, replica_name, [&] (auto & connection) {
        UInt64 num_sends;
        res = connection.findActiveContainingPart(parts, num_sends);
    });
    return res;
}

LSNStatus HaMergeTreeLogExchanger::getLSNStatus(const String & replica_name)
{
    std::lock_guard lock(client_mutex);
    LSNStatus res {};
    requestReplicaUnlocked(lock, replica_name, [&] (auto & connection) {
        res = connection.getLSNStatus();
    });
    return res;
}

std::vector<std::pair<String, LSNStatus>> HaMergeTreeLogExchanger::getLSNStatusList(bool * all_success)
{
    std::lock_guard lock(client_mutex);

    std::vector<std::pair<String, LSNStatus>> results;
    requestUnlocked(lock, true, all_success, [&](auto & connection) {
        results.emplace_back(connection.getRemoteReplica(), connection.getLSNStatus());
    });
    return results;
}

std::map<String, GetMutationStatusResponse> HaMergeTreeLogExchanger::getMutationStatus(const String & mutation_id)
{
    std::lock_guard lock(client_mutex);

    std::map<String, GetMutationStatusResponse> results;
    requestUnlocked(lock, true, nullptr, [&](auto & connection) {
        results.emplace(connection.getRemoteReplica(), connection.getMutationStatus(mutation_id));
    });
    return results;
}

bool HaMergeTreeLogExchangerBase::isActiveReplica(const String & replica) const
{
    std::lock_guard lock(client_mutex);
    auto it = replica_stats.find(replica);
    return it != replica_stats.end() && it->second.status == ReplicaStat::ACTIVE;
}

bool HaMergeTreeLogExchangerBase::isLostReplica(const String & replica) const
{
    std::lock_guard lock(client_mutex);
    auto it = replica_stats.find(replica);
    return it == replica_stats.end() || it->second.status == ReplicaStat::LOST;
}

Strings HaMergeTreeLogExchangerBase::getActiveReplicas() const
{
    Strings active_replicas;
    std::lock_guard lock(client_mutex);
    for (auto it = replica_stats.begin(); it != replica_stats.end(); ++it)
        if (it->second.status == ReplicaStat::ACTIVE)
            active_replicas.push_back(it->first);
    return active_replicas;
}

Strings HaMergeTreeLogExchangerBase::getLostOrFailedReplicas() const
{
    Strings lost_replicas;
    std::lock_guard lock(client_mutex);
    for (auto it = replica_stats.begin(); it != replica_stats.end(); ++it)
        if (it->second.status != ReplicaStat::ACTIVE)
            lost_replicas.push_back(it->first);
    return lost_replicas;
}

/*
HaUniqueMergeTreeLogExchanger::HaUniqueMergeTreeLogExchanger(StorageHaMergeTree & storage_)
    : HaMergeTreeLogExchangerBase(storage_, " (HaUniqueMergeTreeLogExchanger)")
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
    requestUnlocked(lock, true, nullptr, [&res](HaMergeTreeReplicaClient & connection)
    {
        auto status = connection.getManifestStatus();
        res.emplace(connection.getRemoteReplica(), status);
    });
    return res;
}
*/

} // end of namespace DB
