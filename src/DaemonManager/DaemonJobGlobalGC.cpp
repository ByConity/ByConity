/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Core/UUID.h>
#include <DaemonManager/DMDefines.h>
#include <DaemonManager/DaemonFactory.h>
#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/DaemonJobGlobalGC.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <MergeTreeCommon/GlobalGCManager.h>
#include <iterator>
#include <netdb.h>

namespace DB::DaemonManager
{

std::ostream & operator << (std::ostream & os, const std::vector<std::pair<String, long>> & a)
{
    for (auto & p : a)
        os << "Host name: "<< p.first << ", number of table can send: " << p.second << '\n';
    return os;
}

std::ostream & operator << (std::ostream & os, const std::set<String> & a)
{
    std::copy(a.begin(), a.end(), std::ostream_iterator<String>(os, " , "));
    return os;
}

CnchServerClientPtr findClient(
    const std::vector<CnchServerClientPtr> & server_clients,
    const String & rpc_address)
{
    auto it = std::find_if(server_clients.begin(), server_clients.end(),
        [& rpc_address] (const CnchServerClientPtr & client)
        {
            return (client->getRPCAddress() == rpc_address);
        });

    if (it == server_clients.end())
        return nullptr;
    else
        return *it;
}

namespace GlobalGCHelpers
{
//smallest move to the beginning
std::vector<std::pair<String, long>> sortByValue(
    std::vector<std::pair<String, long>> && num_of_table_can_send
)
{
    std::sort(num_of_table_can_send.begin(), num_of_table_can_send.end(),
        [] (const std::pair<String, long> & lhs, const std::pair<String, long> & rhs)
            {
                return (lhs.second < rhs.second);
            });
    return std::move(num_of_table_can_send);
}
/// create the wrapper function for testing
bool sendToServerForGCImpl(CnchServerClient & client, const std::vector<DB::Protos::DataModelTable> & tables_need_gc)
{
    return client.scheduleGlobalGC(tables_need_gc);
}

bool sendToServerForGC(
    const std::vector<DB::Protos::DataModelTable> & tables_need_gc,
    std::vector<std::pair<String, long>> & num_of_table_can_send_sorted,
    const std::vector<CnchServerClientPtr> & server_clients,
    ToServerForGCSender sender,
    LoggerPtr log)
{
    LOG_DEBUG(log, "send {} table to server for GC, they are", tables_need_gc.size());
    for (size_t i = 0; i < tables_need_gc.size(); ++i)
    {
        LOG_DEBUG(log, "table {} : {}.{}", i + 1 , tables_need_gc[i].database(), tables_need_gc[i].name());
    }

    while (!num_of_table_can_send_sorted.empty())
    {
        std::pair<String, long> & server_info = num_of_table_can_send_sorted.back();

        if (static_cast<size_t>(server_info.second) < tables_need_gc.size())
        {
            LOG_WARNING(log, "the number of table need to send bigger than the number of table can send, logic error!");
            num_of_table_can_send_sorted.clear();
            return false;
        }

        CnchServerClientPtr client = findClient(server_clients, server_info.first);
        if (!client)
        {
            LOG_ERROR(log, "can't find client, logic error!");
            num_of_table_can_send_sorted.pop_back();
            continue;
        }

        bool ret = sender(*client.get(), tables_need_gc);
        if (ret)
        {
            server_info.second -= tables_need_gc.size();
            if (server_info.second <= 0)
                num_of_table_can_send_sorted.pop_back();
            else
                num_of_table_can_send_sorted = GlobalGCHelpers::sortByValue(std::move(num_of_table_can_send_sorted));
            return true;
        }
        else
        {
            LOG_WARNING(log, "can't send to server, misconfig or something wrong in server, need investigate");
            num_of_table_can_send_sorted.pop_back();
            continue;
        }
    }

    return false;
}
} /// end namespace GlobalGCHelpers

std::vector<CnchServerClientPtr> getServerClients(
    const Context & context,
    CnchTopologyMaster & topology_master,
    LoggerPtr log)
{
    std::vector<CnchServerClientPtr> res;
    std::list<CnchServerTopology> server_topologies = topology_master.getCurrentTopology();
    if (server_topologies.empty())
    {
        LOG_ERROR(log, "Server topology is empty, something wrong with topology, return empty result");
        return res;
    }

    HostWithPortsVec host_ports = server_topologies.back().getServerList();

    for (const auto & host_port : host_ports)
    {
        String rpc_address = host_port.getRPCAddress();
        CnchServerClientPtr client_ptr = context.getCnchServerClientPool().get(host_port);
        if (!client_ptr)
            continue;
        else
            res.push_back(client_ptr);
    }

    return res;
}

std::vector<std::pair<String, long>> getNumOfTablesCanSend(
    const std::vector<CnchServerClientPtr> & clients,
    LoggerPtr log)
{
    std::vector<std::pair<String, long>> res;
    for (const auto & client : clients)
    {
        if (!client)
            continue;
        String rpc_address = client->getRPCAddress();
        try
        {
            UInt64 num = client->getNumOfTablesCanSendForGlobalGC();
            if (num == 0)
                continue;
            if (num > static_cast<UInt64>(std::numeric_limits<long>::max()))
            {
                LOG_ERROR(log, "The number of table can send return from : {} , is too big, programming error",
                    rpc_address);
                num = GlobalGCManager::MAX_BATCH_WORK_SIZE * 10;
            }
            res.push_back(std::make_pair(rpc_address, num));
        }
        catch (...)
        {
            LOG_INFO(log, "Failed to reach server: {}, detail: ", rpc_address);
            tryLogCurrentException(log, "Failed to reach server: " + rpc_address);
        }
    }
    return res;
}

std::set<String> getDeletingTablesFromServers(
    const std::vector<CnchServerClientPtr> & clients,
    LoggerPtr log)
{
    std::set<UUID> uuids;
    for (const auto & client : clients)
    {
        if (!client)
            continue;
        String rpc_address = client->getRPCAddress();
        try
        {
            std::set<UUID> result_in_one_server = client->getDeletingTablesInGlobalGC();
            uuids.insert(result_in_one_server.begin(), result_in_one_server.end());
        }
        catch (...)
        {
            LOG_INFO(log, "Failed to reach server: {}, detail: ", rpc_address);
            tryLogCurrentException(log, "Failed to reach server: " + rpc_address);
        }
    }
    std::set<String> res;
    std::transform(uuids.begin(), uuids.end(), std::inserter(res, res.end()),
        [] (const UUID & uuid) { return UUIDHelpers::UUIDToString(uuid); });
    return res;
}

namespace
{
bool tryNext(Catalog::IMetaStore::IteratorPtr & trash_table_it)
{
    /// if the next() call got exception for some reason like iterator not found, return false so that iterator is reinitialized later.
    bool ret = false;
    try
    {
        ret = trash_table_it->next();
    }
    catch (Exception &)
    {
        tryLogDebugCurrentException(__PRETTY_FUNCTION__);
    }
    return ret;
}
}

DaemonJobGlobalGC::DaemonJobGlobalGC(ContextMutablePtr global_context_)
    : DaemonJob{std::move(global_context_), CnchBGThreadType::GlobalGC}
    , catalog(getContext()->getCnchCatalog())
{}

void DaemonJobGlobalGC::cleanSnapshotsIfNeeded(TxnTimestamp ts)
{
    auto context = getContext();
    if (snapshot_clean_watch.elapsedSeconds() < context->getSettingsRef().snapshot_clean_interval.totalSeconds())
        return;
    snapshot_clean_watch.restart();

    try
    {
        Stopwatch watch;
        size_t num_removed = 0;
        LOG_DEBUG(log, "Start to clear ttl expired snapshots at {}", ts.toSecond());
        Catalog::Catalog::DataModelDBs all_db_models = catalog->getAllDataBases();
        for (const auto & db_model : all_db_models)
        {
            UUID db_uuid = db_model.has_uuid() ? RPCHelpers::createUUID(db_model.uuid()) : UUIDHelpers::Nil;
            if (db_uuid == UUIDHelpers::Nil)
                continue;
            auto snapshots = catalog->getAllSnapshots(db_uuid);
            for (const auto & snapshot : snapshots)
            {
                UInt64 commit_timestamp = TxnTimestamp(snapshot->commit_time()).toSecond();
                if (commit_timestamp + (snapshot->ttl_in_days() * 3600 * 24) < ts.toSecond())
                {
                    try
                    {
                        catalog->removeSnapshot(db_uuid, snapshot->name());
                        num_removed++;
                        LOG_INFO(
                            log,
                            "Removed ttl expired snapshot {} from {} ({})",
                            toString(*snapshot),
                            db_model.name(),
                            UUIDHelpers::UUIDToString(db_uuid));
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, fmt::format("Failed to remove snapshot {}.{}", db_model.name(), snapshot->name()));
                    }
                }
            }
        }
        LOG_DEBUG(log, "Total removed {} snapshots from {} databases, took {} ms", num_removed, all_db_models.size(), watch.elapsedMilliseconds());
        snapshot_clean_watch.restart();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to clean ttl expired snapshots");
    }
}

bool DaemonJobGlobalGC::executeImpl()
{
    auto context = getContext();
    TxnTimestamp ts = context->tryGetTimestamp();
    if (ts == TxnTimestamp::maxTS())
    {
        ts = TxnTimestamp::fromUnixTimestamp(std::time(nullptr));
        LOG_INFO(log, "TSO isn't available, use wall clock timestamp {} instead", ts.toSecond());
    }

    cleanSnapshotsIfNeeded(ts);

    const UInt64 retention_sec = context->getSettingsRef().cnch_data_retention_time_in_sec.value;

    std::shared_ptr<CnchTopologyMaster> topology_master = context->getCnchTopologyMaster();
    if (!topology_master)
    {
        LOG_ERROR(log, "Failed to get topology master, skip iteration");
        return false;
    }

    const std::vector<CnchServerClientPtr> clients = getServerClients(*context, *topology_master, log);
    std::vector<std::pair<String, long>> num_of_table_can_send = getNumOfTablesCanSend(clients, log);
    if (num_of_table_can_send.empty())
    {
        LOG_INFO(log, "Server already have many work, skip iteration");
        return false;
    }

    {
        std::ostringstream oss;
        oss << "number of tables can send: " << num_of_table_can_send;
        LOG_INFO(log, oss.str());
    }

    std::vector<std::pair<String, long>> num_of_table_can_send_sorted = GlobalGCHelpers::sortByValue(std::move(num_of_table_can_send));

    std::set<String> deleting_uuids_from_servers = getDeletingTablesFromServers(clients, log);

    {
        std::ostringstream oss;
        oss << "deleting uuids from servers: " << deleting_uuids_from_servers;
        LOG_INFO(log, oss.str());
    }

    Stopwatch watch;
    bool new_iteration = false;
    if (!trash_table_it)
    {
        trash_table_it = catalog->getTrashTableIDIterator(BYTEKV_BATCH_SCAN);
        new_iteration = true;
    }
    UInt64 milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "get trash table iterator took {} ms.", milliseconds);
    while(!num_of_table_can_send_sorted.empty())
    {
        if (!tryNext(trash_table_it))
        {
            if (new_iteration)
                break;
            else
            {
                trash_table_it = catalog->getTrashTableIDIterator(BYTEKV_BATCH_SCAN);
                new_iteration = true;
                continue;
            }
        }

        if (this->tables_need_gc.size() >= GlobalGCManager::MAX_BATCH_WORK_SIZE)
        {
            bool ret = GlobalGCHelpers::sendToServerForGC(
                this->tables_need_gc,
                num_of_table_can_send_sorted,
                clients,
                GlobalGCHelpers::sendToServerForGCImpl,
                log);
            if (!ret)
                break;
            else
                this->tables_need_gc.clear();
        }

        Protos::TableIdentifier table_id{};
        table_id.ParseFromString(trash_table_it->value());
        LOG_TRACE(
            log,
            "Found trash table, db={}(uuid={}), name={}(uuid={})",
            table_id.database(),
            table_id.has_db_uuid() ? UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table_id.db_uuid())) : "",
            table_id.name(),
            table_id.uuid());

        if (deleting_uuids_from_servers.find(table_id.uuid()) != deleting_uuids_from_servers.end())
            continue;

        String failure_reason;
        std::optional<DB::Protos::DataModelTable> table_model = DB::GlobalGCHelpers::getCleanableTrashTable(context, table_id, ts, retention_sec, &failure_reason);
        if (table_model.has_value())
        {
            this->tables_need_gc.push_back(*table_model);
        } else {
            LOG_TRACE(log, "Table {} is not cleanable, skip. reason: {}", table_id.uuid(), failure_reason);
        }
    }

    if (!this->tables_need_gc.empty() && !num_of_table_can_send_sorted.empty())
    {
        if (GlobalGCHelpers::sendToServerForGC(
                this->tables_need_gc,
                num_of_table_can_send_sorted,
                clients,
                GlobalGCHelpers::sendToServerForGCImpl,
                log)
        )
            this->tables_need_gc.clear();
    }

    /// Clean database
    auto db_models = catalog->getDatabaseInTrash();

    for (const auto & db : db_models)
    {
        if (TxnTimestamp(db.commit_time()).toSecond() < (ts.toSecond() - retention_sec))
        {
            // remove meta from catalog
            LOG_INFO(log, "Remove db meta for db {}", db.name());
            catalog->clearDatabaseMeta(db.name(), db.commit_time());
        }
    }

    LOG_INFO(log, "Finish executeImpl in global GC");
    return true;
}

void registerGlobalGCDaemon(DaemonFactory & factory)
{
    factory.registerLocalDaemonJob<DaemonJobGlobalGC>("GLOBAL_GC");
}

}
