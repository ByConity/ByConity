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

#include <DaemonManager/DaemonJobServerBGThread.h>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <DaemonManager/DaemonFactory.h>
#include <DaemonManager/DaemonHelper.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/StorageMaterializedView.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB::DaemonManager
{

StorageTrait constructStorageTrait(StoragePtr storage);

DaemonJobServerBGThread::DaemonJobServerBGThread(
    ContextMutablePtr global_context_,
    CnchBGThreadType type_,
    std::unique_ptr<IBackgroundJobExecutor> bg_job_executor_,
    std::unique_ptr<IBGJobStatusPersistentStoreProxy> status_persistent_store_proxy,
    std::unique_ptr<ITargetServerCalculator> target_server_calculator_)
    : DaemonJob(std::move(global_context_), type_),
      status_persistent_store{std::move(status_persistent_store_proxy)},
      bg_job_executor{std::move(bg_job_executor_)},
      target_server_calculator{std::move(target_server_calculator_)}
{}

void fixKafkaActiveStatuses(DaemonJobServerBGThread * daemon_job);
void DaemonJobServerBGThread::init()
{
    if (getType() == CnchBGThreadType::Consumer)
        fixKafkaActiveStatuses(this);
    status_persistent_store =
        std::make_unique<BGJobStatusInCatalog::CatalogBGJobStatusPersistentStoreProxy>(getContext()->getCnchCatalog(), type, getLog());
    bg_job_executor = std::make_unique<BackgroundJobExecutor>(*getContext(), getType());
    target_server_calculator = std::make_unique<TargetServerCalculator>(*getContext(), getType(), getLog());
    /// fetchCnchBGThreadStatus must be called after initialisation of status_persistent_store and etc
    background_jobs = fetchCnchBGThreadStatus();
    DaemonJob::init();
}

std::unordered_map<UUID, StorageID> getUUIDsFromCatalog(DaemonJobServerBGThread & daemon_job)
{
    const Context & context = *daemon_job.getContext();
    std::unordered_map<UUID, StorageID> ret;
    LoggerPtr log = daemon_job.getLog();

    if (daemon_job.getType() == CnchBGThreadType::MaterializedMySQL)
    {
        auto data_models = context.getCnchCatalog()->getAllDataBases();
        for (const auto & data_model : data_models)
        {
            if (Status::isDetached(data_model.status()) || Status::isDeleted(data_model.status()))
                continue;

            if (data_model.has_type() && data_model.type() == DB::Protos::CnchDatabaseType::MaterializedMySQL)
            {
                auto uuid = RPCHelpers::createUUID(data_model.uuid());
                StorageID storage_id(data_model.name(), uuid);

                try
                {
                    /// For MaterializedMySQL, using nullptr for StoragePtr param
                    if (daemon_job.ifNeedDaemonJob(StorageTrait{}, storage_id))
                        ret.insert(std::make_pair(uuid, storage_id));
                }
                catch(...)
                {
                    LOG_WARNING(log, "Fail to schedule for " + storage_id.getFullTableName() + ". Error: " + getCurrentExceptionMessage(true));
                }
            }
        }
    }
    else    /// For Table daemon job
    {
        auto data_models = context.getCnchCatalog()->getAllTables();
        for (const auto & data_model : data_models)
        {
            if (Status::isDetached(data_model.status()) || Status::isDeleted(data_model.status()))
                continue;

            auto uuid = RPCHelpers::createUUID(data_model.uuid());
            StorageID storage_id(data_model.database(), data_model.name(), uuid);
            if (!data_model.server_vw_name().empty())
                storage_id.server_vw_name = data_model.server_vw_name();

            try
            {
                StorageTrait storage_trait;
                if (auto cache = daemon_job.getStorageTraitCache(); cache)
                {
                    auto res = cache->getOrSet(data_model.definition(), [&]()
                    {
                        StorageTrait s = constructStorageTrait(
                            daemon_job.getContext(),
                            data_model.database(),
                            data_model.name(),
                            data_model.definition()
                        );
                        return std::make_shared<StorageTrait>(s);
                    });
                    storage_trait = *res.first;
                }
                else
                {
                    storage_trait = constructStorageTrait(
                        daemon_job.getContext(),
                        data_model.database(),
                        data_model.name(),
                        data_model.definition());
                }

                if (daemon_job.ifNeedDaemonJob(storage_trait, storage_id))
                    ret.insert(std::make_pair(uuid, storage_id));
            }
            catch (Exception & e)
            {
                LOG_WARNING(log, "Fail to schedule for {}.{}. Error: ", data_model.database(), data_model.name(), e.message());
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
            catch (...)
            {
                LOG_WARNING(log, "Fail to construct storage for {}.{}", data_model.database(), data_model.name());
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        }
    }

    return ret;
}


const std::vector<String> getServersInTopology(Context & context, LoggerPtr log)
{
    std::vector<String> ret;
    std::shared_ptr<CnchTopologyMaster> topology_master = context.getCnchTopologyMaster();
    if (!topology_master)
    {
        LOG_ERROR(log, "Failed to get topology master");
        return ret;
    }

    std::list<CnchServerTopology> server_topologies = topology_master->getCurrentTopology();
    if (server_topologies.empty())
    {
        LOG_ERROR(log, "Server topology is empty, something wrong with topology");
        return ret;
    }

    HostWithPortsVec host_ports = server_topologies.back().getServerList();

    std::transform(host_ports.begin(), host_ports.end(), std::back_inserter(ret),
        [] (const HostWithPorts & host_port)
        {
            return host_port.getRPCAddress();
        }
    );

    return ret;
}

std::vector<String> findAliveServers(const std::map<String, UInt64> & new_server_start_time)
{
    std::vector<String> ret;
    std::transform(new_server_start_time.begin(), new_server_start_time.end()
        , std::back_inserter(ret),
        [] (const auto & p) { return p.first;}
    );
    return ret;
}

bool checkIfServerDied(const std::vector<String> & alive_host_port, const String & host_port)
{
    return (alive_host_port.end() ==
        std::find(alive_host_port.begin(), alive_host_port.end(), host_port));
}

std::map<String, UInt64> fetchServerStartTimes(Context & context, CnchTopologyMaster & topology_master, LoggerPtr log)
{
    std::map<String, UInt64> ret;
    std::list<CnchServerTopology> server_topologies = topology_master.getCurrentTopology();
    if (server_topologies.empty())
    {
        LOG_ERROR(log, "Server topology is empty, something wrong with topology, this iteration will be skip!");
        return ret;
    }

    HostWithPortsVec host_ports = server_topologies.back().getServerList();

    for (const auto & host_port : host_ports)
    {
        String rpc_address = host_port.getRPCAddress();
        CnchServerClientPtr client_ptr = context.getCnchServerClientPool().get(host_port);
        if (!client_ptr)
        {
            LOG_WARNING(log, "Not able to connect to server with rpc address {}", rpc_address);
            continue;
        }

        try
        {
            UInt64 ts = client_ptr->getServerStartTime();
            ret.insert(std::make_pair(rpc_address, ts));
        }
        catch (...)
        {
            LOG_INFO(log, "Failed to reach server with rpc address: {}", rpc_address);
        }
    }

    if (ret.size() != host_ports.size())
    {
        LOG_WARNING(log, "There is network partition, return empty result to skip this iteration");
        ret.clear();
    }

    return ret;
}

std::vector<String> DaemonJobServerBGThread::updateServerStartTimeAndFindRestartServers(const std::map<String, UInt64> & new_server_start_time)
{
    std::vector<String> ret;
    std::for_each(new_server_start_time.begin(), new_server_start_time.end(),
        [& ret, this] (const auto & p)
        {
            if (auto it = this->server_start_times.find(p.first); it != this->server_start_times.end())
            {
                if (p.second != it->second)
                    ret.push_back(p.first);
            }
        });

    server_start_times = new_server_start_time;
    return ret;
}

std::unordered_map<UUID, String> getAllTargetServerForBGJob(
    const BackgroundJobs & bg_jobs,
    UInt64 ts,
    DaemonJobServerBGThread & daemon_job)
{
    LoggerPtr log = daemon_job.getLog();
    std::unordered_map<UUID, String> ret;
    for (const auto & p : bg_jobs)
    {
        StorageID storage_id = p.second->getStorageID();
        CnchServerClientPtr client_ptr = nullptr;
        try
        {
            client_ptr = daemon_job.getTargetServer(storage_id, ts);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, " Got exception {}. {} when getTargetServer for {}",
                e.code(), e.displayText(), storage_id.getNameForLogs());
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }

        if (!client_ptr)
        {
            LOG_WARNING(log, "Failed to getTargetServer for {}", storage_id.getNameForLogs());
            continue;
        }

        ret.insert(std::make_pair(p.first, client_ptr->getRPCAddress()));
    }

    return ret;
}

/// TODO: pass const CnchTopologyMaster
ServerInfo DaemonJobServerBGThread::findServerInfo(
    const std::map<String, UInt64> & new_server_start_times,
    const BackgroundJobs & bg_jobs)
{
    ServerInfo ret;
    ret.alive_servers = findAliveServers(new_server_start_times);
    if (ret.alive_servers.empty())
        return ret;
    ret.target_host_map = getAllTargetServerForBGJob(bg_jobs, getContext()->getTimestamp(), *this);
    if (ret.target_host_map.empty())
        return ret;
    ret.restarted_servers = updateServerStartTimeAndFindRestartServers(new_server_start_times);
    return ret;
}

UpdateResult getUpdateBGJobs(
    const BackgroundJobs & background_jobs,
    const std::unordered_map<UUID, StorageID> & new_uuid_map,
    const std::vector<String> & alive_servers
)
{
    /// using tuple because StorageID less than operator and equal isn't appropriate
    using StorageIDData = std::tuple<UUID, String, String, String>;
    std::set<StorageIDData> new_storage_id_set;
    std::transform(new_uuid_map.begin(), new_uuid_map.end(),
        std::inserter(new_storage_id_set, new_storage_id_set.end()),
        [] (const auto & p) {
            return std::make_tuple(
                p.second.uuid,
                p.second.database_name,
                p.second.table_name,
                p.second.server_vw_name);
        });

    std::set<StorageIDData> current_storage_id_set;
    std::transform(background_jobs.begin(), background_jobs.end(),
        std::inserter(current_storage_id_set, current_storage_id_set.end()),
        [] (const auto & p) {
            return std::make_tuple(
                p.second->getStorageID().uuid,
                p.second->getStorageID().database_name,
                p.second->getStorageID().table_name,
                p.second->getStorageID().server_vw_name);
        });

    std::vector<StorageIDData> add_storage_ids;
    std::vector<StorageIDData> remove_storage_id_candidates;
    std::set_difference(current_storage_id_set.begin(), current_storage_id_set.end(),
        new_storage_id_set.begin(), new_storage_id_set.end(),
        std::back_inserter(remove_storage_id_candidates));

    std::set_difference(new_storage_id_set.begin(), new_storage_id_set.end(),
        current_storage_id_set.begin(), current_storage_id_set.end(),
        std::back_inserter(add_storage_ids));

    UUIDs add_uuids;
    UUIDs remove_uuid_candidates;

    std::transform(add_storage_ids.begin(), add_storage_ids.end(),
        std::inserter(add_uuids, add_uuids.end()),
        [] (const StorageIDData & s) { return std::get<0>(s); });

    std::transform(remove_storage_id_candidates.begin(), remove_storage_id_candidates.end(),
        std::inserter(remove_uuid_candidates, remove_uuid_candidates.end()),
        [] (const StorageIDData & s) { return std::get<0>(s); });

    UUIDs remove_uuids;
    std::for_each(background_jobs.begin(), background_jobs.end(),
        [& remove_uuids] (auto & p)
        {
            if (p.second->isRemoved() && (p.second->getJobExpectedStatus() == CnchBGThreadStatus::Removed))
                remove_uuids.insert(p.first);
        }
    );

    std::for_each(
        remove_uuid_candidates.begin(),
        remove_uuid_candidates.end(),
        [& remove_uuids, & background_jobs, & alive_servers] (const UUID & uuid)
        {
            auto it = background_jobs.find(uuid);
            if (it != background_jobs.end())
            {
                bool server_died = checkIfServerDied(alive_servers, it->second->getHostPort());
                if (!server_died)
                {
                    Result ret = it->second->remove(CnchBGThreadAction::Drop, false);
                    if (ret.res)
                        remove_uuids.insert(uuid);
                }
                else
                    remove_uuids.insert(uuid);
            }
        }
    );

    return UpdateResult{std::move(remove_uuids), std::move(add_uuids)};
}

void syncServerBGJob(
    DaemonJobServerBGThread & daemon_job,
    BackgroundJobPtr & job_from_dm,
    std::vector<BGJobInfoFromServer> jobs_from_server)
{
    LoggerPtr log = daemon_job.getLog();
    StorageID storage_id = job_from_dm->getStorageID();
    String job_from_dm_host_port = job_from_dm->getHostPort();
    CnchBGThreadStatus job_from_dm_status = job_from_dm->getJobStatus();
    std::ostringstream log_oss;
    log_oss << "syncServerBGJob: storage id: " << storage_id.getNameForLogs()
        << ", info in DM: [host_port " << job_from_dm_host_port << ", status "
        << toString(job_from_dm_status) << "], info in server:";

    for (size_t i = 0; i < jobs_from_server.size(); ++i)
    {
        log_oss << " [job " << i << " host_port " << jobs_from_server[i].host_port
            << " status " << toString(jobs_from_server[i].status) << "]";
    }
    LOG_INFO(log, log_oss.str());

    std::vector<BGJobInfoFromServer> same_host_port_jobs;
    for (BGJobInfoFromServer & job_from_server : jobs_from_server)
    {
        if (job_from_server.storage_id != storage_id)
        {
            LOG_ERROR(log, "syncServerBGJob: server storage_id is different, error in program logic {}",
                job_from_server.storage_id.getNameForLogs());
            continue;
        }

        if (job_from_server.host_port != job_from_dm_host_port)
        {
            LOG_INFO(log, "syncServerBGJob: remove {} from {}",
                storage_id.getNameForLogs(), job_from_server.host_port);
            daemon_job.getBgJobExecutor().remove(storage_id, job_from_server.host_port);
        }
        else
        {
            same_host_port_jobs.push_back(job_from_server);
        }
    }

    if (same_host_port_jobs.empty())
    {
        if (job_from_dm_status == CnchBGThreadStatus::Running)
        {
            LOG_INFO(log, "syncServerBGJob: same host port job size is empty while the job in DM is running, start job");
            job_from_dm->start(false);
        }
    }
    else if (same_host_port_jobs.size() > 1)
    {
        LOG_ERROR(log, "syncServerBGJob: same host port size > 1, error in program logic");
    }
    else
    {
        BGJobInfoFromServer same_host_port_job = same_host_port_jobs[0];
        if ((job_from_dm_status == CnchBGThreadStatus::Running)
                && (same_host_port_job.status != CnchBGThreadStatus::Running))
        {
            LOG_INFO(log, "syncServerBGJob: job from dm is running but job from server isn't, start job");
            job_from_dm->start(false);
        }
        else if ((job_from_dm_status == CnchBGThreadStatus::Stopped)
                && (same_host_port_job.status == CnchBGThreadStatus::Running))
        {
            LOG_INFO(log, "syncServerBGJob: job from dm isn't running but job from server is, stop job");
            job_from_dm->stop(true, false);
        }
    }
}

void runMissingAndRemoveDuplicateJob(
    DaemonJobServerBGThread & daemon_job,
    BackgroundJobs & check_jobs,
    const std::unordered_multimap<UUID, BGJobInfoFromServer> & jobs_from_server)
{
    LoggerPtr log = daemon_job.getLog();
    std::for_each(check_jobs.begin(), check_jobs.end(),
        [& jobs_from_server, & log, & daemon_job] (auto & p)
        {
            BackgroundJobPtr & job = p.second;
            int count = jobs_from_server.count(p.first);
            if (count == 0)
            {
                if (job->isRunning())
                {
                    LOG_INFO(log, "There is no running job for missing_job {} in server, will run this missing job",
                        job->getStorageID().getNameForLogs());
                    job->start(false);
                }
            }
            else if (count == 1)
            {
                auto it = jobs_from_server.find(p.first);
                const BGJobInfoFromServer & job_from_server = it->second;
                if ((job_from_server.host_port != job->getHostPort())
                    || (job_from_server.status != job->getJobStatus()))
                {
                    LOG_INFO(log, "There are different between job info in server and DM {}"
                        , job->getStorageID().getNameForLogs());
                    syncServerBGJob(daemon_job, job, {job_from_server});
                }
            }
            else
            {
                LOG_INFO(log, "There are more than 1 jobs in server for {}"
                    , job->getStorageID().getNameForLogs());
                std::vector<BGJobInfoFromServer> duplicate_jobs_from_server;
                auto range = jobs_from_server.equal_range(p.first);
                for (auto it = range.first; it != range.second; ++it)
                    duplicate_jobs_from_server.push_back(it->second);

                syncServerBGJob(daemon_job, job, std::move(duplicate_jobs_from_server));
            }
        });
}

std::vector<BGJobInfoFromServer> findZombieJobsInServer(
    const std::set<UUID> & new_bg_jobs_uuids,
    BackgroundJobs & managed_job_exclude_new_job,
    const std::unordered_multimap<UUID, BGJobInfoFromServer> & jobs_from_server)
{
    std::vector<BGJobInfoFromServer> zombie_jobs;

    std::for_each(jobs_from_server.begin(), jobs_from_server.end(),
        [&] (const std::pair<UUID, BGJobInfoFromServer> & job_from_server)
        {
            UUID job_from_server_uuid = job_from_server.first;
            if ((new_bg_jobs_uuids.contains(job_from_server_uuid)) ||
                (managed_job_exclude_new_job.contains(job_from_server_uuid)))
                return;
            zombie_jobs.push_back(job_from_server.second);
        });

    return zombie_jobs;
}

void removeZombieJobsInServer(
    const Context & context,
    DaemonJobServerBGThread & daemon_job,
    const std::vector<BGJobInfoFromServer> & zombie_jobs
)
{
    LoggerPtr log = daemon_job.getLog();
    std::for_each(zombie_jobs.begin(), zombie_jobs.end(), [&] (const BGJobInfoFromServer & j)
        {
            LOG_INFO(log, "Will drop zombie thread for job type {}, table {} on host {}", toString(daemon_job.getType()), j.storage_id.getNameForLogs(), j.host_port);
            context.getCnchServerClientPool().get(j.host_port)->controlCnchBGThread(j.storage_id, daemon_job.getType(), CnchBGThreadAction::Drop);
            LOG_INFO(log, "Droped zombie thread for job type {}, table {} on host {}", toString(daemon_job.getType()), j.storage_id.getNameForLogs(), j.host_port);
        });
}

std::optional<std::unordered_multimap<UUID, BGJobInfoFromServer>> fetchBGThreadFromServer(
    Context & context,
    CnchBGThreadType type,
    LoggerPtr log,
    const std::vector<String> & servers
)
{
    std::unordered_multimap<UUID, BGJobInfoFromServer> bg_jobs_data;
    try
    {
        for (const String & server : servers)
        {
            CnchServerClientPtr client_ptr = context.getCnchServerClientPool().get(server);
            if (!client_ptr)
            {
                LOG_ERROR(log, "Not able to connect to server with address {}", server);
                return {};
            }
            auto tasks = client_ptr->getBackGroundStatus(type);
            LOG_DEBUG(log, "Get {} jobs from server with address {}", tasks.size(), server);
            for (const auto & task : tasks)
            {
                StorageID storage_id = RPCHelpers::createStorageID(task.storage_id());
                CnchBGThreadStatus task_status = CnchBGThreadStatus(task.status());
                bg_jobs_data.insert(std::make_pair(storage_id.uuid, BGJobInfoFromServer{storage_id, task_status, server}));
            }
        }
    }
    catch(...)
    {
        tryLogCurrentException(log, "Failed to get jobs in servers for liveness check");
        bg_jobs_data.clear();
    }

    if (bg_jobs_data.empty())
        return {};
    return bg_jobs_data;
}

size_t checkLivenessIfNeed(
    size_t counter,
    size_t liveness_check_interval,
    Context & context,
    DaemonJobServerBGThread & daemon_job,
    BackgroundJobs & check_bg_jobs,
    const std::set<UUID> & new_bg_jobs_uuids,
    const std::vector<String> & servers,
    BGJobsFromServersFetcher fetch_bg_jobs_from_server /*fetchBGThreadFromServer*/
)
{
    const CnchBGThreadType type = daemon_job.getType();
    LoggerPtr log = daemon_job.getLog();
    if ((counter % liveness_check_interval) != 0)
        return counter + 1;

    LOG_INFO(log, "Check liveness start");
    Stopwatch watch;
    auto bg_jobs_from_server =
        fetch_bg_jobs_from_server(context, type, log, servers);
    UInt64 milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "fetch bg jobs from server took {} ms.", milliseconds);

    if (!bg_jobs_from_server)
        return counter;
    runMissingAndRemoveDuplicateJob(daemon_job, check_bg_jobs, bg_jobs_from_server.value());
    removeZombieJobsInServer(
        context,
        daemon_job,
        findZombieJobsInServer(new_bg_jobs_uuids, check_bg_jobs, bg_jobs_from_server.value()));
    return counter + 1;
}

/// every failed call on BackgroundJob in this function will be retried on next time
bool DaemonJobServerBGThread::executeImpl()
{
    if (suspended())
    {
        LOG_DEBUG(log, "thread suspended");
        std::unique_lock lock(bg_jobs_mutex);
        background_jobs.clear();
        return true;
    }

    Context & context = *getContext();
    std::shared_ptr<CnchTopologyMaster> topology_master = context.getCnchTopologyMaster();
    if (!topology_master)
    {
        LOG_ERROR(log, "Failed to get topology master");
        return false;
    }

    UInt64 milliseconds = 0;
    Stopwatch watch;
    BackgroundJobs background_jobs_clone;
    {
        std::shared_lock shared_lock(bg_jobs_mutex);
        background_jobs_clone = background_jobs;
    }

    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "copy background jobs took {} ms.", milliseconds);

    watch.restart();
    std::unordered_map<UUID, StorageID> new_uuid_map = getUUIDsFromCatalog(*this);
    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "getUUIDsFromCatalog with size {} took {} ms.", new_uuid_map.size(), milliseconds);

    std::map<String, UInt64> new_server_start_times = fetchServerStartTimes(context, *topology_master, log);
    if (new_server_start_times.empty())
    {
        LOG_WARNING(log, "There are network partition, skip this iteration");
        return false;
    }

    const std::vector<String> alive_servers = findAliveServers(new_server_start_times);
    watch.restart();
    UpdateResult update_res = getUpdateBGJobs(background_jobs_clone, new_uuid_map, alive_servers);
    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "getUpdateBGJobs took {} ms.", milliseconds);

    const UUIDs & remove_uuids = update_res.remove_uuids;
    for (auto uuid : remove_uuids)
        LOG_DEBUG(log, "UUID: {} will be removed from background jobs", UUIDHelpers::UUIDToString(uuid));

    const UUIDs & add_uuids = update_res.add_uuids;
    for (auto uuid : add_uuids)
        LOG_DEBUG(log, "UUID: {} will be added into background jobs", UUIDHelpers::UUIDToString(uuid));

    std::vector<BackgroundJobPtr> new_bg_jobs;

    watch.restart();
    if ((!remove_uuids.empty()) || (!add_uuids.empty()))
    {
        {
            std::unique_lock lock(bg_jobs_mutex);
            std::for_each(remove_uuids.begin(), remove_uuids.end(), [this] (UUID uuid)
                {
                    background_jobs.erase(uuid);
                });
        }

        /// never hold a bg_jobs_mutex while call BackgroundJob ctor
        /// because this mutex is using in the callback of brpc
        std::map<UUID, BackgroundJobPtr> new_background_jobs;
        std::for_each(add_uuids.begin(), add_uuids.end(), [this, & new_uuid_map, & new_background_jobs] (UUID uuid)
        {
            new_background_jobs.insert(std::make_pair(uuid, std::make_shared<BackgroundJob>(new_uuid_map.at(uuid), *this)));
        });

        std::unique_lock lock(bg_jobs_mutex);
        std::for_each(new_background_jobs.begin(), new_background_jobs.end(), [this, &new_bg_jobs] (auto & p)
            {
                auto ret = background_jobs.insert(p);
                if (ret.second)
                    new_bg_jobs.push_back(ret.first->second);
            });
    }

    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "update bg jobs took {} ms.", milliseconds);

    std::for_each(remove_uuids.begin(), remove_uuids.end(), [& background_jobs_clone] (UUID uuid)
        {
            background_jobs_clone.erase(uuid);
        });

    std::for_each(new_bg_jobs.begin(), new_bg_jobs.end(), [& background_jobs_clone] (const BackgroundJobPtr & j)
        {
            background_jobs_clone.insert(std::make_pair(j->getUUID(), j));
        });


    if (background_jobs_clone.empty())
    {
        LOG_WARNING(log, "There is no jobs in background_jobs, skip sync");
        return true;
    }

    new_server_start_times = fetchServerStartTimes(context, *topology_master, log);
    if (new_server_start_times.empty())
    {
        LOG_WARNING(log, "There are network partition, skip sync");
        return false;
    }

    ServerInfo server_info = findServerInfo(new_server_start_times, background_jobs_clone);

    if (!server_info.restarted_servers.empty())
        LOG_INFO(log, "Found restart server!");

    if (server_info.alive_servers.empty())
    {
        LOG_WARNING(log, "Failed to found alive_servers, skip sync");
        return false;
    }

    if (server_info.target_host_map.empty())
    {
        LOG_WARNING(log, "no target host found, skip sync");
        return false;
    }

    {
        /// Scope for CacheClearer
        watch.restart();
        // fetch statuses in batch
        auto cache_clearer = status_persistent_store->fetchStatusesIntoCache();
        milliseconds = watch.elapsedMilliseconds();
        if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
            LOG_DEBUG(log, "fetch bg job statuses took {} ms.", milliseconds);

        watch.restart();
        const size_t max_thread_pool_size =
            context.getConfigRef().getInt("daemon_job_for_bg_thread_max_thread_pool_size", 10);
        const size_t thread_pool_size = std::min(server_info.alive_servers.size() * 3, max_thread_pool_size);
        ThreadPool thread_pool{thread_pool_size, thread_pool_size, thread_pool_size * 4, false};

        std::for_each(
            background_jobs_clone.begin(),
            background_jobs_clone.end(),
            [& server_info, & thread_pool] (const auto & p)
            {
                const BackgroundJobPtr & bg_job = p.second;
                thread_pool.scheduleOrThrowOnError(
                    [& bg_job, & server_info] ()
                    {
                        bg_job->sync(server_info);
                    });
            }
        );

        thread_pool.wait();
    }

    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "sync bg jobs took {} ms.", milliseconds);

    watch.restart();
    std::set<UUID> new_bg_job_uuids;
    std::transform(new_bg_jobs.begin(), new_bg_jobs.end(),
        std::inserter(new_bg_job_uuids, new_bg_job_uuids.end()),
        [] (const BackgroundJobPtr & j)
        {
            return j->getUUID();
        });
    counter_for_liveness_check = checkLivenessIfNeed(
        counter_for_liveness_check,
        liveness_check_interval,
        context,
        *this,
        background_jobs_clone,
        new_bg_job_uuids,
        server_info.alive_servers,
        fetchBGThreadFromServer
    );

    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "check liveness took {} ms.", milliseconds);
    return true;
}

CnchServerClientPtr DaemonJobServerBGThread::getTargetServer(const StorageID & storage_id, UInt64 ts) const
{
    return target_server_calculator->getTargetServer(storage_id, ts);
}

BGJobInfos DaemonJobServerBGThread::getBGJobInfos() const
{
    std::vector<BGJobInfo> res;
    BackgroundJobs background_jobs_clone;
    {
        std::shared_lock shared_lock(bg_jobs_mutex);
        background_jobs_clone = background_jobs;
    }

    std::transform(
        background_jobs_clone.begin(),
        background_jobs_clone.end(),
        std::back_inserter(res),
        [] (const std::pair<UUID, BackgroundJobPtr> & p)
            {
                return p.second->getBGJobInfo();
            }
        );
    return res;
}

BackgroundJobPtr DaemonJobServerBGThread::getBackgroundJob(const UUID & uuid) const
{
    std::shared_lock lock(bg_jobs_mutex);
    if (auto it = background_jobs.find(uuid); it != background_jobs.end())
    {
        return it->second;
    }

    return nullptr;
}

/// called from BRPC server, execute synchonously, no retry, persist job status to persistent storage
Result DaemonJobServerBGThread::executeJobAction(const StorageID & storage_id, CnchBGThreadAction action)
{
    Context & context = *getContext();
    LOG_DEBUG(log, "Executing a job action for storage id: {} {}", storage_id.empty() ? "empty storage" : storage_id.getNameForLogs(), toString(action));
    UUID uuid = storage_id.uuid;
    if (storage_id.empty())
    {
        switch (action)
        {
            case CnchBGThreadAction::Remove:
            {
                {
                    std::lock_guard<std::mutex> lock(suspended_mutex);
                    is_suspended = true;
                }
                const std::vector<String> servers = getServersInTopology(context, log);
                if (servers.empty())
                {
                    String error_msg = fmt::format("Failed to {} for all storage id because failed to get servers in topology", toString(action));
                    LOG_WARNING(log, error_msg);
                    return {error_msg, false};
                }

                for (auto & host_port : servers)
                {
                    CnchServerClientPtr server_client = context.getCnchServerClient(host_port);
                    server_client->controlCnchBGThread(StorageID::createEmpty(), type, action);
                    LOG_DEBUG(getLogger(__func__), "Succeed to {} all threads on {}",
                        toString(action), host_port);
                }

                break;
            }
            case CnchBGThreadAction::Start:
                {
                    std::lock_guard<std::mutex> lock(suspended_mutex);
                    is_suspended = false;
                }
                break;
            default:
                String error_msg = fmt::format("With empty storage_id, action {} is invalid", toString(action));
                return {error_msg, false};
        }
        return {"", true};
    }

    switch (action)
    {
        case CnchBGThreadAction::Remove:
        case CnchBGThreadAction::Drop:
        {
            auto bg_ptr = getBackgroundJob(uuid);
            if (!bg_ptr)
            {
                String error_msg{"No job for uuid: " + storage_id.getNameForLogs()};
                LOG_WARNING(log, error_msg);
                return {error_msg, false};
            }
            else
            {
                const std::vector<String> servers = getServersInTopology(context, log);
                if (servers.empty())
                {
                    String error_msg{String("Failed to ") + toString(action) + ": " + storage_id.getNameForLogs()
                        + " because failed to get servers in topology"};
                    LOG_WARNING(log, error_msg);
                    return {error_msg, false};
                }

                bool server_died = checkIfServerDied(servers, bg_ptr->getHostPort());
                if (server_died)
                {
                    std::unique_lock lock(bg_jobs_mutex);
                    background_jobs.erase(uuid);
                    return {"", true};
                }
                else
                {
                    return bg_ptr->remove(action, true);
                }
            }
        }
        case CnchBGThreadAction::Stop:
        {
            auto bg_ptr = getBackgroundJob(uuid);
            if (!bg_ptr)
            {
                LOG_INFO(log, "bg job doesn't exist for uuid: {} hence, create a stop job", storage_id.getNameForLogs());
                auto new_job = std::make_pair(uuid, std::make_shared<BackgroundJob>(storage_id, CnchBGThreadStatus::Stopped, *this, ""));
                std::unique_lock lock(bg_jobs_mutex);
                auto res = background_jobs.insert(std::move(new_job));
                if (!res.second)
                    bg_ptr = res.first->second;
                else
                    return {"", true};
            }
            if (bg_ptr)
                return bg_ptr->stop(false, true);
            break;
        }
        case CnchBGThreadAction::Start:
        {
            auto bg_ptr = getBackgroundJob(uuid);
            if (!bg_ptr)
            {
                auto new_job = std::make_pair(uuid, std::make_shared<BackgroundJob>(storage_id, *this));
                std::unique_lock lock(bg_jobs_mutex);
                auto res = background_jobs.insert(new_job);
                if (res.second)
                    bg_ptr = res.first->second;
                else
                {
                    LOG_INFO(log, "Failed to insert this uuid to background_jobs, the jobs probably has been started recently");
                    bg_ptr = res.first->second;
                }
            }

            if (bg_ptr)
            {
                const String current_host_port = bg_ptr->getHostPort();
                if ((!bg_ptr->isRemoved()) &&
                    (!current_host_port.empty()))
                {
                    const std::vector<String> servers = getServersInTopology(context, log);
                    if (servers.empty())
                    {
                        String error_str = "failed to remove: " + storage_id.getNameForLogs()
                            + " because failed to get servers in topology";
                        LOG_WARNING(log, error_str);
                        return {error_str, false};
                    }

                    bool server_died = checkIfServerDied(servers, current_host_port);
                    if (!server_died)
                    {
                        LOG_INFO(log, "remove bg job: {} in {} before start new job", storage_id.getNameForLogs(), current_host_port);
                        Result res = bg_ptr->remove(CnchBGThreadAction::Remove, false);
                        if (!res.res)
                            return res;
                    }
                }
                return bg_ptr->start(true);
            }
            break;
        }
        case CnchBGThreadAction::Wakeup:
        {
            auto bg_ptr = getBackgroundJob(uuid);
            if (!bg_ptr)
            {
                auto error_msg = fmt::format("bg job doesn't exist for table: {} hence, stop wakeup", storage_id.getNameForLogs());
                return {error_msg, false};
            }
            else
                return bg_ptr->wakeup();
        }
    }
    return {"", false};
}

void DaemonJobForMergeMutate::executeOptimize(const StorageID & storage_id, const String & partition_id, bool enable_try, bool mutations_sync, UInt64 timeout_ms) const
{
    auto bg_ptr = getBackgroundJob(storage_id.uuid);
    if (!bg_ptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "bg job doesn't exist for table: {}, stop wakeup", storage_id.getNameForLogs());
    }

    auto info = bg_ptr->getBGJobInfo();

    if (info.status != CnchBGThreadStatus::Running && info.status != CnchBGThreadStatus::Stopped)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Can't execute optimize, because background job: {} at server {} in {} status",
            storage_id.getNameForLogs(), info.host_port, toString(info.status));
    }

    CnchServerClientPtr server_client = getContext()->getCnchServerClient(info.host_port);
    server_client->executeOptimize(storage_id, partition_id, enable_try, mutations_sync, timeout_ms);
}

BackgroundJobs DaemonJobServerBGThread::fetchCnchBGThreadStatus()
{
    Stopwatch watch;
    watch.restart();
    // fetch statuses in batch
    auto cache_clearer = status_persistent_store->fetchStatusesIntoCache();
    UInt64 milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "fetch bg job statuses took {} ms", milliseconds);

    BackgroundJobs ret;
    CnchServerClientPtrs cnch_servers = getContext()->getCnchServerClientPool().getAll();

    for (const auto & cnch_server : cnch_servers)
    {
        if (!cnch_server)
        {
            LOG_WARNING(log, "Not able to connect to server with address {}", cnch_server->getRPCAddress());
            continue;
        }

        server_start_times[cnch_server->getRPCAddress()] = cnch_server->getServerStartTime();
        auto tasks = cnch_server->getBackGroundStatus(type);
        LOG_DEBUG(log, "Get {} jobs from server with address {}", tasks.size(), cnch_server->getRPCAddress());
        for (const auto & task : tasks)
        {
            StorageID storage_id = RPCHelpers::createStorageID(task.storage_id());

            auto task_status = CnchBGThreadStatus(task.status());
            LOG_DEBUG(log, "bg thread {} is {} on {}", storage_id.getNameForLogs(), toString(task_status), cnch_server->getRPCAddress());

            if (auto it = ret.find(storage_id.uuid); it != ret.end())
            {
                // find duplicate threads
                auto & info = it->second;

                if (task_status == CnchBGThreadStatus::Running)
                {
                    if (CnchBGThreadStatus::Running == info->getJobStatus())
                    {
                        // remove a duplicate running task
                        try
                        {
                            getBgJobExecutor().remove(storage_id, cnch_server->getRPCAddress());
                        }
                        catch (...)
                        {
                            tryLogCurrentException(log, "Fail to remove duplicated task: " + storage_id.getNameForLogs());
                        }
                    }
                    else
                    {
                        // one stopped and one running case, keep the running one
                        ret.erase(it);
                        ret.insert(std::make_pair(storage_id.uuid, std::make_shared<BackgroundJob>(storage_id, CnchBGThreadStatus::Running, *this, cnch_server->getRPCAddress())));
                    }
                }
                else if (task_status == CnchBGThreadStatus::Stopped)
                {
                    // remove duplicate stop task
                    try
                    {
                        getBgJobExecutor().remove(storage_id, cnch_server->getRPCAddress());
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, "Fail to remove duplicated task: " + storage_id.getNameForLogs());
                    }
                }
            }
            else
            {
                ret.insert(std::make_pair(storage_id.uuid, std::make_shared<BackgroundJob>(storage_id, task_status, *this, cnch_server->getRPCAddress())));
            }
        }
    }

    milliseconds = watch.elapsedMilliseconds();
    if (milliseconds >= SLOW_EXECUTION_THRESHOLD_MS)
        LOG_DEBUG(log, "fetchBackgroundJobsFromServer took {} ms.", milliseconds);
    return ret;
}

bool DaemonJobServerBGThread::suspended() const
{
    std::lock_guard<std::mutex> lock(suspended_mutex);
    return is_suspended;
}

bool isCnchMergeTree(const StorageTrait & storage_trait, const StorageID &, const ContextPtr &)
{
    return storage_trait.isCnchMergeTree();
}

bool isCnchTableWithManifest(const StorageTrait & storage_trait, const StorageID &, const ContextPtr &)
{
    return storage_trait.isCnchTableWithManifest();
}

bool isCnchKafka(const StorageTrait & storage_trait, const StorageID &, const ContextPtr &)
{
    return storage_trait.isCnchKafka();
}

bool isMaterializedMySQL(const StorageTrait &, const StorageID & storage_id, const ContextPtr & context)
{
    if (!storage_id.isDatabase())
        return false;
    auto database = context->getCnchCatalog()->getDatabase(storage_id.getDatabaseName(), context, TxnTimestamp::maxTS());
    return dynamic_cast<DatabaseCnchMaterializedMySQL *>(database.get()) != nullptr;
};

bool isCnchUniqueTableAndNeedDedup(const StorageTrait & storage_trait, const StorageID &, const ContextPtr &)
{
    return storage_trait.isCnchUniqueAndNeedDedup();
}

bool isCnchRefreshMaterializedView(const StorageTrait & storage_trait, const StorageID &, const ContextPtr & )
{
    return storage_trait.isCnchRefreshMaterializedView();
}

void registerServerBGThreads(DaemonFactory & factory)
{
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::PartGC, isCnchMergeTree>>("PART_GC");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForMergeMutate>("PART_MERGE");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::Clustering, isCnchMergeTree>>("PART_CLUSTERING");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::Consumer, isCnchKafka>>("CONSUMER");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::DedupWorker, isCnchUniqueTableAndNeedDedup>>("DEDUP_WORKER");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::ObjectSchemaAssemble, isCnchMergeTree>>("OBJECT_SCHEMA_ASSEMBLE");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::MaterializedMySQL, isMaterializedMySQL>>("MATERIALIZED_MYSQL");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::CnchRefreshMaterializedView, isCnchRefreshMaterializedView>>("CNCH_REFRESH_MATERIALIZED_VIEW");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::PartMover, isCnchMergeTree>>("PART_MOVER");
    factory.registerDaemonJobForBGThreadInServer<DaemonJobForCnch<CnchBGThreadType::ManifestCheckpoint, isCnchTableWithManifest>>("MANIFEST_CHECKPOINT");
}

void fixKafkaActiveStatuses(DaemonJobServerBGThread * daemon_job)
{
    LoggerPtr log = daemon_job->getLog();
    ContextMutablePtr context = daemon_job->getContext();
    std::shared_ptr<Catalog::Catalog> catalog = context->getCnchCatalog();
    auto data_models = catalog->getAllTables();
    for (const auto & data_model : data_models)
    {
        if (Status::isDetached(data_model.status()) || Status::isDeleted(data_model.status()))
            continue;

        try
        {
            /// make shortcut to avoid Hive/S3 Storage ctor
            auto ast = getASTCreateQueryFromString(data_model.definition(), daemon_job->getContext());
            if (ast->storage &&
                ast->storage->engine &&
                (!isCnchMergeTreeOrKafka(ast->storage->engine->name))
            )
                continue;

            StoragePtr storage = Catalog::CatalogFactory::getTableByDefinition(
                        daemon_job->getContext(),
                        data_model.database(),
                        data_model.name(),
                        data_model.definition());

            if (!storage)
            {
                LOG_WARNING(log, "Fail to get storagePtr for {}.{}", data_model.database(), data_model.name());
                continue;
            }

            if (daemon_job->ifNeedDaemonJob(constructStorageTrait(storage), storage->getStorageID()))
            {
                if (!catalog->getTableActiveness(storage, TxnTimestamp::maxTS()))
                {
                    LOG_INFO(log, "Found table {}.{} is inactive", data_model.database(), data_model.name());
                    catalog->setBGJobStatus(storage->getStorageID().uuid, CnchBGThreadType::Consumer, CnchBGThreadStatus::Stopped);
                    catalog->setTableActiveness(storage, true, TxnTimestamp::maxTS());
                }
            }
        }
        catch (Exception & e)
        {
            LOG_WARNING(log, "Fail to construct storage for {}.{}. Error: {}", data_model.database(), data_model.name(), e.message());
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        catch (...)
        {
            LOG_WARNING(log, "Fail to construct storage for {}.{}", data_model.database(), data_model.name());
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

StorageTrait::StorageTrait(StorageTrait::Param param)
{
    std::bitset<8> bs;
    if (param.is_cnch_merge_tree)
        bs.set(0);
    if (param.is_cnch_kafka)
        bs.set(1);
    if (param.is_cnch_unique)
        bs.set(2);
    if (param.is_cnch_refresh_materialized_view)
        bs.set(3);
    if (param.is_cnch_table_with_manifest)
        bs.set(4);
    data = bs;
}

StorageTrait constructStorageTrait(StoragePtr storage)
{
    StorageTrait::Param param;

    if (dynamic_cast<StorageCnchKafka *>(storage.get()) != nullptr)
    {
        param.is_cnch_kafka = true;
    }
    else if (StorageCnchMergeTree * cnch_storage = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
    {
        if (cnch_storage->getInMemoryMetadataPtr()->hasUniqueKey())
            param.is_cnch_unique = true;

        if (cnch_storage->getSettings()->enable_publish_version_on_commit)
            param.is_cnch_table_with_manifest = true;

        param.is_cnch_merge_tree = true;
    }
    else if (StorageMaterializedView * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        if (materialized_view->async())
            param.is_cnch_refresh_materialized_view = true;
    }

    return StorageTrait{param};
}

StorageTrait constructStorageTrait(ContextMutablePtr context, const String & db, const String & table, const String & create_query)
{
    auto ast = getASTCreateQueryFromString(create_query, context);
    /// make shortcut because CnchHive and other remote table constructor could take long time
    if (ast->storage &&
        ast->storage->engine &&
        (!isCnchMergeTreeOrKafka(ast->storage->engine->name))
    )
        return StorageTrait{StorageTrait::Param {
                .is_cnch_merge_tree = false,
                .is_cnch_kafka = false,
                .is_cnch_unique = false,
                .is_cnch_refresh_materialized_view = false
            }};

    StoragePtr storage_ptr = Catalog::CatalogFactory::getTableByDefinition(
                            context,
                            db,
                            table,
                            create_query);

    return constructStorageTrait(std::move(storage_ptr));
}

bool operator == (const StorageTrait & lhs, const StorageTrait & rhs)
{
    return lhs.getData() == rhs.getData();
}

bool isCnchMergeTreeOrKafka(const std::string & engine_name)
{
    bool found_cnch = (engine_name.find("Cnch") != std::string::npos);
    bool found_merge_tree = (engine_name.find("MergeTree") != std::string::npos);
    bool found_kafka = (engine_name.find("Kafka") != std::string::npos);
    if ((!found_cnch) ||
        (!found_merge_tree && !found_kafka))
        return false;
    return true;
}

}
