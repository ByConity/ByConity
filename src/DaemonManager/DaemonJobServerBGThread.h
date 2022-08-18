#pragma once
#include <DaemonManager/DaemonJob.h>
#include <Common/LRUCache.h>
#include <Core/UUID.h>
#include <DaemonManager/BackgroundJob.h>
#include <DaemonManager/DMDefines.h>
#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/BackgroudJobExecutor.h>
#include <DaemonManager/BGJobStatusInCatalog.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Protos/RPCHelpers.h>
#include <Common/Status.h>


namespace DB::DaemonManager
{

using CnchServerClientPtrs = std::vector<CnchServerClientPtr>;
using StorageCache = LRUCache<String, IStorage>;
using BGJobStatusInCatalog::IBGJobStatusPersistentStoreProxy;

struct BGJobInfoFromServer
{
    StorageID storage_id;
    CnchBGThreadStatus status;
    String host_port;
};

using BGJobsFromServersFetcher = std::function<std::optional<std::unordered_multimap<UUID, BGJobInfoFromServer>>(
    Context &,
    CnchBGThreadType,
    Poco::Logger *,
    const std::vector<String> &
)>;

class DaemonJobServerBGThread : public DaemonJob
{
public:
    using DaemonJob::DaemonJob;
    DaemonJobServerBGThread(ContextMutablePtr global_context_, CnchBGThreadType type_, std::unique_ptr<IBackgroundJobExecutor> bg_job_executor_);
    void init() override;
    virtual CnchServerClientPtr getTargetServer(const StorageID &, UInt64) const;
    void setStorageCache(StorageCache * cache_) { cache = cache_; }
    StorageCache * getStorageCache() { return cache; }
    void setLivenessCheckInterval(size_t interval) { liveness_check_interval = interval; }
    BackgroundJobPtr getBackgroundJob(const UUID & uuid) const;
    BGJobInfos getBGJobInfos() const;
    IBackgroundJobExecutor & getBgJobExecutor() const { return *bg_job_executor; }
    Result executeJobAction(const StorageID & storage_id, CnchBGThreadAction action);
    virtual bool isTargetTable(const StoragePtr &) const { return true; }
    virtual bool isBGJobStatusStoreInCatalog() const { return false; }
protected:
    bool executeImpl() override;
    ServerInfo findServerInfo(const std::map<String, UInt64> &, const BackgroundJobs &);
    std::vector<String> findRestartServers(const std::map<String, UInt64> &);
    BackgroundJobs fetchCnchBGThreadStatus();

    BackgroundJobs background_jobs;
    mutable std::shared_mutex bg_jobs_mutex;
    std::map<String, UInt64> server_start_times;
    StorageCache * cache = nullptr;
    std::unique_ptr<IBGJobStatusPersistentStoreProxy> status_persistent_store{};
    size_t counter_for_liveness_check = 1;
    size_t liveness_check_interval = LIVENESS_CHECK_INTERVAL;
private:
    std::unique_ptr<IBackgroundJobExecutor> bg_job_executor;
};

using DaemonJobServerBGThreadPtr = std::shared_ptr<DaemonJobServerBGThread>;

class DaemonJobServerBGThreadConsumer : public DaemonJobServerBGThread
{
public:
    using DaemonJobServerBGThread::DaemonJobServerBGThread;
    CnchServerClientPtr getTargetServer(const StorageID &, UInt64) const override;
};

std::unordered_map<UUID, StorageID> getUUIDsFromCatalog(DaemonJobServerBGThread & daemon_job);

struct UpdateResult
{
    UUIDs remove_uuids;
    UUIDs add_uuids;
};

UpdateResult getUpdateBGJobs(
    const BackgroundJobs & background_jobs,
    const std::unordered_map<UUID, StorageID> & new_uuid_map,
    const std::vector<String> & alive_servers
);

bool checkIfServerDied(const std::vector<String> & alive_host_port, const String & host_port);
std::vector<String> findAliveServers(const std::map<String, UInt64> &);
std::set<UUID> getUUIDsFromBackgroundJobs(const BackgroundJobs & background_jobs);
std::unordered_map<UUID, String> getAllTargetServerForBGJob(
    const BackgroundJobs & bg_jobs,
    UInt64 ts,
    DaemonJobServerBGThread & daemon_job);

size_t checkLivenessIfNeed(
    size_t counter,
    size_t liveness_check_interval,
    Context & context,
    DaemonJobServerBGThread & daemon_job,
    BackgroundJobs & check_bg_jobs,
    const std::vector<String> & servers,
    BGJobsFromServersFetcher fetch_bg_jobs_from_server
);

void runMissingAndRemoveDuplicateJob(
    DaemonJobServerBGThread &,
    BackgroundJobs &,
    const std::unordered_multimap<UUID, BGJobInfoFromServer> &);

} /// end namespace DB::DaemonManager
