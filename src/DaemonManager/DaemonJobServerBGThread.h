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

#pragma once
#include <Common/Logger.h>
#include <DaemonManager/DaemonJob.h>
#include <Common/LRUCache.h>
#include <Core/UUID.h>
#include <DaemonManager/BackgroundJob.h>
#include <DaemonManager/DMDefines.h>
#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/BackgroudJobExecutor.h>
#include <DaemonManager/TargetServerCalculator.h>
#include <DaemonManager/BGJobStatusInCatalog.h>
#include <CloudServices/CnchCreateQueryHelper.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <CloudServices/CnchServerClient.h>
#include <Protos/RPCHelpers.h>
#include <Common/Status.h>


namespace DB::DaemonManager
{

using CnchServerClientPtrs = std::vector<CnchServerClientPtr>;
struct StorageTrait
{
public:
    struct Param
    {
        bool is_cnch_merge_tree = false;
        bool is_cnch_kafka = false;
        bool is_cnch_unique = false;
        bool is_cnch_refresh_materialized_view = false;
        bool is_cnch_table_with_manifest = false;
    };

    explicit StorageTrait(Param param);
    StorageTrait() = default;
    bool isCnchMergeTree() const
    {
        return data[IS_CNCH_MERGE_TREE_FLAG];
    }

    bool isCnchKafka() const
    {
        return data[IS_CNCH_KAFKA_FLAG];
    }

    bool isCnchUniqueAndNeedDedup() const
    {
        return data[IS_CNCH_MERGE_TREE_UNIQUE_FLAG];
    }

    bool isCnchRefreshMaterializedView() const
    {
        return data[IS_CNCH_REFRESH_MATERIALIZED_VIEW];
    }

    bool isCnchTableWithManifest() const
    {
        return data[IS_CNCH_TABLE_WITH_MANIFEST_FLAG];
    }

    const std::bitset<8> & getData() const /// for testing
    {
        return data;
    }
private:
    std::bitset<8> data;
    static constexpr int IS_CNCH_MERGE_TREE_FLAG = 0;
    static constexpr int IS_CNCH_KAFKA_FLAG = 1;
    static constexpr int IS_CNCH_MERGE_TREE_UNIQUE_FLAG = 2;
    static constexpr int IS_CNCH_REFRESH_MATERIALIZED_VIEW = 3;
    static constexpr int IS_CNCH_TABLE_WITH_MANIFEST_FLAG = 4;
};

bool operator == (const StorageTrait & lhs, const StorageTrait & rhs);

using StorageTraitCache = LRUCache<String, StorageTrait>;
using BGJobStatusInCatalog::IBGJobStatusPersistentStoreProxy;

struct BGJobInfoFromServer
{
    StorageID storage_id;
    CnchBGThreadStatus status;
    String host_port;
    bool operator == (const BGJobInfoFromServer & other) const
    {
        return (other.storage_id == storage_id) &&
                (other.status == status) &&
                (other.host_port == host_port);
    }
};

using BGJobsFromServersFetcher = std::function<std::optional<std::unordered_multimap<UUID, BGJobInfoFromServer>>(
    Context &,
    CnchBGThreadType,
    LoggerPtr,
    const std::vector<String> &
)>;

class DaemonJobServerBGThread : public DaemonJob
{
public:
    using DaemonJob::DaemonJob;
    void init() override;
    CnchServerClientPtr getTargetServer(const StorageID &, UInt64) const;
    void setStorageTraitCache(StorageTraitCache * cache_) { cache = cache_; }
    StorageTraitCache * getStorageTraitCache() { return cache; }
    void setLivenessCheckInterval(size_t interval) { liveness_check_interval = interval; }
    BackgroundJobPtr getBackgroundJob(const UUID & uuid) const;
    BGJobInfos getBGJobInfos() const;
    Result executeJobAction(const StorageID & storage_id, CnchBGThreadAction action);
    /// StoragePtr for table level DaemonJobServerBGThread, StorageID for database level DaemonJobServerBGThread
    virtual bool ifNeedDaemonJob(const StorageTrait &, const StorageID &) { return false; }
    IBackgroundJobExecutor & getBgJobExecutor() const { return *bg_job_executor; }

    /// for unit test
    DaemonJobServerBGThread(ContextMutablePtr global_context_, CnchBGThreadType type_,
        std::unique_ptr<IBackgroundJobExecutor> bg_job_executor_,
        std::unique_ptr<IBGJobStatusPersistentStoreProxy> js_persistent_store_proxy,
        std::unique_ptr<ITargetServerCalculator> target_server_calculator);

    IBGJobStatusPersistentStoreProxy & getStatusPersistentStore() const { return *status_persistent_store; }
    std::vector<String> updateServerStartTimeAndFindRestartServers(const std::map<String, UInt64> &);
    bool suspended() const override;

protected:
    bool executeImpl() override;
    ServerInfo findServerInfo(const std::map<String, UInt64> &, const BackgroundJobs &);
    BackgroundJobs fetchCnchBGThreadStatus();

    BackgroundJobs background_jobs;
    mutable std::shared_mutex bg_jobs_mutex;
    std::map<String, UInt64> server_start_times;
    size_t counter_for_liveness_check = 1;
    size_t liveness_check_interval = LIVENESS_CHECK_INTERVAL;
    bool is_suspended = false;
    mutable std::mutex suspended_mutex;

private:
    StorageTraitCache * cache = nullptr;
    std::unique_ptr<IBGJobStatusPersistentStoreProxy> status_persistent_store{};
    std::unique_ptr<IBackgroundJobExecutor> bg_job_executor;
    std::unique_ptr<ITargetServerCalculator> target_server_calculator;
};

using DaemonJobServerBGThreadPtr = std::shared_ptr<DaemonJobServerBGThread>;

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
    const std::set<UUID> & new_bg_jobs_uuids,
    const std::vector<String> & servers,
    BGJobsFromServersFetcher fetch_bg_jobs_from_server
);

void runMissingAndRemoveDuplicateJob(
    DaemonJobServerBGThread &,
    BackgroundJobs &,
    const std::unordered_multimap<UUID, BGJobInfoFromServer> &);

template <CnchBGThreadType T, bool (*ifNeedDaemonJobF)(const StorageTrait &, const StorageID &, const ContextPtr & context)>
struct DaemonJobForCnch : public DaemonJobServerBGThread
{
    DaemonJobForCnch(ContextMutablePtr global_context_) : DaemonJobServerBGThread(std::move(global_context_), T) { }
    bool ifNeedDaemonJob(const StorageTrait & storage_trait, const StorageID & storage_id) override { return ifNeedDaemonJobF(storage_trait, storage_id, getContext()); }
};

bool isCnchMergeTree(const StorageTrait & , const StorageID & storage_id, const ContextPtr & context);

bool isCnchTableWithManifest(const StorageTrait &, const StorageID &, const ContextPtr &);

struct DaemonJobForMergeMutate : public DaemonJobForCnch<CnchBGThreadType::MergeMutate, isCnchMergeTree>
{
    using DaemonJobForCnch<CnchBGThreadType::MergeMutate, isCnchMergeTree>::DaemonJobForCnch;
    void executeOptimize(const StorageID & storage_id, const String & partition_id, bool enable_try, bool mutations_sync, UInt64 timeout_ms) const;
};

std::vector<BGJobInfoFromServer> findZombieJobsInServer(
    const std::set<UUID> & new_bg_jobs_uuids,
    BackgroundJobs & managed_job_exclude_new_job,
    const std::unordered_multimap<UUID, BGJobInfoFromServer> & jobs_from_server);

StorageTrait constructStorageTrait(ContextMutablePtr context, const String & db, const String & table, const String & create_query);

bool isCnchMergeTreeOrKafka(const std::string & engine_name);

} /// end namespace DB::DaemonManager
