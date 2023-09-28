/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <Access/RowPolicy.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <CloudServices/CnchBGThreadPartitionSelector.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <IO/ReadSettings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/SymbolAllocator.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/CGroup/CGroupManager.h>
#include <Common/MultiVersion.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/RemoteHostFilter.h>
#include <Common/ThreadPool.h>
#include <Common/isLocalAddress.h>
#include <common/types.h>
#include <CloudServices/CnchBGThreadPartitionSelector.h>
#include <Transaction/TxnTimestamp.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Storages/HDFS/HDFSFileSystem.h>
#include <DaemonManager/DaemonManagerClient_fwd.h>
#include <DataStreams/BlockStreamProfileInfo.h>
#include <Optimizer/OptimizerProfile.h>
#include <Server/AsyncQueryManager.h>
#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <Common/DefaultCatalogName.h>

namespace Poco::Net
{
class IPAddress;
}
namespace DB::Statistics
{
struct StatisticsMemoryStore;
}
namespace zkutil
{
class ZooKeeper;
}


namespace DB
{

namespace IndexFile
{
    class Cache;
    class RemoteFileCache;
}

struct ContextSharedPart;
class ContextAccess;
struct User;
using UserPtr = std::shared_ptr<const User>;
struct EnabledRolesInfo;
class EnabledRowPolicies;
class EnabledQuota;
struct QuotaUsage;
class AccessFlags;
struct AccessRightsElement;
class AccessRightsElements;
class EmbeddedDictionaries;
class ExternalDictionariesLoader;
class ExternalModelsLoader;
class InterserverCredentials;
using InterserverCredentialsPtr = std::shared_ptr<const InterserverCredentials>;
class InterserverIOHandler;
class BackgroundSchedulePool;
class MergeList;
class ManipulationList;
class ReplicatedFetchList;
class Cluster;
class Compiler;
class MarkCache;
class MMappedFileCache;
class UncompressedCache;
class PrimaryIndexCache;
class ProcessList;
class ProcessListEntry;
class PlanSegment;
class QueryStatus;
class QueryCache;
class Macros;
struct Progress;
class Clusters;
class QueryCache;
class QueryLog;
class QueryThreadLog;
class QueryExchangeLog;
class PartLog;
class PartMergeLog;
class ServerPartLog;
class TextLog;
class TraceLog;
class MetricLog;
class AsynchronousMetricLog;
class OpenTelemetrySpanLog;
class MutationLog;
class KafkaLog;
class CloudKafkaLog;
class ProcessorsProfileLog;
class ZooKeeperLog;
class QueryMetricLog;
class QueryWorkerMetricLog;
class CnchQueryLog;
class AutoStatsTaskLog;
struct QueryMetricElement;
struct QueryWorkerMetricElement;
using QueryWorkerMetricElementPtr = std::shared_ptr<QueryWorkerMetricElement>;
using QueryWorkerMetricElements = std::vector<QueryWorkerMetricElementPtr>;
struct ProcessorProfileLogElement;
template <typename>
class ProfileElementConsumer;
struct MergeTreeSettings;
class StorageS3Settings;
struct CnchHiveSettings;
struct CnchFileSettings;
class IDatabase;
class DDLWorker;
class ITableFunction;
class Block;
class ActionLocksManager;
using ActionLocksManagerPtr = std::shared_ptr<ActionLocksManager>;
class ShellCommand;
class ICompressionCodec;
class AccessControlManager;
class IResourceGroup;
struct ResourceGroupInfo;
class IResourceGroupManager;
using ResourceGroupManagerPtr = std::shared_ptr<IResourceGroupManager>;
using ResourceGroupInfoMap = std::unordered_map<String, ResourceGroupInfo>;
class InternalResourceGroupManager;
class VWResourceGroupManager;
class Credentials;
class GSSAcceptorContext;
class SettingsConstraints;
class RemoteHostFilter;
struct StorageID;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;
using DisksMap = std::map<String, DiskPtr>;
class IStoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const IStoragePolicy>;
using StoragePoliciesMap = std::map<String, StoragePolicyPtr>;
class StoragePolicySelector;
using StoragePolicySelectorPtr = std::shared_ptr<const StoragePolicySelector>;
struct PartUUIDs;
using PartUUIDsPtr = std::shared_ptr<PartUUIDs>;
class KeeperDispatcher;

class SegmentScheduler;
using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;
class ChecksumsCache;
template <class T>
class RpcClientPool;
class CnchServerClient;
using CnchServerClientPtr = std::shared_ptr<CnchServerClient>;
using CnchServerClientPool = RpcClientPool<CnchServerClient>;
class CnchWorkerClient;
using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
class CnchWorkerClientPools;
class ICnchBGThread;
using CnchBGThreadPtr = std::shared_ptr<ICnchBGThread>;
class CnchBGThreadsMap;
class CnchBGThreadPartitionSelector;
using PartitionSelectorPtr = std::shared_ptr<CnchBGThreadPartitionSelector>;
struct ClusterTaskProgress;

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;
struct NamedSession;
struct NamedCnchSession;
struct BackgroundTaskSchedulingSettings;
class TxnTimestamp;

class CnchWorkerResource;
class CnchServerResource;
using CnchWorkerResourcePtr = std::shared_ptr<CnchWorkerResource>;
using CnchServerResourcePtr = std::shared_ptr<CnchServerResource>;

class Throttler;
using ThrottlerPtr = std::shared_ptr<Throttler>;

class ZooKeeperMetadataTransaction;
using ZooKeeperMetadataTransactionPtr = std::shared_ptr<ZooKeeperMetadataTransaction>;

/// Callback for external tables initializer
using ExternalTablesInitializer = std::function<void(ContextPtr)>;

/// Callback for initialize input()
using InputInitializer = std::function<void(ContextPtr, const StoragePtr &)>;
/// Callback for reading blocks of data from client for function input()
using InputBlocksReader = std::function<Block(ContextPtr)>;

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

/// Used in distributed task processing
using ReadTaskCallback = std::function<String()>;

class UniqueKeyIndexCache;
using UniqueKeyIndexCachePtr = std::shared_ptr<UniqueKeyIndexCache>;
using UniqueKeyIndexBlockCachePtr = std::shared_ptr<IndexFile::Cache>;
using UniqueKeyIndexFileCachePtr = std::shared_ptr<IndexFile::RemoteFileCache>;
class DeleteBitmapCache;
class CnchStorageCache;
class PartCacheManager;
class IServiceDiscovery;
using ServiceDiscoveryClientPtr = std::shared_ptr<IServiceDiscovery>;
class CnchTopologyMaster;
class CnchServerTopology;
class CnchServerManager;
struct RootConfiguration;
class TxnTimestamp;
class TransactionCoordinatorRcCnch;
class ICnchTransaction;
using TransactionCnchPtr = std::shared_ptr<ICnchTransaction>;

class QueueManager;
using QueueManagerPtr = std::shared_ptr<QueueManager>;

using AsyncQueryManagerPtr = std::shared_ptr<AsyncQueryManager>;

class VirtualWarehousePool;
class VirtualWarehouseHandleImpl;
using VirtualWarehouseHandle = std::shared_ptr<VirtualWarehouseHandleImpl>;
class WorkerGroupHandleImpl;
using WorkerGroupHandle = std::shared_ptr<WorkerGroupHandleImpl>;
class CnchWorkerClient;
using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
class CnchCatalogDictionaryCache;

class VWCustomizedSettings;
using VWCustomizedSettingsPtr = std::shared_ptr<VWCustomizedSettings>;

class WorkerStatusManager;
using WorkerStatusManagerPtr = std::shared_ptr<WorkerStatusManager>;

class WorkerGroupStatus;
using WorkerGroupStatusPtr = std::shared_ptr<WorkerGroupStatus>;
struct QeueueThrottlerDeleter;
using QueueThrottlerDeleterPtr = std::shared_ptr<QeueueThrottlerDeleter>;

class BindingCacheManager;
using BindingCacheManagerPtr = std::shared_ptr<BindingCacheManager>;

class VWCustomizedSettings;
using VWCustomizedSettingsPtr = std::shared_ptr<VWCustomizedSettings>;

class VETosConnectionParams;

enum class ServerType
{
    standalone,
    cnch_server,
    cnch_worker,
    cnch_daemon_manager,
    cnch_resource_manager,
    cnch_tso_server,
    cnch_bytepond,
};

template <class T>
class RpcClientPool;
namespace TSO
{
    class TSOClient;
}
using TSOClientPool = RpcClientPool<TSO::TSOClient>;

namespace Catalog
{
    class Catalog;
}

struct MetastoreConfig;

namespace ResourceManagement
{
    class ResourceManagerClient;
}
using ResourceManagerClientPtr = std::shared_ptr<ResourceManagement::ResourceManagerClient>;

class OptimizerMetrics;
using OptimizerMetricsPtr = std::shared_ptr<OptimizerMetrics>;

using ExcludedRules = std::unordered_set<UInt32>;
using ExcludedRulesMap = std::unordered_map<PlanNodeId, ExcludedRules>;

class PlanCacheManager;

/// An empty interface for an arbitrary object that may be attached by a shared pointer
/// to query context, when using ClickHouse as a library.
struct IHostContext
{
    virtual ~IHostContext() = default;
};

using IHostContextPtr = std::shared_ptr<IHostContext>;

/// A small class which owns ContextShared.
/// We don't use something like unique_ptr directly to allow ContextShared type to be incomplete.
struct SharedContextHolder
{
    ~SharedContextHolder();
    SharedContextHolder();
    explicit SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context);
    SharedContextHolder(SharedContextHolder &&) noexcept;

    SharedContextHolder & operator=(SharedContextHolder &&);

    ContextSharedPart * get() const { return shared.get(); }
    void reset();

private:
    std::unique_ptr<ContextSharedPart> shared;
};

template <class T>
class CopyableAtomic : public std::atomic<T>
{
public:
    CopyableAtomic() = default;

    constexpr CopyableAtomic(T desired) : std::atomic<T>(desired)
    {
    }

    constexpr CopyableAtomic(const CopyableAtomic<T> & other) : CopyableAtomic(other.load(std::memory_order_acquire))
    {
    }

    CopyableAtomic & operator=(const CopyableAtomic<T> & other)
    {
        this->store(other.load(std::memory_order_acquire), std::memory_order_relaxed);
        return *this;
    }
};

/** A set of known objects that can be used in the query.
  * Consists of a shared part (always common to all sessions and queries)
  *  and copied part (which can be its own for each session or query).
  *
  * Everything is encapsulated for all sorts of checks and locks.
  */
class Context : public std::enable_shared_from_this<Context>
{
private:
    ContextSharedPart * shared;

    ClientInfo client_info;
    ExternalTablesInitializer external_tables_initializer_callback;

    InputInitializer input_initializer_callback;
    InputBlocksReader input_blocks_reader;

    std::optional<UUID> user_id;
    std::vector<UUID> current_roles;
    bool use_default_roles = false;
    std::shared_ptr<const ContextAccess> access;
    std::shared_ptr<const EnabledRowPolicies> initial_row_policy;
    CopyableAtomic<IResourceGroup *> resource_group{nullptr}; /// Current resource group.
    String current_database;
    Settings settings; /// Setting for query execution.

    using ProgressCallback = std::function<void(const Progress & progress)>;
    ProgressCallback progress_callback; /// Callback for tracking progress of query execution.

    using FileProgressCallback = std::function<void(const FileProgress & progress)>;
    FileProgressCallback file_progress_callback; /// Callback for tracking progress of file loading.

    QueryStatus * process_list_elem = nullptr; /// For tracking total resource usage for query.
    std::weak_ptr<ProcessListEntry> process_list_entry;
    StorageID insertion_table = StorageID::createEmpty(); /// Saved insertion table in query context

    String default_format; /// Format, used when server formats data by itself and if query does not have FORMAT specification.
        /// Thus, used in HTTP interface. If not specified - then some globally default format is used.
    TemporaryTablesMapping external_tables_mapping;
    Scalars scalars;
    String pipeline_log_path;

    /// write ha related. manage the non host update time for tables during query execution.
    std::unordered_map<UUID, UInt64> session_nhuts{};
    std::shared_ptr<std::mutex> nhut_mutex = std::make_shared<std::mutex>();

    /// Fields for distributed s3 function
    std::optional<ReadTaskCallback> next_task_callback;

    /// This session view cache is used when executing insert actions in cnch server side
    /// and this host is not the master of this table with view dependencies catalog service
    /// will not cache storage instances.
    /// This cache is used during session context when query complete execution this cache
    /// will be deconstructed. TODO: Try to find better solution.
    std::map<StorageID, StoragePtr> session_views_cache;

    /// Record entities accessed by current query, and store this information in system.query_log.
    struct QueryAccessInfo
    {
        QueryAccessInfo() = default;

        QueryAccessInfo(const QueryAccessInfo & rhs)
        {
            std::lock_guard<std::mutex> lock(rhs.mutex);
            databases = rhs.databases;
            tables = rhs.tables;
            columns = rhs.columns;
            projections = rhs.projections;
        }

        QueryAccessInfo(QueryAccessInfo && rhs) = delete;

        QueryAccessInfo & operator=(QueryAccessInfo rhs)
        {
            swap(rhs);
            return *this;
        }

        void swap(QueryAccessInfo & rhs)
        {
            std::swap(databases, rhs.databases);
            std::swap(tables, rhs.tables);
            std::swap(columns, rhs.columns);
            std::swap(projections, rhs.projections);
        }

        /// To prevent a race between copy-constructor and other uses of this structure.
        mutable std::mutex mutex{};
        std::set<std::string> databases{};
        std::set<std::string> tables{};
        std::set<std::string> columns{};
        std::set<std::string> projections;
    };

    QueryAccessInfo query_access_info;

    /// Record names of created objects of factories (for testing, etc)
    struct QueryFactoriesInfo
    {
        std::unordered_set<std::string> aggregate_functions;
        std::unordered_set<std::string> aggregate_function_combinators;
        std::unordered_set<std::string> database_engines;
        std::unordered_set<std::string> data_type_families;
        std::unordered_set<std::string> dictionaries;
        std::unordered_set<std::string> formats;
        std::unordered_set<std::string> functions;
        std::unordered_set<std::string> storages;
        std::unordered_set<std::string> table_functions;
    };

    /// Needs to be chandged while having const context in factories methods
    mutable QueryFactoriesInfo query_factories_info;

    /// TODO: maybe replace with temporary tables?
    StoragePtr view_source;                 /// Temporary StorageValues used to generate alias columns for materialized views
    Tables table_function_results;          /// Temporary tables obtained by execution of table functions. Keyed by AST tree id.
    std::unordered_set<String> partition_ids;

    ContextWeakMutablePtr query_context;
    ContextWeakMutablePtr session_context; /// Session context or nullptr. Could be equal to this.
    ContextWeakMutablePtr global_context; /// Global context. Could be equal to this.

    /// XXX: move this stuff to shared part instead.
    ContextMutablePtr buffer_context; /// Buffer context. Could be equal to this.

    /// A flag, used to distinguish between user query and internal query to a database engine (MaterializePostgreSQL).
    bool is_internal_query = false;

    CnchWorkerResourcePtr worker_resource;
    CnchServerResourcePtr server_resource;

    PlanNodeIdAllocatorPtr id_allocator = nullptr;
    std::shared_ptr<SymbolAllocator> symbol_allocator = nullptr;
    std::shared_ptr<Statistics::StatisticsMemoryStore> stats_memory_store = nullptr;
    std::shared_ptr<OptimizerMetrics> optimizer_metrics = nullptr;
    ExcludedRulesMap exclude_rules_map;
    mutable std::shared_ptr<BindingCacheManager> session_binding_cache_manager = nullptr;

    std::unordered_map<std::string, bool> function_deterministic;

    WorkerGroupStatusPtr worker_group_status;
    std::shared_ptr<OptimizerProfile> optimizer_profile =  nullptr;
    /// Temporary data for query execution accounting.
    TemporaryDataOnDiskScopePtr temp_data_on_disk;

    std::weak_ptr<PlanSegmentProcessListEntry> segment_process_list_entry;
    QueueThrottlerDeleterPtr queue_throttler_ptr;
    bool enable_worker_fault_tolerance = false;

public:
    // Top-level OpenTelemetry trace context for the query. Makes sense only for a query context.
    OpenTelemetryTraceContext query_trace_context;

private:
    friend struct NamedCnchSession;

    using SampleBlockCache = std::unordered_map<std::string, Block>;
    mutable SampleBlockCache sample_block_cache;

    PartUUIDsPtr part_uuids; /// set of parts' uuids, is used for query parts deduplication
    PartUUIDsPtr ignored_part_uuids; /// set of parts' uuids are meant to be excluded from query processing

    NameToNameMap query_parameters; /// Dictionary with query parameters for prepared statements.
        /// (key=name, value)

    IHostContextPtr host_context; /// Arbitrary object that may used to attach some host specific information to query context,
        /// when using ClickHouse as a library in some project. For example, it may contain host
        /// logger, some query identification information, profiling guards, etc. This field is
        /// to be customized in HTTP and TCP servers by overloading the customizeContext(DB::ContextPtr)
        /// methods.

    ZooKeeperMetadataTransactionPtr metadata_transaction; /// Distributed DDL context. I'm not sure if it's a suitable place for this,
        /// but it's the easiest way to pass this through the whole stack from executeQuery(...)
        /// to DatabaseOnDisk::commitCreateTable(...) or IStorage::alter(...) without changing
        /// thousands of signatures.
        /// And I hope it will be replaced with more common Transaction sometime.


    /// VirtualWarehouse for each query, session level
    mutable VirtualWarehouseHandle current_vw;
    mutable WorkerGroupHandle current_worker_group;
    mutable WorkerGroupHandle health_worker_group;

    /// Transaction for each query, query level
    TransactionCnchPtr current_cnch_txn;

    QueryWorkerMetricElements query_worker_metrics;

    /// The extended profile info is from workers and mainly for INSERT operations
    mutable ExtendedProfileInfo extended_profile_info;

    String async_query_id;

    bool read_from_client_finished = false;

    bool is_explain_query = false;

    Context();
    Context(const Context &);
    Context & operator=(const Context &);

public:
    /// Create initial Context with ContextShared and etc.
    static ContextMutablePtr createGlobal(ContextSharedPart * shared);
    static ContextMutablePtr createCopy(const ContextWeakPtr & other);
    static ContextMutablePtr createCopy(const ContextMutablePtr & other);
    static ContextMutablePtr createCopy(const ContextPtr & other);
    static SharedContextHolder createShared();

    void addSessionView(StorageID view_table_id, StoragePtr view_storage);
    StoragePtr getSessionView(StorageID view_table_id);

    void copyFrom(const ContextPtr & other);

    ~Context();

    void setExtendedProfileInfo(const ExtendedProfileInfo & source) const;
    ExtendedProfileInfo getExtendedProfileInfo() const;

    void addQueryWorkerMetricElements(QueryWorkerMetricElementPtr query_worker_metric_element);
    QueryWorkerMetricElements getQueryWorkerMetricElements();

    String getPath() const;
    String getFlagsPath() const;
    String getUserFilesPath() const;
    String getDictionariesLibPath() const;
    String getMetastorePath() const;

    VolumePtr getTemporaryVolume() const;
    TemporaryDataOnDiskScopePtr getTempDataOnDisk() const;
    void setTempDataOnDisk(TemporaryDataOnDiskScopePtr temp_data_on_disk_);

    void setPath(const String & path);
    void setFlagsPath(const String & path);
    void setUserFilesPath(const String & path);
    void setDictionariesLibPath(const String & path);
    void setMetastorePath(const String & path);

    VolumePtr setTemporaryStorage(const String & path, const String & policy_name = "");
    void setTemporaryStoragePath();
    void setTemporaryStoragePath(const String & path, size_t max_size);
    // void setTemporaryStorageInCache(const String & cache_disk_name, size_t max_size);
    void setTemporaryStoragePolicy(const String & policy_name, size_t max_size);

    void setReadyForQuery();
    bool isReadyForQuery() const;

    /// HDFS user
    void setHdfsUser(const String & name);
    String getHdfsUser() const;

    /// HDFS nnproxy
    void setHdfsNNProxy(const String & name);
    String getHdfsNNProxy() const;

    void setHdfsConnectionParams(const HDFSConnectionParams & hdfs_params);
    HDFSConnectionParams getHdfsConnectionParams() const;

    void setLasfsConnectionParams(const Poco::Util::AbstractConfiguration & config);

    void setVETosConnectParams(const VETosConnectionParams & connect_params);
    const VETosConnectionParams & getVETosConnectParams() const;

    /// create backgroud task to synchronize metadata table by table
    void setMetaChecker();
    void setMetaCheckerStatus(bool stop);

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    const Poco::Util::AbstractConfiguration & getConfigRef() const;

    void initRootConfig(const Poco::Util::AbstractConfiguration & poco_config);
    const RootConfiguration & getRootConfig() const;
    void reloadRootConfig(const Poco::Util::AbstractConfiguration & poco_config);

    void initCnchConfig(const Poco::Util::AbstractConfiguration & poco_config);
    const Poco::Util::AbstractConfiguration & getCnchConfigRef() const;

    AccessControlManager & getAccessControlManager();
    const AccessControlManager & getAccessControlManager() const;

    /// Sets external authenticators config (LDAP, Kerberos).
    void setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config);

    /// Creates GSSAcceptorContext instance based on external authenticator params.
    std::unique_ptr<GSSAcceptorContext> makeGSSAcceptorContext() const;

    /** Take the list of users, quotas and configuration profiles from this config.
      * The list of users is completely replaced.
      * The accumulated quota values are not reset if the quota is not deleted.
      */
    void setUsersConfig(const ConfigurationPtr & config);
    ConfigurationPtr getUsersConfig();

    /// Sets the current user, checks the credentials and that the specified host is allowed.
    /// Must be called before getClientInfo() can be called.
    void setUser(const Credentials & credentials, const Poco::Net::SocketAddress & address);
    void setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address);

    /// Sets the current user, *does not check the password/credentials and that the specified host is allowed*.
    /// Must be called before getClientInfo.
    ///
    /// (Used only internally in cluster, if the secret matches)
    void setUserWithoutCheckingPassword(const String & name, const Poco::Net::SocketAddress & address);

    void setQuotaKey(String quota_key_);

    UserPtr getUser() const;
    String getUserName() const;
    std::optional<UUID> getUserID() const;

    void setCurrentRoles(const std::vector<UUID> & current_roles_);
    void setCurrentRolesDefault();
    boost::container::flat_set<UUID> getCurrentRoles() const;
    boost::container::flat_set<UUID> getEnabledRoles() const;
    std::shared_ptr<const EnabledRolesInfo> getRolesInfo() const;

    /// Checks access rights.
    /// Empty database means the current database.
    void checkAccess(const AccessFlags & flags) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database) const;
    void checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    void checkAccess(
        const AccessFlags & flags,
        const std::string_view & database,
        const std::string_view & table,
        const std::string_view & column) const;
    void checkAccess(
        const AccessFlags & flags,
        const std::string_view & database,
        const std::string_view & table,
        const std::vector<std::string_view> & columns) const;
    void checkAccess(
        const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::string_view & column) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const;
    void checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const;
    void checkAccess(const AccessRightsElement & element) const;
    void checkAccess(const AccessRightsElements & elements) const;

    std::shared_ptr<const ContextAccess> getAccess() const;

    WorkerGroupStatusPtr & getWorkerGroupStatusPtr() { return worker_group_status; }
    const WorkerGroupStatusPtr & getWorkerGroupStatusPtr() const { return worker_group_status; }

    void updateAdaptiveSchdulerConfig();
    WorkerStatusManagerPtr getWorkerStatusManager();
    WorkerStatusManagerPtr getWorkerStatusManager() const;
    WorkerGroupHandle tryGetHealthWorkerGroup() const;
    void selectWorkerNodesWithMetrics();

    ASTPtr getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType type) const;

    /// Sets an extra row policy based on `client_info.initial_user`, if it exists.
    /// TODO: we need a better solution here. It seems we should pass the initial row policy
    /// because a shard is allowed to don't have the initial user or it may be another user with the same name.
    void setInitialRowPolicy();

    std::shared_ptr<const EnabledQuota> getQuota() const;
    std::optional<QuotaUsage> getQuotaUsage() const;

    /// We have to copy external tables inside executeQuery() to track limits. Therefore, set callback for it. Must set once.
    void setExternalTablesInitializer(ExternalTablesInitializer && initializer);
    /// This method is called in executeQuery() and will call the external tables initializer.
    void initializeExternalTablesIfSet();

    /// When input() is present we have to send columns structure to client
    void setInputInitializer(InputInitializer && initializer);
    /// This method is called in StorageInput::read while executing query
    void initializeInput(const StoragePtr & input_storage);

    /// Callback for read data blocks from client one by one for function input()
    void setInputBlocksReaderCallback(InputBlocksReader && reader);
    /// Get callback for reading data for input()
    InputBlocksReader getInputBlocksReaderCallback() const;
    void resetInputCallbacks();

    ClientInfo & getClientInfo() { return client_info; }
    const ClientInfo & getClientInfo() const { return client_info; }
    const std::unordered_set<String> & getPartitionIds() const { return partition_ids;}

    void initResourceGroupManager(const ConfigurationPtr & config);
    void setResourceGroup(const IAST * ast);
    IResourceGroup * tryGetResourceGroup() const;
    IResourceGroupManager * tryGetResourceGroupManager();
    IResourceGroupManager * tryGetResourceGroupManager() const;
    void startResourceGroup();
    void stopResourceGroup();

    enum StorageNamespace
    {
        ResolveGlobal = 1u, /// Database name must be specified
        ResolveCurrentDatabase = 2u, /// Use current database
        ResolveCurrentCatalog = 4u, /// Use current catalog
        ResolveOrdinary = ResolveGlobal | ResolveCurrentDatabase
            | ResolveCurrentCatalog, /// If database/catalog name is not specified, use current database/catalog
        ResolveExternal = 8u, /// Try get external table
        ResolveAll = ResolveExternal | ResolveOrdinary /// If database name is not specified, try get external table,
        ///    if external table not found use current database.
    };

    String resolveDatabase(const String & database_name) const;
    StorageID resolveStorageID(StorageID storage_id, StorageNamespace where = StorageNamespace::ResolveAll) const;
    StorageID tryResolveStorageID(StorageID storage_id, StorageNamespace where = StorageNamespace::ResolveAll) const;
    StorageID resolveStorageIDImpl(StorageID storage_id, StorageNamespace where, std::optional<Exception> * exception) const;

    Tables getExternalTables() const;
    void addExternalTable(const String & table_name, TemporaryTableHolder && temporary_table);
    std::shared_ptr<TemporaryTableHolder> removeExternalTable(const String & table_name);

    const Scalars & getScalars() const;
    const Block & getScalar(const String & name) const;
    void addScalar(const String & name, const Block & block);
    bool hasScalar(const String & name) const;

    const QueryAccessInfo & getQueryAccessInfo() const { return query_access_info; }
    void addQueryAccessInfo(
        const String & quoted_database_name,
        const String & full_quoted_table_name,
        const Names & column_names,
        const String & projection_name = {});

    /// Supported factories for records in query_log
    enum class QueryLogFactories
    {
        AggregateFunction,
        AggregateFunctionCombinator,
        Database,
        DataType,
        Dictionary,
        Format,
        Function,
        Storage,
        TableFunction
    };

    const QueryFactoriesInfo & getQueryFactoriesInfo() const { return query_factories_info; }
    void addQueryFactoriesInfo(QueryLogFactories factory_type, const String & created_object) const;

    StoragePtr executeTableFunction(const ASTPtr & table_expression);

    void addViewSource(const StoragePtr & storage);
    StoragePtr getViewSource() const;

    String getCurrentDatabase() const;
    String getCurrentQueryId() const { return client_info.current_query_id; }

    /// Id of initiating query for distributed queries; or current query id if it's not a distributed query.
    String getInitialQueryId() const;

    void setCurrentDatabase(const String & name);
    void setCurrentDatabase(const String & name, ContextPtr local_context);

    void setCurrentCatalog(const String & catalog_name);
    /// Set current_database for global context. We don't validate that database
    /// exists because it should be set before databases loading.
    void setCurrentDatabaseNameInGlobalContext(const String & name);
    void setCurrentQueryId(const String & query_id);

    void killCurrentQuery();

    void setInsertionTable(StorageID db_and_table) { insertion_table = std::move(db_and_table); }
    const StorageID & getInsertionTable() const { return insertion_table; }

    String getDefaultFormat() const; /// If default_format is not specified, some global default format is returned.
    void setDefaultFormat(const String & name);

    MultiVersion<Macros>::Version getMacros() const;
    void setMacros(std::unique_ptr<Macros> && macros);

    Settings getSettings() const;
    void setSettings(const Settings & settings_);

    /// Set settings by name.
    void setSetting(const StringRef & name, const String & value);
    void setSetting(const StringRef & name, const Field & value);
    void applySettingChange(const SettingChange & change);
    void applySettingsChanges(const SettingsChanges & changes);

    /// Checks the constraints.
    void checkSettingsConstraints(const SettingChange & change) const;
    void checkSettingsConstraints(const SettingsChanges & changes) const;
    void checkSettingsConstraints(SettingsChanges & changes) const;
    void clampToSettingsConstraints(SettingsChanges & changes) const;

    /// Returns the current constraints (can return null).
    std::shared_ptr<const SettingsConstraints> getSettingsConstraints() const;

    const EmbeddedDictionaries & getEmbeddedDictionaries() const;
    const ExternalDictionariesLoader & getExternalDictionariesLoader() const;
    CnchCatalogDictionaryCache & getCnchCatalogDictionaryCache() const;
    const ExternalModelsLoader & getExternalModelsLoader() const;
    EmbeddedDictionaries & getEmbeddedDictionaries();
    ExternalDictionariesLoader & getExternalDictionariesLoader();
    CnchCatalogDictionaryCache & getCnchCatalogDictionaryCache();
    ExternalModelsLoader & getExternalModelsLoader();
    ExternalModelsLoader & getExternalModelsLoaderUnlocked();
    void tryCreateEmbeddedDictionaries() const;
    void loadDictionaries(const Poco::Util::AbstractConfiguration & config);

    void setExternalModelsConfig(const ConfigurationPtr & config, const std::string & config_name = "models_config");

    /// I/O formats.
    BlockInputStreamPtr getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size) const;

    /// Don't use streams. Better look at getOutputFormat...
    BlockOutputStreamPtr getOutputStreamParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const;
    BlockOutputStreamPtr getOutputStream(const String & name, WriteBuffer & buf, const Block & sample) const;

    OutputFormatPtr getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const;

    InterserverIOHandler & getInterserverIOHandler();

    /// How other servers can access this for downloading replicated data.
    void setInterserverIOAddress(const String & host, UInt16 port);
    std::pair<String, UInt16> getInterserverIOAddress() const;

    UInt16 getExchangePort(bool check_port_exists = false) const;
    UInt16 getExchangeStatusPort(bool check_port_exists = false) const;

    void setComplexQueryActive(bool active);
    bool getComplexQueryActive();

    /// Credentials which server will use to communicate with others
    void updateInterserverCredentials(const Poco::Util::AbstractConfiguration & config);
    InterserverCredentialsPtr getInterserverCredentials();

    std::pair<String, String> getCnchInterserverCredentials() const;

    /// Interserver requests scheme (http or https)
    void setInterserverScheme(const String & scheme);
    String getInterserverScheme() const;

    /// Storage of allowed hosts from config.xml
    void setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config);
    const RemoteHostFilter & getRemoteHostFilter() const;

    UInt16 getPortFromEnvForConsul(const char * key) const;
    HostWithPorts getHostWithPorts() const;

    /// The port that the server listens for executing SQL queries.
    UInt16 getTCPPort() const;
    /// Get the tcp_port of other server.
    UInt16 getTCPPort(const String & host, UInt16 rpc_port) const;

    std::optional<UInt16> getTCPPortSecure() const;

    /// The port that the server exchange ha log
    UInt16 getHaTCPPort() const;

    /// Allow to use named sessions. The thread will be run to cleanup sessions after timeout has expired.
    /// The method must be called at the server startup.
    void enableNamedSessions();

    void enableNamedCnchSessions();

    std::shared_ptr<NamedSession>
    acquireNamedSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check) const;
    std::shared_ptr<NamedCnchSession>
    acquireNamedCnchSession(const UInt64 & txn_id, std::chrono::steady_clock::duration timeout, bool session_check) const;

    void initCnchServerResource(const TxnTimestamp & txn_id);
    CnchServerResourcePtr getCnchServerResource() const;
    CnchServerResourcePtr tryGetCnchServerResource() const;
    CnchWorkerResourcePtr getCnchWorkerResource() const;
    CnchWorkerResourcePtr tryGetCnchWorkerResource() const;
    void initCnchWorkerResource();

    /// For methods below you may need to acquire the context lock by yourself.

    ContextMutablePtr getQueryContext() const;
    bool hasQueryContext() const { return !query_context.expired(); }
    bool isInternalSubquery() const;

    ContextMutablePtr getSessionContext() const;
    bool hasSessionContext() const { return !session_context.expired(); }

    ContextMutablePtr getGlobalContext() const;
    bool hasGlobalContext() const { return !global_context.expired(); }
    bool isGlobalContext() const
    {
        auto ptr = global_context.lock();
        return ptr && ptr.get() == this;
    }

    ContextMutablePtr getBufferContext() const;

    void setQueryContext(ContextMutablePtr context_) { query_context = context_; }
    void setSessionContext(ContextMutablePtr context_) { session_context = context_; }

    void makeQueryContext() { query_context = shared_from_this(); }
    void makeSessionContext() { session_context = shared_from_this(); }
    void makeGlobalContext()
    {
        initGlobal();
        global_context = shared_from_this();
    }

    const Settings & getSettingsRef() const { return settings; }
    Settings & getSettingsRef() { return settings; }

    VWCustomizedSettingsPtr getVWCustomizedSettings() const;
    void setVWCustomizedSettings(VWCustomizedSettingsPtr vw_customized_settings_ptr_);

    void setProgressCallback(ProgressCallback callback);
    /// Used in InterpreterSelectQuery to pass it to the IBlockInputStream.
    ProgressCallback getProgressCallback() const;

    void setFileProgressCallback(FileProgressCallback && callback) { file_progress_callback = callback; }
    FileProgressCallback getFileProgressCallback() const { return file_progress_callback; }

    void setProcessListEntry(std::shared_ptr<ProcessListEntry> process_list_entry_);
    std::weak_ptr<ProcessListEntry> getProcessListEntry() const;

    /** Set in executeQuery and InterpreterSelectQuery. Then it is used in IBlockInputStream,
      *  to update and monitor information about the total number of resources spent for the query.
      */
    void setProcessListElement(QueryStatus * elem);
    /// Can return nullptr if the query was not inserted into the ProcessList.
    QueryStatus * getProcessListElement() const;

    /// List all queries.
    ProcessList & getProcessList();
    const ProcessList & getProcessList() const;

    /// List all plan segment queries;
    PlanSegmentProcessList & getPlanSegmentProcessList();
    const PlanSegmentProcessList & getPlanSegmentProcessList() const;

    void setPlanSegmentProcessListEntry(std::shared_ptr<PlanSegmentProcessListEntry> segment_process_list_entry_);
    std::weak_ptr<PlanSegmentProcessListEntry> getPlanSegmentProcessListEntry() const;

    void setProcessorProfileElementConsumer(
        std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> processor_log_element_consumer_);
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> getProcessorProfileElementConsumer() const;

    void setIsExplainQuery(const bool & is_explain_query_);
    bool isExplainQuery() const;

    SegmentSchedulerPtr getSegmentScheduler();
    SegmentSchedulerPtr getSegmentScheduler() const;

    BindingCacheManagerPtr getGlobalBindingCacheManager();
    BindingCacheManagerPtr getGlobalBindingCacheManager() const;
    void setGlobalBindingCacheManager(std::shared_ptr<BindingCacheManager> && manager);

    QueueManagerPtr getQueueManager() const;
    AsyncQueryManagerPtr getAsyncQueryManager() const;

    MergeList & getMergeList();
    const MergeList & getMergeList() const;

    void setQueueDeleter(QueueThrottlerDeleterPtr ptr)
    {
        queue_throttler_ptr = ptr;
    }
    ManipulationList & getManipulationList();
    const ManipulationList & getManipulationList() const;

    ReplicatedFetchList & getReplicatedFetchList();
    const ReplicatedFetchList & getReplicatedFetchList() const;

    /// If the current session is expired at the time of the call, synchronously creates and returns a new session with the startNewSession() call.
    /// If no ZooKeeper configured, throws an exception.
    std::shared_ptr<zkutil::ZooKeeper> getZooKeeper() const;
    /// Same as above but return a zookeeper connection from auxiliary_zookeepers configuration entry.
    std::shared_ptr<zkutil::ZooKeeper> getAuxiliaryZooKeeper(const String & name) const;
    std::shared_ptr<AutoStatsTaskLog> getAutoStatsTaskLog() const;

    /// Try to connect to Keeper using get(Auxiliary)ZooKeeper. Useful for
    /// internal Keeper start (check connection to some other node). Return true
    /// if connected successfully (without exception) or our zookeeper client
    /// connection configured for some other cluster without our node.
    bool tryCheckClientConnectionToMyKeeperCluster() const;

#if USE_NURAFT
    std::shared_ptr<KeeperDispatcher> & getKeeperDispatcher() const;
#endif
    void initializeKeeperDispatcher(bool start_async) const;
    void shutdownKeeperDispatcher() const;
    void updateKeeperConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Set auxiliary zookeepers configuration at server starting or configuration reloading.
    void reloadAuxiliaryZooKeepersConfigIfChanged(const ConfigurationPtr & config);
    /// Has ready or expired ZooKeeper
    bool hasZooKeeper() const;
    /// Has ready or expired auxiliary ZooKeeper
    bool hasAuxiliaryZooKeeper(const String & name) const;
    /// Reset current zookeeper session. Do not create a new one.
    void resetZooKeeper() const;
    // Reload Zookeeper
    void reloadZooKeeperIfChanged(const ConfigurationPtr & config) const;

    // TODO: check if this knob is redundant
    void setEnableSSL(bool v);
    bool isEnableSSL() const;

    /// Create a cache of uncompressed blocks of specified size. This can be done only once.
    void setUncompressedCache(size_t max_size_in_bytes);
    std::shared_ptr<UncompressedCache> getUncompressedCache() const;
    void dropUncompressedCache() const;

    /// Create a cache of marks of specified size. This can be done only once.
    void setMarkCache(size_t cache_size_in_bytes);
    std::shared_ptr<MarkCache> getMarkCache() const;
    void dropMarkCache() const;

    /// Create a cache of mapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
    void setMMappedFileCache(size_t cache_size_in_num_entries);
    std::shared_ptr<MMappedFileCache> getMMappedFileCache() const;
    void dropMMappedFileCache() const;

    /// Create a cache of query results for statements which run repeatedly.
    void setQueryCache(const Poco::Util::AbstractConfiguration & config);
    void updateQueryCacheConfiguration(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<QueryCache> getQueryCache() const;
    void dropQueryCache() const;

    /** Clear the caches of the uncompressed blocks and marks.
      * This is usually done when renaming tables, changing the type of columns, deleting a table.
      *  - since caches are linked to file names, and become incorrect.
      *  (when deleting a table - it is necessary, since in its place another can appear)
      * const - because the change in the cache is not considered significant.
      */
    void dropCaches() const;

    /// Settings for merge scheduler.
    void setMergeSchedulerSettings(const Poco::Util::AbstractConfiguration & config);

    /// Settings for MergeTree background tasks stored in config.xml
    BackgroundTaskSchedulingSettings getBackgroundProcessingTaskSchedulingSettings() const;
    BackgroundTaskSchedulingSettings getBackgroundMoveTaskSchedulingSettings() const;

    BackgroundSchedulePool & getBufferFlushSchedulePool() const;
    BackgroundSchedulePool & getSchedulePool() const;
    BackgroundSchedulePool & getMessageBrokerSchedulePool() const;
    BackgroundSchedulePool & getDistributedSchedulePool() const;

    BackgroundSchedulePool & getConsumeSchedulePool() const;
    BackgroundSchedulePool & getRestartSchedulePool() const;
    BackgroundSchedulePool & getHaLogSchedulePool() const;
    BackgroundSchedulePool & getMutationSchedulePool() const;
    BackgroundSchedulePool & getLocalSchedulePool() const;
    BackgroundSchedulePool & getMergeSelectSchedulePool() const;
    BackgroundSchedulePool & getUniqueTableSchedulePool() const;
    BackgroundSchedulePool & getMemoryTableSchedulePool() const;
    BackgroundSchedulePool & getTopologySchedulePool() const;

    ThrottlerPtr getDiskCacheThrottler() const;

    ThrottlerPtr getReplicatedFetchesThrottler() const;
    ThrottlerPtr getReplicatedSendsThrottler() const;

    /// Has distributed_ddl configuration or not.
    bool hasDistributedDDL() const;
    void setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker);
    DDLWorker & getDDLWorker() const;

    std::shared_ptr<Clusters> getClusters() const;
    std::shared_ptr<Cluster> getCluster(const std::string & cluster_name) const;
    std::shared_ptr<Cluster> tryGetCluster(const std::string & cluster_name) const;
    void setClustersConfig(const ConfigurationPtr & config, const String & config_name = "remote_servers");
    /// Sets custom cluster, but doesn't update configuration
    void setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster);
    void reloadClusterConfig() const;

    Compiler & getCompiler();

    /// Call after initialization before using system logs. Call for global context.
    void initializeSystemLogs();

    void initBGPartitionSelector();
    PartitionSelectorPtr getBGPartitionSelector() const;

    /// Call after initialization before using trace collector.
    void initializeTraceCollector();

    bool hasTraceCollector() const;

    /// Nullptr if the query log is not ready for this moment.
    std::shared_ptr<QueryLog> getQueryLog() const;
    std::shared_ptr<QueryThreadLog> getQueryThreadLog() const;
    std::shared_ptr<QueryExchangeLog> getQueryExchangeLog() const;
    std::shared_ptr<TraceLog> getTraceLog() const;
    std::shared_ptr<TextLog> getTextLog() const;
    std::shared_ptr<MetricLog> getMetricLog() const;
    std::shared_ptr<AsynchronousMetricLog> getAsynchronousMetricLog() const;
    std::shared_ptr<OpenTelemetrySpanLog> getOpenTelemetrySpanLog() const;
    std::shared_ptr<MutationLog> getMutationLog() const;
    std::shared_ptr<KafkaLog> getKafkaLog() const;
    std::shared_ptr<CloudKafkaLog> getCloudKafkaLog() const;
    std::shared_ptr<ProcessorsProfileLog> getProcessorsProfileLog() const;
    std::shared_ptr<ZooKeeperLog> getZooKeeperLog() const;

    /// Returns an object used to log operations with parts if it possible.
    /// Provide table name to make required checks.
    std::shared_ptr<PartLog> getPartLog(const String & part_database) const;
    std::shared_ptr<PartMergeLog> getPartMergeLog() const;
    std::shared_ptr<ServerPartLog> getServerPartLog() const;

    void initializeCnchSystemLogs();
    std::shared_ptr<QueryMetricLog> getQueryMetricsLog() const;
    void insertQueryMetricsElement(const QueryMetricElement & element); /// Add the metrics element to the background thread for flushing
    std::shared_ptr<QueryWorkerMetricLog> getQueryWorkerMetricsLog() const;
    void insertQueryWorkerMetricsElement(
        const QueryWorkerMetricElement & element); /// Add the metrics element to the background thread for flushing
    std::shared_ptr<CnchQueryLog> getCnchQueryLog() const;

    const MergeTreeSettings & getMergeTreeSettings() const;
    const MergeTreeSettings & getReplicatedMergeTreeSettings() const;
    const StorageS3Settings & getStorageS3Settings() const;
    const CnchHiveSettings & getCnchHiveSettings() const;
    const CnchFileSettings & getCnchFileSettings() const;

    /// Prevents DROP TABLE if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxTableSizeToDrop(size_t max_size);
    void checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const;

    /// Prevents DROP PARTITION if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxPartitionSizeToDrop(size_t max_size);
    void checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const;

    /// Lets you select the compression codec according to the conditions described in the configuration file.
    std::shared_ptr<ICompressionCodec> chooseCompressionCodec(size_t part_size, double part_size_ratio) const;


    /// Provides storage disks
    DiskPtr getDisk(const String & name) const;

    StoragePoliciesMap getPoliciesMap() const;
    DisksMap getDisksMap() const;
    void updateStorageConfiguration(Poco::Util::AbstractConfiguration & config);

    /// Provides storage politics schemes
    StoragePolicyPtr getStoragePolicy(const String & name) const;

    /// Get the server uptime in seconds.
    time_t getUptimeSeconds() const;

    using ConfigReloadCallback = std::function<void()>;
    void setConfigReloadCallback(ConfigReloadCallback && callback);
    void reloadConfig() const;

    void shutdown();

    bool isInternalQuery() const { return is_internal_query; }
    void setInternalQuery(bool internal) { is_internal_query = internal; }

    ActionLocksManagerPtr getActionLocksManager();

    enum class ApplicationType
    {
        SERVER, /// The program is run as clickhouse-server daemon (default behavior)
        CLIENT, /// clickhouse-client
        LOCAL, /// clickhouse-local
        KEEPER, /// clickhouse-keeper (also daemon)
        TSO, /// clickhouse-tso-server
    };

    ApplicationType getApplicationType() const;
    void setApplicationType(ApplicationType type);

    /// Sets default_profile and system_profile, must be called once during the initialization
    void setDefaultProfiles(const Poco::Util::AbstractConfiguration & config);
    String getDefaultProfileName() const;
    String getSystemProfileName() const;

    /// Base path for format schemas
    String getFormatSchemaPath(bool remote = false) const;
    void setFormatSchemaPath(const String & path, bool remote = false);

    SampleBlockCache & getSampleBlockCache() const;

    /// Query parameters for prepared statements.
    bool hasQueryParameters() const;
    const NameToNameMap & getQueryParameters() const;
    void setQueryParameter(const String & name, const String & value);
    void setQueryParameters(const NameToNameMap & parameters) { query_parameters = parameters; }

    /// Add started bridge command. It will be killed after context destruction
    void addBridgeCommand(std::unique_ptr<ShellCommand> cmd) const;

    IHostContextPtr & getHostContext();
    const IHostContextPtr & getHostContext() const;

    /// Initialize context of distributed DDL query with Replicated database.
    void initZooKeeperMetadataTransaction(ZooKeeperMetadataTransactionPtr txn, bool attach_existing = false);
    /// Returns context of current distributed DDL query or nullptr.
    ZooKeeperMetadataTransactionPtr getZooKeeperMetadataTransaction() const;

    PartUUIDsPtr getPartUUIDs() const;
    PartUUIDsPtr getIgnoredPartUUIDs() const;

    ReadTaskCallback getReadTaskCallback() const;
    void setReadTaskCallback(ReadTaskCallback && callback);

    void setPipelineLogPath(const String & path) { pipeline_log_path = path; }
    String getPipelineLogpath() const { return pipeline_log_path; }

    /// Create a memory cache of data blocks reading from unique key index files.
    void setUniqueKeyIndexBlockCache(size_t cache_size_in_bytes);
    UniqueKeyIndexBlockCachePtr getUniqueKeyIndexBlockCache() const;

    /// Create a local disk cache of unique key index files.
    void setUniqueKeyIndexFileCache(size_t cache_size_in_bytes);
    UniqueKeyIndexFileCachePtr getUniqueKeyIndexFileCache() const;

    /// Create a cache of UniqueKeyIndex objects.
    void setUniqueKeyIndexCache(size_t cache_size_in_bytes);
    UniqueKeyIndexCachePtr getUniqueKeyIndexCache() const;

    /// Create a memory cache of delete bitmaps for data parts.
    void setDeleteBitmapCache(size_t cache_size_in_bytes);
    std::shared_ptr<DeleteBitmapCache> getDeleteBitmapCache() const;

    PlanNodeIdAllocatorPtr & getPlanNodeIdAllocator() { return id_allocator; }
    UInt32 nextNodeId() { return id_allocator->nextId(); }
    void createPlanNodeIdAllocator();

    int step_id = 2000;
    int getStepId() const { return step_id; }
    void setStepId(int step_id_) { step_id = step_id_; }
    int getAndIncStepId() { return ++step_id; }

    int rule_id = 3000;
    int getRuleId() const { return rule_id; }
    void setRuleId(int rule_id_) { rule_id = rule_id_; }
    void incRuleId() { ++rule_id; }

    String graphviz_sub_query_path;
    void setExecuteSubQueryPath(String path) { graphviz_sub_query_path = std::move(path); }
    String getExecuteSubQueryPath() const
    {
        return graphviz_sub_query_path;
    }
    void removeExecuteSubQueryPath()
    {
        graphviz_sub_query_path = "";
    }

    int sub_query_id = 0;
    int incAndGetSubQueryId() { return ++sub_query_id; }

    SymbolAllocatorPtr & getSymbolAllocator() { return symbol_allocator; }
    ExcludedRulesMap & getExcludedRulesMap() { return exclude_rules_map; }

    void createSymbolAllocator();
    std::shared_ptr<Statistics::StatisticsMemoryStore> getStatisticsMemoryStore();

    std::shared_ptr<BindingCacheManager> getSessionBindingCacheManager() const;

    void createOptimizerMetrics();
    OptimizerMetricsPtr & getOptimizerMetrics() { return optimizer_metrics; }

    void setFunctionDeterministic(const std::string & fun_name, bool deterministic)
    {
        function_deterministic[fun_name] = deterministic;
    }

    bool isFunctionDeterministic(const std::string & fun_name) const
    {
        if (function_deterministic.contains(fun_name))
        {
            return function_deterministic.at(fun_name);
        }
        return true;
    }

    void initOptimizerProfile() { optimizer_profile = std::make_unique<OptimizerProfile>(); }

    String getOptimizerProfile(bool print_rule = false);

    void clearOptimizerProfile();

    void logOptimizerProfile(Poco::Logger * log, String prefix, String name, String time, bool is_rule = false);

    const String & getTenantId() const
    {

        if (!tenant_id.empty())
            return tenant_id;
        else
            return settings.tenant_id.toString();
    }


    void setTenantId(const String & id)
    {
        tenant_id = id;
    }

    const String & getCurrentCatalog() const
    {
        if (!current_catalog.empty())
            return current_catalog;
        else
            return settings.default_catalog.toString();
    }


    void setChecksumsCache(size_t cache_size_in_bytes);
    std::shared_ptr<ChecksumsCache> getChecksumsCache() const;

    /// client for service discovery
    void initServiceDiscoveryClient();
    ServiceDiscoveryClientPtr getServiceDiscoveryClient() const;

    void initTSOClientPool(const String & service_name);
    std::shared_ptr<TSO::TSOClient> getCnchTSOClient() const;

    void initTSOElectionReader();
    String tryGetTSOLeaderHostPort() const;
    void updateTSOLeaderHostPort() const;

    UInt64 getTimestamp() const;
    UInt64 tryGetTimestamp(const String & pretty_func_name = "Context") const;
    UInt64 getTimestamps(UInt32 size) const;
    UInt64 getPhysicalTimestamp() const;

    void setCnchStorageCache(size_t max_cache_size);
    std::shared_ptr<CnchStorageCache> getCnchStorageCache() const;

    void setPartCacheManager();
    std::shared_ptr<PartCacheManager> getPartCacheManager() const;

    ThreadPool & getPartCacheManagerThreadPool();

    /// catalog related
    void initCatalog(const MetastoreConfig & catalog_conf, const String & name_space);
    std::shared_ptr<Catalog::Catalog> tryGetCnchCatalog() const;
    std::shared_ptr<Catalog::Catalog> getCnchCatalog() const;

    /// client for Daemon Manager Service
    void initDaemonManagerClientPool(const String & service_name);
    DaemonManagerClientPtr getDaemonManagerClient() const;

    void setCnchServerManager(const Poco::Util::AbstractConfiguration & config);
    std::shared_ptr<CnchServerManager> getCnchServerManager() const;
    void updateServerVirtualWarehouses(const ConfigurationPtr & config);
    void setCnchTopologyMaster();
    std::shared_ptr<CnchTopologyMaster> getCnchTopologyMaster() const;

    void updateQueueManagerConfig() const;
    void setServerType(const String & type_str);
    ServerType getServerType() const;

    String getVirtualWarehousePSM() const;

    void initVirtualWarehousePool();
    VirtualWarehousePool & getVirtualWarehousePool() const;

    StoragePtr tryGetCnchTable(const String & database_name, const String & table_name) const;

    void setCurrentWorkerGroup(WorkerGroupHandle group) const;
    WorkerGroupHandle getCurrentWorkerGroup() const;
    WorkerGroupHandle tryGetCurrentWorkerGroup() const;

    void setCurrentVW(VirtualWarehouseHandle vw);
    VirtualWarehouseHandle getCurrentVW() const;
    VirtualWarehouseHandle tryGetCurrentVW() const;

    void initResourceManagerClient();
    ResourceManagerClientPtr getResourceManagerClient() const;

    UInt16 getRPCPort() const;
    UInt16 getHTTPPort() const;

    //write ha non host update time
    UInt64 getNonHostUpdateTime(const UUID & uuid);

    void initCnchServerClientPool(const String & service_name);
    CnchServerClientPool & getCnchServerClientPool() const;
    CnchServerClientPtr getCnchServerClient(const std::string & host, uint16_t port) const;
    CnchServerClientPtr getCnchServerClient(const std::string & host_port) const;
    CnchServerClientPtr getCnchServerClient(const HostWithPorts & host_with_ports) const;
    CnchServerClientPtr getCnchServerClient() const;

    void initCnchWorkerClientPools();
    CnchWorkerClientPools & getCnchWorkerClientPools() const;

    /// Transaction related APIs
    void initCnchTransactionCoordinator();
    TransactionCoordinatorRcCnch & getCnchTransactionCoordinator() const;
    void setCurrentTransaction(TransactionCnchPtr txn, bool finish_txn = true);
    TransactionCnchPtr
    setTemporaryTransaction(const TxnTimestamp & txn_id, const TxnTimestamp & primary_txn_id = 0, bool with_check = true);
    TransactionCnchPtr getCurrentTransaction() const;
    TxnTimestamp getCurrentTransactionID() const;
    TxnTimestamp getCurrentCnchStartTime() const;

    void initCnchBGThreads();
    CnchBGThreadsMap * getCnchBGThreadsMap(CnchBGThreadType type) const;
    CnchBGThreadPtr getCnchBGThread(CnchBGThreadType type, const StorageID & storage_id) const;
    CnchBGThreadPtr tryGetCnchBGThread(CnchBGThreadType type, const StorageID & storage_id) const;
    void controlCnchBGThread(const StorageID & storage_id, CnchBGThreadType type, CnchBGThreadAction action) const;
    bool removeMergeMutateTasksOnPartitions(const StorageID &, const std::unordered_set<String> &);
    bool getTableReclusterTaskStatus(const StorageID & storage_id) const;
    ClusterTaskProgress getTableReclusterTaskProgress(const StorageID & storage_id) const;
    void startResourceReport();
    void stopResourceReport();
    bool isResourceReportRegistered();

    CnchBGThreadPtr tryGetDedupWorkerManager(const StorageID & storage_id) const;

    InterserverCredentialsPtr getCnchInterserverCredentials();
    std::shared_ptr<Cluster> mockCnchServersCluster() const;

    std::vector<std::pair<UInt64, CnchWorkerResourcePtr>> getAllWorkerResources() const;

    /// Part allocation
    // Consistent hash algorithm for part allocation
    enum PartAllocator : int
    {
        JUMP_CONSISTENT_HASH = 0,
        RING_CONSISTENT_HASH = 1,
        STRICT_RING_CONSISTENT_HASH = 2,
    };
    PartAllocator getPartAllocationAlgo() const;

    String getDefaultCnchPolicyName() const;
    String getCnchAuxilityPolicyName() const;

    void setAsyncQueryId(String id)
    {
        async_query_id = id;
    }
    String & getAsyncQueryId()
    {
        return async_query_id;
    }
    bool isAsyncMode() const;
    void markReadFromClientFinished();
    void waitReadFromClientFinished() const;

    ReadSettings getReadSettings() const;

    bool isEnabledWorkerFaultTolerance() const { return enable_worker_fault_tolerance; }
    void enableWorkerFaultTolerance() { enable_worker_fault_tolerance = true; }
    void disableWorkerFaultTolerance() { enable_worker_fault_tolerance = false; }

    void setPlanCacheManager(std::unique_ptr<PlanCacheManager> && manager);
    PlanCacheManager* getPlanCacheManager();

private:
    String tenant_id;
    String current_catalog;

    std::unique_lock<std::recursive_mutex> getLock() const;

    void initGlobal();

    /// Compute and set actual user settings, client_info.current_user should be set
    void calculateAccessRights();

    template <typename... Args>
    void checkAccessImpl(const Args &... args) const;

    void setProfile(const String & profile);

    EmbeddedDictionaries & getEmbeddedDictionariesImpl(bool throw_on_error) const;

    void checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const;

    StoragePolicySelectorPtr getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const;

    DiskSelectorPtr getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const;

    /// If the password is not set, the password will not be checked
    void setUserImpl(const String & name, const std::optional<String> & password, const Poco::Net::SocketAddress & address);
};

}
