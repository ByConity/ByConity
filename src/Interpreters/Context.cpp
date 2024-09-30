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

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/Credentials.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <Access/QuotaUsage.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/User.h>
#include <Access/AeolusAccessUtil.h>
#include <Catalog/Catalog.h>
#include <CloudServices/CnchBGThreadsMap.h>
#include <CloudServices/CnchMergeMutateThread.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerResource.h>
#include <CloudServices/CnchWorkerClient.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <CloudServices/CnchWorkerResource.h>
#include <CloudServices/ReclusteringManagerThread.h>
#include <CloudServices/ManifestCache.h>
#include <Compression/ICompressionCodec.h>
#include <Coordination/Defines.h>
#include <Coordination/KeeperDispatcher.h>
#include <Core/AnsiSettings.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <DaemonManager/DaemonManagerClient.h>
#include <DataStreams/BlockStreamProfileInfo.h>
#include <Databases/IDatabase.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Disks/DiskLocal.h>
#include <Disks/IO/IOUringReader.h>
#include <Disks/IO/getIOUringReader.h>
#include <Formats/FormatFactory.h>
#include <IO/MMappedFileCache.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/UncompressedCache.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/Cache/QueryCache.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/CnchSystemLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DistributedStages/PlanSegmentProcessList.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderCnchCatalogRepository.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/Lemmatizers.h>
#include <Interpreters/NamedSession.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueueManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/SynonymsExtensions.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Interpreters/VirtualWarehouseQueue.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Interpreters/WorkerStatusManager.h>
#include <MergeTreeCommon/CnchServerManager.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <MergeTreeCommon/GlobalDataManager.h>
#include <Optimizer/OptimizerMetrics.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/Formats/Impl/ArrowColumnCache.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPlan/PlanCache.h>
#include <ResourceGroup/IResourceGroupManager.h>
#include <ResourceGroup/InternalResourceGroupManager.h>
#include <ResourceGroup/VWResourceGroupManager.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <Storages/CompressionCodecSelector.h>
#include <Storages/DiskCache/AbstractCache.h>
#include <Storages/DiskCache/KeyIndexFileCache.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/NexusFS/NexusFS.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/HDFSFileSystem.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/IStorage.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/ChecksumsCache.h>
#include <Storages/MergeTree/CloudTableDefinitionCache.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/MergeTree/DeleteBitmapCache.h>
#include <Storages/MergeTree/GINStoreReader.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/PartCacheManager.h>
#include <Storages/MergeTree/GinIdxFilterResultCache.h>
#include <Storages/StorageS3Settings.h>
#include <Storages/UniqueKeyIndexCache.h>
#include <TSO/TSOClient.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Poco/Mutex.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/UUID.h>
#include <Poco/Util/Application.h>
#include "common/defines.h"
#include "common/types.h"
#include <Common/CGroup/CGroupManagerFactory.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/VWCustomizedSettings.h>
#include <Common/Configurations.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/FieldVisitorToString.h>
#include <Common/Macros.h>
#include <Common/RemoteHostFilter.h>
#include <Common/RpcClientPool.h>
#include <Common/ShellCommand.h>
#include <Common/StackTrace.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/TraceCollector.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/callOnce.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <common/logger_useful.h>
#include "Core/SettingsFields.h"
#include "Disks/DiskType.h"
#include "Server/AsyncQueryManager.h"

#include <Storages/IndexFile/FilterPolicy.h>
#include <Storages/IndexFile/IndexFileWriter.h>
#include <WorkerTasks/ManipulationList.h>

#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Statistics/StatisticsMemoryStore.h>
#include <Transaction/CnchServerTransaction.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <Common/HostWithPorts.h>
#include <Transaction/GlobalTxnCommitter.h>
#include <Statistics/StatisticsMemoryStore.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

#include <ExternalCatalog/CnchExternalCatalogMgr.h>
#include <ExternalCatalog/IExternalCatalogMgr.h>
#include <IO/VETosCommon.h>
#include <IO/OSSCommon.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/RemoteFile/CnchFileSettings.h>

#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Statistics/AutoStatisticsManager.h>
#include <fmt/core.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Processors/IntermediateResult/CacheManager.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
extern const Event ContextLock;
extern const Event CompiledCacheSizeBytes;
extern const Event SelectHealthWorkerMilliSeconds;
}

namespace CurrentMetrics
{
extern const Metric ContextLockWait;
extern const Metric BackgroundMovePoolTask;
extern const Metric BackgroundSchedulePoolTask;
extern const Metric BackgroundBufferFlushSchedulePoolTask;
extern const Metric BackgroundDistributedSchedulePoolTask;
extern const Metric BackgroundMessageBrokerSchedulePoolTask;
extern const Metric BackgroundConsumeSchedulePoolTask;
extern const Metric BackgroundRestartSchedulePoolTask;
extern const Metric BackgroundHaLogSchedulePoolTask;
extern const Metric BackgroundMutationSchedulePoolTask;
extern const Metric BackgroundLocalSchedulePoolTask;
extern const Metric BackgroundMergeSelectSchedulePoolTask;
extern const Metric BackgroundUniqueTableSchedulePoolTask;
extern const Metric BackgroundMemoryTableSchedulePoolTask;
extern const Metric BackgroundCNCHTopologySchedulePoolTask;
extern const Metric BackgroundPartsMetricsSchedulePoolTask;
extern const Metric BackgroundGCSchedulePoolTask;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BAD_GET;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_CATALOG;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_SETTING;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int THERE_IS_NO_SESSION;
    extern const int THERE_IS_NO_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int RESOURCE_MANAGER_NO_LEADER_ELECTED;
    extern const int CNCH_SERVER_NOT_FOUND;
    extern const int CNCH_BG_THREAD_NOT_FOUND;
    extern const int CATALOG_SERVICE_INTERNAL_ERROR;
    extern const int NOT_A_LEADER;
    extern const int INVALID_SETTING_VALUE;
    extern const int BACKUP_JOB_CREATE_FAILED;
    extern const int BACKUP_JOB_CLEAR_FAILED;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int QUERY_WAS_CANCELLED;
}

/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextSharedPart
{
    LoggerPtr log = getLogger("Context");

    /// For access of most of shared objects. Recursive mutex.
    mutable std::recursive_mutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::mutex external_dictionaries_mutex;
    mutable std::mutex external_models_mutex;
    mutable std::mutex cnch_catalog_dict_cache_mutex;
    /// Separate mutex for storage policies. During server startup we may
    /// initialize some important storages (system logs with MergeTree engine)
    /// under context lock.
    mutable std::mutex storage_policies_mutex;
    /// Separate mutex for re-initialization of zookeeper session. This operation could take a long time and must not interfere with another operations.
    mutable std::mutex zookeeper_mutex;
    /// Shared mutex and cv for reading data from clients.
    mutable std::mutex read_mutex;
    std::condition_variable read_cv;

    mutable zkutil::ZooKeeperPtr zookeeper; /// Client for ZooKeeper.
    ConfigurationPtr zookeeper_config; /// Stores zookeeper configs

#if USE_NURAFT
    mutable std::mutex keeper_dispatcher_mutex;
    mutable std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
#endif
    mutable std::mutex auxiliary_zookeepers_mutex;
    mutable std::map<String, zkutil::ZooKeeperPtr> auxiliary_zookeepers; /// Map for auxiliary ZooKeeper clients.
    ConfigurationPtr auxiliary_zookeepers_config; /// Stores auxiliary zookeepers configs

    String interserver_io_host; /// The host name by which this server is available for other servers.
    UInt16 interserver_io_port = 0; /// and port.
    String interserver_scheme; /// http or https

    bool complex_query_active {false};

    MultiVersion<InterserverCredentials> interserver_io_credentials;

    String path;                                            /// Path to the data directory, with a slash at the end.
    String flags_path;                                      /// Path to the directory with some control flags for server maintenance.
    String user_files_path;                                 /// Path to the directory with user provided files, usable by 'file' table function.
    String dictionaries_lib_path;                           /// Path to the directory with user provided binaries and libraries for external dictionaries.
    String metastore_path;                                  /// Path to metastore. We use a seperate path to hold all metastore to make it more easier to manage the metadata on server.
    ConfigurationPtr config;                                /// Global configuration settings.
    ConfigurationPtr cnch_config;                           /// Config used in cnch.
    RootConfiguration root_config;                          /// Predefined global configuration settings.

    String tmp_path; /// Path to the temporary files that occur when processing the request.
    mutable VolumePtr tmp_volume; /// Volume for the the temporary files that occur when processing the request.

    TemporaryDataOnDiskScopePtr temp_data_on_disk; /// Temporary data for query execution accounting.

    String hdfs_user; // libhdfs3 user name
    String hdfs_nn_proxy; // libhdfs3 namenode proxy
    HDFSConnectionParams hdfs_connection_params;
    mutable std::optional<EmbeddedDictionaries> embedded_dictionaries; /// Metrica's dictionaries. Have lazy initialization.
    AdditionalServices additional_services;

    VETosConnectionParams vetos_connection_params;
    OSSConnectionParams oss_connection_params;

    mutable std::optional<CnchCatalogDictionaryCache> cnch_catalog_dict_cache;
    mutable std::optional<ExternalDictionariesLoader> external_dictionaries_loader;
    mutable std::optional<ExternalModelsLoader> external_models_loader;
    ConfigurationPtr external_models_config;
    scope_guard models_repository_guard;

    scope_guard dictionaries_xmls;
    scope_guard dictionaries_cnch_catalog;

    #if USE_NLP
        mutable std::optional<SynonymsExtensions> synonyms_extensions;
        mutable std::optional<Lemmatizers> lemmatizers;
    #endif

    String default_profile_name; /// Default profile name used for default values.
    String system_profile_name; /// Profile used by system processes
    String buffer_profile_name; /// Profile used by Buffer engine for flushing to the underlying
    AccessControlManager access_control_manager;
    mutable ResourceGroupManagerPtr resource_group_manager; /// Known resource groups
    mutable NvmCachePtr nvm_cache; /// nvm cache
    mutable UncompressedCachePtr uncompressed_cache; /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache; /// Cache of marks in compressed files.
    mutable QueryCachePtr query_cache;         /// Cache of query results.
    mutable IntermediateResultCachePtr intermediate_result_cache; /// part cache of queries' results.
    mutable MMappedFileCachePtr
        mmap_cache; /// Cache of mmapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
    mutable OnceFlag cloud_table_definition_cache_initialized;
    /// Cache of CloudMergeTree objects to speed up table creation during query execution.
    /// Used when send_cacheable_table_definitions is enabled
    mutable CloudTableDefinitionCachePtr cloud_table_definition_cache;
    ProcessList process_list; /// Executing queries at the moment.
    SegmentSchedulerPtr segment_scheduler;
    ExchangeStatusTrackerPtr exchange_data_tracker;
    DiskExchangeDataManagerPtr disk_exchange_data_manager;
    QueueManagerPtr queue_manager;
    AsyncQueryManagerPtr async_query_manager;
    MergeList merge_list; /// The list of executable merge (for (Replicated)?MergeTree)
    ManipulationList manipulation_list;
    PlanSegmentProcessList plan_segment_process_list; /// The list of running plansegments in the moment;
    ReplicatedFetchList replicated_fetch_list;
    ConfigurationPtr users_config;                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.

    mutable std::optional<BackgroundSchedulePool> buffer_flush_schedule_pool; /// A thread pool that can do background flush for Buffer tables.
    mutable OnceFlag schedule_pool_initialized;
    mutable std::optional<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    mutable std::optional<BackgroundSchedulePool> distributed_schedule_pool; /// A thread pool that can run different jobs in background (used for distributed sends)
    mutable std::optional<BackgroundSchedulePool> message_broker_schedule_pool; /// A thread pool that can run different jobs in background (used for message brokers, like RabbitMQ and Kafka)

    mutable OnceFlag readers_initialized;
    mutable AsynchronousReaderPtr asynchronous_remote_fs_reader;

    struct ExtraSchedulePool
    {
        OnceFlag is_initialized;
        std::unique_ptr<BackgroundSchedulePool> pool;
    };
    mutable std::array<ExtraSchedulePool, SchedulePool::Size> extra_schedule_pools;
    std::optional<ThreadPool> vector_index_loading_thread_pool;

    mutable OnceFlag disk_cache_throttler_initialized;
    mutable ThrottlerPtr disk_cache_throttler;
    mutable OnceFlag preload_throttler_initialized;
    mutable ThrottlerPtr preload_throttler; /// may be nullptr
    mutable OnceFlag replicated_fetches_throttler_initialized;
    mutable ThrottlerPtr replicated_fetches_throttler; /// A server-wide throttler for replicated fetches
    mutable OnceFlag replicated_sends_throttler_initialized;
    mutable ThrottlerPtr replicated_sends_throttler; /// A server-wide throttler for replicated sends

    MultiVersion<Macros> macros; /// Substitutions extracted from config.
    std::unique_ptr<DDLWorker> ddl_worker; /// Process ddl commands from zk.
    /// Rules for selecting the compression settings, depending on the size of the part.
    mutable std::unique_ptr<CompressionCodecSelector> compression_codec_selector;
    /// Storage disk chooser for MergeTree engines
    mutable std::shared_ptr<const DiskSelector> merge_tree_disk_selector;
    /// Storage policy chooser for MergeTree engines
    mutable std::shared_ptr<const StoragePolicySelector> merge_tree_storage_policy_selector;
    /// global checksums cache;
    mutable ChecksumsCachePtr checksums_cache;
    /// global primary index cache
    mutable PrimaryIndexCachePtr primary_index_cache;

    mutable std::shared_ptr<CompressedDataIndexCache> compressed_data_index_cache;

    mutable std::unique_ptr<GinIdxFilterResultCache> gin_idx_filter_result_cache;

    mutable std::shared_ptr<GINStoreReaderFactory> gin_store_reader_factory;

    mutable ServiceDiscoveryClientPtr sd;
    mutable PartCacheManagerPtr cache_manager; /// Manage cache of parts for cnch tables.
    mutable std::shared_ptr<Catalog::Catalog> cnch_catalog;
    mutable CnchServerManagerPtr server_manager;
    mutable CnchTopologyMasterPtr topology_master;
    mutable ResourceManagerClientPtr rm_client;
    mutable std::unique_ptr<VirtualWarehousePool> vw_pool;
    mutable OnceFlag global_txn_committer_initialized;
    mutable GlobalTxnCommitterPtr global_txn_committer;
    mutable GlobalDataManagerPtr global_data_manager;
    mutable std::shared_ptr<ManifestCache> manifest_cache;

    bool enable_ssl = false;

    ServerType server_type{ServerType::standalone};
    mutable std::unique_ptr<TransactionCoordinatorRcCnch> cnch_txn_coordinator;

    mutable std::unique_ptr<CnchServerClientPool> cnch_server_client_pool;
    mutable std::unique_ptr<CnchWorkerClientPools> cnch_worker_client_pools;

    mutable std::unique_ptr<CnchBGThreadsMapArray> cnch_bg_threads_array;

    std::atomic_bool stop_sync{false};
    BackgroundSchedulePool::TaskHolder meta_checker;

    std::optional<CnchHiveSettings> cnchhive_settings;
    std::optional<CnchHiveSettings> las_settings;
    std::optional<MergeTreeSettings> merge_tree_settings; /// Settings of MergeTree* engines.
    std::optional<CnchFileSettings> cnch_file_settings;   /// Settings of CnchFile engines.
    std::optional<MergeTreeSettings> replicated_merge_tree_settings; /// Settings of ReplicatedMergeTree* engines.
    std::atomic_size_t max_table_size_to_drop = 50000000000lu; /// Protects MergeTree tables from accidental DROP (50GB by default)
    std::atomic_size_t max_partition_size_to_drop = 50000000000lu; /// Protects MergeTree partitions from accidental DROP (50GB by default)
    String format_schema_path; /// Path to a directory that contains schema files used by input formats.
    String remote_format_schema_path;
    ActionLocksManagerPtr action_locks_manager; /// Set of storages' action lockers
    std::unique_ptr<SystemLogs> system_logs; /// Used to log queries and operations on parts
    std::unique_ptr<CnchSystemLogs> cnch_system_logs; /// Used to log queries, kafka etc. Stores data in CnchMergeTree table

    std::optional<StorageS3Settings> storage_s3_settings; /// Settings of S3 storage

    RemoteHostFilter remote_host_filter; /// Allowed URL from config.xml

    std::optional<TraceCollector> trace_collector; /// Thread collecting traces from threads executing queries
    std::optional<NamedSessions> named_sessions; /// Controls named HTTP sessions.
    std::optional<NamedCnchSessions> named_cnch_sessions; /// Controls named Cnch sessions.

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    std::shared_ptr<Clusters> clusters;
    ConfigurationPtr clusters_config; /// Stores updated configs
    mutable std::mutex clusters_mutex; /// Guards clusters and clusters_config
    BindingCacheManagerPtr global_binding_cache_manager;

    mutable DeleteBitmapCachePtr delete_bitmap_cache; /// Cache of delete bitmaps
    mutable UniqueKeyIndexBlockCachePtr unique_key_index_block_cache; /// Shared block cache of unique key indexes
    mutable UniqueKeyIndexFileCachePtr unique_key_index_file_cache; /// Shared file cache of unique key indexes
    mutable UniqueKeyIndexCachePtr unique_key_index_cache; /// Shared object cache of unique key indexes

    std::map<String, UInt16> server_ports;

    bool shutdown_called = false;
    bool restrict_tenanted_users_to_whitelist_settings = false;
    bool restrict_tenanted_users_to_privileged_operations = false;

    mutable std::mutex extra_whitelist_settings_mutex;
    std::unordered_set<String> extra_whitelist_settings;

    Stopwatch uptime_watch;

    Context::ApplicationType application_type = Context::ApplicationType::SERVER;
    std::unique_ptr<TSOClientPool> tso_client_pool;
    std::unique_ptr<DaemonManagerClientPool> daemon_manager_pool;

    std::unique_ptr<ElectionReader> tso_election_reader;

    /// vector of xdbc-bridge commands, they will be killed when Context will be destroyed
    std::vector<std::unique_ptr<ShellCommand>> bridge_commands;

    Context::ConfigReloadCallback config_reload_callback;

    VWCustomizedSettingsPtr vw_customized_settings_ptr;
    mutable std::mutex vw_customized_settings_update_mutex;

    /// @ByteDance
    bool ready_for_query = false; /// Server is ready for incoming queries

    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> processor_log_element_consumer;
    std::unique_ptr<Statistics::AutoStats::AutoStatisticsManager> auto_stats_manager;

    std::unique_ptr<PlanCacheManager> plan_cache_manager;

    std::unique_ptr<PreparedStatementManager> prepared_statement_manager;

    std::optional<String> running_backup_task;
#if USE_LIBURING
    mutable std::once_flag io_uring_reader_initialized;
    mutable std::vector<std::unique_ptr<IOUringReader>> io_uring_reader;
#endif

    mutable NexusFSPtr nexus_fs;

    ContextSharedPart()
        : macros(std::make_unique<Macros>())
    {
        /// TODO: make it singleton (?)
        static std::atomic<size_t> num_calls{0};
        if (++num_calls > 1)
        {
            std::cerr << "Attempting to create multiple ContextShared instances. Stack trace:\n" << StackTrace().toString();
            std::cerr.flush();
            std::terminate();
        }
    }


    ~ContextSharedPart()
    {
        try
        {
            shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown()
    {
        if (shutdown_called)
            return;
        shutdown_called = true;

        /**  After system_logs have been shut down it is guaranteed that no system table gets created or written to.
          *  Note that part changes at shutdown won't be logged to part log.
          */

        if (plan_cache_manager)
            plan_cache_manager.reset();

        if (system_logs)
            system_logs->shutdown();

        if (cnch_system_logs)
            cnch_system_logs->shutdown();

        if (disk_exchange_data_manager)
            disk_exchange_data_manager->shutdown();

        DatabaseCatalog::shutdown();

        /// reset scheduled task before schedule pool shutdown
        if (meta_checker)
            meta_checker->deactivate();

        if (cnch_bg_threads_array)
            cnch_bg_threads_array->shutdown();

        if (cnch_txn_coordinator)
        {
            cnch_txn_coordinator->shutdown();
            /// Need to reset cnch_txn_coordinator before schedule_pool reset, otherwise it may core.
            cnch_txn_coordinator.reset();
        }

        if (server_manager)
            server_manager->shutDown();

        if (topology_master)
            topology_master->shutDown();

        if (cache_manager)
            cache_manager->shutDown();

        if (nvm_cache)
            nvm_cache->shutDown();

        if (nexus_fs)
            nexus_fs->shutDown();

        if (queue_manager)
            queue_manager->shutdown();

        if (cnch_catalog)
            cnch_catalog->shutDown();

        std::unique_ptr<SystemLogs> delete_system_logs;
        std::unique_ptr<CnchSystemLogs> delete_cnch_system_logs;
        {
            auto lock = std::lock_guard(mutex);

            /** Compiled expressions stored in cache need to be destroyed before destruction of static objects.
          * Because CHJIT instance can be static object.
          */
#if USE_EMBEDDED_COMPILER
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
#endif

            global_binding_cache_manager.reset();

            /// Preemptive destruction is important, because these objects may have a refcount to ContextShared (cyclic reference).
            /// TODO: Get rid of this.

            /// Dictionaries may be required:
            /// - for storage shutdown (during final flush of the Buffer engine)
            /// - before storage startup (because of some streaming of, i.e. Kafka, to
            ///   the table with materialized column that has dictGet)
            ///
            /// So they should be created before any storages and preserved until storages will be terminated.
            ///
            /// But they cannot be created before storages since they may required table as a source,
            /// but at least they can be preserved for storage termination.
            prepared_statement_manager.reset();

            dictionaries_xmls.reset();
            dictionaries_cnch_catalog.reset();

            delete_system_logs = std::move(system_logs);
            delete_cnch_system_logs = std::move(cnch_system_logs);
            embedded_dictionaries.reset();
            external_dictionaries_loader.reset();
            cnch_catalog_dict_cache.reset();
            models_repository_guard.reset();
            external_models_loader.reset();
            buffer_flush_schedule_pool.reset();
            schedule_pool.reset();
            distributed_schedule_pool.reset();
            message_broker_schedule_pool.reset();
            for (auto & p : extra_schedule_pools)
                p.pool.reset();
            ddl_worker.reset();

            /// Stop trace collector if any
            trace_collector.reset();
            /// Stop zookeeper connection
            zookeeper.reset();

            named_sessions.reset();
            named_cnch_sessions.reset();
        }

        /// Can be removed w/o context lock
        delete_system_logs.reset();
        delete_cnch_system_logs.reset();
    }

    bool hasTraceCollector() const
    {
        return trace_collector.has_value();
    }

    void initializeTraceCollector(std::shared_ptr<TraceLog> trace_log)
    {
        if (!trace_log)
            return;
        if (hasTraceCollector())
            return;

        trace_collector.emplace(std::move(trace_log));
    }
};

ContextData::ContextData() = default;
ContextData::ContextData(const ContextData &) = default;

Context::Context() = default;
Context::Context(const Context & rhs) : ContextData(rhs), std::enable_shared_from_this<Context>(rhs) {}

SharedContextHolder::SharedContextHolder(SharedContextHolder &&) noexcept = default;
SharedContextHolder & SharedContextHolder::operator=(SharedContextHolder &&) = default;
SharedContextHolder::SharedContextHolder() = default;
SharedContextHolder::~SharedContextHolder() = default;
SharedContextHolder::SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context) : shared(std::move(shared_context))
{
}

void SharedContextHolder::reset()
{
    shared.reset();
}

ContextMutablePtr Context::createGlobal(ContextSharedPart * shared_part)
{
    auto res = std::shared_ptr<Context>(new Context);
    res->shared = shared_part;
    return res;
}

void Context::initGlobal()
{
    assert(!global_context_instance);
    global_context_instance = shared_from_this();
    DatabaseCatalog::init(shared_from_this());
}

SharedContextHolder Context::createShared()
{
    return SharedContextHolder(std::make_unique<ContextSharedPart>());
}

void Context::addSessionView(StorageID view_table_id, StoragePtr view_storage)
{
    auto lock = getLocalLock();
    if (session_views_cache.find(view_table_id) != session_views_cache.end())
       return;
    session_views_cache.emplace(view_table_id, view_storage);
}

StoragePtr Context::getSessionView(StorageID view_table_id)
{
    {
        auto lock = getLocalSharedLock();
        auto it = session_views_cache.find(view_table_id);
        if (it != session_views_cache.end())
        return it->second;
    }

    /// should be done outside the context lock, otherwise may deadlock
    StoragePtr view_storage =  DatabaseCatalog::instance().tryGetTable(view_table_id, shared_from_this());

    if (view_storage)
        addSessionView(view_table_id, view_storage);
    return view_storage;
}

ContextMutablePtr Context::createCopy(const ContextPtr & other)
{
    auto lock = other->getLocalSharedLock();
    return std::shared_ptr<Context>(new Context(*other));
}

ContextMutablePtr Context::createCopy(const ContextWeakPtr & other)
{
    auto ptr = other.lock();
    if (!ptr)
        throw Exception("Can't copy an expired context", ErrorCodes::LOGICAL_ERROR);
    return createCopy(ptr);
}

ContextMutablePtr Context::createCopy(const ContextMutablePtr & other)
{
    return createCopy(std::const_pointer_cast<const Context>(other));
}

Context::~Context() = default;

WorkerStatusManagerPtr Context::getWorkerStatusManager() const
{
    return worker_status_manager;
}

void Context::setWorkerStatusManager()
{
    if (tryGetCurrentWorkerGroup() && tryGetCurrentVW())
    {
        worker_status_manager = current_vw->getWorkerStatusManager(current_worker_group->getID());
    }
}

void Context::adaptiveSelectWorkers(SchedulerMode mode)
{
    if (tryGetCurrentWorkerGroup() && tryGetCurrentVW())
    {
        Stopwatch sw;
        worker_status_manager = current_vw->getWorkerStatusManager(current_worker_group->getID());
        std::vector<WorkerId> worker_ids;
        worker_ids.reserve(current_worker_group->size());
        for (const auto & host_ports : current_worker_group->getHostWithPortsVec())
            worker_ids.emplace_back(
                WorkerStatusManager::getWorkerId(current_worker_group->getVWName(), current_worker_group->getID(), host_ports.id));

        worker_group_status = worker_status_manager->getWorkerGroupStatus(worker_ids, mode);

        auto indices = worker_group_status->selectHealthNode(current_worker_group->getHostWithPortsVec());
        if (indices)
            setCurrentWorkerGroup(std::make_shared<WorkerGroupHandleImpl>(*current_worker_group, *indices));

        ProfileEvents::increment(ProfileEvents::SelectHealthWorkerMilliSeconds, sw.elapsedMilliseconds());
    }
}

InterserverIOHandler & Context::getInterserverIOHandler()
{
    return shared->interserver_io_handler;
}

ReadSettings Context::getReadSettings() const
{
    ReadSettings res;

    res.remote_fs_read_failed_injection = settings.remote_fs_read_failed_injection;

    res.remote_fs_prefetch = settings.remote_filesystem_read_prefetch;
    res.local_fs_prefetch = settings.local_filesystem_read_prefetch;
    res.remote_read_log = settings.enable_remote_read_log ? getRemoteReadLog().get() : nullptr;
    res.enable_io_scheduler = settings.enable_io_scheduler;
    res.enable_io_pfra = settings.enable_io_pfra;
    res.enable_cloudfs = settings.enable_cloudfs;
    res.enable_nexus_fs = settings.enable_nexus_fs;
    res.local_fs_buffer_size
        = settings.max_read_buffer_size_local_fs ? settings.max_read_buffer_size_local_fs : settings.max_read_buffer_size;
    res.remote_fs_buffer_size
        = settings.max_read_buffer_size_remote_fs ? settings.max_read_buffer_size_remote_fs : settings.max_read_buffer_size;
    res.aio_threshold = settings.min_bytes_to_use_direct_io;
    res.mmap_threshold = settings.min_bytes_to_use_mmap_io;
    res.mmap_cache = getMMappedFileCache().get();
    res.remote_read_min_bytes_for_seek = settings.remote_read_min_bytes_for_seek;
    res.disk_cache_mode = settings.disk_cache_mode;
    res.skip_download_if_exceeds_query_cache = settings.skip_download_if_exceeds_query_cache;
    res.parquet_decode_threads = settings.max_download_threads;
    res.filtered_ratio_to_use_skip_read = settings.filtered_ratio_to_use_skip_read;
    res.zero_copy_read_from_cache = settings.enable_zero_copy_read;
    if (settings.enable_io_uring_for_local_fs_read)
    {
        if (getIOUringReader().isSupported())
            res.local_fs_method = LocalFSReadMethod::io_uring;
        else
            LOG_WARNING(getLogger("Context"), "IOUring is not supported, use default local_fs_method");
    }

    return res;
}

std::unique_lock<std::recursive_mutex> Context::getLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock(shared->mutex);
}

/// NOTE: it's an non-recursive lock, caller should be aware of the deadlock risk
std::unique_lock<SharedMutex> Context::getLocalLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock(mutex);
}

/// NOTE: it's an non-recursive lock, caller should be aware of the deadlock risk
std::shared_lock<SharedMutex> Context::getLocalSharedLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::shared_lock(mutex);
}

ProcessList & Context::getProcessList()
{
    return shared->process_list;
}
const ProcessList & Context::getProcessList() const
{
    return shared->process_list;
}
PlanSegmentProcessList & Context::getPlanSegmentProcessList()
{
    return shared->plan_segment_process_list;
}
const PlanSegmentProcessList & Context::getPlanSegmentProcessList() const
{
    return shared->plan_segment_process_list;
}
MergeList & Context::getMergeList()
{
    return shared->merge_list;
}
const MergeList & Context::getMergeList() const
{
    return shared->merge_list;
}
ManipulationList & Context::getManipulationList()
{
    return shared->manipulation_list;
}
const ManipulationList & Context::getManipulationList() const
{
    return shared->manipulation_list;
}
ReplicatedFetchList & Context::getReplicatedFetchList()
{
    return shared->replicated_fetch_list;
}
const ReplicatedFetchList & Context::getReplicatedFetchList() const
{
    return shared->replicated_fetch_list;
}

SegmentSchedulerPtr Context::getSegmentScheduler()
{
    auto lock = getLock(); // checked
    if (!shared->segment_scheduler)
        shared->segment_scheduler = std::make_shared<SegmentScheduler>();
    return shared->segment_scheduler;
}

SegmentSchedulerPtr Context::getSegmentScheduler() const
{
    auto lock = getLock(); // checked
    if (!shared->segment_scheduler)
        shared->segment_scheduler = std::make_shared<SegmentScheduler>();
    return shared->segment_scheduler;
}

void Context::setMockExchangeDataTracker(ExchangeStatusTrackerPtr exchange_data_tracker)
{
    auto lock = getLock(); // checked
    shared->exchange_data_tracker = exchange_data_tracker;
}

ExchangeStatusTrackerPtr Context::getExchangeDataTracker() const
{
    auto lock = getLock(); // checked
    if (!shared->exchange_data_tracker)
    {
        if (shared->server_type == ServerType::cnch_server)
        {
            shared->exchange_data_tracker = std::make_shared<ExchangeStatusTracker>(global_context);
        }
        else
        {
            throw Exception("Exchange data tracker is not supported", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    return shared->exchange_data_tracker;
}

void Context::initDiskExchangeDataManager() const
{
    getDiskExchangeDataManager();
}

DiskExchangeDataManagerPtr Context::getDiskExchangeDataManager() const
{
    auto lock = getLock(); // checked
    if (!shared->disk_exchange_data_manager)
    {
        const auto & bsp_conf = getRootConfig().bulk_synchronous_parallel;
        String id;
        if (getServerType() == ServerType::cnch_worker)
        {
            id = getenv("WORKER_ID") ? getenv("WORKER_ID") : getHostWithPorts().getTCPAddress();
        }
        else
        {
            id = getenv("SERVER_ID") ? getenv("SERVER_ID") : getHostWithPorts().getTCPAddress();
        }
        chassert(!id.empty());
        String manager_path = "bsp/" + id + "/v-1.0.0";
        LOG_DEBUG(shared->log, "Store exchange data with path {}", manager_path);
        DiskExchangeDataManagerOptions options{
            .path = manager_path,
            .storage_policy = bsp_conf.storage_policy,
            .volume = bsp_conf.volume,
            .gc_interval_seconds = bsp_conf.gc_interval_seconds,
            .file_expire_seconds = bsp_conf.file_expire_seconds,
            .max_disk_bytes = bsp_conf.max_disk_bytes};
        shared->disk_exchange_data_manager
            = DiskExchangeDataManager::createDiskExchangeDataManager(global_context, getGlobalContext(), options);
    }
    return shared->disk_exchange_data_manager;
}

void Context::setMockDiskExchangeDataManager(DiskExchangeDataManagerPtr disk_exchange_data_manager)
{
    auto lock = getLock(); // checked
    shared->disk_exchange_data_manager = disk_exchange_data_manager;
}

BindingCacheManagerPtr Context::getGlobalBindingCacheManager() const
{
    auto lock = getLock(); // checked
    if (this->shared->global_binding_cache_manager)
        return this->shared->global_binding_cache_manager;
    return nullptr;
}

BindingCacheManagerPtr Context::getGlobalBindingCacheManager()
{
    auto lock = getLock(); // checked
    if (this->shared->global_binding_cache_manager)
        return this->shared->global_binding_cache_manager;
    return nullptr;
}

void Context::setGlobalBindingCacheManager(std::shared_ptr<BindingCacheManager> && manager)
{
    auto lock = getLock(); // checked
    if (shared->global_binding_cache_manager)
        throw Exception("Global binding cache has been already created.", ErrorCodes::LOGICAL_ERROR);
    shared->global_binding_cache_manager = std::move(manager);
}

std::shared_ptr<BindingCacheManager> Context::getSessionBindingCacheManager() const
{
    auto lock = getLock(); // checked
    if (!this->session_binding_cache_manager)
    {
        this->session_binding_cache_manager = std::make_shared<BindingCacheManager>();
        this->session_binding_cache_manager->initializeSessionBinding();
    }
    return session_binding_cache_manager;
}

QueueManagerPtr Context::getQueueManager() const
{
    auto lock = getLock(); // checked
    if (!shared->queue_manager)
        shared->queue_manager = std::make_shared<QueueManager>(global_context);
    return shared->queue_manager;
}

AsyncQueryManagerPtr Context::getAsyncQueryManager() const
{
    auto lock = getLock(); // checked
    if (!shared->async_query_manager)
        shared->async_query_manager = std::make_shared<AsyncQueryManager>(global_context);
    return shared->async_query_manager;
}

void Context::enableNamedSessions()
{
    shared->named_sessions.emplace();
}

void Context::enableNamedCnchSessions()
{
    shared->named_cnch_sessions.emplace();
}

std::shared_ptr<NamedSession> Context::acquireNamedSession(const String & session_id, size_t timeout, bool session_check) const
{
    if (!shared->named_sessions)
        throw Exception("Support for named sessions is not enabled", ErrorCodes::NOT_IMPLEMENTED);

    auto user_name = client_info.current_user;

    if (user_name.empty())
        throw Exception("Empty user name.", ErrorCodes::LOGICAL_ERROR);

    auto res = shared->named_sessions->acquireSession({session_id, user_name}, shared_from_this(), timeout, session_check);

    if (res->context->getClientInfo().current_user != user_name)
        throw Exception("Session belongs to a different user", ErrorCodes::SESSION_IS_LOCKED);

    return res;
}

std::shared_ptr<NamedCnchSession> Context::acquireNamedCnchSession(const UInt64 & txn_id, size_t timeout, bool session_check, bool return_null_if_not_found) const
{
    if (!shared->named_cnch_sessions)
        throw Exception("Support for named sessions is not enabled", ErrorCodes::NOT_IMPLEMENTED);
    LOG_TRACE(shared->log, "Trying to acquire session for {}\n", txn_id);
    return shared->named_cnch_sessions->acquireSession(txn_id, shared_from_this(), timeout, session_check, return_null_if_not_found);
}

void Context::initCnchServerResource(const TxnTimestamp & txn_id)
{
    if (server_resource)
        return;

    server_resource = std::make_shared<CnchServerResource>(txn_id);
}

void Context::clearCnchServerResource()
{
    server_resource = nullptr;
}

CnchServerResourcePtr Context::getCnchServerResource() const
{
    if (!server_resource)
        throw Exception("Can't get CnchServerResource", ErrorCodes::SESSION_NOT_FOUND);

    return server_resource;
}

CnchServerResourcePtr Context::tryGetCnchServerResource() const
{
    return server_resource;
}

CnchWorkerResourcePtr Context::getCnchWorkerResource() const
{
    if (!worker_resource)
        throw Exception("Can't get CnchWorkerResource", ErrorCodes::SESSION_NOT_FOUND);

    return worker_resource;
}

CnchWorkerResourcePtr Context::tryGetCnchWorkerResource() const
{
    return worker_resource;
}

void Context::initCnchWorkerResource()
{
    auto lock = getLocalLock();
    if (!worker_resource)
        worker_resource = std::make_shared<CnchWorkerResource>();
}

void Context::setExtendedProfileInfo(const ExtendedProfileInfo & source) const
{
    auto lock = getLocalLock();
    extended_profile_info = source;
}

ExtendedProfileInfo Context::getExtendedProfileInfo() const
{
    auto lock = getLocalSharedLock();
    return extended_profile_info;
}

String Context::resolveDatabase(const String & database_name) const
{
    String res = database_name.empty() ? getCurrentDatabase() : database_name;
    if (res.empty())
        throw Exception("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
    return res;
}

String Context::getPath() const
{
    auto lock = getLock(); // checked
    return shared->path;
}

String Context::getFlagsPath() const
{
    auto lock = getLock(); // checked
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    auto lock = getLock(); // checked
    return shared->user_files_path;
}

String Context::getDictionariesLibPath() const
{
    auto lock = getLock(); // checked
    return shared->dictionaries_lib_path;
}

String Context::getMetastorePath() const
{
    auto lock = getLock(); // checked
    return shared->metastore_path;
}

VolumePtr Context::getTemporaryVolume() const
{
    auto lock = getLock(); // checked
    return shared->tmp_volume;
}

TemporaryDataOnDiskScopePtr Context::getTempDataOnDisk() const
{
    {
        auto lock = getLocalSharedLock();
        if (this->temp_data_on_disk)
            return this->temp_data_on_disk;
    }
    auto lock = getLock(); // checked
    return shared->temp_data_on_disk;
}

void Context::setTempDataOnDisk(TemporaryDataOnDiskScopePtr temp_data_on_disk_)
{
    auto lock = getLocalLock();
    this->temp_data_on_disk = std::move(temp_data_on_disk_);
}

void Context::setPath(const String & path)
{
    auto lock = getLock(); // checked

    shared->path = path;

    if (shared->tmp_path.empty() && (!shared->tmp_volume || !shared->temp_data_on_disk))
        shared->tmp_path = shared->path + "tmp/";

    if (shared->flags_path.empty())
        shared->flags_path = shared->path + "flags/";

    if (shared->user_files_path.empty())
        shared->user_files_path = shared->path + "user_files/";

    if (shared->dictionaries_lib_path.empty())
        shared->dictionaries_lib_path = shared->path + "dictionaries_lib/";
}

VolumePtr Context::setTemporaryStorage(const String & path, const String & policy_name)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    if (policy_name.empty())
    {
        shared->tmp_path = path;
        if (!shared->tmp_path.ends_with('/'))
            shared->tmp_path += '/';

        auto disk = std::make_shared<DiskLocal>("_tmp_default", shared->tmp_path, DiskStats{});
        shared->tmp_volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    }
    else
    {
        StoragePolicyPtr tmp_policy = getStoragePolicySelector(lock)->get(policy_name);
        if (tmp_policy->getVolumes().size() != 1)
            throw Exception(
                "Policy " + policy_name + " is used temporary files, such policy should have exactly one volume",
                ErrorCodes::NO_ELEMENTS_IN_CONFIG);
        shared->tmp_volume = tmp_policy->getVolume(0);
    }

    if (shared->tmp_volume->getDisks().empty())
        throw Exception("No disks volume for temporary files", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return shared->tmp_volume;
}

static void setupTmpPath(LoggerPtr log, const std::string & path)
try
{
    LOG_DEBUG(log, "Setting up {} to store temporary data in it", path);

    fs::create_directories(path);

    /// Clearing old temporary files.
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_regular_file())
        {
            if (startsWith(it->path().filename(), "tmp"))
            {
                LOG_DEBUG(log, "Removing old temporary file {}", it->path().string());
                fs::remove(it->path());
            }
            else
                LOG_DEBUG(log, "Found unknown file in temporary path {}", it->path().string());
        }
        /// We skip directories (for example, 'http_buffers' - it's used for buffering of the results) and all other file types.
    }
}
catch (...)
{
    DB::tryLogCurrentException(
        log,
        fmt::format(
            "Caught exception while setup temporary path: {}. "
            "It is ok to skip this exception as cleaning old temporary files is not necessary",
            path));
}

static VolumePtr createLocalSingleDiskVolume(const std::string & path)
{
    auto disk = std::make_shared<DiskLocal>("_tmp_default", path, DiskStats{});
    VolumePtr volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    return volume;
}

void Context::setTemporaryStoragePath()
{
    // todo aron TemporaryStoragePath
    // shared->tmp_path = path;
    // if (!shared->tmp_path.ends_with('/'))
    //      shared->tmp_path += '/';

    // VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path);

    // for (const auto & disk : volume->getDisks())
    // {
    //      setupTmpPath(shared->log, disk->getPath());
    // }

    // shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(shared->tmp_volume, 0);
}

void Context::setTemporaryStoragePath(const String & path, size_t max_size)
{
    shared->tmp_path = path;
    if (!shared->tmp_path.ends_with('/'))
        shared->tmp_path += '/';

    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path);

    for (const auto & disk : volume->getDisks())
    {
        setupTmpPath(shared->log, disk->getPath());
    }

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);
}


void Context::setTemporaryStoragePolicy(const String & policy_name, size_t max_size)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    StoragePolicyPtr tmp_policy = getStoragePolicySelector(lock)->get(policy_name);
    if (tmp_policy->getVolumes().size() != 1)
        throw Exception(
            ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Policy '{}' is used temporary files, such policy should have exactly one volume",
            policy_name);
    VolumePtr volume = tmp_policy->getVolume(0);

    if (volume->getDisks().empty())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No disks volume for temporary files");

    for (const auto & disk : volume->getDisks())
    {
        if (!disk)
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Temporary disk is null");

        /// Check that underlying disk is local (can be wrapped in decorator)
        DiskPtr disk_ptr = disk;

        if (dynamic_cast<const DiskLocal *>(disk_ptr.get()) == nullptr)
        {
            const auto * disk_raw_ptr = disk_ptr.get();
            throw Exception(
                ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "Disk '{}' ({}) is not local and can't be used for temporary files",
                disk_ptr->getName(),
                typeid(*disk_raw_ptr).name());
        }

        setupTmpPath(shared->log, disk->getPath());
    }

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);
}

//void Context::setTemporaryStorageInCache(const String & cache_disk_name, size_t max_size)
//{
//    auto disk_ptr = getDisk(cache_disk_name);
//    if (!disk_ptr)
//         throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Disk '{}' is not found", cache_disk_name);
//
//    const auto * disk_object_storage_ptr = dynamic_cast<const DiskObjectStorage *>(disk_ptr.get());
//    if (!disk_object_storage_ptr)
//         throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Disk '{}' does not use cache", cache_disk_name);
//
//    auto file_cache = disk_object_storage_ptr->getCache();
//    if (!file_cache)
//         throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Cache '{}' is not found", file_cache->getBasePath());
//
//    LOG_DEBUG(shared->log, "Using file cache ({}) for temporary files", file_cache->getBasePath());
//
//    shared->tmp_path = file_cache->getBasePath();
//    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path);
//    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, file_cache.get(), max_size);
//}

void Context::setFlagsPath(const String & path)
{
    auto lock = getLock(); // checked
    shared->flags_path = path;
}

void Context::setUserFilesPath(const String & path)
{
    auto lock = getLock(); // checked
    shared->user_files_path = path;
}

void Context::setDictionariesLibPath(const String & path)
{
    auto lock = getLock(); // checked
    shared->dictionaries_lib_path = path;
}

void Context::setMetastorePath(const String & path)
{
    auto lock = getLock(); // checked
    shared->metastore_path = path;
}

void Context::setConfig(const ConfigurationPtr & config)
{
    auto lock = getLock(); // checked
    shared->config = config;
    shared->access_control_manager.setExternalAuthenticatorsConfig(*shared->config);
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    auto lock = getLock(); // checked
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}

const Poco::Util::AbstractConfiguration & Context::getConfigRefWithLock(const std::unique_lock<std::recursive_mutex> &) const
{
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}

void Context::initRootConfig(const Poco::Util::AbstractConfiguration & config)
{
    shared->root_config.loadFromPocoConfig(config, "");
}

void Context::initCnchConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("cnch_config"))
    {
        const auto cnch_config_path = config.getString("cnch_config");
        ConfigProcessor config_processor(cnch_config_path);
        const auto loaded_config = config_processor.loadConfig();
        shared->cnch_config = loaded_config.configuration;
    }
    else
        throw Exception("cnch_config not found", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
}

const Poco::Util::AbstractConfiguration & Context::getCnchConfigRef() const
{
    return shared->cnch_config ? *shared->cnch_config : getConfigRef();
}

void Context::updateRootConfig(std::function<void (RootConfiguration &)> update_callback)
{
    update_callback(shared->root_config);
}

const RootConfiguration & Context::getRootConfig() const
{
    return shared->root_config;
}

void Context::reloadRootConfig(const Poco::Util::AbstractConfiguration & config)
{
    shared->root_config.reloadFromPocoConfig(config);
}


AccessControlManager & Context::getAccessControlManager()
{
    return shared->access_control_manager;
}

const AccessControlManager & Context::getAccessControlManager() const
{
    return shared->access_control_manager;
}

void Context::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock(); // checked
    shared->access_control_manager.setExternalAuthenticatorsConfig(config);
}

std::unique_ptr<GSSAcceptorContext> Context::makeGSSAcceptorContext() const
{
    auto lock = getLock(); // checked
    return std::make_unique<GSSAcceptorContext>(shared->access_control_manager.getExternalAuthenticators().getKerberosParams());
}

bool Context::mustEnableAdditionalService(AdditionalService::Value svc, bool need_throw) const
{
    if (need_throw)
    {
         shared->additional_services.throwIfDisabled(svc);
         return true;
    }
    else
    {
         return shared->additional_services.enabled(svc);
    }
}

void Context::updateAdditionalServices(const Poco::Util::AbstractConfiguration & config)
{
    shared->additional_services.parseAdditionalServicesFromConfig(config);
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    auto lock = getLock(); // checked
    shared->users_config = config;
    shared->access_control_manager.setUsersConfig(*shared->users_config);
    if (getServerType() == ServerType::cnch_server || getServerType() == ServerType::cnch_worker)
    {
        if (!shared->resource_group_manager)
            initResourceGroupManager(config);

        if (shared->resource_group_manager)
            shared->resource_group_manager->initialize(*shared->users_config);
    }
}

ConfigurationPtr Context::getUsersConfig()
{
    auto lock = getLock(); // checked
    return shared->users_config;
}

VWCustomizedSettingsPtr Context::getVWCustomizedSettings() const
{
    std::lock_guard<std::mutex> lock(shared->vw_customized_settings_update_mutex);
    return shared->vw_customized_settings_ptr;
}

void Context::setVWCustomizedSettings(VWCustomizedSettingsPtr vw_customized_settings_ptr_)
{
    if (shared->vw_customized_settings_ptr)
        LOG_INFO(
            shared->log,
            "VWCustomizedSetting before update:[{}] and after update:[{}]",
            shared->vw_customized_settings_ptr->toString(),
            vw_customized_settings_ptr_->toString());
    else
        LOG_INFO(shared->log, "VWCustomizedSetting :[{}]", vw_customized_settings_ptr_->toString());

    std::lock_guard<std::mutex> lock(shared->vw_customized_settings_update_mutex);
    shared->vw_customized_settings_ptr = vw_customized_settings_ptr_;
}


void Context::initResourceGroupManager(const ConfigurationPtr & )
{
    LOG_DEBUG(shared->log, "Skip initialize resource group");

    // if (!config->has("resource_groups"))
    // {
    //     LOG_DEBUG(getLogger("Context"), "No config found. Not creating Resource Group Manager");
    //     return ;
    // }
    // auto resource_group_manager_type = config->getRawString("resource_groups.type", "vw");
    // if (resource_group_manager_type == "vw")
    // {
    //     if (!getResourceManagerClient())
    //     {
    //         LOG_ERROR(getLogger("Context"), "Cannot create VW Resource Group Manager since Resource Manager client is not initialised.");
    //         return;
    //     }
    //     LOG_DEBUG(getLogger("Context"), "Creating VW Resource Group Manager");
    //     shared->resource_group_manager = std::make_shared<VWResourceGroupManager>(getGlobalContext());
    // }
    // else if (resource_group_manager_type == "internal")
    // {
    //     LOG_DEBUG(getLogger("Context"), "Creating Internal Resource Group Manager");
    //     shared->resource_group_manager = std::make_shared<InternalResourceGroupManager>();
    // }
    // else
    //     throw Exception("Unknown Resource Group Manager type", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
}

void Context::setResourceGroup(const IAST * ast)
{
    IResourceGroup * group = nullptr;
    {
        auto lock = getLock(); // checked
        if (shared->resource_group_manager && shared->resource_group_manager->isInUse())
            group = shared->resource_group_manager->selectGroup(*this, ast);
    }
    auto lock = getLocalLock();
    resource_group = group;
}

IResourceGroup * Context::tryGetResourceGroup() const
{
    return resource_group.load(std::memory_order_acquire);
}

IResourceGroupManager * Context::tryGetResourceGroupManager()
{
    if (shared->resource_group_manager)
        return shared->resource_group_manager.get();
    return nullptr;
}

IResourceGroupManager * Context::tryGetResourceGroupManager() const
{
    if (shared->resource_group_manager)
        return shared->resource_group_manager.get();
    return nullptr;
}

void Context::startResourceGroup()
{
    shared->resource_group_manager->enable();
}
void Context::stopResourceGroup()
{
    shared->resource_group_manager->disable();
}

void Context::setUser(const Credentials & credentials, const Poco::Net::SocketAddress & address)
{
    /// Find a user with such name and check the credentials.
    /// NOTE: getAccessControlManager().login and other AccessControl's functions may require some IO work,
    /// so Context::getLock() must be unlocked while we're doing this.
    auto new_user_id = getAccessControlManager().login(credentials, address.host());

    ContextAccessParams params;
    {
        auto lock = getLocalLock();
        client_info.current_user = credentials.getUserName();
        client_info.current_address = address;

        //#if defined(ARCADIA_BUILD)
        /// This is harmful field that is used only in foreign "Arcadia" build.
        client_info.current_password.clear();
        if (const auto * basic_credentials = dynamic_cast<const BasicCredentials *>(&credentials))
            client_info.current_password = basic_credentials->getPassword();
        //#endif

        String tenant = getTenantId();
        params = getAccessControlManager().getContextAccessParams(
            new_user_id, /* current_roles = */ {}, /* use_default_roles = */ true, settings, current_database, client_info,
            tenant,
            has_tenant_id_in_username,
            getServerType() != ServerType::cnch_server);
    }

    auto new_access = getAccessControlManager().getContextAccess(params);

    auto lock = getLocalLock();
    user_id = new_user_id;
    access = std::move(new_access);

    auto default_profile_info = access->getDefaultProfileInfo();
    settings_constraints_and_current_profiles = default_profile_info->getConstraintsAndProfileIDs();
    current_roles.clear();
    use_default_roles = true;

    applySettingsChangesWithLock(default_profile_info->settings, /*internal*/ true, lock);
}

String Context::formatUserName(const String & name)
{
    //CNCH multi-tenant user name pattern from gateway client: {tenant_id}`{user_name}
    String user = name;
    if (auto pos = user.find('`'); pos != String::npos)
    {
        auto tenant_id = String(user.c_str(), pos);
        this->setSetting("tenant_id", tenant_id); /// {tenant_id}`*
        this->setTenantId(tenant_id);
        auto sub_user = user.substr(pos + 1);
        if (sub_user != "default")
        {
            has_tenant_id_in_username = true;
            user[pos] = '.';            ///{tenant_id}`{user_name}=>{tenant_id}.{user_name}
        }
        else
        {
            user = std::move(sub_user); ///{tenant_id}`default=>default
        }
    }
    return user;
}

void Context::setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address)
{
    setUser(BasicCredentials(formatUserName(name), password), address);
}

void Context::setUserWithoutCheckingPassword(const String & name, const Poco::Net::SocketAddress & address)
{
    setUser(AlwaysAllowCredentials(formatUserName(name)), address);
}

std::shared_ptr<const User> Context::getUser() const
{
    return getAccess()->getUser();
}

void Context::setQuotaKey(String quota_key_)
{
    auto lock = getLocalLock();
    client_info.quota_key = std::move(quota_key_);
}

String Context::getUserName() const
{
    return getAccess()->getUserName();
}

std::optional<UUID> Context::getUserID() const
{
    auto lock = getLocalSharedLock();
    return user_id;
}

void Context::setCurrentRoles(const std::vector<UUID> & current_roles_)
{
    auto lock = getLocalLock();
    if (current_roles == current_roles_ && !use_default_roles)
        return;
    current_roles = current_roles_;
    use_default_roles = false;
    calculateAccessRightsWithLock(lock);
}

void Context::setCurrentRolesDefault()
{
    auto lock = getLocalLock();
    if (use_default_roles)
        return;
    current_roles.clear();
    use_default_roles = true;
    calculateAccessRightsWithLock(lock);
}

boost::container::flat_set<UUID> Context::getCurrentRoles() const
{
    return getRolesInfo()->current_roles;
}

boost::container::flat_set<UUID> Context::getEnabledRoles() const
{
    return getRolesInfo()->enabled_roles;
}

std::shared_ptr<const EnabledRolesInfo> Context::getRolesInfo() const
{
    return getAccess()->getRolesInfo();
}


void Context::calculateAccessRightsWithLock(const std::unique_lock<SharedMutex> &)
{
    if (user_id)
    {
        auto params = getAccessControlManager().getContextAccessParams(
            *user_id, current_roles, use_default_roles, settings, current_database, client_info,
            tenant_id, has_tenant_id_in_username, false);
        access = getAccessControlManager().getContextAccess(params);
    }
}




bool Context::isExternalDb(const std::string_view& database) const
{
        std::optional<std::string> catalog_name;
        std::optional<std::string> db_name;
        std::tie(catalog_name, db_name) = getCatalogNameAndDatabaseName(database);


        if (!catalog_name.has_value())
        {
            auto current_catalog = getCurrentCatalog();
            catalog_name = current_catalog.empty() ? "cnch" : current_catalog;
        }
        // skip access check to resources under hive catalog.
        if (getOriginalDatabaseName(catalog_name.value()) != "cnch")
        {
            return true;
        }
        return false;
}

template <typename... OtherArgs>
std::string_view getDatabase([[maybe_unused]] const AccessFlags & args1, const std::string_view & arg2, const OtherArgs &...)
{
    return arg2;
}


template <typename... OtherArgs>
std::string_view getDatabase([[maybe_unused]] const AccessFlags & args1, const std::string & arg2, const OtherArgs &...)
{
    return arg2;
}

template <typename... Args>
void Context::checkAccessImpl(const Args &... args) const
{
    if constexpr (sizeof...(Args) <= 1)
    {
        getAccess()->checkAccess(args...);
    }
    else
    {
        static_assert(sizeof...(Args) > 1, "Logical Error");
        using FirstType = typename std::tuple_element<0, std::tuple<Args...>>::type;
        using SecondType = typename std::tuple_element<1, std::tuple<Args...>>::type;
        static_assert(std::is_same_v<std::decay_t<FirstType>, AccessFlags>, "First argument must be AccessFlags");
        static_assert(
            std::is_same_v<std::decay_t<SecondType>, std::string_view> || std::is_same_v<std::decay_t<SecondType>, std::string>,
            "Second argument must be std::string_view");

        // we know the second argument is database.
        if(isExternalDb(getDatabase(args...)))
        {
            return;
        }
        getAccess()->checkAccess(args...);
    }
}

template <typename... Args>
bool Context::isGrantedImpl(const Args &... args) const
{
    if constexpr (sizeof...(Args) <= 1)
    {
        getAccess()->isGranted(args...);
    }
    else
    {
        static_assert(sizeof...(Args) > 1, "Logical Error");
        using FirstType = typename std::tuple_element<0, std::tuple<Args...>>::type;
        using SecondType = typename std::tuple_element<1, std::tuple<Args...>>::type;
        static_assert(std::is_same_v<std::decay_t<FirstType>, AccessFlags>, "First argument must be AccessFlags");
        static_assert(
            std::is_same_v<std::decay_t<SecondType>, std::string_view> || std::is_same_v<std::decay_t<SecondType>, std::string>,
            "Second argument must be std::string_view");

        // we know the second argument is database.
        if(isExternalDb(getDatabase(args...)))
        {
            return true;
        }
        return getAccess()->isGranted(args...);
    }
}

void Context::checkAccess(const AccessFlags & flags) const
{
    return checkAccessImpl(flags);
}
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database) const
{
    return checkAccessImpl(flags, database);
}
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const
{
    return checkAccessImpl(flags, database, table);
}
void Context::checkAccess(
    const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const
{
    return checkAccessImpl(flags, database, table, column);
}
void Context::checkAccess(
    const AccessFlags & flags,
    const std::string_view & database,
    const std::string_view & table,
    const std::vector<std::string_view> & columns) const
{
    return checkAccessImpl(flags, database, table, columns);
}
void Context::checkAccess(
    const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const
{
    return checkAccessImpl(flags, database, table, columns);
}
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id) const
{
    checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName());
}
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::string_view & column) const
{
    checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), column);
}
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const
{
    checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns);
}
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const
{
    checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns);
}
void Context::checkAccess(const AccessRightsElement & element) const
{
    return checkAccessImpl(element);
}
void Context::checkAccess(const AccessRightsElements & elements) const
{
    return checkAccessImpl(elements);
}

bool Context::isGranted(const AccessFlags & flags, const StorageID & table_id, const std::string_view & column) const
{
    return isGrantedImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), column);
}

bool Context::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const
{
    return isGrantedImpl(flags, database, table, column);
}

void Context::grantAllAccess()
{
    auto lock = getLocalLock();
    access = ContextAccess::getFullAccess();
}

std::shared_ptr<const ContextAccess> Context::getAccess() const
{
    // If its a worker node and prefer_cnch_catalog is false, this is a query from server
    // and access check has already been done in server. We can return full access.
    if (getServerType() == ServerType::cnch_worker && !getSettingsRef().prefer_cnch_catalog)
        return ContextAccess::getFullAccess();

    auto lock = getLocalSharedLock();
    return access ? access : ContextAccess::getFullAccess();
}

void Context::checkAeolusTableAccess(const String & database_name, const String & table_name) const
{
    /// avoid check temporary and system table.
    if (database_name == DatabaseCatalog::TEMPORARY_DATABASE || database_name == DatabaseCatalog::SYSTEM_DATABASE)
        return;

    String full_table_name = database_name.empty() ? table_name : database_name+"."+table_name;
    if (!aeolusCheck(*this, full_table_name))
    {
        throw Exception("Access denied to " + full_table_name , ErrorCodes::DATABASE_ACCESS_DENIED);
    }
}

ASTPtr Context::getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType type) const
{
    ASTPtr condition;
    {
        auto lock = getLocalSharedLock();
        condition = initial_row_policy ? initial_row_policy->getCondition(database, table_name, type) : nullptr;
    }
    return getAccess()->getRowPolicyCondition(database, table_name, type, condition);
}

void Context::setInitialRowPolicy()
{
    String initial_user_copy;
    {
        auto lock = getLocalLock();
        initial_user_copy = client_info.initial_user;
    }
    auto initial_user_id = getAccessControlManager().find<User>(initial_user_copy);
    auto initial_row_policy_local = initial_user_id ? getAccessControlManager().getEnabledRowPolicies(*initial_user_id, {}) : nullptr;
    auto lock = getLocalLock();
    initial_row_policy = initial_row_policy_local;
}


std::shared_ptr<const EnabledQuota> Context::getQuota() const
{
    return getAccess()->getQuota();
}


std::optional<QuotaUsage> Context::getQuotaUsage() const
{
    return getAccess()->getQuotaUsage();
}

void Context::setCurrentProfileWithLock(const String & profile_name, const std::unique_lock<SharedMutex> & lock)
{
    try
    {
        UUID profile_id = getAccessControlManager().getID<SettingsProfile>(profile_name);
        setCurrentProfileWithLock(profile_id, lock);
    }
    catch (Exception & e)
    {
        e.addMessage(", while trying to set settings profile {}", profile_name);
        throw;
    }
}

void Context::setCurrentProfileWithLock(const UUID & profile_id, const std::unique_lock<SharedMutex> & lock)
{
    auto profile_info = getAccessControlManager().getSettingsProfileInfo(profile_id);
    setCurrentProfileWithLock(*profile_info, lock);
}

void Context::setCurrentProfileWithLock(const SettingsProfilesInfo & profiles_info, const std::unique_lock<SharedMutex> & lock)
{
    checkSettingsConstraintsWithLock(profiles_info.settings);
    applySettingsChangesWithLock(profiles_info.settings, true, lock);
    settings_constraints_and_current_profiles = profiles_info.getConstraintsAndProfileIDs(settings_constraints_and_current_profiles);
}

void Context::setCurrentProfile(const String & profile_name)
{
    auto lock = getLocalLock();
    setCurrentProfileWithLock(profile_name, lock);
}

void Context::setCurrentProfile(const UUID & profile_id)
{
    auto lock = getLocalLock();
    setCurrentProfileWithLock(profile_id, lock);
}

std::vector<UUID> Context::getCurrentProfiles() const
{
    auto lock = getLocalSharedLock();
    return settings_constraints_and_current_profiles->current_profiles;
}

std::vector<UUID> Context::getEnabledProfiles() const
{
    auto lock = getLocalSharedLock();
    return settings_constraints_and_current_profiles->enabled_profiles;
}

const Scalars & Context::getScalars() const
{
    return scalars;
}


const Block & Context::getScalar(const String & name) const
{
    auto it = scalars.find(name);
    if (scalars.end() == it)
    {
        // This should be a logical error, but it fails the sql_fuzz test too
        // often, so 'bad arguments' for now.
        throw Exception("Scalar " + backQuoteIfNeed(name) + " doesn't exist (internal bug)", ErrorCodes::BAD_ARGUMENTS);
    }
    return it->second;
}


Tables Context::getExternalTables() const
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    auto lock = getLocalSharedLock();

    Tables res;
    for (const auto & table : external_tables_mapping)
        res[table.first] = table.second->getTable();

    auto query_context_ptr = query_context.lock();
    auto session_context_ptr = session_context.lock();
    if (query_context_ptr && query_context_ptr.get() != this)
    {
        Tables buf = query_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    else if (session_context_ptr && session_context_ptr.get() != this)
    {
        Tables buf = session_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    return res;
}


void Context::addExternalTable(const String & table_name, TemporaryTableHolder && temporary_table)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    auto lock = getLocalLock();
    if (external_tables_mapping.end() != external_tables_mapping.find(table_name))
        throw Exception("Temporary table " + backQuoteIfNeed(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    external_tables_mapping.emplace(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}


std::shared_ptr<TemporaryTableHolder> Context::removeExternalTable(const String & table_name)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    std::shared_ptr<TemporaryTableHolder> holder;
    {
        auto lock = getLocalLock();
        auto iter = external_tables_mapping.find(table_name);
        if (iter == external_tables_mapping.end())
            return {};
        holder = iter->second;
        external_tables_mapping.erase(iter);
    }
    return holder;
}


void Context::addScalar(const String & name, const Block & block)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    scalars[name] = block;
}


bool Context::hasScalar(const String & name) const
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    return scalars.count(name);
}


void Context::addQueryAccessInfo(
    const String & quoted_database_name, const String & full_quoted_table_name, const Names & column_names, const String & projection_name)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    std::lock_guard<std::mutex> lock(query_access_info.mutex);
    query_access_info.databases.emplace(quoted_database_name);
    query_access_info.tables.emplace(full_quoted_table_name);
    for (const auto & column_name : column_names)
        query_access_info.columns.emplace(full_quoted_table_name + "." + backQuoteIfNeed(column_name));
    if (!projection_name.empty())
        query_access_info.projections.emplace(full_quoted_table_name + "." + backQuoteIfNeed(projection_name));
}


void Context::addQueryFactoriesInfo(QueryLogFactories factory_type, const String & created_object) const
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    auto lock = getLocalLock();

    switch (factory_type)
    {
        case QueryLogFactories::AggregateFunction:
            query_factories_info.aggregate_functions.emplace(created_object);
            break;
        case QueryLogFactories::AggregateFunctionCombinator:
            query_factories_info.aggregate_function_combinators.emplace(created_object);
            break;
        case QueryLogFactories::Database:
            query_factories_info.database_engines.emplace(created_object);
            break;
        case QueryLogFactories::DataType:
            query_factories_info.data_type_families.emplace(created_object);
            break;
        case QueryLogFactories::Dictionary:
            query_factories_info.dictionaries.emplace(created_object);
            break;
        case QueryLogFactories::Format:
            query_factories_info.formats.emplace(created_object);
            break;
        case QueryLogFactories::Function:
            query_factories_info.functions.emplace(created_object);
            break;
        case QueryLogFactories::Storage:
            query_factories_info.storages.emplace(created_object);
            break;
        case QueryLogFactories::TableFunction:
            query_factories_info.table_functions.emplace(created_object);
    }
}


StoragePtr Context::executeTableFunction(const ASTPtr & table_expression)
{
    /// Slightly suboptimal.
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);

    StoragePtr & res = table_function_results[key];

    if (!res)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression, shared_from_this());

        /// Run it and remember the result
        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());
    }

    return res;
}


void Context::addViewSource(const StoragePtr & storage)
{
    if (view_source)
        throw Exception(
            "Temporary view source storage " + backQuoteIfNeed(view_source->getName()) + " already exists.",
            ErrorCodes::TABLE_ALREADY_EXISTS);
    view_source = storage;
}


StoragePtr Context::getViewSource() const
{
    return view_source;
}

void Context::setSettingWithLock(const StringRef & name, const String & value, const std::unique_lock<SharedMutex> & lock)
{
    if (name == "profile")
    {
        setCurrentProfileWithLock(value, lock);
        return;
    }
    settings.set(std::string_view{name}, value);

    if (ContextAccessParams::dependsOnSettingName(name.toView()))
        calculateAccessRightsWithLock(lock);
}

void Context::setSettingWithLock(const StringRef & name, const Field & value, const std::unique_lock<SharedMutex> & lock)
{
    if (name == "profile")
    {
        setCurrentProfileWithLock(value.safeGet<String>(), lock);
        return;
    }
    settings.set(std::string_view{name}, value);

    if (ContextAccessParams::dependsOnSettingName(name.toView()))
        calculateAccessRightsWithLock(lock);
}

Settings Context::getSettings() const
{
    auto lock = getLocalSharedLock();
    return settings;
}

void Context::setSettings(const Settings & settings_)
{
    auto lock = getLocalLock();
    auto old_readonly = settings.readonly;
    auto old_allow_ddl = settings.allow_ddl;
    auto old_allow_introspection_functions = settings.allow_introspection_functions;

    settings = settings_;

    if ((settings.readonly != old_readonly) || (settings.allow_ddl != old_allow_ddl)
        || (settings.allow_introspection_functions != old_allow_introspection_functions))
        calculateAccessRightsWithLock(lock);
}

void Context::setSetting(const StringRef & name, const String & value)
{
    auto lock = getLocalLock();
    setSettingWithLock(name, value, lock);
}

void Context::setSetting(const StringRef & name, const Field & value)
{
    auto lock = getLocalLock();
    setSettingWithLock(name, value, lock);
}

void Context::applySettingChangeWithLock(const SettingChange & change, const std::unique_lock<SharedMutex> & lock)
{
    try
    {
        setSettingWithLock(change.name, change.value, lock);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
            "in attempt to set the value of setting '{}' to {}", change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}

void Context::applySettingsChangesWithLock(const SettingsChanges & changes, bool internal, const std::unique_lock<SharedMutex> & lock)
{
    // set ansi related settings first, as they may be overwritten explicitly later
    std::optional<String> dialect_type_opt;
    std::function<void(const SettingsChanges &)> find_dialect_type_if_any = [&](const SettingsChanges & setting_changes) {
        for (const auto & change : setting_changes)
        {
            if (change.name == "profile" && getServerType() == ServerType::cnch_server)
            {
                UUID profile_id = getAccessControlManager().getID<SettingsProfile>(change.value.safeGet<String>());
                auto profile_info = getAccessControlManager().getSettingsProfileInfo(profile_id);

                find_dialect_type_if_any(profile_info->settings);
            }

            if (change.name == "dialect_type")
            {
                auto value_str = change.value.safeGet<String>();

                if (!dialect_type_opt)
                    dialect_type_opt = value_str;
                else if (*dialect_type_opt != value_str)
                    throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Multiple dialect_type value found");
            }
        }
    };
    find_dialect_type_if_any(changes);

    // NOTE: tenanted users connect to server using tenant id given in connection info.
    // allow only whitelisted settings for tenanted users
    if (is_tenant_user() && !internal && !isInternalQuery () && getIsRestrictSettingsToWhitelist() && (getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY || session_context.lock().get() == this) && !getCurrentTenantId().empty())
    {
        for (const auto & change : changes)
        {
            if (!SettingsChanges::WHITELIST_SETTINGS.contains(change.name) && !isExtraRestrictSettingsToWhitelist(change.name))
                throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown or disabled setting " + change.name +
                    "for tenant user. Contact the admin about whether it is needed to add it to tenant_whitelist_settings"
                    " in configuration");
        }
    }

    // skip if a previous setting change is in process
    // skip if current and target are same
    bool apply_ansi_related_settings = dialect_type_opt && !settings.dialect_type.pending
        && settings.dialect_type.value != SettingFieldDialectTypeTraits::fromString(*dialect_type_opt);

    if (apply_ansi_related_settings)
    {
        setSettingWithLock("dialect_type", *dialect_type_opt, lock);
        ANSI::onSettingChanged(&settings);
        settings.dialect_type.pending = true;
    }

    for (const SettingChange & change : changes)
    {
        if (change.name == "profile" && getServerType() != ServerType::cnch_server)
            continue;
        applySettingChangeWithLock(change, lock);
    }
    applySettingsQuirks(settings);

    if (apply_ansi_related_settings)
        settings.dialect_type.pending = false;
}

void Context::applySettingChange(const SettingChange & change)
{
    try
    {
        setSetting(change.name, change.value);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
            "in attempt to set the value of setting '{}' to {}", change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}


void Context::applySettingsChanges(const SettingsChanges & changes, bool internal)
{
    auto lock = getLocalLock();
    applySettingsChangesWithLock(changes, internal, lock);
}

void Context::checkSettingsConstraintsWithLock(const SettingChange & change) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(settings, change);
}

void Context::checkSettingsConstraintsWithLock(const SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(settings, changes);
}

void Context::checkSettingsConstraintsWithLock(SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(settings, changes);
}

void Context::clampToSettingsConstraintsWithLock(SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.clamp(settings, changes);
}

void Context::checkSettingsConstraints(const SettingChange & change) const
{
    auto lock = getLocalSharedLock();
    checkSettingsConstraintsWithLock(change);
}

void Context::checkSettingsConstraints(const SettingsChanges & changes) const
{
    auto lock = getLocalSharedLock();
    checkSettingsConstraintsWithLock(changes);
}

void Context::checkSettingsConstraints(SettingsChanges & changes) const
{
    auto lock = getLocalSharedLock();
    checkSettingsConstraintsWithLock(changes);
}

void Context::clampToSettingsConstraints(SettingsChanges & changes) const
{
    auto lock = getLocalSharedLock();
    clampToSettingsConstraintsWithLock(changes);
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfilesWithLock() const
{
    if (settings_constraints_and_current_profiles)
        return settings_constraints_and_current_profiles;
    static auto no_constraints_or_profiles = std::make_shared<SettingsConstraintsAndProfileIDs>(getAccessControlManager());
    return no_constraints_or_profiles;
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfiles() const
{
    auto lock = getLocalSharedLock();
    return getSettingsConstraintsAndCurrentProfilesWithLock();
}

String Context::getCurrentDatabase() const
{
    String tenant_db;
    {
        auto lock = getLocalLock();
        tenant_db = current_database;
    }

    return formatTenantDatabaseName(tenant_db);
}


String Context::getInitialQueryId() const
{
    return client_info.initial_query_id;
}


void Context::setCoordinatorAddress(const Protos::AddressInfo & address)
{
    coordinator_address = address;
}

void Context::setCoordinatorAddress(const AddressInfo & address)
{
    coordinator_address = address;
}

AddressInfo Context::getCoordinatorAddress() const
{
    return coordinator_address;
}

void Context::setPlanSegmentInstanceId(const PlanSegmentInstanceId & instance_id)
{
    plan_segment_instance_id = instance_id;
}

PlanSegmentInstanceId Context::getPlanSegmentInstanceId() const
{
    return plan_segment_instance_id;
};

void Context::initPlanSegmentExHandler()
{
    plan_segment_ex_handler = std::make_shared<ExceptionHandler>();
}

ExceptionHandlerPtr Context::getPlanSegmentExHandler() const
{
    return plan_segment_ex_handler;
}

void Context::setCurrentDatabaseNameInGlobalContext(const String & name)
{
    if (!isGlobalContext())
        throw Exception(
            "Cannot set current database for non global context, this method should be used during server initialization",
            ErrorCodes::LOGICAL_ERROR);
    auto lock = getLocalLock();

    if (!current_database.empty())
        throw Exception("Default database name cannot be changed in global context without server restart", ErrorCodes::LOGICAL_ERROR);

    current_database = name;
}

void Context::setCurrentDatabase(const String & name)
{
    DatabaseCatalog::instance().assertDatabaseExists(name, hasQueryContext() ? getQueryContext(): shared_from_this());
    auto lock = getLocalLock();
    current_database = name;
    calculateAccessRightsWithLock(lock);
}

void Context::setCurrentDatabase(const String & name, ContextPtr local_context)
{
    bool use_cnch_catalog = false;
    auto [catalog_opt, database_opt] = getCatalogNameAndDatabaseName(name);
    DatabaseCatalog::instance().assertDatabaseExists(name, local_context);

    if (catalog_opt.has_value())
    {
        auto catalog_name = catalog_opt.value();
        if (catalog_name.empty() || getOriginalDatabaseName(catalog_name) == "cnch")
        {
            use_cnch_catalog = true;
        }
        else if (!(ExternalCatalog::Mgr::instance().isCatalogExist(catalog_name)))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "catalog {} does not exist", catalog_name);
        }
    } else
    {
        use_cnch_catalog = true;
    }

    auto db_name_with_tenant_id = appendTenantIdOnly(database_opt.value());
    auto lock = getLocalLock();
    if(use_cnch_catalog){
        current_catalog = "";
        current_database = db_name_with_tenant_id;
        LOG_TRACE(shared->log, "use cnch catalog, db_name: {}", db_name_with_tenant_id);
    } else {
        current_catalog = catalog_opt.value();
        current_database =  database_opt.value();
        LOG_TRACE(shared->log, "use external catalog, catalog_name: {}, db_name: {}", current_catalog, current_database);
    }
    calculateAccessRightsWithLock(lock);
}

void Context::setCurrentCatalog(const String & catalog_name)
{
    if (catalog_name == "" || catalog_name == "cnch")
    {
        auto lock = getLocalLock();
        current_catalog = "";
        current_database = "";
        return;
    }
    bool exists = ExternalCatalog::Mgr::instance().isCatalogExist(catalog_name);
    if (!exists)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "catalog {} does not exist", catalog_name);
    }
    auto lock = getLocalLock();
    current_catalog = catalog_name;
    current_database = "default";
}


void Context::setCurrentQueryId(const String & query_id)
{
    /// Generate random UUID, but using lower quality RNG,
    ///  because Poco::UUIDGenerator::generateRandom method is using /dev/random, that is very expensive.
    /// NOTE: Actually we don't need to use UUIDs for query identifiers.
    /// We could use any suitable string instead.
    union
    {
        char bytes[16];
        struct
        {
            UInt64 a;
            UInt64 b;
        } words;
        UUID uuid{};
    } random;

    random.words.a = thread_local_rng(); //-V656
    random.words.b = thread_local_rng(); //-V656

    if (client_info.client_trace_context.trace_id != UUID())
    {
        // Use the OpenTelemetry trace context we received from the client, and
        // create a new span for the query.
        query_trace_context = client_info.client_trace_context;
        query_trace_context.span_id = thread_local_rng();
    }
    else if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        // If this is an initial query without any parent OpenTelemetry trace, we
        // might start the trace ourselves, with some configurable probability.
        std::bernoulli_distribution should_start_trace{settings.opentelemetry_start_trace_probability};

        if (should_start_trace(thread_local_rng))
        {
            // Use the randomly generated default query id as the new trace id.
            query_trace_context.trace_id = random.uuid;
            query_trace_context.span_id = thread_local_rng();
            // Mark this trace as sampled in the flags.
            query_trace_context.trace_flags = 1;
        }
    }

    String query_id_to_set = query_id;
    if (query_id_to_set.empty()) /// If the user did not submit his query_id, then we generate it ourselves.
    {
        /// Use protected constructor.
        struct QueryUUID : Poco::UUID
        {
            QueryUUID(const char * bytes, Poco::UUID::Version version) : Poco::UUID(bytes, version)
            {
            }
        };

        query_id_to_set = QueryUUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();
    }

    client_info.current_query_id = query_id_to_set;
}

void Context::killCurrentQuery()
{
    if (process_list_elem)
    {
        process_list_elem->cancelQuery(true, false);
    }
    getSegmentScheduler()->cancelPlanSegmentsFromCoordinator(
        client_info.initial_query_id, ErrorCodes::QUERY_WAS_CANCELLED, "Cancelled by Client.", shared_from_this());
    getPlanSegmentProcessList().tryCancelPlanSegmentGroup(client_info.initial_query_id);
};

String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

ContextMutablePtr Context::getQueryContext() const
{
    auto ptr = query_context.lock();
    if (!ptr)
        throw Exception("There is no query or query context has expired", ErrorCodes::THERE_IS_NO_QUERY);
    return ptr;
}

bool Context::isInternalSubquery() const
{
    auto ptr = query_context.lock();
    return ptr && ptr.get() != this;
}

ContextMutablePtr Context::getSessionContext() const
{
    auto ptr = session_context.lock();
    if (!ptr)
        throw Exception("There is no session or session context has expired", ErrorCodes::THERE_IS_NO_SESSION);
    return ptr;
}

ContextMutablePtr Context::getGlobalContext() const
{
    auto ptr = global_context.lock();
    if (!ptr)
        throw Exception("There is no global context or global context has expired", ErrorCodes::LOGICAL_ERROR);
    return ptr;
}

ContextMutablePtr Context::getBufferContext() const
{
    if (!buffer_context)
        throw Exception("There is no buffer context", ErrorCodes::LOGICAL_ERROR);
    return buffer_context;
}


const EmbeddedDictionaries & Context::getEmbeddedDictionaries() const
{
    return getEmbeddedDictionariesImpl(false);
}

EmbeddedDictionaries & Context::getEmbeddedDictionaries()
{
    return getEmbeddedDictionariesImpl(false);
}


const ExternalDictionariesLoader & Context::getExternalDictionariesLoader() const
{
    return const_cast<Context *>(this)->getExternalDictionariesLoader();
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoader()
{
    std::lock_guard lock(shared->external_dictionaries_mutex);
    if (!shared->external_dictionaries_loader)
        shared->external_dictionaries_loader.emplace(getGlobalContext());
    return *shared->external_dictionaries_loader;
}

CnchCatalogDictionaryCache & Context::getCnchCatalogDictionaryCache() const
{
    return const_cast<Context *>(this)->getCnchCatalogDictionaryCache();
}

CnchCatalogDictionaryCache & Context::getCnchCatalogDictionaryCache()
{
    std::lock_guard lock(shared->cnch_catalog_dict_cache_mutex);
    if (!shared->cnch_catalog_dict_cache)
        shared->cnch_catalog_dict_cache.emplace(getGlobalContext());
    return *shared->cnch_catalog_dict_cache;
}

const ExternalModelsLoader & Context::getExternalModelsLoader() const
{
    return const_cast<Context *>(this)->getExternalModelsLoader();
}

ExternalModelsLoader & Context::getExternalModelsLoader()
{
    std::lock_guard lock(shared->external_models_mutex);
    return getExternalModelsLoaderUnlocked();
}

ExternalModelsLoader & Context::getExternalModelsLoaderUnlocked()
{
    if (!shared->external_models_loader)
        shared->external_models_loader.emplace(getGlobalContext());
    return *shared->external_models_loader;
}

void Context::setExternalModelsConfig(const ConfigurationPtr & config, const std::string & config_name)
{
    std::lock_guard lock(shared->external_models_mutex);

    if (shared->external_models_config && isSameConfigurationWithMultipleKeys(*config, *shared->external_models_config, "", config_name))
        return;

    shared->external_models_config = config;
    shared->models_repository_guard.reset();
    shared->models_repository_guard
        = getExternalModelsLoaderUnlocked().addConfigRepository(std::make_unique<ExternalLoaderXMLConfigRepository>(*config, config_name));
}


EmbeddedDictionaries & Context::getEmbeddedDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard lock(shared->embedded_dictionaries_mutex);

    if (!shared->embedded_dictionaries)
    {
        auto geo_dictionaries_loader = std::make_unique<GeoDictionariesLoader>();

        shared->embedded_dictionaries.emplace(std::move(geo_dictionaries_loader), getGlobalContext(), throw_on_error);
    }

    return *shared->embedded_dictionaries;
}


void Context::tryCreateEmbeddedDictionaries() const
{
    static_cast<void>(getEmbeddedDictionariesImpl(true));
}

void Context::loadDictionaries(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.getBool("dictionaries_lazy_load", true))
    {
        tryCreateEmbeddedDictionaries();
        getExternalDictionariesLoader().enableAlwaysLoadEverything(true);
    }
    shared->dictionaries_xmls = getExternalDictionariesLoader().addConfigRepository(
        std::make_unique<ExternalLoaderXMLConfigRepository>(config, "dictionaries_config"));

    if ((getServerType() == ServerType::cnch_worker) || (getServerType() == ServerType::cnch_server))
        shared->dictionaries_cnch_catalog = getExternalDictionariesLoader().addConfigRepository(
            std::make_unique<ExternalLoaderCnchCatalogRepository>(shared_from_this()));
}

#if USE_NLP

    SynonymsExtensions & Context::getSynonymsExtensions() const
    {
        auto lock = getLock(); // checked

        if (!shared->synonyms_extensions)
            shared->synonyms_extensions.emplace(getConfigRef());

        return *shared->synonyms_extensions;
    }

    Lemmatizers & Context::getLemmatizers() const
    {
        auto lock = getLock(); // checked

        if (!shared->lemmatizers)
            shared->lemmatizers.emplace(getConfigRef());

        return *shared->lemmatizers;
    }
#endif

void Context::setProgressCallback(ProgressCallback callback)
{
    /// Callback is set to a session or to a query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
    return progress_callback;
}

void Context::setSendTCPProgress(std::function<void()> callback)
{
    send_tcp_progress = callback;
}

std::function<void()> Context::getSendTCPProgress() const
{
    return send_tcp_progress;
}

void Context::setProcessListEntry(std::shared_ptr<ProcessListEntry> process_list_entry_)
{
    process_list_entry = process_list_entry_;
    if (process_list_entry_)
        process_list_elem = &process_list_entry_->get();
    else
        process_list_elem = nullptr;
}

std::weak_ptr<ProcessListEntry> Context::getProcessListEntry() const
{
    return process_list_entry;
}

void Context::setPlanSegmentProcessListEntry(std::shared_ptr<PlanSegmentProcessListEntry> segment_process_list_entry_)
{
    segment_process_list_entry = segment_process_list_entry_;
}

std::weak_ptr<PlanSegmentProcessListEntry> Context::getPlanSegmentProcessListEntry() const
{
    return segment_process_list_entry;
}

void Context::setProcessorProfileElementConsumer(
    std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> processor_log_element_consumer_)
{
    auto lock = getLock(); // checked
    shared->processor_log_element_consumer = processor_log_element_consumer_;
}

std::shared_ptr<ProfileElementConsumer<ProcessorProfileLogElement>> Context::getProcessorProfileElementConsumer() const
{
    auto lock = getLock(); // checked

    if (!shared->processor_log_element_consumer)
        return {};
    return shared->processor_log_element_consumer;
}

void Context::setIsExplainQuery(const bool & is_explain_query_)
{
    is_explain_query = is_explain_query_;
}

bool Context::isExplainQuery() const
{
    return is_explain_query;
}

void Context::setProcessListElement(QueryStatus * elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
}

QueryStatus * Context::getProcessListElement() const
{
    return process_list_elem;
}

void Context::setNvmCache(const Poco::Util::AbstractConfiguration &config)
{
    auto lock = getLock(); // checked

    if (shared->nvm_cache)
        throw Exception("Nvmcache cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    NvmCacheConfig conf;
    conf.loadFromConfig("nvm_cache", config);

    if (!conf.isEnable())
        return;

    std::vector<std::string> paths;
    auto disks = getStoragePolicy(conf.getPolicyName())->getVolumeByName(conf.getVolumeName(), true)->getDisks();
    for  (auto & disk : disks)
    {
        chassert(disk->getType() == DiskType::Type::Local);
        paths.push_back(std::filesystem::path(disk->getPath()) / NvmCacheConfig::FILE_NAME);
    }

    if (paths.size() > 1)
        conf.setRaidFiles(paths, conf.getFileSize(), true);
    else
        conf.setSimpleFile(paths[0], conf.getFileSize(), true);
    conf.setEnginesSelector([](HybridCache::EngineTag tag){ return static_cast<size_t>(tag); });

    auto cache = createNvmCache(std::move(conf), nullptr, nullptr, false, false);
    std::shared_ptr<HybridCache::AbstractCache> cache_ptr = std::move(cache);

    shared->nvm_cache = std::static_pointer_cast<NvmCache>(cache_ptr);
    shared->uncompressed_cache->setNvmCache(shared->nvm_cache);
}

NvmCachePtr Context::getNvmCache() const
{
    auto lock = getLock(); // checked
    return shared->nvm_cache;
}

void Context::dropNvmCache() const
{
    auto lock = getLock(); // checked
    if (shared->nvm_cache)
        shared->nvm_cache->reset();
}

void Context::setFooterCache(size_t max_size_in_bytes)
{
    auto lock = getLock(); // checked
    if (max_size_in_bytes)
        ArrowFooterCache::initialize(max_size_in_bytes);
}

void Context::setUncompressedCache(size_t max_size_in_bytes, bool shard_mode)
{
    auto lock = getLock(); // checked

    if (shared->uncompressed_cache)
        throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);
    shared->uncompressed_cache = std::make_shared<UncompressedCache>(max_size_in_bytes, shard_mode);
}


UncompressedCachePtr Context::getUncompressedCache() const
{
    auto lock = getLock(); // checked
    return shared->uncompressed_cache;
}


void Context::dropUncompressedCache() const
{
    auto lock = getLock(); // checked
    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();
}


void Context::setMarkCache(size_t cache_size_in_bytes)
{
    auto lock = getLock(); // checked

    if (shared->mark_cache)
        throw Exception("Mark cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes);
}

MarkCachePtr Context::getMarkCache() const
{
    auto lock = getLock(); // checked
    return shared->mark_cache;
}

void Context::dropMarkCache() const
{
    auto lock = getLock(); // checked
    if (shared->mark_cache)
        shared->mark_cache->reset();
}

std::shared_ptr<CloudTableDefinitionCache> Context::tryGetCloudTableDefinitionCache() const
{
    if (hasSessionTimeZone())
        return nullptr;
    callOnce(shared->cloud_table_definition_cache_initialized, [&] {
        const Poco::Util::AbstractConfiguration & config = getConfigRef();
        auto cache_size = config.getUInt(".cloud_table_definition_cache_size", 50000);
        if (getServerType() == ServerType::cnch_worker && cache_size)
            shared->cloud_table_definition_cache = std::make_shared<CloudTableDefinitionCache>(cache_size);
    });
    return shared->cloud_table_definition_cache;
}

void Context::setQueryCache(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock(); // checked

    if (shared->query_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query cache has been already created.");

    shared->query_cache = std::make_shared<QueryCache>();
    shared->query_cache->updateConfiguration(config);
}

void Context::updateQueryCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock(); // checked
    if (shared->query_cache)
        shared->query_cache->updateConfiguration(config);
}

QueryCachePtr Context::getQueryCache() const
{
    auto lock = getLock(); // checked
    return shared->query_cache;
}

void Context::dropQueryCache() const
{
    auto lock = getLock(); // checked
    if (shared->query_cache)
        shared->query_cache->reset();
}

void Context::setIntermediateResultCache(size_t cache_size_in_bytes)
{
    auto lock = getLock(); // checked

    if (shared->intermediate_result_cache)
        throw Exception("Intermediate result cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->intermediate_result_cache = std::make_shared<IntermediateResult::CacheManager>(cache_size_in_bytes);
}

IntermediateResultCachePtr Context::getIntermediateResultCache() const
{
    auto lock = getLock(); // checked
    return shared->intermediate_result_cache;
}

void Context::dropIntermediateResultCache() const
{
    auto lock = getLock(); // checked
    if (shared->intermediate_result_cache)
        shared->intermediate_result_cache->reset();
}

void Context::setMMappedFileCache(size_t cache_size_in_num_entries)
{
    auto lock = getLock(); // checked

    if (shared->mmap_cache)
        throw Exception("Mapped file cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mmap_cache = std::make_shared<MMappedFileCache>(cache_size_in_num_entries);
}

MMappedFileCachePtr Context::getMMappedFileCache() const
{
    auto lock = getLock(); // checked
    return shared->mmap_cache;
}

void Context::dropMMappedFileCache() const
{
    auto lock = getLock(); // checked
    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}


void Context::dropCaches() const
{
    auto lock = getLock(); // checked

    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();

    if (shared->mark_cache)
        shared->mark_cache->reset();

    if (shared->mmap_cache)
        shared->mmap_cache->reset();

    if (shared->nvm_cache)
        shared->nvm_cache->reset();
}


void Context::setMergeSchedulerSettings(const Poco::Util::AbstractConfiguration & config)
{
    settings.enable_merge_scheduler = config.getBool("enable_merge_scheduler", false);
    settings.slow_query_ms = config.getUInt64("slow_query_ms", 0);
    settings.max_rows_to_schedule_merge = config.getUInt64("max_rows_to_schedule_merge", 500000000);
    settings.strict_rows_to_schedule_merge = config.getUInt64("strict_rows_to_schedule_merge", 50000000);
    settings.total_rows_to_schedule_merge = config.getUInt64("total_rows_to_schedule_merge", 0);
}

BackgroundSchedulePool & Context::getBufferFlushSchedulePool() const
{
    auto lock = getLock(); // checked
    if (!shared->buffer_flush_schedule_pool)
        shared->buffer_flush_schedule_pool.emplace(
            settings.background_buffer_flush_schedule_pool_size, CurrentMetrics::BackgroundBufferFlushSchedulePoolTask, "BgBufSchPool");
    return *shared->buffer_flush_schedule_pool;
}

BackgroundTaskSchedulingSettings Context::getBackgroundProcessingTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do
        = config.getDouble("background_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min
        = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max
        = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier
        = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part
        = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);
    return task_settings;
}

BackgroundTaskSchedulingSettings Context::getBackgroundMoveTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_move_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part
        = config.getDouble("background_move_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do
        = config.getDouble("background_move_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min
        = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max
        = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier
        = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part
        = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);

    return task_settings;
}

BackgroundSchedulePool & Context::getSchedulePool() const
{
    callOnce(shared->schedule_pool_initialized, [&]{
        shared->schedule_pool.emplace(
            settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundSchedulePoolTask,
            "BgSchPool");
    });
    return *shared->schedule_pool;
}

BackgroundSchedulePool & Context::getDistributedSchedulePool() const
{
    auto lock = getLock(); // checked
    if (!shared->distributed_schedule_pool)
        shared->distributed_schedule_pool.emplace(
            settings.background_distributed_schedule_pool_size, CurrentMetrics::BackgroundDistributedSchedulePoolTask, "BgDistSchPool");
    return *shared->distributed_schedule_pool;
}

BackgroundSchedulePool & Context::getMessageBrokerSchedulePool() const
{
    auto lock = getLock(); // checked
    if (!shared->message_broker_schedule_pool)
        shared->message_broker_schedule_pool.emplace(
            settings.background_message_broker_schedule_pool_size, CurrentMetrics::BackgroundMessageBrokerSchedulePoolTask, "BgMBSchPool");
    return *shared->message_broker_schedule_pool;
}

BackgroundSchedulePool & Context::getConsumeSchedulePool() const
{
    auto & item = shared->extra_schedule_pools[SchedulePool::Consume];
    callOnce(item.is_initialized, [&] {
        CpuSetPtr cpu_set;
        if (auto & cgroup_manager = CGroupManagerFactory::instance(); cgroup_manager.isInit())
        {
            cpu_set = cgroup_manager.getCpuSet("hakafka");
        }

        item.pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_consume_schedule_pool_size,
            CurrentMetrics::BackgroundConsumeSchedulePoolTask,
            "BgConsumePool",
            std::move(cpu_set));

    });
    return *item.pool;
}

BackgroundSchedulePool & Context::getLocalSchedulePool() const
{
    auto & item = shared->extra_schedule_pools[SchedulePool::Local];
    callOnce(item.is_initialized, [&] {
        item.pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_local_schedule_pool_size,
            CurrentMetrics::BackgroundLocalSchedulePoolTask,
            "BgLocalPool"
        );
    });
    return *item.pool;
}

BackgroundSchedulePool & Context::getMergeSelectSchedulePool() const
{
    auto & item = shared->extra_schedule_pools[SchedulePool::MergeSelect];
    callOnce(item.is_initialized, [&] {
        item.pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundMergeSelectSchedulePoolTask,
            "BgMSelectPool");
    });
    return *item.pool;
}

BackgroundSchedulePool & Context::getUniqueTableSchedulePool() const
{
    auto & item = shared->extra_schedule_pools[SchedulePool::UniqueTable];
    callOnce(item.is_initialized, [&] {
        item.pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_unique_table_schedule_pool_size,
            CurrentMetrics::BackgroundUniqueTableSchedulePoolTask,
            "BgUniqPool");
    });
    return *item.pool;
}

BackgroundSchedulePool & Context::getTopologySchedulePool() const
{
    auto & item = shared->extra_schedule_pools[SchedulePool::CNCHTopology];
    callOnce(item.is_initialized, [&] {
        item.pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_topology_thread_pool_size,
            CurrentMetrics::BackgroundCNCHTopologySchedulePoolTask,
            "CNCHTopoPool");
    });
    return *item.pool;
}

BackgroundSchedulePool & Context::getMetricsRecalculationSchedulePool() const
{
    auto & item = shared->extra_schedule_pools[SchedulePool::PartsMetrics];
    callOnce(item.is_initialized, [&] {
        item.pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_metrics_recalculation_schedule_pool_size,
            CurrentMetrics::BackgroundPartsMetricsSchedulePoolTask,
            "PtMetricsPool");
    });
    return *item.pool;
}

BackgroundSchedulePool & Context::getExtraSchedulePool(
    SchedulePool::Type pool_type, SettingFieldUInt64 pool_size, CurrentMetrics::Metric metric, const char * name) const
{
    auto & item = shared->extra_schedule_pools[pool_type];
    callOnce(item.is_initialized, [&] {
        item.pool = std::make_unique<BackgroundSchedulePool>( pool_size, metric, name);
    });
    return *item.pool;
}

ThrottlerPtr Context::getDiskCacheThrottler() const
{
    callOnce(shared->disk_cache_throttler_initialized, [&] {
        shared->disk_cache_throttler = std::make_shared<Throttler>(settings.max_bandwidth_for_disk_cache);
    });
    return shared->disk_cache_throttler;
}

ThrottlerPtr Context::getReplicatedSendsThrottler() const
{
    callOnce(shared->replicated_sends_throttler_initialized, [&] {
        shared->replicated_sends_throttler = std::make_shared<Throttler>(
            settings.max_replicated_sends_network_bandwidth_for_server);
    });
    return shared->replicated_sends_throttler;
}

ThrottlerPtr Context::getReplicatedFetchesThrottler() const
{
    callOnce(shared->replicated_fetches_throttler_initialized, [&] {
        shared->replicated_fetches_throttler = std::make_shared<Throttler>(
            settings.max_replicated_fetches_network_bandwidth_for_server);
    });
    return shared->replicated_fetches_throttler;
}

bool Context::hasDistributedDDL() const
{
    return getConfigRef().has("distributed_ddl");
}

void Context::setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker)
{
    auto lock = getLock(); // checked
    if (shared->ddl_worker)
        throw Exception("DDL background thread has already been initialized", ErrorCodes::LOGICAL_ERROR);
    ddl_worker->startup();
    shared->ddl_worker = std::move(ddl_worker);
}

DDLWorker & Context::getDDLWorker() const
{
    auto lock = getLock(); // checked
    if (!shared->ddl_worker)
    {
        if (!hasZooKeeper())
            throw Exception("There is no Zookeeper configuration in server config", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        if (!hasDistributedDDL())
            throw Exception("There is no DistributedDDL configuration in server config", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        throw Exception("DDL background thread is not initialized", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }
    return *shared->ddl_worker;
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);

    if (hasZooKeeper())
    {
        const auto & config = shared->zookeeper_config ? *shared->zookeeper_config : getConfigRef();
        ServiceEndpoints endpoints;
        if (getConfigRef().has("service_discovery.keeper"))
            endpoints = getServiceDiscoveryClient()->lookupEndpoints(getConfigRef().getString("service_discovery.keeper.psm"));
        else if (getConfigRef().has("service_discovery.tso"))
            endpoints = getServiceDiscoveryClient()->lookupEndpoints(getConfigRef().getString("service_discovery.tso.psm"));

        if (!shared->zookeeper)
            shared->zookeeper = std::make_shared<zkutil::ZooKeeper>(config, "zookeeper", getZooKeeperLog(), endpoints);
        else if (shared->zookeeper->expired())
            shared->zookeeper = shared->zookeeper->startNewSession(endpoints);
    }

    return shared->zookeeper;
}

UInt32 Context::getZooKeeperSessionUptime() const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    if (!shared->zookeeper || shared->zookeeper->expired())
        return 0;
    return shared->zookeeper->getSessionUptime();
}

namespace
{

    bool checkZooKeeperConfigIsLocal(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_name, keys);

        for (const auto & key : keys)
        {
            if (startsWith(key, "node"))
            {
                String host = config.getString(config_name + "." + key + ".host");
                if (isLocalAddress(DNSResolver::instance().resolveHost(host)))
                    return true;
            }
        }
        return false;
    }

}


bool Context::tryCheckClientConnectionToMyKeeperCluster() const
{
    try
    {
        /// If our server is part of main Keeper cluster
        if (checkZooKeeperConfigIsLocal(getConfigRef(), "zookeeper"))
        {
            LOG_DEBUG(shared->log, "Keeper server is participant of the main zookeeper cluster, will try to connect to it");
            getZooKeeper();
            /// Connected, return true
            return true;
        }
        else
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            getConfigRef().keys("auxiliary_zookeepers", keys);

            /// If our server is part of some auxiliary_zookeeper
            for (const auto & aux_zk_name : keys)
            {
                if (checkZooKeeperConfigIsLocal(getConfigRef(), "auxiliary_zookeepers." + aux_zk_name))
                {
                    LOG_DEBUG(
                        shared->log,
                        "Our Keeper server is participant of the auxiliary zookeeper cluster ({}), will try to connect to it",
                        aux_zk_name);
                    getAuxiliaryZooKeeper(aux_zk_name);
                    /// Connected, return true
                    return true;
                }
            }
        }

        /// Our server doesn't depend on our Keeper cluster
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void Context::initializeKeeperDispatcher(bool start_async) const
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);

    if (shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize Keeper multiple times");

    const auto & config = getConfigRef();
    if (config.has("keeper_server"))
    {
        bool is_standalone_app = getApplicationType() == ApplicationType::KEEPER;
        if (start_async)
        {
            assert(!is_standalone_app);
            LOG_INFO(
                shared->log,
                "Connected to ZooKeeper (or Keeper) before internal Keeper start or we don't depend on our Keeper cluster"
                ", will wait for Keeper asynchronously");
        }
        else
        {
            LOG_INFO(
                shared->log,
                "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start,"
                "will wait for Keeper synchronously");
        }

        shared->keeper_dispatcher = std::make_shared<KeeperDispatcher>();
        shared->keeper_dispatcher->initialize(config, is_standalone_app, start_async);
    }
#endif
}

#if USE_NURAFT
std::shared_ptr<KeeperDispatcher> & Context::getKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return shared->keeper_dispatcher;
}
#endif

void Context::shutdownKeeperDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (shared->keeper_dispatcher)
    {
        shared->keeper_dispatcher->shutdown();
        shared->keeper_dispatcher.reset();
    }
#endif
}


void Context::updateKeeperConfiguration(const Poco::Util::AbstractConfiguration & config)
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        return;

    shared->keeper_dispatcher->updateConfiguration(config);
#endif
}


zkutil::ZooKeeperPtr Context::getAuxiliaryZooKeeper(const String & name) const
{
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);

    auto zookeeper = shared->auxiliary_zookeepers.find(name);
    if (zookeeper == shared->auxiliary_zookeepers.end())
    {
        const auto & config = shared->auxiliary_zookeepers_config ? *shared->auxiliary_zookeepers_config : getConfigRef();
        if (!config.has("auxiliary_zookeepers." + name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown auxiliary ZooKeeper name '{}'. If it's required it can be added to the section <auxiliary_zookeepers> in "
                "config.xml",
                name);

        zookeeper
            = shared->auxiliary_zookeepers
                  .emplace(
                      name,
                      std::make_shared<zkutil::ZooKeeper>(config, "auxiliary_zookeepers." + name, getZooKeeperLog(), ServiceEndpoints{}))
                  .first;
    }
    else if (zookeeper->second->expired())
        zookeeper->second = zookeeper->second->startNewSession();

    return zookeeper->second;
}

void Context::resetZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper.reset();
}

static void reloadZooKeeperIfChangedImpl(
    const ConfigurationPtr & config,
    const std::string & config_name,
    zkutil::ZooKeeperPtr & zk,
    std::shared_ptr<ZooKeeperLog> zk_log,
    const ServiceEndpoints & endpoints)
{
    if (!zk || zk->configChanged(*config, config_name, endpoints))
    {
        if (zk)
            zk->finalize();

        zk = std::make_shared<zkutil::ZooKeeper>(*config, config_name, std::move(zk_log), endpoints);
    }
}

void Context::reloadZooKeeperIfChanged(const ConfigurationPtr & config) const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper_config = config;

    ServiceEndpoints endpoints;
    if (getConfigRef().has("service_discovery.keeper"))
        endpoints = getServiceDiscoveryClient()->lookupEndpoints("service_discovery.keeper.psm");
    reloadZooKeeperIfChangedImpl(config, "zookeeper", shared->zookeeper, getZooKeeperLog(), endpoints);
}

void Context::reloadAuxiliaryZooKeepersConfigIfChanged(const ConfigurationPtr & config)
{
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);

    shared->auxiliary_zookeepers_config = config;

    for (auto it = shared->auxiliary_zookeepers.begin(); it != shared->auxiliary_zookeepers.end();)
    {
        if (!config->has("auxiliary_zookeepers." + it->first))
            it = shared->auxiliary_zookeepers.erase(it);
        else
        {
            reloadZooKeeperIfChangedImpl(config, "auxiliary_zookeepers." + it->first, it->second, getZooKeeperLog(), {});
            ++it;
        }
    }
}

bool Context::hasZooKeeper() const
{
    /**
     * Now, we support some methods for configuring zookeeper.
     * The first method is to add all nodes and settings into <zookeeper> label.
     * <zookeeper>
     *     <nodes>
     *       ...
     *     </nodes>
     *     ... <!-- some settings -->
     * </zookeeper>
     * The second method is to add settings to the <zookeeper> and obtain nodes from service discovery
     * If all settings of zookeeper use the default value,
     *   1. if you obtain nodes from `service_discovery.keeper`, the <zookeeper> label could be omitted.
     *   2. otherwise, please keep empty <zookeeper> label in configuration file to avoid ambiguity.
     */
    return getConfigRef().has("zookeeper") || getConfigRef().has("service_discovery.keeper");
}

bool Context::hasAuxiliaryZooKeeper(const String & name) const
{
    return getConfigRef().has("auxiliary_zookeepers." + name);
}

void Context::setEnableSSL(bool v)
{
    shared->enable_ssl = v;
}

bool Context::isEnableSSL() const
{
    return shared->enable_ssl;
}

InterserverCredentialsPtr Context::getInterserverCredentials()
{
    return shared->interserver_io_credentials.get();
}

std::pair<String, String> Context::getCnchInterserverCredentials() const
{
    String user_name = getSettingsRef().username_for_internal_communication.toString();
    auto lock = getLock(); // checked
    auto password = shared->users_config->getString("users." + user_name + ".password", "");

    return {user_name, password};
}

void Context::updateInterserverCredentials(const Poco::Util::AbstractConfiguration & config)
{
    auto credentials = InterserverCredentials::make(config, "interserver_http_credentials");
    shared->interserver_io_credentials.set(std::move(credentials));
}

void Context::setInterserverIOAddress(const String & host, UInt16 port)
{
    shared->interserver_io_host = host;
    shared->interserver_io_port = port;
}

std::pair<String, UInt16> Context::getInterserverIOAddress() const
{
    if (shared->interserver_io_host.empty() || shared->interserver_io_port == 0)
        throw Exception(
            "Parameter 'interserver_http(s)_port' required for replication is not specified in configuration file.",
            ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return {shared->interserver_io_host, shared->interserver_io_port};
}

UInt16 Context::getExchangePort(bool) const
{
    return getRPCPort();
}

UInt16 Context::getExchangeStatusPort(bool) const
{
    return getRPCPort();
}

void Context::setComplexQueryActive(bool active)
{
    shared->complex_query_active = active;
}

bool Context::getComplexQueryActive()
{
    return shared->complex_query_active;
}


void Context::setInterserverScheme(const String & scheme)
{
    shared->interserver_scheme = scheme;
}

String Context::getInterserverScheme() const
{
    return shared->interserver_scheme;
}

void Context::setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config)
{
    shared->remote_host_filter.setValuesFromConfig(config);
}

const RemoteHostFilter & Context::getRemoteHostFilter() const
{
    return shared->remote_host_filter;
}

UInt16 Context::getPortFromEnvForConsul(const char * key) const
{
    if (shared->server_type == ServerType::cnch_server || shared->server_type == ServerType::cnch_worker)
    {
        auto sd_client = this->getServiceDiscoveryClient();
        if (sd_client->getName() == "consul")
        {
            const char * value = getenv(key);
            if (value != nullptr)
                return parse<UInt16>(value);
        }
    }

    return 0;
}

HostWithPorts Context::getHostWithPorts() const
{
    auto get_host_with_port = [this] ()
    {
        String host = getHostIPFromEnv();
        String id = getWorkerID(shared_from_this());
        if (id.empty())
            id = host;

        return HostWithPorts{
            std::move(host), getRPCPort(), getTCPPort(), getHTTPPort(), getExchangePort(), getExchangeStatusPort(), std::move(id)};
    };

    static HostWithPorts cache = get_host_with_port();
    return cache;
}

UInt16 Context::getTCPPort() const
{
    if (auto env_port = getPortFromEnvForConsul("PORT0"))
        return env_port;

    const auto & config = getConfigRef();
    return config.getInt("tcp_port", DBMS_DEFAULT_PORT);
}

UInt16 Context::getTCPPort(const String & host, UInt16 rpc_port) const
{
    String psm = getConfigRef().getString("service_discovery.server.psm", "data.cnch.server");
    HostWithPortsVec server_vector = getServiceDiscoveryClient()->lookup(psm, ComponentType::SERVER);

    for (auto & server : server_vector)
    {
        if (isSameHost(server.getHost(), host) && rpc_port == server.rpc_port)
            return server.tcp_port;
    }

    throw Exception(
        "Can't get tcp_port by host: " + host + " and rpc_port: " + std::to_string(rpc_port), ErrorCodes::CNCH_SERVER_NOT_FOUND);
}

std::optional<UInt16> Context::getTCPPortSecure() const
{
    const auto & config = getConfigRef();
    if (config.has("tcp_port_secure"))
        return config.getInt("tcp_port_secure");
    return {};
}

void Context::registerServerPort(String port_name, UInt16 port)
{
    shared->server_ports.emplace(std::move(port_name), port);
}

UInt16 Context::getServerPort(const String & port_name) const
{
    auto it = shared->server_ports.find(port_name);
    if (it == shared->server_ports.end())
        throw Exception(ErrorCodes::BAD_GET, "There is no port named {}", port_name);
    else
        return it->second;
}

UInt16 Context::getHaTCPPort() const
{
    const auto & config = getConfigRef();
    return config.getInt("ha_tcp_port");
}

std::shared_ptr<Cluster> Context::getCluster(const std::string & cluster_name) const
{
    auto res = getClusters()->getCluster(cluster_name);
    if (res)
        return res;
    if (!cluster_name.empty())
        res = tryGetReplicatedDatabaseCluster(cluster_name, shared_from_this());
    if (res)
        return res;

    throw Exception("Requested cluster '" + cluster_name + "' not found", ErrorCodes::BAD_GET);
}


std::shared_ptr<Cluster> Context::tryGetCluster(const std::string & cluster_name) const
{
    return getClusters()->getCluster(cluster_name);
}


void Context::reloadClusterConfig() const
{
    while (true)
    {
        ConfigurationPtr cluster_config;
        {
            std::lock_guard lock(shared->clusters_mutex);
            cluster_config = shared->clusters_config;
        }

        const auto & config = cluster_config ? *cluster_config : getConfigRef();
        auto new_clusters = std::make_shared<Clusters>(config, settings);

        {
            std::lock_guard lock(shared->clusters_mutex);
            if (shared->clusters_config.get() == cluster_config.get())
            {
                shared->clusters = std::move(new_clusters);
                return;
            }

            // Clusters config has been suddenly changed, recompute clusters
        }
    }
}


std::shared_ptr<Clusters> Context::getClusters() const
{
    std::lock_guard lock(shared->clusters_mutex);
    if (!shared->clusters)
    {
        const auto & config = shared->clusters_config ? *shared->clusters_config : getConfigRef();
        shared->clusters = std::make_shared<Clusters>(config, settings);
    }

    return shared->clusters;
}


/// On repeating calls updates existing clusters and adds new clusters, doesn't delete old clusters
void Context::setClustersConfig(const ConfigurationPtr & config, const String & config_name)
{
    std::lock_guard lock(shared->clusters_mutex);

    /// Do not update clusters if this part of config wasn't changed.
    if (shared->clusters && isSameConfiguration(*config, *shared->clusters_config, config_name))
        return;

    auto old_clusters_config = shared->clusters_config;
    shared->clusters_config = config;

    if (!shared->clusters)
        shared->clusters = std::make_unique<Clusters>(*shared->clusters_config, settings, config_name);
    else
        shared->clusters->updateClusters(*shared->clusters_config, settings, config_name, old_clusters_config);
}


void Context::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(shared->clusters_mutex);

    if (!shared->clusters)
        throw Exception("Clusters are not set", ErrorCodes::LOGICAL_ERROR);

    shared->clusters->setCluster(cluster_name, cluster);
}


void Context::initializeSystemLogs()
{
    auto lock = getLock(); // checked
    shared->system_logs = std::make_unique<SystemLogs>(getGlobalContext(), getConfigRef());
}

void Context::initializeTraceCollector()
{
    shared->initializeTraceCollector(getTraceLog());
}

bool Context::hasTraceCollector() const
{
    return shared->hasTraceCollector();
}

std::shared_ptr<QueryLog> Context::getQueryLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_log;
}


std::shared_ptr<QueryThreadLog> Context::getQueryThreadLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_thread_log;
}


std::shared_ptr<QueryExchangeLog> Context::getQueryExchangeLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_exchange_log;
}


std::shared_ptr<PartLog> Context::getPartLog(const String & part_database) const
{
    auto lock = getLock(); // checked

    /// No part log or system logs are shutting down.
    if (!shared->system_logs)
        return {};

    /// Will not log operations on system tables (including part_log itself).
    /// It doesn't make sense and not allow to destruct PartLog correctly due to infinite logging and flushing,
    /// and also make troubles on startup.
    if (part_database == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    return shared->system_logs->part_log;
}


std::shared_ptr<PartMergeLog> Context::getPartMergeLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs || !shared->system_logs->part_merge_log)
        return {};

    return shared->system_logs->part_merge_log;
}


std::shared_ptr<ServerPartLog> Context::getServerPartLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs || !shared->system_logs->server_part_log)
        return {};

    return shared->system_logs->server_part_log;
}

void Context::initializeCnchSystemLogs()
{
    if ((shared->server_type != ServerType::cnch_server) && (shared->server_type != ServerType::cnch_worker))
        return;
    auto lock = getLock(); // checked
    shared->cnch_system_logs = std::make_unique<CnchSystemLogs>(getGlobalContext());
}

void Context::insertViewRefreshTaskLog(const ViewRefreshTaskLogElement & element) const
{
    auto view_refresh_task_log = getViewRefreshTaskLog();
    if (view_refresh_task_log)
        view_refresh_task_log->add(element);
    else
        LOG_WARNING(shared->log, "View Refresh Task Log has not been initialized.");
}

std::shared_ptr<CnchQueryLog> Context::getCnchQueryLog() const
{
    auto lock = getLock(); // checked

    if (!shared->cnch_system_logs)
        return {};

    return shared->cnch_system_logs->getCnchQueryLog();
}

std::shared_ptr<CnchAutoStatsTaskLog> Context::getCnchAutoStatsTaskLog() const
{
    auto lock = getLock();

    if (!shared->cnch_system_logs)
        return {};

    return shared->cnch_system_logs->getCnchAutoStatsTaskLog();
}

std::shared_ptr<ViewRefreshTaskLog> Context::getViewRefreshTaskLog() const
{
    auto lock = getLock(); // checked

    if (!shared->cnch_system_logs)
        return {};

    return shared->cnch_system_logs->getViewRefreshTaskLog();
}

std::shared_ptr<TraceLog> Context::getTraceLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->trace_log;
}


std::shared_ptr<TextLog> Context::getTextLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->text_log;
}


std::shared_ptr<MetricLog> Context::getMetricLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->metric_log;
}


std::shared_ptr<AsynchronousMetricLog> Context::getAsynchronousMetricLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_metric_log;
}


std::shared_ptr<OpenTelemetrySpanLog> Context::getOpenTelemetrySpanLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->opentelemetry_span_log;
}

std::shared_ptr<KafkaLog> Context::getKafkaLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->kafka_log;
}

std::shared_ptr<CloudKafkaLog> Context::getCloudKafkaLog() const
{
    auto lock = getLock(); // checked
    if (!shared->cnch_system_logs)
        return {};

    return shared->cnch_system_logs->getKafkaLog();
}

std::shared_ptr<CloudMaterializedMySQLLog> Context::getCloudMaterializedMySQLLog() const
{
    auto lock = getLock(); // checked
    if (!shared->cnch_system_logs)
        return {};

    return shared->cnch_system_logs->getMaterializedMySQLLog();
}

std::shared_ptr<CloudUniqueTableLog> Context::getCloudUniqueTableLog() const
{
    auto lock = getLock(); // checked
    if (!shared->cnch_system_logs)
        return {};

    return shared->cnch_system_logs->getUniqueTableLog();
}

std::shared_ptr<MutationLog> Context::getMutationLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->mutation_log;
}


std::shared_ptr<ProcessorsProfileLog> Context::getProcessorsProfileLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->processors_profile_log;
}

std::shared_ptr<RemoteReadLog> Context::getRemoteReadLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->remote_read_log;
}

std::shared_ptr<ZooKeeperLog> Context::getZooKeeperLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->zookeeper_log;
}

std::shared_ptr<AutoStatsTaskLog> Context::getAutoStatsTaskLog() const
{
    auto lock = getLock(); // checked

    if (!shared->system_logs)
        return {};

    return shared->system_logs->auto_stats_task_log;
}

CompressionCodecPtr Context::chooseCompressionCodec(size_t part_size, double part_size_ratio) const
{
    auto lock = getLock(); // checked

    if (!shared->compression_codec_selector)
    {
        constexpr auto config_name = "compression";
        const auto & config = getConfigRefWithLock(lock);

        if (config.has(config_name))
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>(config, "compression");
        else
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>();
    }

    return shared->compression_codec_selector->choose(part_size, part_size_ratio);
}


DiskPtr Context::getDisk(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    return disk_selector->get(name);
}

StoragePolicyPtr Context::getStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);

    return policy_selector->get(name);
}

StoragePolicyPtr Context::tryGetStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);

    return policy_selector->tryGet(name);
}


DisksMap Context::getDisksMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getDiskSelector(lock)->getDisksMap();
}

StoragePoliciesMap Context::getPoliciesMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getStoragePolicySelector(lock)->getPoliciesMap();
}

DiskSelectorPtr Context::getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const
{
    if (!shared->merge_tree_disk_selector)
    {
        constexpr auto config_name = "storage_configuration.disks";
        const auto & config = getConfigRef();

        shared->merge_tree_disk_selector = std::make_shared<DiskSelector>(config, config_name, shared_from_this());
    }
    return shared->merge_tree_disk_selector;
}

StoragePolicySelectorPtr Context::getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const
{
    if (!shared->merge_tree_storage_policy_selector)
    {
        constexpr auto config_name = "storage_configuration.policies";
        const auto & config = getConfigRef();

        shared->merge_tree_storage_policy_selector
            = std::make_shared<StoragePolicySelector>(config, config_name, getDiskSelector(lock), getDefaultCnchPolicyName());
    }
    return shared->merge_tree_storage_policy_selector;
}


void Context::updateStorageConfiguration(Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    if (shared->merge_tree_disk_selector)
        shared->merge_tree_disk_selector
            = shared->merge_tree_disk_selector->updateFromConfig(config, "storage_configuration.disks", shared_from_this());

    if (shared->merge_tree_storage_policy_selector)
    {
        try
        {
            shared->merge_tree_storage_policy_selector = shared->merge_tree_storage_policy_selector->updateFromConfig(
                config, "storage_configuration.policies", shared->merge_tree_disk_selector, getDefaultCnchPolicyName());
        }
        catch (Exception & e)
        {
            LOG_ERROR(
                shared->log, "An error has occurred while reloading storage policies, storage policies were not applied: {}", e.message());
        }
    }

#if !defined(ARCADIA_BUILD)
    if (shared->storage_s3_settings)
    {
        shared->storage_s3_settings->loadFromConfig("s3", config, getSettingsRef());
    }
#endif
}

const CnchHiveSettings & Context::getCnchHiveSettings() const
{
    auto lock = getLock(); // checked

    if (!shared->cnchhive_settings)
    {
        const auto & config = getConfigRefWithLock(lock);
        CnchHiveSettings cnchhive_settings;
        cnchhive_settings.loadFromConfig("hive", config);
        shared->cnchhive_settings.emplace(cnchhive_settings);
    }

    return *shared->cnchhive_settings;
}

const CnchHiveSettings & Context::getCnchLasSettings() const
{
    auto lock = getLock(); // checked

    if (!shared->las_settings)
    {
        const auto & config = getConfigRefWithLock(lock);
        CnchHiveSettings las_settings;
        las_settings.loadFromConfig("las", config);
        shared->las_settings.emplace(las_settings);
    }
    return *shared->las_settings;
}

const MergeTreeSettings & Context::getMergeTreeSettings(bool skip_unknown_settings) const
{
    auto lock = getLock(); // checked

    if (!shared->merge_tree_settings)
    {
        const auto & config = getConfigRefWithLock(lock);
        MergeTreeSettings mt_settings;
        mt_settings.loadFromConfig("merge_tree", config, skip_unknown_settings);
        shared->merge_tree_settings.emplace(mt_settings);
    }

    return *shared->merge_tree_settings;
}

const CnchFileSettings & Context::getCnchFileSettings() const
{
    auto lock = getLock(); // checked

    if (!shared->cnch_file_settings)
    {
        const auto & config = getConfigRefWithLock(lock);
        shared->cnch_file_settings.emplace();
        shared->cnch_file_settings->loadFromConfig("cnch_file", config);
    }

    return *shared->cnch_file_settings;
}

const MergeTreeSettings & Context::getReplicatedMergeTreeSettings() const
{
    auto lock = getLock(); // checked

    if (!shared->replicated_merge_tree_settings)
    {
        const auto & config = getConfigRefWithLock(lock);
        MergeTreeSettings mt_settings;
        mt_settings.loadFromConfig("merge_tree", config);
        mt_settings.loadFromConfig("replicated_merge_tree", config);
        shared->replicated_merge_tree_settings.emplace(mt_settings);
    }

    return *shared->replicated_merge_tree_settings;
}

const StorageS3Settings & Context::getStorageS3Settings() const
{
#if !defined(ARCADIA_BUILD)
    auto lock = getLock(); // checked

    if (!shared->storage_s3_settings)
    {
        const auto & config = getConfigRefWithLock(lock);
        shared->storage_s3_settings.emplace().loadFromConfig("s3", config, getSettingsRef());
    }

    return *shared->storage_s3_settings;
#else
    throw Exception("S3 is unavailable in Arcadia", ErrorCodes::NOT_IMPLEMENTED);
#endif
}

void Context::checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const
{
    if (!max_size_to_drop || size <= max_size_to_drop)
        return;

    fs::path force_file(getFlagsPath() + "force_drop_table");
    bool force_file_exists = fs::exists(force_file);

    if (force_file_exists)
    {
        try
        {
            fs::remove(force_file);
            return;
        }
        catch (...)
        {
            /// User should recreate force file on each drop, it shouldn't be protected
            tryLogCurrentException("Drop table check", "Can't remove force file to enable table or partition drop");
        }
    }

    String size_str = formatReadableSizeWithDecimalSuffix(size);
    String max_size_to_drop_str = formatReadableSizeWithDecimalSuffix(max_size_to_drop);
    throw Exception(
        ErrorCodes::TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT,
        "Table or Partition in {}.{} was not dropped.\nReason:\n"
        "1. Size ({}) is greater than max_[table/partition]_size_to_drop ({})\n"
        "2. File '{}' intended to force DROP {}\n"
        "How to fix this:\n"
        "1. Either increase (or set to zero) max_[table/partition]_size_to_drop in server config\n"
        "2. Either create forcing file {} and make sure that ClickHouse has write permission for it.\n"
        "Example:\nsudo touch '{}' && sudo chmod 666 '{}'",
        backQuoteIfNeed(database),
        backQuoteIfNeed(table),
        size_str,
        max_size_to_drop_str,
        force_file.string(),
        force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist",
        force_file.string(),
        force_file.string(),
        force_file.string());
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_table_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_table_size_to_drop = shared->max_table_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, table_size, max_table_size_to_drop);
}


void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_partition_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}


BlockInputStreamPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size) const
{
    return std::make_shared<InputStreamFromInputFormat>(
        FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size));
}

BlockInputStreamPtr Context::getInputStreamByFormatNameAndBuffer(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size, const ColumnsDescription& columns) const
{
    if (getSettingsRef().insert_null_as_default && columns.hasDefaults())
    {
        auto input_format = FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size);
        // Construct pipeline to addingDefaultsTransform
        Pipe pipe(input_format);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns, *input_format, shared_from_this());
        });

        QueryPipeline pipeline;
        pipeline.init(std::move(pipe));

        // Construct an inputStream by pipeline
        BlockInputStreamPtr adding_defaults_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

        return adding_defaults_stream;
    }
    else
    {
        auto input_stream = std::make_shared<InputStreamFromInputFormat>(
            FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size));

        return input_stream;
    }
}

BlockOutputStreamPtr Context::getOutputStreamParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputStreamParallelIfPossible(name, buf, sample, shared_from_this());
}

BlockOutputStreamPtr Context::getOutputStream(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputStream(name, buf, sample, shared_from_this());
}

OutputFormatPtr Context::getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample, bool out_to_directory) const
{
    return FormatFactory::instance().getOutputFormatParallelIfPossible(name, buf, sample, shared_from_this(), out_to_directory);
}

OutputFormatPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormat(name, buf, sample, shared_from_this());
}


time_t Context::getUptimeSeconds() const
{
    auto lock = getLock(); // checked
    return shared->uptime_watch.elapsedSeconds();
}


void Context::setConfigReloadCallback(ConfigReloadCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->config_reload_callback)
        throw Exception("Can't reload config because config_reload_callback is not set.", ErrorCodes::LOGICAL_ERROR);

    shared->config_reload_callback();
}


void Context::shutdown()
{
    // Disk selector might not be initialized if there was some error during
    // its initialization. Don't try to initialize it again on shutdown.
    if (shared->merge_tree_disk_selector)
    {
        for (auto & [disk_name, disk] : getDisksMap())
        {
            LOG_INFO(shared->log, "Shutdown disk {}", disk_name);
            disk->shutdown();
        }
    }

    shared->shutdown();
}


Context::ApplicationType Context::getApplicationType() const
{
    return shared->application_type;
}

void Context::setApplicationType(ApplicationType type)
{
    /// Lock isn't required, you should set it at start
    shared->application_type = type;
}

bool Context::getIsRestrictSettingsToWhitelist() const
{
    return shared->restrict_tenanted_users_to_whitelist_settings;
}

void Context::setIsRestrictSettingsToWhitelist(bool is_restrict)
{
    /// Lock isn't required, you should set it at start
    shared->restrict_tenanted_users_to_whitelist_settings = is_restrict;
}

void Context::addRestrictSettingsToWhitelist(const std::vector<String>& setting_names) const
{
    for (auto & name : setting_names)
        SettingsChanges::WHITELIST_SETTINGS.emplace(name);
}

void Context::setExtraRestrictSettingsToWhitelist(std::unordered_set<String>&& new_settings)
{
    std::lock_guard<std::mutex> lock(shared->extra_whitelist_settings_mutex);
    shared->extra_whitelist_settings.swap(new_settings);
}

bool Context::isExtraRestrictSettingsToWhitelist(const String & name) const
{
    std::lock_guard<std::mutex> lock(shared->extra_whitelist_settings_mutex);
    return shared->extra_whitelist_settings.find(name) != shared->extra_whitelist_settings.end();
}

bool Context::getBlockPrivilegedOp() const
{
    return shared->restrict_tenanted_users_to_privileged_operations;
}

void Context::setBlockPrivilegedOp(bool is_restrict)
{
    shared->restrict_tenanted_users_to_privileged_operations = is_restrict;
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    getAccessControlManager().setDefaultProfileName(shared->default_profile_name);

    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setCurrentProfile(shared->system_profile_name);

    applySettingsQuirks(settings, getLogger("SettingsQuirks"));

    shared->buffer_profile_name = config.getString("buffer_profile", shared->system_profile_name);
    buffer_context = Context::createCopy(shared_from_this());
    buffer_context->setCurrentProfile(shared->buffer_profile_name);
}

String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

String Context::getSystemProfileName() const
{
    return shared->system_profile_name;
}

String Context::getFormatSchemaPath(bool remote) const
{
    return remote ? shared->remote_format_schema_path : shared->format_schema_path;
}

void Context::setFormatSchemaPath(const String & path, bool remote)
{
    if (remote)
    {
        shared->remote_format_schema_path = path;
    }
    else
    {
        shared->format_schema_path = path;
    }
}

Context::SampleBlockCache & Context::getSampleBlockCache() const
{
    return getQueryContext()->sample_block_cache;
}


bool Context::hasQueryParameters() const
{
    return !query_parameters.empty();
}


const NameToNameMap & Context::getQueryParameters() const
{
    return query_parameters;
}


void Context::setQueryParameter(const String & name, const String & value)
{
    if (!query_parameters.emplace(name, value).second)
        throw Exception("Duplicate name " + backQuote(name) + " of query parameter", ErrorCodes::BAD_ARGUMENTS);
}


void Context::addBridgeCommand(std::unique_ptr<ShellCommand> cmd) const
{
    auto lock = getLock(); // checked
    shared->bridge_commands.emplace_back(std::move(cmd));
}


IHostContextPtr & Context::getHostContext()
{
    return host_context;
}


const IHostContextPtr & Context::getHostContext() const
{
    return host_context;
}


std::shared_ptr<ActionLocksManager> Context::getActionLocksManager()
{
    auto lock = getLock(); // checked

    if (!shared->action_locks_manager)
        shared->action_locks_manager = std::make_shared<ActionLocksManager>(shared_from_this());

    return shared->action_locks_manager;
}


void Context::setExternalTablesInitializer(ExternalTablesInitializer && initializer)
{
    if (external_tables_initializer_callback)
        throw Exception("External tables initializer is already set", ErrorCodes::LOGICAL_ERROR);

    external_tables_initializer_callback = std::move(initializer);
}

void Context::initializeExternalTablesIfSet()
{
    if (external_tables_initializer_callback)
    {
        external_tables_initializer_callback(shared_from_this());
        /// Reset callback
        external_tables_initializer_callback = {};
    }
}


void Context::setInputInitializer(InputInitializer && initializer)
{
    if (input_initializer_callback)
        throw Exception("Input initializer is already set", ErrorCodes::LOGICAL_ERROR);

    input_initializer_callback = std::move(initializer);
}


void Context::initializeInput(const StoragePtr & input_storage)
{
    if (!input_initializer_callback)
        throw Exception("Input initializer is not set", ErrorCodes::LOGICAL_ERROR);

    input_initializer_callback(shared_from_this(), input_storage);
    /// Reset callback
    input_initializer_callback = {};
}


void Context::setInputBlocksReaderCallback(InputBlocksReader && reader)
{
    if (input_blocks_reader)
        throw Exception("Input blocks reader is already set", ErrorCodes::LOGICAL_ERROR);

    input_blocks_reader = std::move(reader);
}


InputBlocksReader Context::getInputBlocksReaderCallback() const
{
    return input_blocks_reader;
}


void Context::resetInputCallbacks()
{
    if (input_initializer_callback)
        input_initializer_callback = {};

    if (input_blocks_reader)
        input_blocks_reader = {};
}


StorageID Context::resolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    Poco::Logger * log = &Poco::Logger::get("resolveStorageID");
    LOG_TRACE(log, "input " + storage_id.database_name + " " + storage_id.table_name);
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    /// skip session resource check if database is null to make temporary table can be found (e.g., join case _data1)
    if (getServerType() == ServerType::cnch_worker && !storage_id.database_name.empty())
    {
        if (auto worker_resource = tryGetCnchWorkerResource())
        {
            if (auto storage = worker_resource->tryGetTable(storage_id))
                return storage->getStorageID();
        }
    }

    StorageID resolved = StorageID::createEmpty();
    std::optional<Exception> exc;
    {
        auto lock = getLock(); // checked
        resolved = resolveStorageIDImpl(std::move(storage_id), where, &exc);
    }
    if (exc)
        throw Exception(*exc);
    if (!resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        resolved.uuid
            = DatabaseCatalog::instance().getDatabase(resolved.database_name, shared_from_this())->tryGetTableUUID(resolved.table_name);
    }

    return resolved;
}

StorageID Context::tryResolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    {
        auto lock = getLock(); // checked
        resolved = resolveStorageIDImpl(std::move(storage_id), where, nullptr);
    }
    if (resolved && !resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        auto db = DatabaseCatalog::instance().tryGetDatabase(resolved.database_name, shared_from_this());
        if (db)
            resolved.uuid = db->tryGetTableUUID(resolved.table_name);
    }
    return resolved;
}

// TODO(renming)
// table -> first check in temporary table, then resolve as current_catalog.current_database.table
StorageID Context::resolveStorageIDImpl(StorageID storage_id, StorageNamespace where, std::optional<Exception> * exception) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    if (!storage_id)
    {
        if (exception)
            exception->emplace("Both table name and UUID are empty", ErrorCodes::UNKNOWN_TABLE);
        return storage_id;
    }

    bool look_for_external_table = where & StorageNamespace::ResolveExternal;
    bool in_current_database = where & StorageNamespace::ResolveCurrentDatabase;
    bool in_specified_database = where & StorageNamespace::ResolveGlobal;


    if (!storage_id.database_name.empty())
    {
        if (in_specified_database)
            return storage_id; /// NOTE There is no guarantees that table actually exists in database.
        if (exception)
            exception->emplace(
                "External and temporary tables have no database, but " + storage_id.database_name + " is specified",
                ErrorCodes::UNKNOWN_TABLE);
        return StorageID::createEmpty();
    }

    //TODO(renming):: add current_catalog_here?
    // if(storage_id.catalog_name != DefaultCatalogName)
    // {
    //     ExternalCatalog::Mgr::Instance().getCatalog(storage_id.catalog_name).
    // }

    /// Database name is not specified. It's temporary table or table in current database.

    if (look_for_external_table)
    {
        /// Global context should not contain temporary tables
        assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);

        auto resolved_id = StorageID::createEmpty();
        auto try_resolve = [&](ContextPtr context) -> bool {
            const auto & tables = context->external_tables_mapping;
            auto it = tables.find(storage_id.getTableName());
            if (it == tables.end())
                return false;
            resolved_id = it->second->getGlobalTableID();
            return true;
        };

        /// Firstly look for temporary table in current context
        if (try_resolve(shared_from_this()))
            return resolved_id;

        /// If not found and current context was created from some query context, look for temporary table in query context
        auto query_context_ptr = query_context.lock();
        bool is_local_context = query_context_ptr && query_context_ptr.get() != this;
        if (is_local_context && try_resolve(query_context_ptr))
            return resolved_id;

        /// If not found and current context was created from some session context, look for temporary table in session context
        auto session_context_ptr = session_context.lock();
        bool is_local_or_query_context = session_context_ptr && session_context_ptr.get() != this;
        if (is_local_or_query_context && try_resolve(session_context_ptr))
            return resolved_id;
    }


    /// Temporary table not found. It's table in current database.
    if (in_current_database)
    {
        if (current_database.empty())
        {
            if (exception)
                exception->emplace("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
            return StorageID::createEmpty();
        }
        storage_id.database_name = current_database;
        /// NOTE There is no guarantees that table actually exists in database.
        return storage_id;
    }

    if (exception)
        exception->emplace("Cannot resolve database name for table " + storage_id.getNameForLogs(), ErrorCodes::UNKNOWN_TABLE);
    return StorageID::createEmpty();
}

void Context::initZooKeeperMetadataTransaction(ZooKeeperMetadataTransactionPtr txn, [[maybe_unused]] bool attach_existing)
{
    assert(!metadata_transaction);
    assert(attach_existing || query_context.lock().get() == this);
    metadata_transaction = std::move(txn);
}

ZooKeeperMetadataTransactionPtr Context::getZooKeeperMetadataTransaction() const
{
    assert(!metadata_transaction || hasQueryContext());
    return metadata_transaction;
}

PartUUIDsPtr Context::getPartUUIDs() const
{
    auto lock = getLocalLock(); // checked
    if (!part_uuids)
        /// For context itself, only this initialization is not const.
        /// We could have done in constructor.
        /// TODO: probably, remove this from Context.
        const_cast<PartUUIDsPtr &>(part_uuids) = std::make_shared<PartUUIDs>();

    return part_uuids;
}


ReadTaskCallback Context::getReadTaskCallback() const
{
    if (!next_task_callback.has_value())
        throw Exception(fmt::format("Next task callback is not set for query {}", getInitialQueryId()), ErrorCodes::LOGICAL_ERROR);
    return next_task_callback.value();
}


void Context::setReadTaskCallback(ReadTaskCallback && callback)
{
    next_task_callback = callback;
}

PartUUIDsPtr Context::getIgnoredPartUUIDs() const
{
    auto lock = getLocalLock(); // checked
    if (!ignored_part_uuids)
        const_cast<PartUUIDsPtr &>(ignored_part_uuids) = std::make_shared<PartUUIDs>();

    return ignored_part_uuids;
}

void Context::setReadyForQuery()
{
    shared->ready_for_query = true;
}

bool Context::isReadyForQuery() const
{
    return shared->ready_for_query;
}

void Context::setHdfsUser(const String & name)
{
    shared->hdfs_user = name;
}

String Context::getHdfsUser() const
{
    return shared->hdfs_user;
}

void Context::setHdfsNNProxy(const String & name)
{
    shared->hdfs_nn_proxy = name;
}

String Context::getHdfsNNProxy() const
{
    return shared->hdfs_nn_proxy;
}


void Context::setHdfsConnectionParams(const HDFSConnectionParams & params)
{
    shared->hdfs_connection_params = params;
}

HDFSConnectionParams Context::getHdfsConnectionParams() const
{
    return shared->hdfs_connection_params;
}

void Context::setLasfsConnectionParams(const Poco::Util::AbstractConfiguration & config) {
    if(config.has("lasfs_config")){
        setSetting("lasfs_service_name",config.getString("lasfs_config.lasfs_service_name",""));
        setSetting("lasfs_endpoint",config.getString("lasfs_config.lasfs_endpoint",""));
        setSetting("lasfs_region",config.getString("lasfs_config.lasfs_region",""));
    }
}

void Context::setVETosConnectParams(const VETosConnectionParams & connect_params)
{
    auto lock = getLock(); // checked
    shared->vetos_connection_params = connect_params;
}

const VETosConnectionParams & Context::getVETosConnectParams() const
{
    auto lock = getLock(); // checked
    return shared->vetos_connection_params;
}

void Context::setOSSConnectParams(const OSSConnectionParams & connect_params)
{
    shared->oss_connection_params = connect_params;
}

const OSSConnectionParams & Context::getOSSConnectParams() const
{
    return shared->oss_connection_params;
}

void Context::setUniqueKeyIndexBlockCache(size_t cache_size_in_bytes)
{
    auto lock = getLock(); // checked
    if (shared->unique_key_index_block_cache)
        throw Exception("Unique key index block cache has been already created", ErrorCodes::LOGICAL_ERROR);
    shared->unique_key_index_block_cache = IndexFile::NewLRUCache(cache_size_in_bytes);
}

UniqueKeyIndexBlockCachePtr Context::getUniqueKeyIndexBlockCache() const
{
    auto lock = getLock(); // checked
    return shared->unique_key_index_block_cache;
}

void Context::setUniqueKeyIndexFileCache(size_t cache_size_in_bytes)
{
    auto lock = getLock(); // checked
    if (shared->unique_key_index_file_cache)
        throw Exception("Unique key index file cache has been already created", ErrorCodes::LOGICAL_ERROR);
    shared->unique_key_index_file_cache = std::make_shared<KeyIndexFileCache>(*this, cache_size_in_bytes);
}

UniqueKeyIndexFileCachePtr Context::getUniqueKeyIndexFileCache() const
{
    auto lock = getLock(); // checked
    return shared->unique_key_index_file_cache;
}

void Context::setUniqueKeyIndexCache(size_t cache_size_in_bytes)
{
    auto lock = getLock(); // checked
    if (shared->unique_key_index_cache)
        throw Exception("Unique key index cache has been already created", ErrorCodes::LOGICAL_ERROR);
    shared->unique_key_index_cache = std::make_shared<UniqueKeyIndexCache>(cache_size_in_bytes);
}

std::shared_ptr<UniqueKeyIndexCache> Context::getUniqueKeyIndexCache() const
{
    auto lock = getLock(); // checked
    return shared->unique_key_index_cache;
}

void Context::setDeleteBitmapCache(size_t cache_size_in_bytes)
{
    auto lock = getLock(); // checked
    if (shared->delete_bitmap_cache)
        throw Exception("Delete bitmap cache has been already created", ErrorCodes::LOGICAL_ERROR);
    shared->delete_bitmap_cache = std::make_shared<DeleteBitmapCache>(cache_size_in_bytes);
}

DeleteBitmapCachePtr Context::getDeleteBitmapCache() const
{
    auto lock = getLock(); // checked
    return shared->delete_bitmap_cache;
}

void Context::setMetaChecker()
{
    auto meta_checker = [this]() {
        LoggerPtr log = getLogger("MetaChecker");

        Stopwatch stopwatch;
        LOG_DEBUG(log, "Start to run metadata synchronization task.");

        size_t task_min_interval = 0;
        size_t table_count = 0;

        if (this->shared->stop_sync)
        {
            /// if task stopped. we should make sure it is not been scheduled too often.
            task_min_interval = 5 * 60 * 1000;
            LOG_WARNING(log, "Metadata synchronization task has been stopped.");
        }
        else
        {
            auto database_snapshots = DatabaseCatalog::instance().getNonCnchDatabases();

            for (const auto & database_snapshot : database_snapshots)
            {
                try
                {
                    String current_database_name = database_snapshot.first;
                    DatabasePtr current_database = database_snapshot.second;

                    DatabaseCatalog::instance().assertDatabaseExists(current_database_name, shared_from_this());
                    for (auto tb_it = current_database->getTablesIterator(this->shared_from_this()); tb_it->isValid(); tb_it->next())
                    {
                        String current_table_name = tb_it->name();
                        StoragePtr current_table = tb_it->table();
                        /// skip if current table is removed or the table is not in MergeTree family.
                        if (!current_table || !endsWith(current_table->getName(), "MergeTree"))
                            continue;
                        /// lock current table to avoid conflict with drop query.
                        auto lock = current_table->lockForShare("SYNC_META_TASK", this->getSettingsRef().lock_acquire_timeout);

                        MergeTreeData & data = dynamic_cast<MergeTreeData &>(*current_table);
                        LOG_INFO(log, "Start check metadata of table " + current_database_name + "." + current_table_name);

                        /// To avoid blocking whole task, we may skip current table if failed to get data part lock.
                        data.trySyncMetaData();
                        table_count++;
                    }
                }
                catch (...)
                {
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);
                }
            }
        }

        LOG_DEBUG(log, "Finish the metadata synchronization task for {} tables in {}ms.", table_count, stopwatch.elapsedMilliseconds());
        /// default interval is 10min.
        size_t delay_ms = this->getSettingsRef().meta_sync_task_interval_ms.totalMilliseconds();
        this->shared->meta_checker->scheduleAfter(std::max(delay_ms, task_min_interval));
    };

    shared->meta_checker = getLocalSchedulePool().createTask("MetaCheck", meta_checker);
    shared->meta_checker->activate();
    /// do not start sync immediately, delay 10min
    shared->meta_checker->scheduleAfter(10 * 60 * 1000);
}

void Context::setMetaCheckerStatus(bool stop)
{
    shared->stop_sync = stop;
}

void Context::setChecksumsCache(const ChecksumsCacheSettings & settings_)
{
    if (shared->checksums_cache)
        throw Exception("Checksums cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->checksums_cache = std::make_shared<ChecksumsCache>(settings_);
}

std::shared_ptr<ChecksumsCache> Context::getChecksumsCache() const
{
    return shared->checksums_cache;
}

void Context::setCompressedDataIndexCache(size_t cache_size_in_bytes)
{
    if (shared->compressed_data_index_cache)
        throw Exception("Compressed data index cache has been already created.", ErrorCodes::LOGICAL_ERROR);
    shared->compressed_data_index_cache = std::make_shared<CompressedDataIndexCache>(cache_size_in_bytes);
}

std::shared_ptr<CompressedDataIndexCache> Context::getCompressedDataIndexCache() const
{
    return shared->compressed_data_index_cache;
}

void Context::setGinIndexFilterResultCache(size_t cache_size_in_bytes)
{
    if (shared->gin_idx_filter_result_cache)
        throw Exception("GinIndex postings cache has been already created.",
            ErrorCodes::LOGICAL_ERROR);
    if (cache_size_in_bytes != 0)
        shared->gin_idx_filter_result_cache = std::make_unique<GinIdxFilterResultCache>(
            cache_size_in_bytes, 8);
}

GinIdxFilterResultCache* Context::getGinIndexFilterResultCache() const
{
    return shared->gin_idx_filter_result_cache.get();
}

void Context::setGINStoreReaderFactory(const GINStoreReaderFactorySettings & settings_)
{
    if (shared->gin_store_reader_factory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GINStoreReaderFactory has already "
            "been created");

    shared->gin_store_reader_factory = std::make_shared<GINStoreReaderFactory>(settings_);
}

std::shared_ptr<GINStoreReaderFactory> Context::getGINStoreReaderFactory() const
{
    return shared->gin_store_reader_factory;
}

void Context::setPrimaryIndexCache(size_t cache_size_in_bytes)
{
    if (shared->primary_index_cache)
        throw Exception("Primary index cache has already been created.", ErrorCodes::LOGICAL_ERROR);
    shared->primary_index_cache = std::make_shared<PrimaryIndexCache>(cache_size_in_bytes);
}

std::shared_ptr<PrimaryIndexCache> Context::getPrimaryIndexCache() const
{
    return shared->primary_index_cache;
}

void Context::updateQueueManagerConfig() const
{
    getQueueManager()->loadConfig(getRootConfig().queue_manager);
}

void Context::initServiceDiscoveryClient()
{
    const auto & cnch_config = getCnchConfigRef();
    shared->sd = ServiceDiscoveryFactory::instance().create(cnch_config);
}

ServiceDiscoveryClientPtr Context::getServiceDiscoveryClient() const
{
    return shared->sd;
}

void Context::initTSOClientPool(const String & service_name)
{
    shared->tso_client_pool = std::make_unique<TSOClientPool>(
        service_name, [sd = shared->sd, service_name] { return sd->lookup(service_name, ComponentType::TSO); });
}

std::shared_ptr<TSO::TSOClient> Context::getCnchTSOClient() const
{
    if (!shared->tso_client_pool)
        throw Exception("Cnch tso client pool is not initialized", ErrorCodes::LOGICAL_ERROR);

    auto host_port = tryGetTSOLeaderHostPort();

    if (host_port.empty())
        updateTSOLeaderHostPort();

    if (auto updated_host_port = tryGetTSOLeaderHostPort(); !updated_host_port.empty())
    {
        LOG_TRACE(shared->log, "TSO Leader host-port is: {} ", updated_host_port);
        return shared->tso_client_pool->get(updated_host_port);
    }
    else
        throw Exception(ErrorCodes::NOT_A_LEADER, "Can't get leader for tso");
}

void Context::initTSOElectionReader()
{
    auto prefix = getRootConfig().service_discovery_kv.election_prefix.value;
    shared->tso_election_reader = std::make_unique<ElectionReader>(
        std::make_shared<TSOKvStorage>(getCnchCatalog()->getMetastore()),
        prefix + getRootConfig().service_discovery_kv.tso_host_path.value);
}

String Context::tryGetTSOLeaderHostPort() const
{
    if (!shared || !shared->tso_election_reader)
        return "";
    if (auto leader_info = shared->tso_election_reader->tryGetLeaderInfo())
        return createHostPortString(leader_info->getHost(), leader_info->getRPCPort());

    return "";
}

void Context::updateTSOLeaderHostPort() const
{
    shared->tso_election_reader->refresh();
}

UInt64 Context::getTimestamp() const
{
    return TSO::getTSOResponse(*this, TSO::TSORequestType::GetTimestamp);
}

UInt64 Context::tryGetTimestamp(const String & pretty_func_name) const
{
    try
    {
        return getTimestamp();
    }
    catch (...)
    {
        if (!getConfigRef().getBool("tso_service.use_fallback", true))
            throw;
        tryLogCurrentException(
            pretty_func_name.c_str(), fmt::format("Unable to reach TSO from {} during call to tryGetTimestamp", tryGetTSOLeaderHostPort()));
        return TxnTimestamp::fallbackTS();
    }
}

UInt64 Context::getTimestamps(UInt32 size) const
{
    return TSO::getTSOResponse(*this, TSO::TSORequestType::GetTimestamps, size);
}

UInt64 Context::getPhysicalTimestamp() const
{
    // 46 bit of TSO timestamp is used to store physical part
    const auto tso_ts = tryGetTimestamp();
    if (TxnTimestamp::fallbackTS() == tso_ts)
        return 0;
    return TxnTimestamp(tso_ts).toMillisecond();
}

void Context::setPartCacheManager()
{
    auto lock = getLock(); // checked

    if (shared->cache_manager)
        throw Exception("Part cache manager has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->cache_manager = std::make_shared<PartCacheManager>(shared_from_this(), total_memory_tracker.getHardLimit());
}

PartCacheManagerPtr Context::getPartCacheManager() const
{
    /// no need to lock because PartCacheManager is initialized during server start up,
    /// there is no concurrent setPartCacheManager and getPartCacheManager usage.
    return shared->cache_manager;
}

void Context::setManifestCache()
{
    auto lock = getLock();

    if (!shared->manifest_cache)
        shared->manifest_cache = std::make_shared<ManifestCache>(getSettingsRef().max_manifest_cache_size, std::chrono::seconds(getSettingsRef().manifest_cache_min_lifetime.value.totalSeconds()));
}

std::shared_ptr<ManifestCache> Context::getManifestCache() const
{
    if (!shared->manifest_cache)
        throw Exception("Manifest cache is not initialized", ErrorCodes::LOGICAL_ERROR);

    return shared->manifest_cache;
}

void Context::initCatalog(const MetastoreConfig & catalog_conf, const String & name_space, bool writable)
{
    shared->cnch_catalog = std::make_unique<Catalog::Catalog>(*this, catalog_conf, name_space, writable);
}

std::shared_ptr<Catalog::Catalog> Context::tryGetCnchCatalog() const
{
    return shared->cnch_catalog;
}

std::shared_ptr<Catalog::Catalog> Context::getCnchCatalog() const
{
    if (!shared->cnch_catalog)
        throw Exception("Cnch catalog is not initialized", ErrorCodes::LOGICAL_ERROR);

    return shared->cnch_catalog;
}

void Context::initDaemonManagerClientPool(const String & service_name)
{
    shared->daemon_manager_pool = std::make_unique<DaemonManagerClientPool>(
        service_name, [sd = shared->sd, service_name] { return sd->lookup(service_name, ComponentType::DAEMON_MANAGER); });
}

DaemonManagerClientPtr Context::getDaemonManagerClient() const
{
    if (!shared->daemon_manager_pool)
        throw Exception("Cnch daemon manager client pool is not initialized", ErrorCodes::LOGICAL_ERROR);
    return shared->daemon_manager_pool->get();
}

void Context::setCnchServerManager(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock(); // checked
    if (shared->server_manager)
        throw Exception("Server manager has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->server_manager = std::make_shared<CnchServerManager>(shared_from_this(), config);
}

std::shared_ptr<CnchServerManager> Context::getCnchServerManager() const
{
    auto lock = getLock(); // checked
    if (!shared->server_manager)
        throw Exception("Server manager is not initiailized.", ErrorCodes::LOGICAL_ERROR);

    return shared->server_manager;
}

void Context::updateServerVirtualWarehouses(const ConfigurationPtr & config)
{
    std::shared_ptr<CnchServerManager> server_manager;
    {
        auto lock = getLock(); // checked
        server_manager = shared->server_manager;
    }
    if (server_manager)
        server_manager->updateServerVirtualWarehouses(*config);
}

void Context::setCnchTopologyMaster()
{
    auto lock = getLock(); // checked
    if (shared->topology_master)
        throw Exception("Topology master has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->topology_master = std::make_shared<CnchTopologyMaster>(shared_from_this());
}

std::shared_ptr<CnchTopologyMaster> Context::getCnchTopologyMaster() const
{
    auto lock = getLock(); // checked
    if (!shared->topology_master)
        throw Exception("Topology master is not initialized.", ErrorCodes::LOGICAL_ERROR);

    return shared->topology_master;
}

GlobalTxnCommitterPtr Context::getGlobalTxnCommitter() const
{
    callOnce(shared->global_txn_committer_initialized, [&] {
        shared->global_txn_committer = std::make_shared<GlobalTxnCommitter>(getGlobalContext());
    });
    return shared->global_txn_committer;
}

void Context::initGlobalDataManager() const
{
    shared->global_data_manager = std::make_shared<GlobalDataManager>(shared_from_this());
}

GlobalDataManagerPtr Context::getGlobalDataManager() const
{
    if (!shared->global_data_manager)
        throw Exception("GlobalDataManager is not initialized", ErrorCodes::LOGICAL_ERROR);

    return shared->global_data_manager;
}

UInt16 Context::getRPCPort() const
{
    if (auto env_port = getPortFromEnvForConsul("PORT1"))
        return env_port;

    /// In the current implementation, we needed to read rpc_port from the configuration of the components,
    /// as they might not be set separately rpc_port configuration in root_config.
    if (getServerType() == ServerType::cnch_resource_manager)
        return getRootConfig().resource_manager.port;

    if (getServerType() == ServerType::cnch_tso_server)
        return getRootConfig().tso_service.port;

    return getRootConfig().rpc_port;
}

UInt16 Context::getHTTPPort() const
{
    if (auto env_port = getPortFromEnvForConsul("PORT2"))
        return env_port;

    return getRootConfig().http_port;
}

void Context::setServerType(const String & type_str)
{
    if (type_str == "standalone")
        shared->server_type = ServerType::standalone;
    else if (type_str == "server")
        shared->server_type = ServerType::cnch_server;
    else if (type_str == "worker")
        shared->server_type = ServerType::cnch_worker;
    else if (type_str == "daemon_manager")
        shared->server_type = ServerType::cnch_daemon_manager;
    else if (type_str == "resource_manager")
        shared->server_type = ServerType::cnch_resource_manager;
    else if (type_str == "tso_server")
        shared->server_type = ServerType::cnch_tso_server;
    else if (type_str == "bytepond")
        shared->server_type = ServerType::cnch_bytepond;
    else
        throw Exception("Unknown server type: " + type_str, ErrorCodes::BAD_ARGUMENTS);
}

ServerType Context::getServerType() const
{
    return shared->server_type;
}

String Context::getServerTypeString() const
{
    String type_str;
    switch (shared->server_type)
    {
        case ServerType::standalone:
            type_str = "standalone";
            break;
        case ServerType::cnch_server:
            type_str = "cnch_server";
            break;
        case ServerType::cnch_worker:
            type_str = "cnch_worker";
            break;
        case ServerType::cnch_daemon_manager:
            type_str = "cnch_daemon_manager";
            break;
        case ServerType::cnch_resource_manager:
            type_str = "cnch_resource_manager";
            break;
        case ServerType::cnch_tso_server:
            type_str = "cnch_tso_server";
            break;
        case ServerType::cnch_bytepond:
            type_str = "cnch_bytepond";
            break;
        default:
            throw Exception("Unknown server type: " + std::to_string(static_cast<int>(shared->server_type)), ErrorCodes::BAD_ARGUMENTS);
    }
    return type_str;
}

UInt64 Context::getNonHostUpdateTime(const UUID & uuid)
{
    {
        std::lock_guard<std::mutex> lock(*nhut_mutex);
        if (auto it = session_nhuts.find(uuid); it != session_nhuts.end())
            return it->second;
    }

    UInt64 fetched_nhut = getCnchCatalog()->getNonHostUpdateTimestampFromByteKV(uuid);

    {
        std::lock_guard<std::mutex> lock(*nhut_mutex);
        session_nhuts.emplace(uuid, fetched_nhut);
    }

    return fetched_nhut;
}

void Context::initCnchServerClientPool(const String & service_name)
{
    shared->cnch_server_client_pool = std::make_unique<CnchServerClientPool>(
        service_name, [sd = shared->sd, service_name] { return sd->lookup(service_name, ComponentType::SERVER); });
}

CnchServerClientPool & Context::getCnchServerClientPool() const
{
    if (!shared->cnch_server_client_pool)
        throw Exception("Cnch server client pool is not initialized", ErrorCodes::LOGICAL_ERROR);
    return *shared->cnch_server_client_pool;
}

CnchServerClientPtr Context::getCnchServerClient(const std::string & host, uint16_t port) const
{
    if (!shared->cnch_server_client_pool)
        throw Exception("Cnch server client pool is not initialized", ErrorCodes::LOGICAL_ERROR);
    return shared->cnch_server_client_pool->get(host, port);
}

CnchServerClientPtr Context::getCnchServerClient(const std::string & host_port) const
{
    if (!shared->cnch_server_client_pool)
        throw Exception("Cnch server client pool is not initialized", ErrorCodes::LOGICAL_ERROR);
    return shared->cnch_server_client_pool->get(host_port);
}

CnchServerClientPtr Context::getCnchServerClient() const
{
    if (!shared->cnch_server_client_pool)
        throw Exception("Cnch server client pool is not initialized", ErrorCodes::LOGICAL_ERROR);
    return shared->cnch_server_client_pool->get();
}

CnchServerClientPtr Context::getCnchServerClient(const HostWithPorts & host_with_ports) const
{
    if (!shared->cnch_server_client_pool)
        throw Exception("Cnch server client pool is not initialized", ErrorCodes::LOGICAL_ERROR);
    return shared->cnch_server_client_pool->get(host_with_ports);
}

void Context::initCnchWorkerClientPools()
{
    shared->cnch_worker_client_pools = std::make_unique<CnchWorkerClientPools>(getServiceDiscoveryClient());
}

CnchWorkerClientPools & Context::getCnchWorkerClientPools() const
{
    if (!shared->cnch_worker_client_pools)
        throw Exception("Cnch worker client pools are not initialized", ErrorCodes::LOGICAL_ERROR);
    return *shared->cnch_worker_client_pools;
}


String Context::getVirtualWarehousePSM() const
{
    return getRootConfig().service_discovery.vw_psm;
}

void Context::initVirtualWarehousePool()
{
    shared->vw_pool = std::make_unique<VirtualWarehousePool>(getGlobalContext());
}

VirtualWarehousePool & Context::getVirtualWarehousePool() const
{
    if (!shared->vw_pool)
        throw Exception("VirtualWarehousePool is not initialized.", ErrorCodes::LOGICAL_ERROR);

    return *shared->vw_pool;
}

StoragePtr Context::tryGetCnchTable(const String &, const String &) const
{
    throw Exception("Not implemented yet. ", ErrorCodes::NOT_IMPLEMENTED);
}

void Context::setCurrentWorkerGroup(WorkerGroupHandle worker_group) const
{
    current_worker_group = std::move(worker_group);
}

WorkerGroupHandle Context::getCurrentWorkerGroup() const
{
    if (!current_worker_group)
        throw Exception("Worker group is not set", ErrorCodes::LOGICAL_ERROR);
    return current_worker_group;
}

WorkerGroupHandle Context::tryGetCurrentWorkerGroup() const
{
    return current_worker_group;
}

void Context::setCurrentVW(VirtualWarehouseHandle vw)
{
    current_vw = std::move(vw);
}

VirtualWarehouseHandle Context::getCurrentVW() const
{
    if (!current_vw)
        throw Exception("Virtual warehouse is not set", ErrorCodes::LOGICAL_ERROR);
    return current_vw;
}

VirtualWarehouseHandle Context::tryGetCurrentVW() const
{
    return current_vw;
}

void Context::initResourceManagerClient()
{
    LOG_DEBUG(shared->log, "Initialising Resource Manager Client");
    const auto & root_config = getRootConfig();
    const auto & max_retry_count = root_config.resource_manager.init_client_tries;
    const auto & retry_interval_ms = root_config.resource_manager.init_client_retry_interval_ms;

    size_t retry_count = 0;
    do
    {
        String host_port;
        try
        {
            auto lock = getLock(); // checked
            shared->rm_client = std::make_shared<ResourceManagerClient>(getGlobalContext());
            LOG_DEBUG(shared->log, "Initialised Resource Manager Client on try: {}", retry_count);
            return;
        }
        catch (...)
        {
            tryLogCurrentException("Context::initResourceManagerClient", __PRETTY_FUNCTION__);
            usleep(retry_interval_ms * 1000);
        }
    } while (retry_count++ < max_retry_count);

    throw Exception("Unable to initialise Resource Manager Client", ErrorCodes::RESOURCE_MANAGER_NO_LEADER_ELECTED);
}

ResourceManagerClientPtr Context::getResourceManagerClient() const
{
    return shared->rm_client;
}

void Context::initCnchBGThreads()
{
    auto lock = getLock(); // checked
    shared->cnch_bg_threads_array = std::make_unique<CnchBGThreadsMapArray>(shared_from_this());
}

UInt32 Context::getEpoch()
{
    return shared->cnch_bg_threads_array->getEpoch();
}

CnchBGThreadsMap * Context::getCnchBGThreadsMap(CnchBGThreadType type) const
{
    return shared->cnch_bg_threads_array->at(type);
}

CnchBGThreadPtr Context::getCnchBGThread(CnchBGThreadType type, const StorageID & storage_id) const
{
    return getCnchBGThreadsMap(type)->getThread(storage_id);
}

CnchBGThreadPtr Context::tryGetCnchBGThread(CnchBGThreadType type, const StorageID & storage_id) const
{
    return getCnchBGThreadsMap(type)->tryGetThread(storage_id);
}

void Context::controlCnchBGThread(const StorageID & storage_id, CnchBGThreadType type, CnchBGThreadAction action) const
{
    getCnchBGThreadsMap(type)->controlThread(storage_id, action);
}

bool Context::getTableReclusterTaskStatus(const StorageID & storage_id) const
{
    CnchBGThreadsMap * thread_map = getCnchBGThreadsMap(CnchBGThreadType::Clustering);
    if (!thread_map)
        throw Exception("Fail to get merge thread map", ErrorCodes::SYSTEM_ERROR);
    CnchBGThreadPtr bg_thread_ptr = thread_map->tryGetThread(storage_id);
    if (!bg_thread_ptr)
    {
        LOG_DEBUG(shared->log, "Fail to get reclustering manager thread for " + storage_id.getNameForLogs());
        return false;
    }

    ReclusteringManagerThread * reclustering_manager_thread = dynamic_cast<ReclusteringManagerThread *>(bg_thread_ptr.get());
    if (!reclustering_manager_thread)
        throw Exception("Fail to cast to ReclusteringManagerThread", ErrorCodes::LOGICAL_ERROR);
    return reclustering_manager_thread->getTableReclusterStatus();
}

bool Context::removeMergeMutateTasksOnPartitions(const StorageID & storage_id, const std::unordered_set<String> & partitions)
{
    CnchBGThreadsMap * thread_map = getCnchBGThreadsMap(CnchBGThreadType::MergeMutate);
    if (!thread_map)
        throw Exception("Fail to get merge thread map", ErrorCodes::SYSTEM_ERROR);
    CnchBGThreadPtr bg_thread_ptr = thread_map->tryGetThread(storage_id);
    if (!bg_thread_ptr)
    {
        LOG_DEBUG(shared->log, "Fail to get merge thread for {}", storage_id.getNameForLogs());
        return false;
    }

    CnchMergeMutateThread * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(bg_thread_ptr.get());
    if (!merge_mutate_thread)
        throw Exception("Fail to cast to CnchMergeMutateThread", ErrorCodes::LOGICAL_ERROR);
    return merge_mutate_thread->removeTasksOnPartitions(partitions);
}

ClusterTaskProgress Context::getTableReclusterTaskProgress(const StorageID & storage_id) const
{
    CnchBGThreadsMap * thread_map = getCnchBGThreadsMap(CnchBGThreadType::MergeMutate);
    if (!thread_map)
        throw Exception("Fail to get merge thread map", ErrorCodes::SYSTEM_ERROR);
    CnchBGThreadPtr bg_thread_ptr = thread_map->tryGetThread(storage_id);
    ClusterTaskProgress cluster_task_progress;
    if (!bg_thread_ptr)
    {
        LOG_DEBUG(shared->log, "Fail to get merge thread for " + storage_id.getNameForLogs());
        return cluster_task_progress;
    }

    CnchMergeMutateThread * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(bg_thread_ptr.get());
    if (!merge_mutate_thread)
        throw Exception("Fail to cast to CnchMergeMutateThread", ErrorCodes::LOGICAL_ERROR);
    return merge_mutate_thread->getReclusteringTaskProgress();
}

void Context::startResourceReport()
{
    if (getServerType() != ServerType::cnch_worker)
        return;
    shared->cnch_bg_threads_array->startResourceReport();
}

void Context::stopResourceReport()
{
    if (getServerType() != ServerType::cnch_worker)
        return;
    shared->cnch_bg_threads_array->stopResourceReport();
}

bool Context::isResourceReportRegistered()
{
    if (getServerType() != ServerType::cnch_worker)
        return false;
    return shared->cnch_bg_threads_array->isResourceReportRegistered();
}

CnchBGThreadPtr Context::tryGetDedupWorkerManager(const StorageID & storage_id) const
{
    return tryGetCnchBGThread(CnchBGThreadType::DedupWorker, storage_id);
}

std::multimap<StorageID, MergeTreeMutationStatus> Context::collectMutationStatusesByTables(std::unordered_set<UUID> table_uuids) const
{
    /// If the query is for a specified table's mutation status,
    /// we need to ensure always return correct result, or throw exception when result is not available.
    bool throw_on_fail = table_uuids.size() == 1;

    std::multimap<StorageID, MergeTreeMutationStatus> res;

    auto threads = getCnchBGThreadsMap(CnchBGThreadType::MergeMutate)->getAll();

    for (const auto & [uuid, task]: threads)
    {
        if (!table_uuids.count(uuid))
            continue;

        if (task->getThreadStatus() == CnchBGThreadStatus::Stopped)
        {
            if (throw_on_fail)
                throw Exception("Table's MergeMutateThread is stopped. Please start it first.", ErrorCodes::CNCH_BG_THREAD_NOT_FOUND);
            continue;
        }

        try
        {
            auto * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(task.get());
            auto statuses = merge_mutate_thread->getAllMutationStatuses();
            for (auto & status : statuses)
                res.emplace(task->getStorageID(), status);

            table_uuids.erase(uuid);
        }
        catch (Exception & e)
        {
            // Can't get Table by uuid, table maybe already deleted.
            if (e.code() != ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                throw;
            }
            else
                LOG_DEBUG(shared->log, "Can't get Table by uuid, table maybe already deleted, skip it.");
        }
    }

    if (throw_on_fail && !table_uuids.empty())
        throw Exception("Table's MergeMutateThread is not found on the server. Please check table's host server first.", ErrorCodes::CNCH_BG_THREAD_NOT_FOUND);

    return res;
}

void Context::initCnchTransactionCoordinator()
{
    auto lock = getLock(); // checked
    shared->cnch_txn_coordinator = std::make_unique<TransactionCoordinatorRcCnch>(shared_from_this());
}

TransactionCoordinatorRcCnch & Context::getCnchTransactionCoordinator() const
{
    auto lock = getLock(); // checked
    if (auto * ptr = shared->cnch_txn_coordinator.get())
    {
        return *ptr;
    }
    throw Exception("CnchTransactionCoordinator is not initialized", ErrorCodes::SYSTEM_ERROR);
}

void Context::setCurrentTransaction(TransactionCnchPtr txn, bool finish_txn)
{
    TransactionCnchPtr prev_txn;
    {
        auto lock = getLocalSharedLock();
        prev_txn = current_cnch_txn;
    }

    if (prev_txn && finish_txn && getServerType() == ServerType::cnch_server)
        getCnchTransactionCoordinator().finishTransaction(prev_txn);

    if (current_thread && txn)
        CurrentThread::get().setTransactionId(txn->getTransactionID());

    auto lock = getLocalLock();
    current_cnch_txn = std::move(txn);
}

TransactionCnchPtr Context::setTemporaryTransaction(const TxnTimestamp & txn_id, const TxnTimestamp & primary_txn_id, bool with_check)
{
    TransactionCnchPtr cnch_txn;
    if (shared->server_type == ServerType::cnch_server)
    {
        std::optional<TransactionRecord> txn_record = with_check ? getCnchCatalog()->tryGetTransactionRecord((txn_id)) : std::nullopt;

        if (!txn_record)
        {
            txn_record = std::make_optional<TransactionRecord>();
            txn_record->setID(txn_id).setType(CnchTransactionType::Implicit).setStatus(CnchTransactionStatus::Running);
            txn_record->read_only = true;
        }

        cnch_txn = std::make_shared<CnchServerTransaction>(getGlobalContext(), std::move(*txn_record));
    }
    else
        cnch_txn = std::make_shared<CnchWorkerTransaction>(getGlobalContext(), txn_id, primary_txn_id);

    auto lock = getLocalLock();
    std::swap(current_cnch_txn, cnch_txn);
    return current_cnch_txn;
}

TransactionCnchPtr Context::getCurrentTransaction() const
{
    auto lock = getLocalSharedLock();
    return current_cnch_txn;
}

TxnTimestamp Context::tryGetCurrentTransactionID() const
{
    auto lock = getLocalSharedLock();
    return current_cnch_txn ? current_cnch_txn->getTransactionID() : TxnTimestamp{};
}

TxnTimestamp Context::getCurrentTransactionID() const
{
    auto lock = getLocalSharedLock();

    if (!current_cnch_txn)
        throw Exception("Transaction is not set (empty)", ErrorCodes::LOGICAL_ERROR);

    auto txn_id = current_cnch_txn->getTransactionID();
    if (0 == UInt64(txn_id))
        throw Exception("Transaction is not set (zero)", ErrorCodes::LOGICAL_ERROR);

    return txn_id;
}

TxnTimestamp Context::getCurrentCnchStartTime() const
{
    auto lock = getLocalSharedLock();
    if (!current_cnch_txn)
        throw Exception("Transaction is not set", ErrorCodes::LOGICAL_ERROR);
    return current_cnch_txn->getStartTime();
}

// In CNCH, form a virtual cluster which include all servers.
std::shared_ptr<Cluster> Context::mockCnchServersCluster() const
{
    // get CNCH servers by PSM
    String psm_name = this->getCnchServerClientPool().getServiceName();
    auto sd_client = this->getServiceDiscoveryClient();

    auto endpoints = sd_client->lookup(psm_name, ComponentType::SERVER);

    std::vector<Cluster::Addresses> addresses;

    auto user_password = getCnchInterserverCredentials();

    // create new cluster from scratch
    for (auto & e : endpoints)
    {
        Cluster::Address address(e.getTCPAddress(), user_password.first, user_password.second, this->getTCPPort(), false);
        // assume there are only one replica in each shard
        addresses.push_back({address});
    }

    // as CNCH server might be out-of-service for unknown reason, it is ok to skip it
    //auto local_settings = context.getSettings();
    //local_settings.skip_unavailable_shards = true;
    return std::make_shared<Cluster>(this->getSettings(), addresses, false);
}

std::vector<std::pair<UInt64, CnchWorkerResourcePtr>> Context::getAllWorkerResources() const
{
    if (!shared->named_cnch_sessions)
        return {};

    return shared->named_cnch_sessions->getAllWorkerResources();
}

Context::PartAllocator Context::getPartAllocationAlgo(MergeTreeSettingsPtr table_settings) const
{
    auto algorithm = table_settings->cnch_part_allocation_algorithm >= 0 ? table_settings->cnch_part_allocation_algorithm : settings.cnch_part_allocation_algorithm;
    LOG_DEBUG(shared->log, "Send query with cnch_part_allocation_algorithm = {}, system setting = {}, table setting = {}", algorithm, settings.cnch_part_allocation_algorithm, table_settings->cnch_part_allocation_algorithm);

    switch (algorithm)
    {
        case 0:
            return PartAllocator::JUMP_CONSISTENT_HASH;
        case 1:
            return PartAllocator::RING_CONSISTENT_HASH;
        case 2:
            return PartAllocator::STRICT_RING_CONSISTENT_HASH;
        case 3:
            return PartAllocator::DISK_CACHE_STEALING_DEBUG;
        default:
            return PartAllocator::JUMP_CONSISTENT_HASH;
    }
}

Context::HybridPartAllocator Context::getHybridPartAllocationAlgo() const
{
    switch (settings.cnch_hybrid_part_allocation_algorithm)
    {
        case 0: return HybridPartAllocator::HYBRID_MODULO_CONSISTENT_HASH;
        case 1: return HybridPartAllocator::HYBRID_RING_CONSISTENT_HASH;
        case 2: return HybridPartAllocator::HYBRID_BOUNDED_LOAD_CONSISTENT_HASH;
        case 3: return HybridPartAllocator::HYBRID_RING_CONSISTENT_HASH_ONE_STAGE;
        case 4: return HybridPartAllocator::HYBRID_STRICT_RING_CONSISTENT_HASH_ONE_STAGE;
        default: return HybridPartAllocator::HYBRID_BOUNDED_LOAD_CONSISTENT_HASH;
    }
}

bool Context::hasSessionTimeZone() const
{
    return !settings.session_timezone.value.empty();
}

void Context::createPlanNodeIdAllocator(int max_id)
{
    id_allocator = std::make_shared<PlanNodeIdAllocator>(max_id);
}

void Context::createSymbolAllocator()
{
    symbol_allocator = std::make_shared<SymbolAllocator>();
}

void Context::createOptimizerMetrics()
{
    optimizer_metrics = std::make_shared<OptimizerMetrics>();
}

std::shared_ptr<Statistics::StatisticsMemoryStore> Context::getStatisticsMemoryStore()
{
    auto lock = getLocalLock();
    if (!this->stats_memory_store)
    {
        this->stats_memory_store = std::make_shared<Statistics::StatisticsMemoryStore>();
    }
    return stats_memory_store;
}

String Context::getDefaultCnchPolicyName() const
{
    return getConfigRef().getString("storage_configuration.cnch_default_policy", "cnch_default_hdfs");
}

String Context::getOptimizerProfile(bool print_rule)
{
    if (optimizer_profile)
    {
        String profile = optimizer_profile->getOptimizerProfile(print_rule);
        clearOptimizerProfile();
        return profile;
    }
    else
        throw Exception("OptimizerProfile is not initialized", ErrorCodes::LOGICAL_ERROR);
}

void Context::clearOptimizerProfile()
{
    if (!optimizer_profile)
        return;
    optimizer_profile->clear();
    optimizer_profile = nullptr;
}

void Context::logOptimizerProfile(LoggerPtr log, String prefix, String name, String time, bool is_rule)
{
    if (settings.log_optimizer_run_time && log)
        LOG_DEBUG(log, prefix + name + " " + time);

    if (optimizer_profile)
        optimizer_profile->setTime(name, time, is_rule);
}

String Context::getCnchAuxilityPolicyName() const
{
    return getConfigRef().getString("storage_configuration.cnch_auxility_policy", "default");
}

bool Context::isAsyncMode() const
{
    return getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY && getServerType() == ServerType::cnch_server
        && getSettingsRef().enable_async_execution;
}

void Context::markReadFromClientFinished()
{
    {
        std::lock_guard lk(shared->read_mutex);
        read_from_client_finished = true;
    }
    shared->read_cv.notify_all();
}

void Context::waitReadFromClientFinished() const
{
    int64_t timeout = getSettingsRef().receive_timeout.totalMilliseconds();
    std::unique_lock lk(shared->read_mutex);
    if (!shared->read_cv.wait_for(lk, std::chrono::milliseconds(timeout), [this] { return read_from_client_finished; }))
        throw Exception("Timeout exceeded while reading data from client.", ErrorCodes::TIMEOUT_EXCEEDED);
}

void Context::setAutoStatisticsManager(std::unique_ptr<Statistics::AutoStats::AutoStatisticsManager> && manager)
{
    auto lock = getLock();//ok
    shared->auto_stats_manager = std::move(manager);
}

Statistics::AutoStats::AutoStatisticsManager * Context::getAutoStatisticsManager() const
{
    auto lock = getLock();//ok
    return shared->auto_stats_manager.get();
}

void Context::setPlanCacheManager(std::unique_ptr<PlanCacheManager> && manager)
{
    auto lock = getLock(); // checked
    shared->plan_cache_manager = std::move(manager);
}

PlanCacheManager* Context::getPlanCacheManager()
{
    auto lock = getLock(); // checked
    return shared->plan_cache_manager ? shared->plan_cache_manager.get() : nullptr;
}

bool Context::trySetRunningBackupTask(const String & backup_id)
{
    auto lock = getLock();
    if (!shared->running_backup_task)
    {
        shared->running_backup_task = backup_id;
        return true;
    }
    else if (shared->running_backup_task != backup_id)
    {
        LOG_INFO(
            shared->log,
            "Cannot create new backup task {} because there is one running backup task {}",
            backup_id,
            shared->running_backup_task.value());
        return false;
    }
    return true;
}

bool Context::hasRunningBackupTask() const
{
    auto lock = getLock();
    return shared->running_backup_task.has_value();
}

std::optional<String> Context::getRunningBackupTask() const
{
    auto lock = getLock();
    return shared->running_backup_task;
}

bool Context::checkRunningBackupTask(const String & backup_id) const
{
    auto lock = getLock();
    return shared->running_backup_task && shared->running_backup_task == backup_id;
}

void Context::removeRunningBackupTask(const String & backup_id)
{
    auto lock = getLock();
    if (!shared->running_backup_task || shared->running_backup_task != backup_id)
    {
        LOG_WARNING(shared->log, "Cannot remove backup task {}, which is not running in current server", backup_id);
        return;
    }
    shared->running_backup_task.reset();
}

void Context::setPreparedStatementManager(std::unique_ptr<PreparedStatementManager> && manager)
{
    auto lock = getLock(); // checked
    shared->prepared_statement_manager = std::move(manager);
}

PreparedStatementManager * Context::getPreparedStatementManager()
{
    auto lock = getLock(); // checked
    return shared->prepared_statement_manager ? shared->prepared_statement_manager.get() : nullptr;
}

UInt32 Context::getQueryMaxExecutionTime() const
{
    // max is 4294967295/1000/60=71582 min
    if (getSettingsRef().max_execution_time.totalSeconds() != 0)
        return std::min(getSettingsRef().max_execution_time.totalSeconds() * UInt64(1000), UInt64(UINT32_MAX));
    else if (getSettingsRef().exchange_timeout_ms != 0)
        return std::min(UInt64(getSettingsRef().exchange_timeout_ms), UInt64(UINT32_MAX));
    else
        return 100 * 60 * 1000; // default as 100min
}

timespec Context::getQueryExpirationTimeStamp() const
{
    if (!query_expiration_timestamp)
        throw Exception("query_expiration_timestamp has not set.", ErrorCodes::LOGICAL_ERROR);
    return query_expiration_timestamp.value();
}

void Context::initQueryExpirationTimeStamp()
{
    auto initial_query_start_time_ms = client_info.initial_query_start_time_microseconds / 1000;
    // Internal queries are those executed without an independent client context,
    // thus should not set initial_query_start_time, because it might introduce data race.
    if (initial_query_start_time_ms == 0)
        initial_query_start_time_ms = time_in_milliseconds(std::chrono::system_clock::now());

    UInt64 query_expiration_ms = initial_query_start_time_ms + getQueryMaxExecutionTime();
    query_expiration_timestamp = {.tv_sec = time_t(query_expiration_ms / 1000), .tv_nsec = long((query_expiration_ms % 1000) * 1000000)};
}

AsynchronousReaderPtr Context::getThreadPoolReader() const
{
    callOnce(shared->readers_initialized, [&] {
        const Poco::Util::AbstractConfiguration & config = getConfigRef();
        auto pool_size = config.getUInt(".threadpool_remote_fs_reader_pool_size", 250);
        auto queue_size = config.getUInt(".threadpool_remote_fs_reader_queue_size", 1000000);
        shared->asynchronous_remote_fs_reader = std::make_shared<ThreadPoolRemoteFSReader>(pool_size, queue_size);
    });
    return shared->asynchronous_remote_fs_reader;
}

#if USE_LIBURING
IOUringReader & Context::getIOUringReader() const
{
    std::call_once(shared->io_uring_reader_initialized, [&] {
        const Poco::Util::AbstractConfiguration & config = getConfigRef();
        auto num_iouring_readers = config.getUInt(".iouring_reader_num", 32);
        auto num_sq_entries = config.getUInt(".iouring_reader_sq_entry_num", 512);
        shared->io_uring_reader.resize(num_iouring_readers);
        for (auto &i : shared->io_uring_reader)
            i = createIOUringReader(num_sq_entries);
    });

    auto &reader = shared->io_uring_reader[getThreadId() % shared->io_uring_reader.size()];
    if (!reader)
        throw Exception("Try to get a null IOUringReader.", ErrorCodes::SYSTEM_ERROR);
    return *reader;
}
#endif

void Context::initNexusFS(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock(); // checked

    String config_name(NexusFSConfig::CONFIG_NAME);
    bool enable = config.getBool(config_name + ".enable", false);
    String policy_name = config.getString(config_name + ".policy", "default");
    String volume_name = config.getString(config_name + ".volume", "local");

    if (!enable)
    {
        shared->nexus_fs.reset();
        return;
    }
    if (shared->nexus_fs)
        throw Exception("NexusFS has been already initialized.", ErrorCodes::LOGICAL_ERROR);

    NexusFSConfig conf;
    auto disks = getStoragePolicy(policy_name)->getVolumeByName(volume_name, true)->getDisks();
    for  (auto & disk : disks)
    {
        chassert(disk->getType() == DiskType::Type::Local);
        conf.file_paths.push_back(std::filesystem::path(disk->getPath()) / NexusFSConfig::FILE_NAME);
    }
    conf.loadFromConfig(config);

    shared->nexus_fs = std::make_unique<NexusFS>(std::move(conf));

    if (shared->nexus_fs)
    {
        if (!shared->nexus_fs->recover())
            LOG_WARNING(&Poco::Logger::get("NexusFS"), "No recovery data found. Setup with clean cache.");
    }
    else
        LOG_ERROR(shared->log, "Fail to initialize NexusFS.");
}

NexusFSPtr Context::getNexusFS() const
{
    return shared->nexus_fs;
}

}
