#pragma once

#include <Interpreters/Cluster.h>
#include <Interpreters/PartLog.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/DataPartsExchange.h>
#include <Storages/MergeTree/HaMergeTreeAddress.h>
#include <Storages/MergeTree/HaMergeTreeLogExchanger.h>
#include <Storages/MergeTree/HaUniqueMergeTreeAlterThread.h>
#include <Storages/MergeTree/HaUniqueMergeTreeCleanupThread.h>
#include <Storages/MergeTree/HaUniqueMergeTreeLogExchanger.h>
#include <Storages/MergeTree/HaUniqueMergeTreeRestartingThread.h>
#include <Storages/MergeTree/LSNStatus.h>
#include <Storages/MergeTree/LeaderElection.h>
#include <Storages/MergeTree/MergeScheduler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class HaReplicaEndpointHolder;

/// Things that's different from StorageHaMergeTree
/// - log entries are replayed in-order serially
/// - don't support re-sharding
/// - don't support HDFS storage

class StorageHaUniqueMergeTree : public shared_ptr_helper<StorageHaUniqueMergeTree>, public MergeTreeData
{
    friend struct shared_ptr_helper<StorageHaUniqueMergeTree>;
public:
    void startup() override;
    void shutdown() override;
    ~StorageHaUniqueMergeTree() override;

    String getName() const override { return "HaUniqueMergeTree"; }

    bool supportsReplication() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr query_context,
        QueryProcessingStage::Enum processed_stage,
        const size_t max_block_size,
        const unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    bool optimize(const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        ContextPtr query_context) override;

    void alter(
        const AlterCommands & commands,
        ContextPtr local_context,
        TableLockHolder & table_lock_holder,
        const ASTPtr & query) override;

    void removeZKNodes(bool & last_replica);
    void drop() override;

    // ReplicatedMergeTreeAddress getHaUniqueMergeTreeAddress() const;
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;
    ActionLock getActionLock(StorageActionBlockType action_type) override;
    bool createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot);
    void createReplica(const StorageMetadataPtr & metadata_snapshot);
    time_t getAbsoluteDelay(const String & pred = "") const;

    /// Start to repair the replica by cloning logs and data from other replica.
    /// Pre-condition: is_lost == 1 and unique_table_state is REPAIR_MANIFEST
    /// Post-condition: is_lost == 1 and unique_table_state may be one of
    /// - REPAIR_MANIFEST : fail to make any progress, caller should try again later
    /// - REPAIR_DATA : manifest is repaired but data is under-repair
    /// - REPAIRED : manifest and data have been repaired
    void enterRepairMode();

    /// Return "" if current leader is unknown or not active.
    String activeLeaderReplicaName() const;

    /// Return all manifest logs and set `committed_version'. Used to implement unique_logs system table.
    ManifestStore::LogEntries getManifestLogs(UInt64 & committed_version);

    struct Status
    {
        bool is_leader;
        bool is_readonly;
        bool is_session_expired;
        String status;
        String cached_leader;
        String zookeeper_path;
        String replica_name;
        String replica_path;
        UInt64 absolute_delay;
        UInt64 checkpoint_lsn;
        UInt64 commit_lsn;
        UInt64 latest_lsn;
        UInt64 cluster_commit_lsn;
        /// read latest value from ZK
        UInt64 cluster_latest_lsn;
        UInt8 total_replicas;
        UInt8 active_replicas;
    };

    /// Get the status of the table. If with_zk_fields = false - do not fill in the fields that require queries to ZK.
    void getStatus(Status & res, bool with_zk_fields = true);

    bool getThrowIfAttachTableLevelPartition() const { return throw_attach_table_level_partition; }
    void setThrowIfAttachTableLevelPartition(bool val) { throw_attach_table_level_partition = val; }

    bool waitForLogSynced(UInt64 max_wait_milliseconds);

protected:
    StorageHaUniqueMergeTree(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);

    String getZKLeaderPath() const { return zookeeper_path + "/leader_election"; }
    String getZKLatestLSNPath() const { return zookeeper_path + "/latest_lsn"; }

    /// Allow to verify that the session in ZooKeeper is still alive.
    void checkSessionIsNotExpired(zkutil::ZooKeeperPtr & zookeeper);
    UInt64  allocateBlockNumberDirect(zkutil::ZooKeeperPtr & zookeeper);
    std::pair<UInt64, Coordination::RequestPtr> allocLSNAndMakeSetRequest(zkutil::ZooKeeperPtr & zookeeper);
    UInt64 allocLSNAndSet(zkutil::ZooKeeperPtr & zookeeper);

    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;

    bool verifyTableStructureAlterIfNeed();
    bool verifyTableStructureAlterIfNeed(const String& metadata, const String& columns);
    bool verifyTableStructureForVersionAlterIfNeed(UInt64 version);
    void alterTableSchema(const String& metadata, const String& columns, TableLockHolder & lock);

private:
    friend class HaUniqueMergeTreeBlockOutputStream;
    friend class HaUniqueMergeTreeCleanupThread;
    friend class HaUniqueMergeTreeReplicaEndpoint;
    friend class HaUniqueMergeTreeRestartingThread;
    friend class HaUniqueMergeTreeAlterThread;
    friend struct MergingResourceHolder;

    /// NOTE: don't acquire this lock in leader-only tasks because it would deadlock:
    /// 1. exitLeaderElection acquired the lock and wait for leader-only task to exit
    /// 2. leader-only task tried to acquire the lock which would block forever
    using ElectionLock = std::unique_lock<std::mutex>;
    ElectionLock electionLock() const { return ElectionLock(election_mutex); }
    void enterElectionUnlocked(const ElectionLock &);
    void exitElectionUnlocked(const ElectionLock &);
    void enterLeaderElection()
    {
        auto lock = electionLock();
        enterElectionUnlocked(lock);
    }
    void exitLeaderElection()
    {
        auto lock = electionLock();
        exitElectionUnlocked(lock);
    }

    /// Update cached leader information if time since last update exceeds threshold.
    /// Return cached leader replica name.
    String refreshLeaderIfNeeded(const zkutil::ZooKeeperPtr & zookeeper);
    /// Return cached leader replica name (could be empty when unknown).
    /// Also set *addr to leader address if addr is not null.
    String getCachedLeader(HaMergeTreeAddress * addr = nullptr) const;
    /// save local commit version to zk
    void saveCommitVersionToZkIfNeeded(const zkutil::ZooKeeperPtr & zookeeper);
    /// fetch commit versions of all replicas and update `unique_commit_version'
    void refreshClusterCommitVersion(const zkutil::ZooKeeperPtr & zookeeper);

    void becomeLeaderTask();
    void updateLogTask();
    void replayLogTask();
    void checkpointLogTask();
    void repairDataTask();
    void alterThread();
    /// Check some constrains for partial update feature
    void checkPartialUpdateConstraint();

    void executeSyncParts(const ManifestLogEntry & entry);
    void executeDeatchParts(const ManifestLogEntry & entry);
    void executeResetManifest(const ManifestLogEntry & entry);
    void executeCheckpointManifest(const ManifestLogEntry & entry);
    void executeModifySchemaManifest(const ManifestLogEntry & entry);

    String chooseReplicaToFetch(UInt64 version);

    /// If part_name is empty, it means that the corresponding operation just deletes some rows with the help of _delete_flag_ column.
    void fetchAndLoadData(
        const String & replica,
        const String & part_name, const Strings & updated_part_names, UInt64 delete_version,
        const ThrottlerPtr & throttler);

    using MergeSelectLock = std::unique_lock<std::mutex>;
    MergeSelectLock mergeSelectLock(bool acquire = true) const
    {
        if (acquire)
            return MergeSelectLock(merge_selecting_mutex);
        return MergeSelectLock(merge_selecting_mutex, std::defer_lock);
    }


    // void TTLWorker(MergeSelectLock & lock, zkutil::ZooKeeperPtr & zookeeper);
    bool doMerge(bool aggressive, const String & partition_id, bool final, bool enable_try);

    /// Add `lock` as a arugment to enforce the caller to hold the unique write lock before dropping any parts.
    void leaderDropParts(zkutil::ZooKeeperPtr & zookeeper, const DataPartsVector & parts_to_drop, const UniqueWriteLock & lock);
    void leaderDeatchParts(zkutil::ZooKeeperPtr & zookeeper, const DataPartsVector & parts_to_drop, const UniqueWriteLock & lock);


    std::optional<Cluster::Address> findClusterAddress(const HaMergeTreeAddress & target) const;
    bool sendRequestToLeaderReplica(const ASTPtr & query, ContextPtr context);

    bool isSelfLost();
    void markSelfLost();
    void markSelfNonLost();

    HaMergeTreeAddress getHaUniqueMergeTreeAddress() const;

    /// Do I need to complete background threads (except restarting_thread)?
    std::atomic<bool> partial_shutdown_called {false};
    /// Event that is signalled (and is reset) by the restarting_thread when the ZooKeeper session expires.
    Poco::Event partial_shutdown_event {false};     /// Poco::Event::EVENT_MANUALRESET

    void saveRepairVersionToZk(UInt64 repair_version);
    void removeRepairVersionFromZk();

    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context, const ASTPtr & query) override;
    void dropPartitionWhere(const ASTPtr & predicate, bool detach, ContextPtr context, const ASTPtr & query) override;

    PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr query_context) override;
    void movePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, ContextPtr query_context) override;
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    template <class Func>
    void foreachCommittedParts(Func && func) const;
    std::optional<UInt64> totalRows(const Settings & /*settings*/) const override;
    std::optional<UInt64> totalBytes(const Settings & /*settings*/) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr query_context) const override;

    /// link: https://clickhouse.com/docs/en/sql-reference/statements/alter/partition/
    void replacePartitionFrom(const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*query_context*/) override
    {
        /// Copies the data partition from one table to another and replaces
        throw Exception("Method replacePartitionFrom is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    void movePartitionToTable(const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*query_context*/) override
    {
        /// Moves the data partition from one table to another.
        throw Exception("Method movePartitionToTable is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    void fetchPartition(const ASTPtr & /*partition*/, const StorageMetadataPtr & /*metadata_snapshot*/, const String & /*from*/, bool /*fetch_part*/, ContextPtr /*query_context*/) override
    {
        ///  Downloads a part or partition from another server.
        throw Exception("Method fetchPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    void startBackgroundMovesIfNeeded() override
    {
        /// Not needed for now.
        throw Exception("Method startBackgroundMovesIfNeeded is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    bool scheduleDataProcessingJob(IBackgroundJobExecutor & executor) override;

    /// FIXME (UNIQUEY KEY): Implement drop part later
    void dropPart(const String & /*part_name*/, bool /*detach*/, ContextPtr /*query_context*/) override
    {
        throw Exception("not supported", ErrorCodes::NOT_IMPLEMENTED);
    }
    void dropPartNoWaitNoThrow(const String & /*part_name*/) override
    {
        throw Exception("Method dropPartNoWaitNoThrow is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    // mutation is not supported yet
    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & /*part*/) const override
    {
        return {};
    }
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override
    {
        throw Exception("Method getMutationsStatus is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

public:
    void assertNotReadonly() const;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper();

    /// If true, the table is offline and can not be written to it.
    std::atomic_bool is_readonly {false};
    /// If false - ZooKeeper is available, but there is no table metadata. It's safe to drop table in this case.
    bool has_metadata_in_zookeeper = true;

    bool isLeader() const { return is_leader.load(std::memory_order_relaxed); }
    String zookeeperPath() const { return zookeeper_path; }
    String replicaName() const { return replica_name; }
    String replicaPath() const { return replica_path; }
    HaMergeTreeAddress getReplicaAddress(const String & replica_name_);

    static void dropReplica(zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica, Poco::Logger * logger);

    static bool removeTableNodesFromZooKeeper(
        zkutil::ZooKeeperPtr zookeeper,
        const String & zookeeper_path,
        const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock,
        Poco::Logger * logger);

    bool checkFixedGranularityInZookeeper();

    String zookeeper_name;
    String zookeeper_path;
    String replica_name;
    String replica_path;

    /// True if replica was created for existing table with fixed granularity
    bool other_replicas_fixed_granularity = false;

    int metadata_version = 0;

    /** /replicas/me/is_active.
      */
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;

    /** Is this replica "leading". The leader replica selects the parts to merge.
      * It can be false only when old ClickHouse versions are working on the same cluster, because now we allow multiple leaders.
      */
    std::atomic<bool> is_leader {false};
    zkutil::LeaderElectionPtr leader_election;

    InterserverIOEndpointPtr data_parts_exchange_endpoint;
    InterserverIOEndpointHolderPtr data_parts_exchange_endpoint_holder;

    void setTableStructure(
    ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff);

private:
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;
    /// NOTE: background_executor shares pool between many storages, even when we defined a private variable here.
    /// see `BackgroundJobsExecutor.cpp` for more detail
    UniqueTableBackgroundJobsExecutor background_executor;

    HaUniqueMergeTreeLogExchanger log_exchanger;

    DataPartsExchange::Fetcher fetcher;

    /// When activated, replica is initialized and startup() method could exit
    Poco::Event startup_event;

    /// protect is_leader, ensure serial execution of enter/exitLeaderElection and becomeLeaderTask
    mutable std::mutex election_mutex;

    /// It is acquired for each iteration of the selection of parts to merge
    /// or each OPTIMIZE query.
    mutable std::mutex merge_selecting_mutex;

    mutable std::mutex term_mutex;
    /// increased each time leader status is changed,
    /// used as the logical time to order election event
    UInt64 election_term {0};
    UInt64 last_term_of_become_leader {0};
    UInt64 last_term_of_exit_leader {0};
    time_t become_leader_start_time {0};
    UInt64 become_leader_at_version {0};

    /// cache the value of /leader_election
    mutable std::mutex refresh_leader_mutex;
    String leader_replica;
    HaMergeTreeAddress leader_address;
    time_t leader_replica_last_refresh_time {0};

    /// used by saveCommitVersionToZkIfNeeded
    UInt64 last_save_commit_version {0};
    time_t last_save_commit_version_time {0};

    /// last time when successfully sync logs with leader
    std::atomic<time_t> last_update_log_time {0};

    /// should be hold before manifest lock
    mutable std::mutex replay_log_mutex;
    /// execute_reset_manifest[i] = whether we need to execute the reset manifest log at version i
    std::map<UInt64, bool> execute_reset_manifest;

    BackgroundSchedulePool::TaskHolder repair_data_task;
    /// non-leader tasks
    BackgroundSchedulePool::TaskHolder become_leader_task;
    BackgroundSchedulePool::TaskHolder update_log_task;
    BackgroundSchedulePool::TaskHolder replay_log_task;
    /// leader-only tasks
    /// will be started when we become leader and stopped when we exit leader (including storage shutdown)
    BackgroundSchedulePool::TaskHolder checkpoint_log_task;

    /// A thread that alters parts asynchronously.
    HaUniqueMergeTreeAlterThread alter_thread;

    /// Every part has a mutation version to indicate the least mutation it applied,
    ///     for a part have never had mutation before, its mutation is min_block;
    /// .   else its muation is the leasted mutaion version appied to it
    ///
    /// This value is the maxium mutation vaules for all parts.
    SimpleIncrement increment;

    using MutationLock = std::unique_lock<std::mutex>;
    MutationLock mutationLock() const { return MutationLock(mutation_mutex); }
    mutable std::mutex mutation_mutex;
    std::map<String, MergeTreeMutationEntry> current_mutations_by_id;
    std::multimap<Int64, MergeTreeMutationEntry &> current_mutations_by_version;

    struct LogExecutingInfo
    {
        UInt64 version;
        UInt32 num_failed;
        UInt64 retry_backoff_ms; /// meaningful only when num_failed > 0
    };
    LogExecutingInfo last_executing_log {}; /// zero-initialize

    std::shared_ptr<HaReplicaEndpointHolder> replica_endpoint_holder;

    /// A thread that removes old parts, delete files, temp directories and obsolete znodes.
    HaUniqueMergeTreeCleanupThread cleanup_thread;

    /// A thread that processes reconnection to ZooKeeper when the session expires.
    HaUniqueMergeTreeRestartingThread restarting_thread;

    /// Protected by MergeSelectLock
    time_t last_clear_empty_part_time {0};
    DataParts parts_under_merge;

    std::atomic<bool> shutdown_called {false};

    /// Protected by UniqueWriteLock
    /// All input parts of a merge task share the same merge state

    std::map<DataPartPtr, UniqueMergeStatePtr, MergeTreeData::LessDataPart> running_merge_states;

    struct RepairState
    {
        UInt64 version;
        UInt64 checkpoint_version = 0;
        String leader;
        time_t start_time;
        std::vector<std::pair<DataPartPtr, UInt64>> delete_files_to_fetch;
        std::vector<std::pair<String, UInt64>> parts_to_fetch;
        UInt32 total;
        UInt32 num_repaired = 0;
        UInt32 num_failed = 0;
        UInt64 retry_backoff_ms = 0; /// meaningful only when num_failed > 0
    };
    using RepairStatePtr = std::unique_ptr<RepairState>;
    RepairStatePtr current_repair_state;

    /// Whether throw if we are trying to attach partition for a table level uniqueness storage.
    bool throw_attach_table_level_partition{true};

    static constexpr auto default_zookeeper_name = "default";
};

}

