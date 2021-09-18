#pragma once
#include <Storages/MergeTree/MergeTreeData.h>

#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/DataPartsExchange.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <Storages/MergeTree/HaMergeTreeLogManager.h>
#include <Storages/MergeTree/HaMergeTreeLogExchanger.h>
#include <Storages/MergeTree/HaMergeTreeQueue.h>
#include <Storages/MergeTree/HaMergeTreeRestartingThread.h>
#include <Storages/MergeTree/LeaderElection.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/MergeTree/HaMergeTreeAddress.h>

/// XXX
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMergeStrategyPicker.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>

namespace DB
{

using HaMergeTreeQuorumAddedParts = ReplicatedMergeTreeQuorumAddedParts;
using HaMergeTreeTableMetadata = ReplicatedMergeTreeTableMetadata;

class StorageHaMergeTree final : public ext::shared_ptr_helper<StorageHaMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageHaMergeTree>;

public:
    void startup() override;
    void shutdown() override;
    ~StorageHaMergeTree() override;

    std::string getName() const override { return "Ha" + merging_params.getModeName() + "MergeTree"; }

    bool supportsParallelInsert() const override { return true; }
    bool supportsReplication() const override { return true; }
    bool supportsDeduplication() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, const Context & context) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        const Context & query_context) override;

    void alter(const AlterCommands & commands, const Context & query_context, TableLockHolder & table_lock_holder) override;

    void mutate(const MutationCommands & commands, const Context & context) override;
    void waitMutation(const String & znode_name, size_t mutations_sync) const;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    CancellationCode killMutation(const String & mutation_id) override;

    /** Removes a replica from ZooKeeper. If there are no other replicas, it deletes the entire table from ZooKeeper.
      */
    void drop() override;

    void dropLogEntries();

    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context & query_context, TableExclusiveLockHolder &) override;

    void checkTableCanBeRenamed() const override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    bool supportsIndexForIn() const override { return true; }

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    /// Wait when replication queue size becomes less or equal than queue_size
    /// If timeout is exceeded returns false
    bool waitForShrinkingQueueSize(size_t queue_size = 0, UInt64 max_wait_milliseconds = 0);

    /** For the system table replicas. */
    struct Status
    {
        bool is_leader;
        bool can_become_leader;
        bool is_readonly;
        bool is_session_expired;
        HaMergeTreeQueue::Status queue;
        UInt32 parts_to_check;
        String zookeeper_path;
        String replica_name;
        String replica_path;
        Int32 columns_version;
        UInt64 log_max_index;
        UInt64 log_pointer;
        UInt64 absolute_delay;
        UInt8 total_replicas;
        UInt8 active_replicas;
        /// If the error has happened fetching the info from ZooKeeper, this field will be set.
        String zookeeper_exception;
    };

    /// Get the status of the table. If with_zk_fields = false - do not fill in the fields that require queries to ZK.
    void getStatus(Status & res, bool with_zk_fields = true);

    using LogEntriesData = std::vector<HaMergeTreeLogEntryData>;
    void getQueue(LogEntriesData & res, String & replica_name);

    /// Get replica delay relative to current time.
    time_t getAbsoluteDelay() const;

    /// If the absolute delay is greater than min_relative_delay_to_measure,
    /// will also calculate the difference from the unprocessed time of the best replica.
    /// NOTE: Will communicate to ZooKeeper to calculate relative delay.
    void getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay);

    /// Add a part to the queue of parts whose data you want to check in the background thread.
    void enqueuePartForCheck([[maybe_unused]] const String & part_name, [[maybe_unused]] time_t delay_to_check_seconds = 0)
    {
        /// TODO:
    }

    CheckResults checkData(const ASTPtr & query, const Context & context) override;

    /// Checks ability to use granularity
    bool canUseAdaptiveGranularity() const override;

    int getMetadataVersion() const { return metadata_version; }

    /** Remove a specific replica from zookeeper.
     */
    static void dropReplica(zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica, Poco::Logger * logger);

    /// Removes table from ZooKeeper after the last replica was dropped
    static bool removeTableNodesFromZooKeeper(
        zkutil::ZooKeeperPtr zookeeper,
        const String & zookeeper_path,
        const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock,
        Poco::Logger * logger);

    /// Get job to execute in background pool (merge, mutate, drop range and so on)
    std::optional<JobAndPool> getDataProcessingJob() override;

private:
    /// Get a sequential consistent view of current parts.
    ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock getMaxAddedBlocks() const;

    friend class HaMergeTreeQueue;
    friend class HaMergeTreeRestartingThread;
    friend class HaMergeTreeBlockOutputStream;
    friend class HaMergeTreePartCheckThread;
    friend class HaMergeTreeCleanupThread;
    friend class HaMergeTreeReplicaEndpoint;
    friend class HaMergeTreeLogExchangerBase;
    friend class HaMergeTreeLogExchanger;
    friend class MergeTreeData;

    using LogEntry = HaMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;

    /// using MergeStrategyPicker = ReplicatedMergeTreeMergeStrategyPicker;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper();

    /// If true, the table is offline and can not be written to it.
    std::atomic_bool is_readonly {false};
    /// If false - ZooKeeper is available, but there is no table metadata. It's safe to drop table in this case.
    bool has_metadata_in_zookeeper = true;

    static constexpr auto default_zookeeper_name = "default";
    String zookeeper_name;
    String zookeeper_path;
    String replica_name;
    String replica_path;

    /** /replicas/me/is_active.
      */
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;

    /** Is this replica "leading". The leader replica selects the parts to merge.
      * It can be false only when old ClickHouse versions are working on the same cluster, because now we allow multiple leaders.
      */
    std::atomic<bool> is_leader {false};
    zkutil::LeaderElectionPtr leader_election;

    InterserverIOEndpointPtr data_parts_exchange_endpoint;

    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;

    // MergeStrategyPicker merge_strategy_picker;

    std::unique_ptr<HaMergeTreeLogManager> log_manager;
    HaMergeTreeLogExchanger log_exchanger;

    /** The queue of what needs to be done on this replica to catch up with everyone. It is taken from ZooKeeper (/replicas/me/queue/).
     * In ZK entries in chronological order. Here it is not necessary.
     */
    HaMergeTreeQueue queue;
    std::atomic<time_t> last_queue_update_start_time{0};
    std::atomic<time_t> last_queue_update_finish_time{0};

    DataPartsExchange::Fetcher fetcher;


    /// When activated, replica is initialized and startup() method could exit
    Poco::Event startup_event;

    /// Do I need to complete background threads (except restarting_thread)?
    std::atomic<bool> partial_shutdown_called {false};

    /// Event that is signalled (and is reset) by the restarting_thread when the ZooKeeper session expires.
    Poco::Event partial_shutdown_event {false};     /// Poco::Event::EVENT_MANUALRESET

    /// Limiting parallel fetches per one table
    std::atomic_uint current_table_fetches {0};

    int metadata_version = 0;
    /// Threads.

    BackgroundJobsExecutor background_executor;
    BackgroundMovesExecutor background_moves_executor;

    /// A task that keeps track of the updates in the logs of all replicas and loads them into the queue.
    bool queue_update_in_progress = false;
    BackgroundSchedulePool::TaskHolder queue_updating_task;

    BackgroundSchedulePool::TaskHolder mutations_updating_task;

    /// A task that selects parts to merge.
    BackgroundSchedulePool::TaskHolder merge_selecting_task;
    /// It is acquired for each iteration of the selection of parts to merge or each OPTIMIZE query.
    std::mutex merge_selecting_mutex;

    /// A task that marks finished mutations as done.
    BackgroundSchedulePool::TaskHolder mutations_finalizing_task;

    /// A thread that processes reconnection to ZooKeeper when the session expires.
    HaMergeTreeRestartingThread restarting_thread;

    /// True if replica was created for existing table with fixed granularity
    bool other_replicas_fixed_granularity = false;

    /// Do not allow RENAME TABLE if zookeeper_path contains {database} or {table} macro
    const bool allow_renaming;

    const size_t replicated_fetches_pool_size;

    template <class Func>
    void foreachCommittedParts(Func && func, bool select_sequential_consistency) const;

    /** Creates the minimum set of nodes in ZooKeeper and create first replica.
      * Returns true if was created, false if exists.
      */
    bool createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot);

    /** Creates a replica in ZooKeeper and adds to the queue all that it takes to catch up with the rest of the replicas.
      */
    void createReplica(const StorageMetadataPtr & metadata_snapshot);

    /** Create nodes in the ZK, which must always be, but which might not exist when older versions of the server are running.
      */
    void createNewZooKeeperNodes();

    void checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot);

    /// A part of ALTER: apply metadata changes only (data parts are altered separately).
    /// Must be called under IStorage::lockForAlter() lock.
    void setTableStructure(ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff);

    /** Check that the set of parts corresponds to that in ZK (/replicas/me/parts/).
      * If any parts described in ZK are not locally, throw an exception.
      * If any local parts are not mentioned in ZK, remove them.
      *  But if there are too many, throw an exception just in case - it's probably a configuration error.
      */
    void checkParts(bool skip_sanity_checks);

    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;

    /** Updates the queue.
      */
    void queueUpdatingTask();

    void mutationsUpdatingTask();

    /// Postcondition:
    /// either leader_election is fully initialized (node in ZK is created and the watching thread is launched)
    /// or an exception is thrown and leader_election is destroyed.
    void enterLeaderElection();

    /// Postcondition:
    /// is_leader is false, merge_selecting_thread is stopped, leader_election is nullptr.
    /// leader_election node in ZK is either deleted, or the session is marked expired.
    void exitLeaderElection();

    /** Selects the parts to merge and writes to the log.
      */
    void mergeSelectingTask();

    /// Checks if some mutations are done and marks them as done.
    void mutationsFinalizingTask();

    /// Throw an exception if the table is readonly.
    void assertNotReadonly() const;

    /// Info about how other replicas can access this one.
    HaMergeTreeAddress getHaMergeTreeAddress() const;

    // Partition helpers
    void dropPartition(const ASTPtr & partition, bool detach, bool drop_part, const Context & query_context, bool throw_if_noop) override;
    PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, const Context & query_context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & query_context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & query_context) override;
    void fetchPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, const String & from, const Context & query_context) override;

    /// Check granularity of already existing replicated table in zookeeper if it exists
    /// return true if it's fixed
    bool checkFixedGranualrityInZookeeper();

    MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    void startBackgroundMovesIfNeeded() override;


    std::pair<UInt64, Coordination::RequestPtr> allocLSNAndMakeSetRequest();
    UInt64 allocateBlockNumberDirect(zkutil::ZooKeeperPtr & zookeeper, const String & zookeeper_block_id_path = "");

    String getZKLatestLSNPath() const { return zookeeper_path + "/latest_lsn"; }
    String getZKCommittedLSNPath() const { return zookeeper_path + "/committed_lsn"; }
    String getZKReplicaUpdatedLSNPath() const { return replica_path + "/updated_lsn"; }

protected:
    /** If not 'attach', either creates a new table in ZK, or adds a replica to an existing table.
      */
    StorageHaMergeTree(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        Context & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag,
        bool allow_renaming_);
};


/** There are three places for each part, where it should be
  * 1. In the RAM, data_parts, all_data_parts.
  * 2. In the filesystem (FS), the directory with the data of the table.
  * 3. in ZooKeeper (ZK).
  *
  * When adding a part, it must be added immediately to these three places.
  * This is done like this
  * - [FS] first write the part into a temporary directory on the filesystem;
  * - [FS] rename the temporary part to the result on the filesystem;
  * - [RAM] immediately afterwards add it to the `data_parts`, and remove from `data_parts` any parts covered by this one;
  * - [RAM] also set the `Transaction` object, which in case of an exception (in next point),
  *   rolls back the changes in `data_parts` (from the previous point) back;
  * - [ZK] then send a transaction (multi) to add a part to ZooKeeper (and some more actions);
  * - [FS, ZK] by the way, removing the covered (old) parts from filesystem, from ZooKeeper and from `all_data_parts`
  *   is delayed, after a few minutes.
  *
  * There is no atomicity here.
  * It could be possible to achieve atomicity using undo/redo logs and a flag in `DataPart` when it is completely ready.
  * But it would be inconvenient - I would have to write undo/redo logs for each `Part` in ZK, and this would increase already large number of interactions.
  *
  * Instead, we are forced to work in a situation where at any time
  *  (from another thread, or after server restart), there may be an unfinished transaction.
  *  (note - for this the part should be in RAM)
  * From these cases the most frequent one is when the part is already in the data_parts, but it's not yet in ZooKeeper.
  * This case must be distinguished from the case where such a situation is achieved due to some kind of damage to the state.
  *
  * Do this with the threshold for the time.
  * If the part is young enough, its lack in ZooKeeper will be perceived optimistically - as if it just did not have time to be added there
  *  - as if the transaction has not yet been executed, but will soon be executed.
  * And if the part is old, its absence in ZooKeeper will be perceived as an unfinished transaction that needs to be rolled back.
  *
  * PS. Perhaps it would be better to add a flag to the DataPart that a part is inserted into ZK.
  * But here it's too easy to get confused with the consistency of this flag.
  */
#define MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER (5 * 60)

}
