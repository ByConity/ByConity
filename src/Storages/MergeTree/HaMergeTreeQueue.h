#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/HaMergeTreeAltersSequence.h>
#include <Storages/MergeTree/HaMergeTreeFutureParts.h>
#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <Storages/MergeTree/HaMergeTreeMutationEntry.h>
#include <Storages/MergeTree/HaQueueExecutingEntrySet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/HaMergeTreeMutationStatus.h>
#include <Common/ActionBlocker.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <boost/multi_index/global_fun.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/noncopyable.hpp>
#include <boost/range/iterator_range_core.hpp>

namespace DB
{

class StorageHaMergeTree;
class HaMergeTreeMergePredicate;
class HaMergeTreeLogManager;
class HaMergeTreeLogExchanger;

constexpr time_t VERY_LARGE_DELAY = 60 * 60 * 24 * 10; // 10 days

/**
 *  Ha Queue holds the unprocessed log entries.
 *
 *  Queue would select them for execution periodically by some policies:
 *  1. Always select DROP_RANGE firstly.
 *  2. CLONE_PART / GET_PART / MERGE_PARTS build their coverage trees of which the priority
 *     is the min LSN of all nodes. For entries in tree, prefer parent than children, and
 *     parent nodes take over the priority of leftmost child.
 *  3. Others follow LSN order simply.
 *
 *  Once insert log entries to this queue:
 *  1. First, write to log manager (i.e. to disk)
 *  2. Then, put unprocessed entries to list
 *  3. Next, broadcast them to peer replica
 *  4. Finally, awake queue execution task
 */

class HaMergeTreeQueue : boost::noncopyable
{
public:
    using LogEntry = HaMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;
    using LogEntryVec = LogEntry::Vec;
    using LSNList = std::vector<UInt64>;

    using MutationEntry = HaMergeTreeMutationEntry;
    using MutationEntryPtr = HaMergeTreeMutationEntryPtr;

private:
    friend class HaQueueExecutingEntrySet;
    friend class HaMergeTreeMergePredicate;

    struct TagSequenced {};
    struct TagByLSN {};

    static UInt64 getLSN(const LogEntryPtr & e) { return e->lsn; }

    using UnprocessedSet = boost::multi_index_container<
        LogEntryPtr,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByLSN>,
                boost::multi_index::global_fun<const LogEntryPtr &, UInt64, getLSN>
            >,
            boost::multi_index::sequenced<boost::multi_index::tag<TagSequenced>>
        >
    >;

    struct ByTime
    {
        bool operator()(const LogEntryPtr & lhs, const LogEntryPtr & rhs) const
        {
            return std::forward_as_tuple(lhs->create_time, lhs->lsn)
                < std::forward_as_tuple(rhs->create_time, rhs->lsn);
        }
    };
    using InsertsByTime = std::set<LogEntryPtr, ByTime>;

    struct LSNStat
    {
        time_t first_attempt_time{0};
        time_t last_attempt_time{0};
        UInt32 num_tries{0};
    };

    StorageHaMergeTree & storage;
    MergeTreeDataFormatVersion format_version;

    String zookeeper_path;
    String replica_name;
    String replica_path;

    String logger_name;
    Poco::Logger * log = nullptr;

    mutable std::mutex state_mutex;

    UnprocessedSet unprocessed_set;
    UnprocessedSet::index<TagSequenced>::type & unprocessed_queue;
    UnprocessedSet::index<TagByLSN>::type & unprocessed_by_lsn;

    /// future merged or mutated parts as a result of executing the logs,
    /// used by MergePredicate to avoid generating conflict merge / mutate logs
    /// - if a part is covered by `merge_mutate_future_parts`, do not select it for merge/mutation.
    HaMergeTreeFutureParts merge_mutate_future_parts;

    std::array<std::atomic<size_t>, LogEntry::TypesCount> unprocessed_counts {0};
    std::atomic<size_t> unprocessed_merges_of_self {0};
    std::atomic<size_t> unprocessed_mutations_of_self {0};

    InsertsByTime unprocessed_inserts_by_time;
    std::atomic<time_t> min_unprocessed_insert_time{std::numeric_limits<time_t>::max()};
    std::atomic<time_t> max_processed_insert_time{0};
    std::atomic<time_t> last_queue_update_time{0};

    struct MutationStatus
    {
        MutationStatus(MutationEntryPtr entry_, MergeTreeDataFormatVersion format_version_)
            : entry(std::move(entry_))
            , parts_to_do(format_version_)
        {
        }

        MutationEntryPtr entry;

        /// Local committed parts we have to mutate to complete mutation.
        /// We use ActiveDataPartSet structure to be able to manage covering and covered parts.
        ActiveDataPartSet parts_to_do;

        /// Note that is_done is not equivalent to parts_to_do.size() == 0
        /// (even if parts_to_do.size() == 0 some relevant parts can still commit in the future).
        bool is_done = false;
        /// records the time when is_done is set to true
        time_t finish_time = 0;

        /// has the log manager synced all logs that could add new part to mutate.
        /// if not, we need to trigger poll logs to queue
        bool is_log_synced = false;

        String latest_failed_part;
        MergeTreePartInfo latest_failed_part_info;
        time_t latest_fail_time = 0;
        String latest_fail_reason;
    };

    /// Znode ID of the latest mutation that is done.
    String mutation_pointer;
    /// maximum mutation version that has been loaded
    std::atomic<Int64> max_loaded_mutation_version {0};
    std::map<String, MutationStatus> mutations_by_znode;   /// znode name -> normal mutation status
    std::map<Int64, MutationStatus*> mutations_by_version; /// mutation version -> normal mutation status
    /// "alter metadata mutation" is maintained in a separate map below because it doesn't mutate any part
    /// and is executed immediately after being loaded from ZK.
    std::map<String, MutationStatus> mutations_for_alter_metadata; /// znode name -> alter metadata mutation status

    /// Ensures that only one thread is simultaneously pulling logs.
    std::mutex pull_logs_to_queue_mutex;

    /// Ensures that only one thread is simultaneously updating mutations.
    std::mutex update_mutations_mutex;

    /// This sequence control ALTERs execution in replication queue.
    /// We need it because alters have to be executed sequentially (one by one).
    HaMergeTreeAltersSequence alter_sequence;

    /// In order to reduce error messages about failed to fetch log entries,
    /// prepare a list of LSNs to be sync-ed ahead, and record attempts stat.
    ///
    /// Once failed to sync, those LSNs would be postponed by `Exponential Backoff`
    std::map<UInt64, LSNStat> prepared_lsn_list;

    /// Point to next un-fetched LSN. All logs with smaller LSN have been fetched to log manager.
    UInt64 next_prepared_lsn{0};

private:
    LSNList prepareRequestLSNs(zkutil::ZooKeeperPtr zookeeper, bool ignore_backoff = false);
    void onFetchedEntries(const LSNList & requests, const LogEntryVec & entries, bool all_success);
    void processFailedLSNs(const LSNList & failed_lsns, bool all_success);

    size_t insertWithStats(const LogEntryPtr & entry, std::lock_guard<std::mutex> &);

    bool canExecuteClone(const HaMergeTreeLogEntryPtr & clone_entry);

    /// If there is any alter metadata mutation that can execute according to alter sequence, try execute it.
    void tryExecuteMetadataAlter(bool & some_mutations_are_probably_done, std::lock_guard<std::mutex> & pull_logs_lock);

    /// Load new mutation entries. If something new is loaded, schedule storage.merge_selecting_task.
    void updateMutations(zkutil::ZooKeeperPtr & zookeeper, std::lock_guard<std::mutex> & update_mutations_lock, Coordination::WatchCallback watch_callback = {});

    void removeCoveredPartsFromMutation(const LogEntry & entry, std::lock_guard<std::mutex> & states_lock);

    Int64 getCurrentMutationVersionImpl(const String & partition_id, Int64 data_version, std::lock_guard<std::mutex> & /* state_lock */) const;

public:
    ActionBlocker actions_blocker;

    HaMergeTreeLogManager & getLogManager() const;
    HaMergeTreeLogExchanger & getLogExchanger() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;

    explicit HaMergeTreeQueue(StorageHaMergeTree & storage_);

    void initialize(const MergeTreeData::DataParts & parts);
    void clear();

    void load(zkutil::ZooKeeperPtr zookeeper);

    LogEntryVec pullLogsImpl(
        zkutil::ZooKeeperPtr & zookeeper, std::lock_guard<std::mutex> & /*pull_logs_to_queue_lock*/,
        Coordination::WatchCallback watch_callback = {}, bool ignore_backoff = false);
    LogEntryVec pullLogs(zkutil::ZooKeeperPtr zookeeper);
    /// return whether local logs are up-to-date after pull
    bool pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallback watch_callback = {}, bool ignore_backoff = false);

    size_t markRedundantEntries(LogEntryVec & prepared_entries);

    void write(const LogEntry & e, bool broadcast = true) { write(LogEntryVec{std::make_shared<LogEntry>(e)}, broadcast); }
    void write(LogEntryPtr e, bool broadcast = true) { write(LogEntryVec{std::move(e)}, broadcast); }
    void write(const LogEntryVec & entries, bool broadcast = true);

    HaQueueExecutingEntrySetPtr tryCreateExecutingSet(const LogEntryPtr & e);
    HaQueueExecutingEntrySetPtr selectEntryToProcess(size_t & num_avail_fetches);
    HaQueueExecutingEntrySetPtr selectEntryToProcessLocked(size_t & num_avail_fetches, LogEntry::Vec & useless_entries, std::lock_guard<std::mutex> &);

    void onLogExecutionError(LogEntryPtr & entry, const std::exception_ptr & exception);

    void removeProcessedEntry(LogEntryPtr & entry);
    void removeProcessedEntries(const LogEntryVec & entries);

    MutationEntryPtr selectAlterMetadataToProcess(const String & last_processed);
    void finishMetadataAlter(const MutationEntryPtr & entry);

    struct OperationsInQueue
    {
        size_t merges = 0;
        size_t merges_of_self = 0;
        size_t mutations = 0;
        size_t mutations_of_self = 0;
    };

    OperationsInQueue countMergesAndPartMutations() const;

    /// Count the total number of active mutations that are not finished.
    size_t countUnfinishedMutations(bool include_alter_metadata = false) const;

    /// Count the total number of active mutations that are finished (is_done = true).
    size_t countFinishedMutations(bool include_alter_metadata = false) const;

    /// Remove a mutation from ZooKeeper and from the local set. Returns the removed entry or nullptr
    /// if it could not be found.
    HaMergeTreeMutationEntryPtr removeMutation(zkutil::ZooKeeperPtr zookeeper, const String & mutation_id);

    /// re-populate parts_to_do for the specified mutation
    void forceReloadMutation(const String & mutation_id);

    /// Return the version (block number) of the last mutation that we don't need to apply to the part
    /// with getDataVersion() == data_version. (Either this mutation was already applied or the part
    /// was created after the mutation).
    /// If there is no such mutation or it has already been executed and deleted, return 0.
    Int64 getCurrentMutationVersion(const String & partition_id, Int64 data_version) const;

    /// Return pointer to the first unfinished mutation that needs hang check.
    /// Return nullptr if no such mutation exists.
    MutationEntryPtr getMutationForHangCheck() const;

    MutationEntryPtr getExecutablePartsToMutate(const String & mutation_id, Strings & out_parts) const;

    Strings getPartsToMutateWithoutLogs(const String & mutation_id) const;

    /// Return empty commands if mutation with the given version doesn't exist (e.g., killed)
    MutationCommands getMutationCommands(Int64 mutation_version);

    /// Mark finished mutations as done.
    /// Return whether we have finalized any mutation.
    bool tryFinalizeMutations(zkutil::ZooKeeperPtr zookeeper);

    MutationEntryPtr findDuplicateMutationByQueryId(
        zkutil::ZooKeeperPtr & zookeeper, const MutationEntry & entry, String & max_znode_checked) const;

    struct Status
    {
        UInt32 queue_size{0};
        UInt32 clones_in_queue{0};
        UInt32 inserts_in_queue{0};
        UInt32 merges_in_queue{0};
        UInt32 merges_of_self{0};
        UInt32 part_mutations_in_queue{0};
        UInt32 part_mutations_of_self{0};

        time_t last_queue_update{0};
        time_t queue_oldest_time{0};
        time_t clones_oldest_time{0};
        time_t inserts_oldest_time{0};
        time_t merges_oldest_time{0};
        time_t part_mutations_oldest_time{0};

        String oldest_part_to_clone;
        String oldest_part_to_get;
        String oldest_part_to_merge_to;
        String oldest_part_to_mutate_to;
    };

    Status getStatus() const;

    std::vector<HaMergeTreeMutationStatus> getMutationsStatus() const;

    /// Return empty optional if mutation was killed. Otherwise return partially
    /// filled mutation status with information about error (latest_fail*) and is_done.
    /// If out_mutation_pointer is not nullptr, set *out_mutation_pointer to current mutation pointer.
    std::optional<HaMergeTreeMutationStatus> getPartialMutationsStatus(
        const String & znode_name, bool is_alter_metadata, String * out_mutation_pointer = nullptr) const;

    std::pair<time_t, time_t> getAbsoluteDelay() const;

    LogEntryVec lookupEntriesByLSN(const LSNList & lsns);

    void getUnprocessedEntries(std::vector<HaMergeTreeLogEntryData> & entries) const;

    size_t getQueueSize() const;

    void checkAddMetadataAlter(const MutationCommands & commands) const;

    HaMergeTreeMergePredicate getMergePredicate(zkutil::ZooKeeperPtr&);
};

class HaMergeTreeMergePredicate
{
public:
    HaMergeTreeMergePredicate(HaMergeTreeQueue& queue_, zkutil::ZooKeeperPtr & zookeeper);

    // could two parts be merged
    bool operator()
        (const MergeTreeData::DataPartPtr & left,
         const MergeTreeData::DataPartPtr & right,
         bool enable_logging = false) const;

    /// For consistency
    std::function< bool(
            const MergeTreeData::DataPartPtr &,
            const MergeTreeData::DataPartPtr &,
            bool enable_logging)>
        getMergeChecker() const { return *this; }

    std::function<bool(const MergeTreeData::DataPartPtr &)> getAssignmentChecker() const;

    std::optional<std::pair<Int64, int>> getDesiredMutationVersion(const MergeTreeData::DataPartPtr & part) const;

private:
    Int32 findMergeInterval(const MergeTreeData::DataPartPtr &) const;

    HaMergeTreeQueue & queue;
    ActiveDataPartSet future_merge_parts;
    ActiveDataPartSet future_parts;
    time_t snapshot_time;
    constexpr static Int32 INTERVAL_GAP = -1;
    std::unordered_map<String, Int32> part_to_interval;
    Poco::Logger * log{nullptr};
};

}
