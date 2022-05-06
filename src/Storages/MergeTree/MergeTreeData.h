#pragma once

#include <Common/SimpleIncrement.h>
#include <Common/MultiVersion.h>
#include <Common/QueueForAsyncTask.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/DataDestinationType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/MergeTree/ManifestStore.h>
#include <Storages/MergeTree/MergeTreeMeta.h>
#include <Interpreters/PartLog.h>
#include <Disks/StoragePolicy.h>
#include <Interpreters/Aggregator.h>
#include <Storages/extractKeyExpressionList.h>
#include <Storages/PartitionCommands.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/global_fun.hpp>
#include <boost/range/iterator_range_core.hpp>

namespace DB
{

class AlterCommands;
class MergeTreePartsMover;
class MutationCommands;
class Context;
class BitmapTracker;
class MergeTreeBitmapIndex;
class MergeTreeMarkBitmapIndex;
using MergeTreeBitmapIndexPtr = std::shared_ptr<MergeTreeBitmapIndex>;
using MergeTreeMarkBitmapIndexPtr = std::shared_ptr<MergeTreeMarkBitmapIndex>;

struct JobAndPool;
class DiskUniqueKeyIndexCache;
class DiskUniqueRowStoreCache;

/// Auxiliary struct holding information about the future merged or mutated part.
struct EmergingPartInfo
{
    String disk_name;
    String partition_id;
    size_t estimate_bytes;
};

struct CurrentlySubmergingEmergingTagger;

struct SelectQueryOptions;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
using ManyExpressionActions = std::vector<ExpressionActionsPtr>;
class MergeTreeDeduplicationLog;
class IBackgroundJobExecutor;
class IBitEngineDictionaryManager;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

/// State of HaUniqueMergeTree table, valid state transfer includes
/// * INIT -> NORMAL : when is_lost == 0
/// * INIT -> BROKEN : when resetUniqueTableToVersion failed during startup
/// * INIT -> REPAIR_MANIFEST : when is_lost == 1 and begin to recover
/// * BROKEN -> REPAIR_MANIFEST : after set is_lost to 1 and begin to recover
/// * REPAIR_MANIFEST -> REPAIR_DATA : when manifest is repaired and begin to recover data
/// * REPAIR_MANIFEST -> REPAIRED : when manifest is repaired and no data to recover
/// * REPAIR_DATA -> REPAIRED : when data is recovered
/// * REPAIR_DATA -> REPAIR_MANIFEST : when some unrecoverable error happens
/// * REPAIRED -> NORMAL : when is_lost is set to 0 and all bg threads are started
/// * NORMAL -> BROKEN : when resetUniqueTableToVersion failed somehow
/// Note: If you modifies the state transfer logic above, please update the state transfer table in `setUniqueTableState'
enum class UniqueTableState
{
    INIT,
    BROKEN,
    REPAIR_MANIFEST,
    REPAIR_DATA,
    REPAIRED,
    NORMAL
};

String toString(UniqueTableState state);


/// Data structure for *MergeTree engines.
/// Merge tree is used for incremental sorting of data.
/// The table consists of several sorted parts.
/// During insertion new data is sorted according to the primary key and is written to the new part.
/// Parts are merged in the background according to a heuristic algorithm.
/// For each part the index file is created containing primary key values for every n-th row.
/// This allows efficient selection by primary key range predicate.
///
/// Additionally:
///
/// The date column is specified. For each part min and max dates are remembered.
/// Essentially it is an index too.
///
/// Data is partitioned by the value of the partitioning expression.
/// Parts belonging to different partitions are not merged - for the ease of administration (data sync and backup).
///
/// File structure of old-style month-partitioned tables (format_version = 0):
/// Part directory - / min-date _ max-date _ min-id _ max-id _ level /
/// Inside the part directory:
/// checksums.txt - contains the list of all files along with their sizes and checksums.
/// columns.txt - contains the list of all columns and their types.
/// primary.idx - contains the primary index.
/// [Column].bin - contains compressed column data.
/// [Column].mrk - marks, pointing to seek positions allowing to skip n * k rows.
///
/// File structure of tables with custom partitioning (format_version >= 1):
/// Part directory - / partition-id _ min-id _ max-id _ level /
/// Inside the part directory:
/// The same files as for month-partitioned tables, plus
/// count.txt - contains total number of rows in this part.
/// partition.dat - contains the value of the partitioning expression.
/// minmax_[Column].idx - MinMax indexes (see IMergeTreeDataPart::MinMaxIndex class) for the columns required by the partitioning expression.
///
/// Several modes are implemented. Modes determine additional actions during merge:
/// - Ordinary - don't do anything special
/// - Collapsing - collapse pairs of rows with the opposite values of sign_columns for the same values
///   of primary key (cf. CollapsingSortedBlockInputStream.h)
/// - Replacing - for all rows with the same primary key keep only the latest one. Or, if the version
///   column is set, keep the latest row with the maximal version.
/// - Summing - sum all numeric columns not contained in the primary key for all rows with the same primary key.
/// - Aggregating - merge columns containing aggregate function states for all rows with the same primary key.
/// - Graphite - performs coarsening of historical data for Graphite (a system for quantitative monitoring).

/// The MergeTreeData class contains a list of parts and the data structure parameters.
/// To read and modify the data use other classes:
/// - MergeTreeDataSelectExecutor
/// - MergeTreeDataWriter
/// - MergeTreeDataMergerMutator

class MergeTreeData : public IStorage, public WithMutableContext
{
public:
    /// Function to call if the part is suspected to contain corrupt data.
    using BrokenPartCallback = std::function<void (const String &)>;
    using DataPart = IMergeTreeDataPart;

    using MutableDataPartPtr = std::shared_ptr<DataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;
    /// After the DataPart is added to the working set, it cannot be changed.
    using DataPartPtr = std::shared_ptr<const DataPart>;

    using DataPartState = IMergeTreeDataPart::State;
    using DataPartStates = std::initializer_list<DataPartState>;
    using DataPartStateVector = std::vector<DataPartState>;

    using PinnedPartUUIDsPtr = std::shared_ptr<const PinnedPartUUIDs>;

    using BitEngineDictionaryManagerPtr = std::shared_ptr<IBitEngineDictionaryManager>;
    using MetaStorePtr = std::shared_ptr<MergeTreeMeta>;

    constexpr static auto FORMAT_VERSION_FILE_NAME = "format_version.txt";
    constexpr static auto DETACHED_DIR_NAME = "detached";
    constexpr static auto BITENGINE_DICTIONARY_DIR_NAME = "bitmap_dictionary";

    /// Auxiliary structure for index comparison. Keep in mind lifetime of MergeTreePartInfo.
    struct DataPartStateAndInfo
    {
        DataPartState state;
        const MergeTreePartInfo & info;
    };

    /// Auxiliary structure for index comparison
    struct DataPartStateAndPartitionID
    {
        DataPartState state;
        String partition_id;
    };

    STRONG_TYPEDEF(String, PartitionID)

    /// Alter conversions which should be applied on-fly for part. Build from of
    /// the most recent mutation commands for part. Now we have only rename_map
    /// here (from ALTER_RENAME) command, because for all other type of alters
    /// we can deduce conversions for part from difference between
    /// part->getColumns() and storage->getColumns().
    struct AlterConversions
    {
        /// Rename map new_name -> old_name
        std::unordered_map<String, String> rename_map;

        bool isColumnRenamed(const String & new_name) const { return rename_map.count(new_name) > 0; }
        String getColumnOldName(const String & new_name) const { return rename_map.at(new_name); }
    };

    struct LessDataPart
    {
        using is_transparent = void;

        bool operator()(const DataPartPtr & lhs, const MergeTreePartInfo & rhs) const { return lhs->info < rhs; }
        bool operator()(const MergeTreePartInfo & lhs, const DataPartPtr & rhs) const { return lhs < rhs->info; }
        bool operator()(const DataPartPtr & lhs, const DataPartPtr & rhs) const { return lhs->info < rhs->info; }
        bool operator()(const MergeTreePartInfo & lhs, const PartitionID & rhs) const { return lhs.partition_id < rhs.toUnderType(); }
        bool operator()(const PartitionID & lhs, const MergeTreePartInfo & rhs) const { return lhs.toUnderType() < rhs.partition_id; }
    };

    struct LessStateDataPart
    {
        using is_transparent = void;

        bool operator() (const DataPartStateAndInfo & lhs, const DataPartStateAndInfo & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.info)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.info);
        }

        bool operator() (DataPartStateAndInfo info, const DataPartState & state) const
        {
            return static_cast<size_t>(info.state) < static_cast<size_t>(state);
        }

        bool operator() (const DataPartState & state, DataPartStateAndInfo info) const
        {
            return static_cast<size_t>(state) < static_cast<size_t>(info.state);
        }

        bool operator() (const DataPartStateAndInfo & lhs, const DataPartStateAndPartitionID & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.info.partition_id)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.partition_id);
        }

        bool operator() (const DataPartStateAndPartitionID & lhs, const DataPartStateAndInfo & rhs) const
        {
            return std::forward_as_tuple(static_cast<UInt8>(lhs.state), lhs.partition_id)
                   < std::forward_as_tuple(static_cast<UInt8>(rhs.state), rhs.info.partition_id);
        }
    };

    using DataParts = std::set<DataPartPtr, LessDataPart>;
    using DataPartsVector = std::vector<DataPartPtr>;

    using DataPartsLock = std::unique_lock<std::shared_mutex>;
    using DataPartsReadLock = std::shared_lock<std::shared_mutex>;
    DataPartsLock lockParts() const { return DataPartsLock(data_parts_mutex); }
    DataPartsReadLock lockPartsRead() const { return DataPartsReadLock(data_parts_mutex); }

    using DeleteBitmapGetter = std::function<DeleteBitmapPtr(const DataPartPtr &)>;
    using DataPartsDeleteSnapshot = std::map<DataPartPtr, DeleteBitmapPtr, LessDataPart>;
    DataPartsDeleteSnapshot getLatestDeleteSnapshot(const DataPartsVector & parts) const
    {
        DataPartsDeleteSnapshot res;
        auto lock = lockPartsRead();
        for (auto & part : parts) {
            res.insert({part, part->getDeleteBitmap()});
        }
        return res;
    }

    UniqueTableState getUniqueTableState() const { return unique_table_state; }
    void setUniqueTableState(UniqueTableState new_state)
    {
        static std::set<std::pair<UniqueTableState, UniqueTableState>> valid_transfer {
            { UniqueTableState::INIT, UniqueTableState::NORMAL },
            { UniqueTableState::INIT, UniqueTableState::BROKEN },
            { UniqueTableState::INIT, UniqueTableState::REPAIR_MANIFEST },
            { UniqueTableState::BROKEN, UniqueTableState::REPAIR_MANIFEST },
            { UniqueTableState::REPAIR_MANIFEST, UniqueTableState::REPAIR_DATA },
            { UniqueTableState::REPAIR_MANIFEST, UniqueTableState::REPAIRED },
            { UniqueTableState::REPAIR_DATA, UniqueTableState::REPAIRED },
            { UniqueTableState::REPAIR_DATA, UniqueTableState::REPAIR_MANIFEST },
            { UniqueTableState::REPAIRED, UniqueTableState::NORMAL },
            { UniqueTableState::NORMAL, UniqueTableState::BROKEN }
        };
        if (valid_transfer.count({unique_table_state, new_state}) == 0)
            throw Exception("Can't transfer from " + toString(unique_table_state) + " to " + toString(new_state) + " for " + log_name,
                            ErrorCodes::LOGICAL_ERROR);
        LOG_DEBUG(log, "Updated state from {} to {}", toString(unique_table_state), toString(new_state));
        unique_table_state = new_state;
    }

    using UniqueKeySet = std::unordered_set<String>;
    struct UniqueMergeState
    {
        String new_part_name;
        ActionBlocker blocker; /// used to cancel running merge if needed
        UniqueKeySet delete_buffer; /// hold key deleted during merge

        bool isCancelled() const { return blocker.isCancelled(); }
        void cancel()
        {
            blocker.cancelForever();
            delete_buffer.clear();
        }
    };
    using UniqueMergeStatePtr = std::shared_ptr<UniqueMergeState>;

    /// For unique table: reset parts and delete bitmaps to the snapshot at the given version.
    /// All parts and delete files in the snapshot should present,
    /// otherwise exception is thrown and caller should set table state to broken
    void resetUniqueTableToVersion(const DataPartsLock &, UInt64 version);

    /// if true, uniqueness is only enforced at partition level instead of table level
    bool unique_within_partition;
    /// name of is_offline column, empty string if not specified
    String is_offline_column;

    MergeTreeDataPartType choosePartType(size_t bytes_uncompressed, size_t rows_count) const;
    MergeTreeDataPartType choosePartTypeOnDisk(size_t bytes_uncompressed, size_t rows_count) const;

    /// After this method setColumns must be called
    MutableDataPartPtr createPart(const String & name,
        MergeTreeDataPartType type, const MergeTreePartInfo & part_info,
        const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part = nullptr) const;

    /// Create part, that already exists on filesystem.
    /// After this methods 'loadColumnsChecksumsIndexes' must be called.
    MutableDataPartPtr createPart(const String & name,
        const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part = nullptr) const;

    MutableDataPartPtr createPart(const String & name, const MergeTreePartInfo & part_info,
        const VolumePtr & volume, const String & relative_path, const IMergeTreeDataPart * parent_part = nullptr) const;

    MergeTreeBitmapIndexPtr getMergeTreeBitmapIndex() const { return bitmap_index; }
    MergeTreeMarkBitmapIndexPtr getMergeTreeMarkBitmapIndex() const { return mark_bitmap_index; }
    void addPartForBackgroundTask(const DataPartPtr & part, const Context & context_, bool without_recode = false);
    bool supportsMapImplicitColumn() const override { return true; }

    /// Auxiliary object to add a set of parts into the working set in two steps:
    /// * First, as PreCommitted parts (the parts are ready, but not yet in the active set).
    /// * Next, if commit() is called, the parts are added to the active set and the parts that are
    ///   covered by them are marked Outdated.
    /// If neither commit() nor rollback() was called, the destructor rollbacks the operation.
    class Transaction : private boost::noncopyable
    {
    public:
        Transaction(MergeTreeData & data_) : data(data_) {}

        DataPartsVector commit(MergeTreeData::DataPartsLock * acquired_parts_lock = nullptr);

        void rollback();

        /// Immediately remove parts from table's data_parts set and change part
        /// state to temporary. Useful for new parts which not present in table.
        void rollbackPartsToTemporaryState();

        size_t size() const { return precommitted_parts.size(); }
        bool isEmpty() const { return precommitted_parts.empty(); }

        ~Transaction()
        {
            try
            {
                rollback();
            }
            catch (...)
            {
                tryLogCurrentException("~MergeTreeData::Transaction");
            }
        }

    private:
        friend class MergeTreeData;

        MergeTreeData & data;
        DataParts precommitted_parts;

        void clear() { precommitted_parts.clear(); }
    };

    /// Used by UniqueMergeTree to update parts and delete bitmaps atomically
    class SyncPartsTransaction : private boost::noncopyable
    {
    public:
        SyncPartsTransaction(
            MergeTreeData & data_, const DataPartsDeleteSnapshot & new_deletes_, UInt64 commit_version_)
            : data(data_), new_deletes(new_deletes_), commit_version(commit_version_), new_part_txn(data_), committed(false) {}

        ~SyncPartsTransaction()
        {
            if (!committed)
                rollback();
        }

        /// pre-commit new part, return existing parts covered by the new part
        DataPartsVector preCommit(MutableDataPartPtr & new_part);

        /// commit new part and new deletes atomically
        void commit();

        /// rollback pre-committed new part and delete files for old parts
        void rollback();

    private:
        friend class MergeTreeData;
        MergeTreeData & data;
        const DataPartsDeleteSnapshot & new_deletes;
        UInt64 commit_version;
        Transaction new_part_txn;
        bool committed;
    };

    using PathWithDisk = std::pair<String, DiskPtr>;

    /// An object that stores the names of temporary files created in the part directory during ALTER of its
    /// columns.
    /// FIXME (UNIQUE KEY): Taken from the stable_v2, mainly for unique table to do alter
    class AlterDataPartTransaction : private boost::noncopyable
    {
    public:
        /// Renames temporary files, finishing the ALTER of the part.
        void commit();

        /// If commit() was not called, deletes temporary files, canceling the ALTER.
        ~AlterDataPartTransaction();

        const String & getPartName() const { return data_part->name; }

        /// Review the changes before the commit.
        const NamesAndTypesList & getNewColumns() const { return new_columns; }
        const DataPart::Checksums & getNewChecksums() const { return new_checksums; }

    private:
        friend class MergeTreeData;

        AlterDataPartTransaction(DataPartPtr data_part_) : data_part(data_part_) {}

        void clear()
        {
            data_part = nullptr;
        }

        void innerCommit();

        DataPartPtr data_part;
        DataPartsLock alter_lock;

        /// Ensure that the versions of the checksum have been set to the versions of the data part
        DataPart::Checksums new_checksums;
        NamesAndTypesList new_columns;
        /// If the value is an empty string, the file is not temporary, and it must be deleted.
        NameToNameMap rename_map;

        /// Files need to add to part, we append this files to "data" file in remote storage.
        Names add_files;

        /// some file need be deleted
        Names clear_files;
    };

    using AlterDataPartTransactionPtr = std::unique_ptr<AlterDataPartTransaction>;

    struct PartsTemporaryRename : private boost::noncopyable
    {
        PartsTemporaryRename(
            const MergeTreeData & storage_,
            const String & source_dir_)
            : storage(storage_)
            , source_dir(source_dir_)
        {
        }

        void addPart(const String & old_name, const String & new_name);

        /// Renames part from old_name to new_name
        void tryRenameAll();

        /// Renames all added parts from new_name to old_name if old name is not empty
        ~PartsTemporaryRename();

        const MergeTreeData & storage;
        const String source_dir;
        std::vector<std::pair<String, String>> old_and_new_names;
        std::unordered_map<String, PathWithDisk> old_part_name_to_path_and_disk;
        bool renamed = false;
    };

    /// Parameters for various modes.
    struct MergingParams
    {
        /// Merging mode. See above.
        enum Mode
        {
            Ordinary            = 0,    /// Enum values are saved. Do not change them.
            Collapsing          = 1,
            Summing             = 2,
            Aggregating         = 3,
            Replacing           = 5,
            Graphite            = 6,
            VersionedCollapsing = 7,
            Unique              = 102,
        };

        Mode mode;

        /// For Collapsing and VersionedCollapsing mode.
        String sign_column;

        /// For Summing mode. If empty - columns_to_sum is determined automatically.
        Names columns_to_sum;

        /// For Replacing/VersionedCollapsing/Unique mode. Can be empty for Replacing and Unique.
        String version_column;
        /// For Unique mode, users can also use value of partition expr as version.
        /// As a result, all rows inside a partition share the same version, removing
        /// the cost to writing and reading an extra version column.
        static constexpr auto partition_value_as_version = "__partition__";

        /// For Graphite mode.
        Graphite::Params graphite_params;

        /// Check that needed columns are present and have correct types.
        void check(const StorageInMemoryMetadata & metadata) const;

        String getModeName() const;

        bool partitionValueAsVersion() const { return version_column == partition_value_as_version; }
        bool hasExplicitVersionColumn() const { return !version_column.empty() && !partitionValueAsVersion(); }
    };

    /// Attach the table corresponding to the directory in full_path inside policy (must end with /), with the given columns.
    /// Correctness of names and paths is not checked.
    ///
    /// date_column_name - if not empty, the name of the Date column used for partitioning by month.
    ///     Otherwise, partition_by_ast is used for partitioning.
    ///
    /// order_by_ast - a single expression or a tuple. It is used as a sorting key
    ///     (an ASTExpressionList used for sorting data in parts);
    /// primary_key_ast - can be nullptr, an expression, or a tuple.
    ///     Used to determine an ASTExpressionList values of which are written in the primary.idx file
    ///     for one row in every `index_granularity` rows to speed up range queries.
    ///     Primary key must be a prefix of the sorting key;
    ///     If it is nullptr, then it will be determined from order_by_ast.
    ///
    /// require_part_metadata - should checksums.txt and columns.txt exist in the part directory.
    /// attach - whether the existing table is attached or the new table is created.
    MergeTreeData(const StorageID & table_id_,
                  const String & relative_data_path_,
                  const StorageInMemoryMetadata & metadata_,
                  ContextMutablePtr context_,
                  const String & date_column_name,
                  const MergingParams & merging_params_,
                  std::unique_ptr<MergeTreeSettings> settings_,
                  bool require_part_metadata_,
                  bool attach,
                  BrokenPartCallback broken_part_callback_ = [](const String &){});

    ~MergeTreeData() override;

    bool getQueryProcessingStageWithAggregateProjection(
        ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot, SelectQueryInfo & query_info) const;

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr query_context,
        QueryProcessingStage::Enum to_stage,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & info) const override;

    ReservationPtr reserveSpace(UInt64 expected_size, VolumePtr & volume) const;

    static bool partsContainSameProjections(const DataPartPtr & left, const DataPartPtr & right);

    StoragePolicyPtr getStoragePolicy() const override;

    bool supportsPrewhere() const override { return true; }

    bool supportsFinal() const override
    {
        return merging_params.mode == MergingParams::Collapsing
            || merging_params.mode == MergingParams::Summing
            || merging_params.mode == MergingParams::Aggregating
            || merging_params.mode == MergingParams::Replacing
            || merging_params.mode == MergingParams::VersionedCollapsing;
    }

    bool supportsSubcolumns() const override { return true; }

    NamesAndTypesList getVirtuals() const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, ContextPtr, const StorageMetadataPtr & metadata_snapshot) const override;

    /// Load the set of data parts from disk. Call once - immediately after the object is created.
    void loadDataParts(bool skip_sanity_checks, bool attach = false);

    /** ----------------------- COMPATIBLE CODE BEGIN-------------------------- */
    /*  compatible with old metastore. remove this later  */

    /// load data parts from old version metastore and copy the metadata into new metastore.
    bool preLoadDataParts(bool skip_sanity_checks, bool attach);
    /*  -----------------------  COMPATIBLE CODE END -------------------------- */

    /// load parts from file system;
    void loadPartsFromFileSystem(PartNamesWithDisks part_names_with_disks, PartNamesWithDisks wal_with_disks, bool skip_sanity_checks, bool attach, DataPartsLock &);

    String getLogName() const { return log_name; }

    /// A global unique id for the storage. If storage UUID is not empty, use the storage UUID. Otherwise, use the address of current object.
    String getStorageUniqueID() const;

    Int64 getMaxBlockNumber() const;

    /// Returns a copy of the list so that the caller shouldn't worry about locks.
    DataParts getDataParts(const DataPartStates & affordable_states) const;

    /// Returns sorted list of the parts with specified states
    ///  out_states will contain snapshot of each part state
    DataPartsVector getDataPartsVector(
        const DataPartStates & affordable_states, DataPartStateVector * out_states = nullptr, bool require_projection_parts = false) const;

    /// Returns absolutely all parts (and snapshot of their states)
    DataPartsVector getAllDataPartsVector(DataPartStateVector * out_states = nullptr, bool require_projection_parts = false) const;

    /// Returns all detached parts
    DetachedPartsInfo getDetachedParts() const;

    void validateDetachedPartName(const String & name) const;

    void dropDetached(const ASTPtr & partition, bool part, ContextPtr context);

    MutableDataPartsVector tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
            ContextPtr context, PartsTemporaryRename & renamed_parts, bool in_preattach = false);

    using PartNamesWithDisks = std::vector<std::pair<String, DiskPtr>>;

    MutableDataPartsVector tryLoadPartsInPathToAttach(const PartNamesWithDisks & parts_with_disk, const String & relative_path);

    virtual void attachPartsInDirectory(const PartNamesWithDisks & /*local_parts*/, const String & /*relative path*/, ContextPtr /*local_context*/)
    {
        throw Exception("Not implemented.", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Returns Committed parts
    DataParts getDataParts() const;
    DataPartsVector getDataPartsVector() const;

    /// Returns a committed part with the given name or a part containing it. If there is no such part, returns nullptr.
    DataPartPtr getActiveContainingPart(const String & part_name) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info) const;
    DataPartPtr getActiveContainingPart(const MergeTreePartInfo & part_info, DataPartState state, DataPartsLock & lock) const;

    /// Swap part with it's identical copy (possible with another path on another disk).
    /// If original part is not active or doesn't exist exception will be thrown.
    void swapActivePart(MergeTreeData::DataPartPtr part_copy);

    /// Returns all parts in specified partition
    DataPartsVector getDataPartsVectorInPartition(DataPartState state, const String & partition_id);

    /// Returns the part with the given name and state or nullptr if no such part.
    DataPartPtr getPartIfExists(const String & part_name, const DataPartStates & valid_states);
    DataPartPtr getPartIfExistsWithoutLock(const String & part_name, const DataPartStates & valid_states);
    DataPartPtr getPartIfExists(const MergeTreePartInfo & part_info, const DataPartStates & valid_states);
    DataPartPtr getPartIfExistsWithoutLock(const MergeTreePartInfo & part_info, const DataPartStates & valid_states);
    bool hasPart(const String & part_name, const DataPartStates & valid_states);
    bool hasPart(const MergeTreePartInfo & part_info, const MergeTreeData::DataPartStates & valid_states);

    /// For a target part that will be fetched from another replica, find whether the local has an old version part.
    /// When mutating a part, its mutate version will be changed. For example, all_0_0_0 -> all_0_0_0_1, all_0_0_0_1 is the target part, all_0_0_0 is the old version part.
    /// Due to mutation commands may modify only few files in the old part, so a lot of files are not necessary to transfer. 
    /// In this case, if the local has an old version part, transfer its checksum to the replica, then the replica will give the information.
    DataPartPtr getOldVersionPartIfExists(const String & part_name);

    DataPartsVector getPartsByPredicate(const ASTPtr & predicate);

    /// Split CLEAR COLUMN IN PARTITION WHERE command into multiple CLEAR COLUMN IN PARTITION commands.
    void handleClearColumnInPartitionWhere(MutationCommands & mutation_commands, const AlterCommands & alter_commands);

    /// Total size of active parts in bytes.
    size_t getTotalActiveSizeInBytes() const;

    size_t getTotalActiveSizeInRows() const;

    size_t getPartsCount() const;
    size_t getMaxPartsCountForPartitionWithState(DataPartState state) const;
    size_t getMaxPartsCountForPartition() const;
    size_t getMaxInactivePartsCountForPartition() const;

    /// Get min value of part->info.getDataVersion() for all active parts.
    /// Makes sense only for ordinary MergeTree engines because for them block numbering doesn't depend on partition.
    std::optional<Int64> getMinPartDataVersion() const;

    /// If the table contains too many active parts, sleep for a while to give them time to merge.
    /// If until is non-null, wake up from the sleep earlier if the event happened.
    void delayInsertOrThrowIfNeeded(Poco::Event * until = nullptr) const;

    /// Renames temporary part to a permanent part and adds it to the parts set.
    /// It is assumed that the part does not intersect with existing parts.
    /// If increment != nullptr, part index is determining using increment. Otherwise part index remains unchanged.
    /// If out_transaction != nullptr, adds the part in the PreCommitted state (the part will be added to the
    /// active set later with out_transaction->commit()).
    /// Else, commits the part immediately.
    /// Returns true if part was added. Returns false if part is covered by bigger part.
    bool renameTempPartAndAdd(MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr, MergeTreeDeduplicationLog * deduplication_log = nullptr);

    /// The same as renameTempPartAndAdd but the block range of the part can contain existing parts.
    /// Returns all parts covered by the added part (in ascending order).
    /// If out_transaction == nullptr, marks covered parts as Outdated.
    DataPartsVector renameTempPartAndReplace(
        MutableDataPartPtr & part, SimpleIncrement * increment = nullptr, Transaction * out_transaction = nullptr, MergeTreeDeduplicationLog * deduplication_log = nullptr);

    /// Low-level version of previous one, doesn't lock mutex
    bool renameTempPartAndReplace(
            MutableDataPartPtr & part, SimpleIncrement * increment, Transaction * out_transaction, DataPartsLock & lock,
            DataPartsVector * out_covered_parts = nullptr, MergeTreeDeduplicationLog * deduplication_log = nullptr);

    bool renameTempPartInDetachDirecotry(MutableDataPartPtr & new_part, const DataPartPtr & old_part);

    /// Remove parts from working set immediately (without wait for background
    /// process). Transfer part state to temporary. Have very limited usage only
    /// for new parts which aren't already present in table.
    void removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove);

    /// Removes parts from the working set parts.
    /// Parts in add must already be in data_parts with PreCommitted, Committed, or Outdated states.
    /// If clear_without_timeout is true, the parts will be deleted at once, or during the next call to
    /// clearOldParts (ignoring old_parts_lifetime).
    void removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock = nullptr);
    void removePartsFromWorkingSet(const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock & acquired_lock);

    /// Removes all parts from the working set parts
    ///  for which (partition_id = drop_range.partition_id && min_block >= drop_range.min_block && max_block <= drop_range.max_block).
    /// Used in REPLACE PARTITION command;
    DataPartsVector removePartsInRangeFromWorkingSet(const MergeTreePartInfo & drop_range, bool clear_without_timeout,
                                                     DataPartsLock & lock);

    /// Renames the part to detached/<prefix>_<part> and removes it from data_parts,
    //// so it will not be deleted in clearOldParts.
    /// If restore_covered is true, adds to the working set inactive parts, which were merged into the deleted part.
    void forgetPartAndMoveToDetached(const DataPartPtr & part, const String & prefix = "", bool restore_covered = false);

    /// If the part is Obsolete and not used by anybody else, immediately delete it from filesystem and remove from memory.
    void tryRemovePartImmediately(DataPartPtr && part);

    /// Returns old inactive parts that can be deleted. At the same time removes them from the list of parts but not from the disk.
    /// If 'force' - don't wait for old_parts_lifetime.
    DataPartsVector grabOldParts(bool force = false);

    /// Reverts the changes made by grabOldParts(), parts should be in Deleting state.
    void rollbackDeletingParts(const DataPartsVector & parts);

    /// Removes parts from data_parts, they should be in Deleting state
    void removePartsFinally(const DataPartsVector & parts);

    /// Remove parts from data_parts with checking its state, this routine is
    /// used in light weight detach.
    void removePartsFinallyUnsafe(const DataPartsVector & parts, DataPartsLock * acquired_lock = nullptr);

    /// Delete irrelevant parts from memory and disk.
    /// If 'force' - don't wait for old_parts_lifetime.
    void clearOldPartsFromFilesystem(bool force = false);
    void clearPartsFromFilesystem(const DataPartsVector & parts);

    /// Delete WAL files containing parts, that all already stored on disk.
    void clearOldWriteAheadLogs();

    /// Delete all directories which names begin with "tmp"
    /// Set non-negative parameter value to override MergeTreeSettings temporary_directories_lifetime
    /// Must be called with locked lockForShare() because use relative_data_path.
    void clearOldTemporaryDirectories(ssize_t custom_directories_lifetime_seconds = -1);

    void clearEmptyParts();

    /// After the call to dropAllData() no method can be called.
    /// Deletes the data directory and flushes the uncompressed blocks cache and the marks cache.
    void dropAllData();

    /// Drop data directories if they are empty. It is safe to call this method if table creation was unsuccessful.
    void dropIfEmpty();

    /// Moves the entire data directory. Flushes the uncompressed blocks cache
    /// and the marks cache. Must be called with locked lockExclusively()
    /// because changes relative_data_path.
    void rename(const String & new_table_path, const StorageID & new_table_id) override;

    /// Check if the ALTER can be performed:
    /// - all needed columns are present.
    /// - all type conversions can be done.
    /// - columns corresponding to primary key, indices, sign, sampling expression and date are not affected.
    /// If something is wrong, throws an exception.
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    virtual bool supportsClearColumnInPartitionWhere() const { return false; }

    /// Checks if the Mutation can be performed.
    /// (currently no additional checks: always ok)
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    /// Checks that partition name in all commands is valid
    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override;

    /// Change MergeTreeSettings
    void changeSettings(
        const ASTPtr & new_settings,
        TableLockHolder & table_lock_holder);

    /// Should be called if part data is suspected to be corrupted.
    void reportBrokenPart(const String & name) const
    {
        broken_part_callback(name);
    }

    /// TODO (alesap) Duplicate method required for compatibility.
    /// Must be removed.
    static ASTPtr extractKeyExpressionList(const ASTPtr & node)
    {
        return DB::extractKeyExpressionList(node);
    }

    /** Create local backup (snapshot) for parts with specified prefix.
      * Backup is created in directory clickhouse_dir/shadow/i/, where i - incremental number,
      *  or if 'with_name' is specified - backup is created in directory with specified name.
      */
    PartitionCommandsResultInfo freezePartition(
        const ASTPtr & partition,
        const StorageMetadataPtr & metadata_snapshot,
        const String & with_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Freezes all parts.
    PartitionCommandsResultInfo freezeAll(
        const String & with_name,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Unfreezes particular partition.
    PartitionCommandsResultInfo unfreezePartition(
        const ASTPtr & partition,
        const String & backup_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Unfreezes all parts.
    PartitionCommandsResultInfo unfreezeAll(
        const String & backup_name,
        ContextPtr context,
        TableLockHolder & table_lock_holder);

    /// Moves partition to specified Disk
    void movePartitionToDisk(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr context);

    /// Moves partition to specified Volume
    void movePartitionToVolume(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr context);

    void checkPartitionCanBeDropped(const ASTPtr & partition) override;

    void checkPartCanBeDropped(const String & part_name);

    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr query_context,
        const ASTPtr & query) override;

    size_t getColumnCompressedSize(const std::string & name) const
    {
        auto lock = lockPartsRead();
        const auto it = column_sizes.find(name);
        return it == std::end(column_sizes) ? 0 : it->second.data_compressed;
    }

    ColumnSizeByName getColumnSizes() const override
    {
        auto lock = lockPartsRead();
        return column_sizes;
    }

    /// For ATTACH/DETACH/DROP PARTITION.
    String getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr context) const;

    /// Extracts MergeTreeData of other *MergeTree* storage
    ///  and checks that their structure suitable for ALTER TABLE ATTACH PARTITION FROM
    /// Tables structure should be locked.
    MergeTreeData & checkStructureAndGetMergeTreeData(const StoragePtr & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const;
    MergeTreeData & checkStructureAndGetMergeTreeData(IStorage & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const;

    MergeTreeData::MutableDataPartPtr cloneAndLoadDataPartOnSameDisk(
        const MergeTreeData::DataPartPtr & src_part, const String & tmp_part_prefix, const MergeTreePartInfo & dst_part_info, const StorageMetadataPtr & metadata_snapshot);

    virtual std::vector<MergeTreeMutationStatus> getMutationsStatus() const = 0;

    /// Returns true if table can create new parts with adaptive granularity
    /// Has additional constraint in replicated version
    virtual bool canUseAdaptiveGranularity() const
    {
        const auto settings = getSettings();
        return settings->index_granularity_bytes != 0 &&
            (settings->enable_mixed_granularity_parts || !has_non_adaptive_index_granularity_parts);
    }

    /// Get constant pointer to storage settings.
    /// Copy this pointer into your scope and you will
    /// get consistent settings.
    MergeTreeSettingsPtr getSettings() const
    {
        return storage_settings.get();
    }

    /// expose the metastore for metadata management. may return nullptr if enable_metastore is not set.
    MetaStorePtr getMetastore() const {return metastore; }

    /// get metastore path
    String getMetastorePath() const;

    String getRelativeDataPath() const { return relative_data_path; }

    /// Get table path on disk
    String getFullPathOnDisk(const DiskPtr & disk) const;

    /// Get disk where part is located.
    /// `additional_path` can be set if part is not located directly in table data path (e.g. 'detached/')
    DiskPtr getDiskForPart(const String & part_name, const String & additional_path = "") const;

    /// Get full path for part. Uses getDiskForPart and returns the full relative path.
    /// `additional_path` can be set if part is not located directly in table data path (e.g. 'detached/')
    std::optional<String> getFullRelativePathForPart(const String & part_name, const String & additional_path = "") const;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override;

    using PathsWithDisks = std::vector<PathWithDisk>;
    PathsWithDisks getRelativeDataPathsWithDisks() const;

    /// Reserves space at least 1MB.
    ReservationPtr reserveSpace(UInt64 expected_size) const;

    /// Reserves space at least 1MB on specific disk or volume.
    static ReservationPtr reserveSpace(UInt64 expected_size, SpacePtr space);
    static ReservationPtr tryReserveSpace(UInt64 expected_size, SpacePtr space);

    /// Reserves space at least 1MB preferring best destination according to `ttl_infos`.
    ReservationPtr reserveSpacePreferringTTLRules(
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 expected_size,
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move,
        size_t min_volume_index = 0,
        bool is_insert = false,
        DiskPtr selected_disk = nullptr) const;

    ReservationPtr tryReserveSpacePreferringTTLRules(
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 expected_size,
        const IMergeTreeDataPart::TTLInfos & ttl_infos,
        time_t time_of_move,
        size_t min_volume_index = 0,
        bool is_insert = false,
        DiskPtr selected_disk = nullptr) const;

    /// Reserves space for the part based on the distribution of "big parts" in the same partition.
    /// Parts with estimated size larger than `min_bytes_to_rebalance_partition_over_jbod` are
    /// considered as big. The priority is lower than TTL. If reservation fails, return nullptr.
    ReservationPtr balancedReservation(
        const StorageMetadataPtr & metadata_snapshot,
        size_t part_size,
        size_t max_volume_index,
        const String & part_name,
        const MergeTreePartInfo & part_info,
        MergeTreeData::DataPartsVector covered_parts,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        const IMergeTreeDataPart::TTLInfos * ttl_infos,
        bool is_insert = false);

    /// Choose disk with max available free space
    /// Reserves 0 bytes
    ReservationPtr makeEmptyReservationOnLargestDisk() const { return getStoragePolicy()->makeEmptyReservationOnLargestDisk(); }

    Disks getDisksByType(DiskType::Type type) const { return getStoragePolicy()->getDisksByType(type); }

    /// Return alter conversions for part which must be applied on fly.
    AlterConversions getAlterConversionsForPart(MergeTreeDataPartPtr part) const;
    /// Returns destination disk or volume for the TTL rule according to current storage policy
    /// 'is_insert' - is TTL move performed on new data part insert.
    SpacePtr getDestinationForMoveTTL(const TTLDescription & move_ttl, bool is_insert = false) const;

    /// Checks if given part already belongs destination disk or volume for the
    /// TTL rule.
    bool isPartInTTLDestination(const TTLDescription & ttl, const IMergeTreeDataPart & part) const;

    /// Get count of total merges with TTL in MergeList (system.merges) for all
    /// tables (not only current table).
    /// Method is cheap and doesn't require any locks.
    size_t getTotalMergesWithTTLInMergeList() const;

    using WriteAheadLogPtr = std::shared_ptr<MergeTreeWriteAheadLog>;
    WriteAheadLogPtr getWriteAheadLog();

    MergeTreeDataFormatVersion format_version;

    /// Merging params - what additional actions to perform during merge.
    const MergingParams merging_params;

    bool is_custom_partitioned = false;

    /// Used only for old syntax tables. Never changes after init.
    Int64 minmax_idx_date_column_pos = -1; /// In a common case minmax index includes a date column.
    Int64 minmax_idx_time_column_pos = -1; /// In other cases, minmax index often includes a dateTime column.

    /// Get partition key expression on required columns
    static ExpressionActionsPtr getMinMaxExpr(const KeyDescription & partition_key, const ExpressionActionsSettings & settings);
    /// Get column names required for partition key
    static Names getMinMaxColumnsNames(const KeyDescription & partition_key);
    /// Get column types required for partition key
    static DataTypes getMinMaxColumnsTypes(const KeyDescription & partition_key);

    ExpressionActionsPtr getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const;
    ExpressionActionsPtr getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot) const;

    /// Get compression codec for part according to TTL rules and <compression>
    /// section from config.xml.
    CompressionCodecPtr getCompressionCodecForPart(size_t part_size_compressed, const IMergeTreeDataPart::TTLInfos & ttl_infos, time_t current_time) const;

    /// Record current query id where querying the table. Throw if there are already `max_queries` queries accessing the same table.
    void insertQueryIdOrThrow(const String & query_id, size_t max_queries) const;

    /// Remove current query id after query finished.
    void removeQueryId(const String & query_id) const;

    /// Return the partition expression types as a Tuple type. Return DataTypeUInt8 if partition expression is empty.
    DataTypePtr getPartitionValueType() const;

    /// Construct a block consisting only of possible virtual columns for part pruning.
    /// If one_part is true, fill in at most one part.
    Block getBlockWithVirtualPartColumns(const MergeTreeData::DataPartsVector & parts, bool one_part) const;

    /// Limiting parallel sends per one table, used in DataPartsExchange
    std::atomic_uint current_table_sends {0};

    /// For generating names of temporary parts during insertion.
    SimpleIncrement insert_increment;

    bool has_non_adaptive_index_granularity_parts = false;

    /// Parts that currently moving from disk/volume to another.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    /// Moving may conflict with merges and mutations, but this is OK, because
    /// if we decide to move some part to another disk, than we
    /// assuredly will choose this disk for containing part, which will appear
    /// as result of merge or mutation.
    DataParts currently_moving_parts;

    /// Mutex for currently_moving_parts
    mutable std::mutex moving_parts_mutex;

    PinnedPartUUIDsPtr getPinnedPartUUIDs() const;

    /// Schedules background job to like merge/mutate/fetch an executor
    virtual bool scheduleDataProcessingJob(IBackgroundJobExecutor & executor) = 0;
    /// Schedules job to move parts between disks/volumes and so on.
    bool scheduleDataMovingJob(IBackgroundJobExecutor & executor);
    bool areBackgroundMovesNeeded() const;

    /// used for sync metadata manually by `system sync meatadata db.table`
    void syncMetaData();

    void trySyncMetaData();

    /// add wal information to metastore when new wal file is created.
    void addWriteAheadLog(const String & file_name, const DiskPtr & disk) const;

    /// remove wal information from metastore if it is deleted from filesystem.
    void removeWriteAheadLog(const String & file_name) const;

    /// Lock part in zookeeper for use common S3 data in several nodes
    /// Overridden in StorageReplicatedMergeTree
    virtual void lockSharedData(const IMergeTreeDataPart &) const {}

    /// Unlock common S3 data part in zookeeper
    /// Overridden in StorageReplicatedMergeTree
    virtual bool unlockSharedData(const IMergeTreeDataPart &) const { return true; }

    /// Fetch part only if some replica has it on shared storage like S3
    /// Overridden in StorageReplicatedMergeTree
    virtual bool tryToFetchIfShared(const IMergeTreeDataPart &, const DiskPtr &, const String &) { return false; }

    /// Parts that currently submerging (merging to bigger parts) or emerging
    /// (to be appeared after merging finished). These two variables have to be used
    /// with `currently_submerging_emerging_mutex`.
    DataParts currently_submerging_big_parts;
    std::map<String, EmergingPartInfo> currently_emerging_big_parts;
    /// Mutex for currently_submerging_parts and currently_emerging_parts
    mutable std::mutex currently_submerging_emerging_mutex;

    /// Get throttler for replicated fetches
    ThrottlerPtr getFetchesThrottler() const
    {
        return replicated_fetches_throttler;
    }

    /// Get throttler for replicated sends
    ThrottlerPtr getSendsThrottler() const
    {
        return replicated_sends_throttler;
    }

    /// For unique table: perform unique index gc on all parts
    void performUniqueIndexGc();
    /// For unique table: delete old delete files that would never be used any more.
    void clearOldDeleteFilesFromFilesystem();
    /// For unique table: init version_type
    void initVersionType();
    /// For unique table: alter data part, manily used for removing column files from disk
    AlterDataPartTransactionPtr alterDataPartForUniqueTable(const DataPartPtr & part, const NamesAndTypesList & new_columns);
    /// Get required partition vector with query info
    DataPartsVector getRequiredPartitions(const SelectQueryInfo & query_info, ContextPtr context);

    void checkColumnsValidity(const ColumnsDescription & columns) const override;

    // bitengine dictionary mananger
    BitEngineDictionaryManagerPtr bitengine_dictionary_manager;
    inline bool isBitEngineMode() const { return bitengine_dictionary_manager != nullptr; }
    bool isBitEngineEncodeColumn(const String & name) const;

protected:

    friend class IMergeTreeDataPart;
    friend class MergeTreeDataMergerMutator;
    friend struct ReplicatedMergeTreeTableMetadata;
    friend class StorageReplicatedMergeTree;
    friend class MergeTreeDataWriter;
    friend class BitEngineDictionaryManager;

    bool require_part_metadata;

    /// Relative path data, changes during rename for ordinary databases use
    /// under lockForShare if rename is possible.
    String relative_data_path;


    /// Current column sizes in compressed and uncompressed form.
    ColumnSizeByName column_sizes;

    /// Engine-specific methods
    BrokenPartCallback broken_part_callback;

    /// physical address of current object. used as identifier of the MergeTreeData if UUID doesn't exits.
    String storage_address;

    String log_name;
    Poco::Logger * log;

    /// Storage settings.
    /// Use get and set to receive readonly versions.
    MultiVersion<MergeTreeSettings> storage_settings;

    /// Used to determine which UUIDs to send to root query executor for deduplication.
    mutable std::shared_mutex pinned_part_uuids_mutex;
    PinnedPartUUIDsPtr pinned_part_uuids;

    // manage all bitmap index related task
    MergeTreeBitmapIndexPtr bitmap_index;

    // manage all mark bitmap index related task
    MergeTreeMarkBitmapIndexPtr mark_bitmap_index;

    /// Work with data parts

    struct TagByInfo{};
    struct TagByStateAndInfo{};

    static const MergeTreePartInfo & dataPartPtrToInfo(const DataPartPtr & part)
    {
        return part->info;
    }

    static DataPartStateAndInfo dataPartPtrToStateAndInfo(const DataPartPtr & part)
    {
        return {part->getState(), part->info};
    }

    using DataPartsIndexes = boost::multi_index_container<DataPartPtr,
        boost::multi_index::indexed_by<
            /// Index by Info
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByInfo>,
                boost::multi_index::global_fun<const DataPartPtr &, const MergeTreePartInfo &, dataPartPtrToInfo>
            >,
            /// Index by (State, Info), is used to obtain ordered slices of parts with the same state
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByStateAndInfo>,
                boost::multi_index::global_fun<const DataPartPtr &, DataPartStateAndInfo, dataPartPtrToStateAndInfo>,
                LessStateDataPart
            >
        >
    >;

    /// Current set of data parts.
    mutable std::shared_mutex data_parts_mutex;
    DataPartsIndexes data_parts_indexes;
    DataPartsIndexes::index<TagByInfo>::type & data_parts_by_info;
    DataPartsIndexes::index<TagByStateAndInfo>::type & data_parts_by_state_and_info;

    MergeTreePartsMover parts_mover;

    using DataPartIteratorByInfo = DataPartsIndexes::index<TagByInfo>::type::iterator;
    using DataPartIteratorByStateAndInfo = DataPartsIndexes::index<TagByStateAndInfo>::type::iterator;

    boost::iterator_range<DataPartIteratorByStateAndInfo> getDataPartsStateRange(DataPartState state) const
    {
        auto begin = data_parts_by_state_and_info.lower_bound(state, LessStateDataPart());
        auto end = data_parts_by_state_and_info.upper_bound(state, LessStateDataPart());
        return {begin, end};
    }

    boost::iterator_range<DataPartIteratorByInfo> getDataPartsPartitionRange(const String & partition_id) const
    {
        auto begin = data_parts_by_info.lower_bound(PartitionID(partition_id), LessDataPart());
        auto end = data_parts_by_info.upper_bound(PartitionID(partition_id), LessDataPart());
        return {begin, end};
    }

    std::optional<UInt64> totalRowsByPartitionPredicateImpl(
        const SelectQueryInfo & query_info, ContextPtr context, const DataPartsVector & parts) const;

    static decltype(auto) getStateModifier(DataPartState state)
    {
        return [state] (const DataPartPtr & part) { part->setState(state); };
    }

    void modifyPartState(DataPartIteratorByStateAndInfo it, DataPartState state)
    {
        if (!data_parts_by_state_and_info.modify(it, getStateModifier(state)))
            throw Exception("Can't modify " + (*it)->getNameWithState(), ErrorCodes::LOGICAL_ERROR);
    }

    void modifyPartState(DataPartIteratorByInfo it, DataPartState state)
    {
        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception("Can't modify " + (*it)->getNameWithState(), ErrorCodes::LOGICAL_ERROR);
    }

    void modifyPartState(const DataPartPtr & part, DataPartState state)
    {
        auto it = data_parts_by_info.find(part->info);
        if (it == data_parts_by_info.end() || (*it).get() != part.get())
            throw Exception("Part " + part->name + " doesn't exist", ErrorCodes::LOGICAL_ERROR);

        if (!data_parts_by_state_and_info.modify(data_parts_indexes.project<TagByStateAndInfo>(it), getStateModifier(state)))
            throw Exception("Can't modify " + (*it)->getNameWithState(), ErrorCodes::LOGICAL_ERROR);
    }

    /// unique table only
    /// -----------------
    UniqueTableState unique_table_state {UniqueTableState::INIT};
    /// source of truth for active parts and the corresponding delete files,
    /// used to achieve atomic updates of parts + delete files as well as
    /// eventually consistency among replicas
    ManifestStorePtr manifest_store;
    /// an outdated part can be removed only if its removed version <= `unique_commit_version'.
    /// when a version is considered committed depends on the actual implementation. E.g.,
    /// for HaUniqueMergeTree, a version is committed only if all replicas have committed that version.
    std::atomic<UInt64> unique_commit_version {0};
    /// used to serialize write and merge
    mutable std::mutex unique_write_mutex;
    std::atomic<bool> disable_delete_file_gc {false};
    /// make sure only one thread can perform delete file gc at a time
    mutable std::mutex delete_file_gc_mutex;

    std::shared_ptr<DiskUniqueKeyIndexCache> unique_key_index_cache;
    std::shared_ptr<DiskUniqueRowStoreCache> unique_row_store_cache;

    /// write lock for unique table to prevent concurrent insert & merge.
    /// lock order: merge select lock -> table structure lock -> unique write lock
    using UniqueWriteLock = std::unique_lock<std::mutex>;
    UniqueWriteLock uniqueWriteLock() const { return UniqueWriteLock(unique_write_mutex); }
    /// Used to serialize calls to grabOldParts.
    std::mutex grab_old_parts_mutex;
    /// The same for clearOldTemporaryDirectories.
    std::mutex clear_old_temporary_directories_mutex;

    void checkProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach = false) const;

    void setProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach = false);

    void checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key);

    void checkTTLExpressions(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata) const;

    void checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const;

    /// Calculates column sizes in compressed form for the current state of data_parts. Call with data_parts mutex locked.
    void calculateColumnSizesImpl();
    /// Adds or subtracts the contribution of the part to compressed column sizes.
    void addPartContributionToColumnSizes(const DataPartPtr & part);
    void removePartContributionToColumnSizes(const DataPartPtr & part);

    /// If there is no part in the partition with ID `partition_id`, returns empty ptr. Should be called under the lock.
    DataPartPtr getAnyPartInPartition(const String & partition_id, DataPartsLock & data_parts_lock) const;

    /// Return parts in the Committed set that are covered by the new_part_info or the part that covers it.
    /// Will check that the new part doesn't already exist and that it doesn't intersect existing part.
    DataPartsVector getActivePartsToReplace(
        const MergeTreePartInfo & new_part_info,
        const String & new_part_name,
        DataPartPtr & out_covering_part,
        DataPartsLock & data_parts_lock) const;

    void renamePartAndDropMetadata(const String& name, const DataPartPtr& sourcePart);
    void renamePartAndInsertMetadata(const String& name, const DataPartPtr& sourcePart);

    /// Checks whether the column is in the primary key, possibly wrapped in a chain of functions with single argument.
    bool isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node, const StorageMetadataPtr & metadata_snapshot) const;

    /// Common part for |freezePartition()| and |freezeAll()|.
    using MatcherFn = std::function<bool(const String &)>;
    PartitionCommandsResultInfo freezePartitionsByMatcher(MatcherFn matcher, const StorageMetadataPtr & metadata_snapshot, const String & with_name, ContextPtr context);
    PartitionCommandsResultInfo unfreezePartitionsByMatcher(MatcherFn matcher, const String & backup_name, ContextPtr context);

    // Partition helpers
    bool canReplacePartition(const DataPartPtr & src_part) const;

    /// Tries to drop part in background without any waits or throwing exceptions in case of errors.
    virtual void dropPartNoWaitNoThrow(const String & part_name) = 0;

    virtual void dropPart(const String & part_name, bool detach, ContextPtr context) = 0;
    virtual void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context, const ASTPtr & query) = 0;
    virtual void dropPartitionWhere(const ASTPtr & predicate, bool detach, ContextPtr context, const ASTPtr & query);
    virtual PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) = 0;
    virtual void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) = 0;
    virtual void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) = 0;

    virtual void fetchPartition(
        const ASTPtr & partition,
        const StorageMetadataPtr & metadata_snapshot,
        const String & from,
        bool fetch_part,
        ContextPtr query_context);
    virtual void fetchPartitionWhere(
        const ASTPtr & predicate, const StorageMetadataPtr & metadata_snapshot, const String & from, ContextPtr query_context);

    virtual void repairPartition(const ASTPtr & partition, bool part, const String & from, ContextPtr);

    virtual void movePartitionToShard(const ASTPtr & partition, bool move_part, const String & to, ContextPtr query_context);

    virtual void movePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, ContextPtr context);

    virtual void ingestPartition(const PartitionCommand & /*command*/, ContextPtr /*context*/) { throw Exception("IngestPartition not implement", ErrorCodes::NOT_IMPLEMENTED); }

    virtual void samplePartitionWhere(
                            const String /*dst_database*/, 
                            const String /*dst_table*/,
                            const ASTPtr & /*sharding_expression*/, 
                            const ASTPtr & /*predicate*/,
                            const ContextPtr & /*query_context*/) { throw Exception("Sample Partition not implement", ErrorCodes::NOT_IMPLEMENTED); }

    void preattachPartition(const ASTPtr & partition, ContextPtr context);
    virtual void bitengineRecodePartition(const ASTPtr & partition, bool detach, ContextPtr context, bool can_skip);
    virtual void bitengineRecodePartitionWhere(const ASTPtr & predicate, bool detach, ContextPtr context, bool can_skip);

    void writePartLog(
        PartLogElement::Type type,
        const ExecutionStatus & execution_status,
        UInt64 elapsed_ns,
        const String & new_part_name,
        const DataPartPtr & result_part,
        const DataPartsVector & source_parts,
        const MergeListEntry * merge_entry);

    /// If part is assigned to merge or mutation (possibly replicated)
    /// Should be overridden by children, because they can have different
    /// mechanisms for parts locking
    virtual bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const = 0;

    /// Return most recent mutations commands for part which weren't applied
    /// Used to receive AlterConversions for part and apply them on fly. This
    /// method has different implementations for replicated and non replicated
    /// MergeTree because they store mutations in different way.
    virtual MutationCommands getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const = 0;
    /// Moves part to specified space, used in ALTER ... MOVE ... queries
    bool movePartsToSpace(const DataPartsVector & parts, SpacePtr space);

    /// Throttlers used in DataPartsExchange to lower maximum fetch/sends
    /// speed.
    ThrottlerPtr replicated_fetches_throttler;
    ThrottlerPtr replicated_sends_throttler;

    MetaStorePtr metastore;

private:
    /// RAII Wrapper for atomic work with currently moving parts
    /// Acquire them in constructor and remove them in destructor
    /// Uses data.currently_moving_parts_mutex
    struct CurrentlyMovingPartsTagger
    {
        MergeTreeMovingParts parts_to_move;
        MergeTreeData & data;
        CurrentlyMovingPartsTagger(MergeTreeMovingParts && moving_parts_, MergeTreeData & data_);

        ~CurrentlyMovingPartsTagger();
    };

    using CurrentlyMovingPartsTaggerPtr = std::shared_ptr<CurrentlyMovingPartsTagger>;

    /// Move selected parts to corresponding disks
    bool moveParts(const CurrentlyMovingPartsTaggerPtr & moving_tagger);

    void syncMetaImpl(DataPartsLock &);

    /// Select parts for move and disks for them. Used in background moving processes.
    CurrentlyMovingPartsTaggerPtr selectPartsForMove();

    /// Check selected parts for movements. Used by ALTER ... MOVE queries.
    CurrentlyMovingPartsTaggerPtr checkPartsForMove(const DataPartsVector & parts, SpacePtr space);

    bool canUsePolymorphicParts(const MergeTreeSettings & settings, String * out_reason = nullptr) const;

    /// collect all existing parts as well as wals from filesystem.
    void searchAllPartsOnFilesystem(std::map<String, DiskPtr> & parts_with_disks, std::map<String, DiskPtr> & wal_with_disks) const;

    std::mutex write_ahead_log_mutex;
    WriteAheadLogPtr write_ahead_log;

    virtual void startBackgroundMovesIfNeeded() = 0;

    bool allow_nullable_key{};

    bool enable_metastore{};

    void addPartContributionToDataVolume(const DataPartPtr & part);
    void removePartContributionToDataVolume(const DataPartPtr & part);

    void increaseDataVolume(size_t bytes, size_t rows, size_t parts);
    void decreaseDataVolume(size_t bytes, size_t rows, size_t parts);

    void setDataVolume(size_t bytes, size_t rows, size_t parts);

    std::atomic<size_t> total_active_size_bytes = 0;
    std::atomic<size_t> total_active_size_rows = 0;
    std::atomic<size_t> total_active_size_parts = 0;

    // Record all query ids which access the table. It's guarded by `query_id_set_mutex` and is always mutable.
    mutable std::set<String> query_id_set;
    mutable std::mutex query_id_set_mutex;

    // Get partition matcher for FREEZE / UNFREEZE queries.
    MatcherFn getPartitionMatcher(const ASTPtr & partition, ContextPtr context) const;

    /// Returns default settings for storage with possible changes from global config.
    virtual std::unique_ptr<MergeTreeSettings> getDefaultSettings() const = 0;
};

/// RAII struct to record big parts that are submerging or emerging.
/// It's used to calculate the balanced statistics of JBOD array.
struct CurrentlySubmergingEmergingTagger
{
    MergeTreeData & storage;
    String emerging_part_name;
    MergeTreeData::DataPartsVector submerging_parts;
    Poco::Logger * log;

    CurrentlySubmergingEmergingTagger(
        MergeTreeData & storage_, const String & name_, MergeTreeData::DataPartsVector && parts_, Poco::Logger * log_)
        : storage(storage_), emerging_part_name(name_), submerging_parts(std::move(parts_)), log(log_)
    {
    }

    ~CurrentlySubmergingEmergingTagger();
};

}
