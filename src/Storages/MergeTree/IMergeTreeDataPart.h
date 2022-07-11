#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Core/Block.h>
#include <common/types.h>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeProjections.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeDataPartVersions.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Transaction/TxnTimestamp.h>
#include <Storages/UniqueKeyIndex.h>
#include <Storages/UniqueRowStore.h>
#include <Poco/Path.h>
#include <Common/HashTable/HashMap.h>
#include <common/types.h>
#include <roaring.hh>


namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class IMergeTreeDataPart;
using IMergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
struct ColumnSize;
class MergeTreeMetaBase;
class MergeTreeData;
struct FutureMergedMutatedPart;
class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class IMergeTreeReader;
class IMergeTreeDataPartWriter;
class MarkCache;
class UncompressedCache;

using Roaring = roaring::Roaring;
using DeleteBitmapPtr = std::shared_ptr<const Roaring>;
using MutableDeleteBitmapPtr = std::shared_ptr<Roaring>;
using DeleteBitmapsVector = std::vector<DeleteBitmapPtr>;

/// Description of the data part.
class IMergeTreeDataPart : public std::enable_shared_from_this<IMergeTreeDataPart>
{
public:
    //static constexpr auto DATA_FILE_EXTENSION = ".bin";
    static constexpr UInt64 NOT_INITIALIZED_COMMIT_TIME = 0;

    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;
    using ChecksumsPtr = std::shared_ptr<Checksums>;
    using ValueSizeMap = std::map<std::string, double>;

    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    using MergeTreeWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;

    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    using NameToNumber = std::unordered_map<std::string, size_t>;

    using Index = Columns;
    using IndexPtr = std::shared_ptr<Index>;

    using Type = MergeTreeDataPartType;

    using Versions = std::shared_ptr<MergeTreeDataPartVersions>;

    IMergeTreeDataPart(
        const MergeTreeMetaBase & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path,
        Type part_type_,
        const IMergeTreeDataPart * parent_part_);

    IMergeTreeDataPart(
        MergeTreeMetaBase & storage_,
        const String & name_,
        const VolumePtr & volume,
        const std::optional<String> & relative_path,
        Type part_type_,
        const IMergeTreeDataPart * parent_part_);

    virtual MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns_,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        MergeTreeBitMapIndexReader * bitmap_index_reader = nullptr,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = ReadBufferFromFileBase::ProfileCallback{}) const = 0;

    virtual MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity = {}) const = 0;

    virtual bool isStoredOnDisk() const = 0;

    virtual bool supportsVerticalMerge() const { return false; }

    /// NOTE: Returns zeros if column files are not found in checksums.
    /// Otherwise return information about column size on disk.
    ColumnSize getColumnSize(const String & column_name, const IDataType & /* type */) const;

    /// Return information about column size on disk for all columns in part
    ColumnSize getTotalColumnsSize() const { return total_columns_size; }

    virtual String getFileNameForColumn(const NameAndTypePair & column) const = 0;

    virtual ~IMergeTreeDataPart();

    using ColumnToSize = std::map<std::string, UInt64>;
    /// Populates columns_to_size map (compressed size).
    void accumulateColumnSizes(ColumnToSize & /* column_to_size */) const;

    Type getType() const { return part_type; }

    String getTypeName() const { return getType().toString(); }

    virtual void setColumns(const NamesAndTypesList & new_columns);

    const NamesAndTypesList & getColumns() const { return columns; }
    NamesAndTypesList getNamesAndTypes() const { return columns; }

    /// Throws an exception if part is not stored in on-disk format.
    void assertOnDisk() const;

    void remove() const;

    void projectionRemove(const String & parent_to, bool keep_shared_data = false) const;

    /// Initialize columns (from columns.txt if exists, or create from column files if not).
    /// Load checksums from checksums.txt if exists. Load index if required.
    virtual void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency);

    String getMarksFileExtension() const { return index_granularity_info.marks_file_extension; }

    /// Generate the new name for this part according to `new_part_info` and min/max dates from the old name.
    /// This is useful when you want to change e.g. block numbers or the mutation version of the part.
    String getNewName(const MergeTreePartInfo & new_part_info) const;

    /// Returns column position in part structure or std::nullopt if it's missing in part.
    ///
    /// NOTE: Doesn't take column renames into account, if some column renames
    /// take place, you must take original name of column for this part from
    /// storage and pass it to this method.
    std::optional<size_t> getColumnPosition(const String & column_name) const;

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinimumCompressedSize(const StorageMetadataPtr & metadata_snapshot) const;

    bool contains(const IMergeTreeDataPart & other) const { return info.contains(other.info); }

    /// If the partition key includes date column (a common case), this function will return min and max values for that column.
    std::pair<DayNum, DayNum> getMinMaxDate() const;

    /// otherwise, if the partition key includes dateTime column (also a common case), this function will return min and max values for that column.
    std::pair<time_t, time_t> getMinMaxTime() const;

    bool isEmpty() const { return rows_count == 0; }

    /// Compute part block id for zero level part. Otherwise throws an exception.
    String getZeroLevelPartBlockID() const;

    auto const & get_name() const { return name; }
    auto const & get_info() const { return info; }
    auto const & get_partition() const { return partition; }
    auto const & get_deleted() const { return deleted; }
    auto const & get_commit_time() const { return commit_time; }

    const MergeTreeMetaBase & storage;

    String name;
    MergeTreePartInfo info;

    /// MOCK for MergeTreeCNCHDataDumper
    IndexPtr prepared_index;

    ChecksumsPtr prepared_checksums;

    std::atomic<bool> has_bitmap {false};

    /// Part unique identifier.
    /// The intention is to use it for identifying cases where the same part is
    /// processed by multiple shards.
    UUID uuid = UUIDHelpers::Nil;

    VolumePtr volume;

    /// A directory path (relative to storage's path) where part data is actually stored
    /// Examples: 'detached/tmp_fetch_<name>', 'tmp_<name>', '<name>'
    mutable String relative_path;
    MergeTreeIndexGranularityInfo index_granularity_info;

    size_t rows_count = 0;


    time_t modification_time = 0;
    /// When the part is removed from the working set. Changes once.
    mutable std::atomic<time_t> remove_time { std::numeric_limits<time_t>::max() };

    /// If true, the destructor will delete the directory with the part.
    bool is_temp = false;

    /// If true it means that there are no ZooKeeper node for this part, so it should be deleted only from filesystem
    bool is_duplicate = false;

    /// If true it means that this part is under recoding so that we need take care to read this part when the query
    /// has been rewritten.
    mutable std::atomic<bool> is_encoding {false};

    /// Frozen by ALTER TABLE ... FREEZE ... It is used for information purposes in system.parts table.
    mutable std::atomic<bool> is_frozen {false};

    /// Flag for keep S3 data when zero-copy replication over S3 turned on.
    mutable bool force_keep_shared_data = false;

    /**
     * Part state is a stage of its lifetime. States are ordered and state of a part could be increased only.
     * Part state should be modified under data_parts mutex.
     *
     * Possible state transitions:
     * Temporary -> Precommitted:    we are trying to commit a fetched, inserted or merged part to active set
     * Precommitted -> Outdated:     we could not add a part to active set and are doing a rollback (for example it is duplicated part)
     * Precommitted -> Committed:    we successfully committed a part to active dataset
     * Precommitted -> Outdated:     a part was replaced by a covering part or DROP PARTITION
     * Outdated -> Deleting:         a cleaner selected this part for deletion
     * Deleting -> Outdated:         if an ZooKeeper error occurred during the deletion, we will retry deletion
     * Committed -> DeleteOnDestroy: if part was moved to another disk
     */
    enum class State
    {
        Temporary,       /// the part is generating now, it is not in data_parts list
        PreCommitted,    /// the part is in data_parts, but not used for SELECTs
        Committed,       /// active data part, used by current and upcoming SELECTs
        Outdated,        /// not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes
        Deleting,        /// not active data part with identity refcounter, it is deleting right now by a cleaner
        DeleteOnDestroy, /// part was moved to another disk and should be deleted in own destructor
    };

    static constexpr auto all_part_states =
    {
        State::Temporary, State::PreCommitted, State::Committed, State::Outdated, State::Deleting,
        State::DeleteOnDestroy
    };

    using TTLInfo = MergeTreeDataPartTTLInfo;
    using TTLInfos = MergeTreeDataPartTTLInfos;

    TTLInfos ttl_infos;

    /// Current state of the part. If the part is in working set already, it should be accessed via data_parts mutex
    void setState(State new_state) const;
    State getState() const;

    /// Returns name of state
    static String stateToString(State state);
    String stateString() const;

    String getNameWithState() const
    {
        return name + " (state " + stateString() + ")";
    }

    /// Returns true if state of part is one of affordable_states
    bool checkState(const std::initializer_list<State> & affordable_states) const
    {
        for (auto affordable_state : affordable_states)
        {
            if (state == affordable_state)
                return true;
        }
        return false;
    }

    /// Throws an exception if state of the part is not in affordable_states
    void assertState(const std::initializer_list<State> & affordable_states) const;

    /// Primary key (correspond to primary.idx file).
    /// Always loaded in RAM. Contains each index_granularity-th value of primary key tuple.
    /// Note that marks (also correspond to primary key) is not always in RAM, but cached. See MarkCache.h.
    IndexPtr index = std::make_shared<Columns>();

    MergeTreePartition partition;

    /// Amount of rows between marks
    /// As index always loaded into memory
    MergeTreeIndexGranularity index_granularity;

    /// Index that for each part stores min and max values of a set of columns. This allows quickly excluding
    /// parts based on conditions on these columns imposed by a query.
    /// Currently this index is built using only columns required by partition expression, but in principle it
    /// can be built using any set of columns.
    struct MinMaxIndex
    {
        /// A direct product of ranges for each key column. See Storages/MergeTree/KeyCondition.cpp for details.
        std::vector<Range> hyperrectangle;
        bool initialized = false;

    public:
        MinMaxIndex() = default;

        /// For month-based partitioning.
        MinMaxIndex(DayNum min_date, DayNum max_date)
            : hyperrectangle(1, Range(min_date, true, max_date, true))
            , initialized(true)
        {
        }

        void load(const MergeTreeMetaBase & data, const DiskPtr & disk_, const String & part_path);
        void load(const MergeTreeMetaBase & data, ReadBuffer & buf);
        void store(const MergeTreeMetaBase & data, const DiskPtr & disk_, const String & part_path, Checksums & checksums) const;
        void store(const Names & column_names, const DataTypes & data_types, const DiskPtr & disk_, const String & part_path, Checksums & checksums) const;
        void store(const MergeTreeMetaBase & data, const String & part_path, WriteBuffer & buf) const;

        void update(const Block & block, const Names & column_names);
        void merge(const MinMaxIndex & other);
    };

    MinMaxIndex minmax_idx;


	Versions versions;

    /// only be used if the storage enables persistent checksums.
    ChecksumsPtr checksums_ptr {nullptr};

    /// Columns with values, that all have been zeroed by expired ttl
    NameSet expired_columns;

    CompressionCodecPtr default_codec;

    /// load checksum on demand. return ChecksumsPtr from global cache or its own checksums_ptr;
    ChecksumsPtr getChecksums() const;

    /// Get primary index, load if primary index is not initialized.
    IndexPtr getIndex() const;

    /// For data in RAM ('index')
    UInt64 getIndexSizeInBytes() const;
    UInt64 getIndexSizeInAllocatedBytes() const;
    UInt64 getMarksCount() const;

    UInt64 getBytesOnDisk() const { return bytes_on_disk; }
    void setBytesOnDisk(UInt64 bytes_on_disk_) { bytes_on_disk = bytes_on_disk_; }

    size_t getFileSizeOrZero(const String & file_name) const;
    off_t getFileOffsetOrZero(const String & file_name) const;

    /// Returns path to part dir relatively to disk mount point
    String getFullRelativePath() const;

    /// Returns full path to part dir
    String getFullPath() const;

    /// MOCK for MergeTreeCNCHDataDumper
    void setPreparedIndex(IndexPtr index_) { prepared_index = std::move(index_); }
    const IndexPtr & getPreparedIndex() const { return prepared_index; }

    /// Moves a part to detached/ directory and adds prefix to its name
    void renameToDetached(const String & prefix) const;

    /// Makes checks and move part to new directory
    /// Changes only relative_dir_name, you need to update other metadata (name, is_temp) explicitly
    virtual void renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const;

    /// Makes clone of a part in detached/ directory via hard links
    virtual void makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const;

    /// Makes full clone of part in specified subdirectory (relative to storage data directory, e.g. "detached") on another disk
    void makeCloneOnDisk(const DiskPtr & disk, const String & directory_name) const;

    /// Check if there is only one map column not kv store and enable_compact_map_data.
    bool hasOnlyOneCompactedMapColumnNotKV() const;

    /// Checks that .bin and .mrk files exist.
    ///
    /// NOTE: Doesn't take column renames into account, if some column renames
    /// take place, you must take original name of column for this part from
    /// storage and pass it to this method.
    virtual bool hasColumnFiles(const NameAndTypePair & /* column */) const { return false; }

    /// Returns true if this part shall participate in merges according to
    /// settings of given storage policy.
    bool shallParticipateInMerges(const StoragePolicyPtr & storage_policy) const;

    /// Calculate the total size of the entire directory with all the files
    static UInt64 calculateTotalSizeOnDisk(const DiskPtr & disk_, const String & from);
    void calculateColumnsSizesOnDisk();

    String getRelativePathForPrefix(const String & prefix) const;

    bool isProjectionPart() const { return parent_part != nullptr; }

    const IMergeTreeDataPart * getParentPart() const { return parent_part; }

    const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & getProjectionParts() const { return projection_parts; }

    void addProjectionPart(const String & projection_name, std::shared_ptr<IMergeTreeDataPart> && projection_part)
    {
        projection_parts.emplace(projection_name, std::move(projection_part));
    }

    bool hasProjection(const String & projection_name) const
    {
        return projection_parts.find(projection_name) != projection_parts.end();
    }

    void loadProjections(bool require_columns_checksums, bool check_consistency);

    /// Return set of metadat file names without checksums. For example,
    /// columns.txt or checksums.txt itself.
    NameSet getFileNamesWithoutChecksums() const;

    /// File with compression codec name which was used to compress part columns
    /// by default. Some columns may have their own compression codecs, but
    /// default will be stored in this file.
    static inline constexpr auto DEFAULT_COMPRESSION_CODEC_FILE_NAME = "default_compression_codec.txt";

    static inline constexpr auto DELETE_ON_DESTROY_MARKER_FILE_NAME = "delete-on-destroy.txt";

    static inline constexpr auto UUID_FILE_NAME = "uuid.txt";

    /// Checks that all TTLs (table min/max, column ttls, so on) for part
    /// calculated. Part without calculated TTL may exist if TTL was added after
    /// part creation (using alter query with materialize_ttl setting).
    bool checkAllTTLCalculated(const StorageMetadataPtr & metadata_snapshot) const;

    /// Returns serialization for column according to files in which column is written in part.
    SerializationPtr getSerializationForColumn(const NameAndTypePair & column) const;

    /// Return some uniq string for file
    /// Required for distinguish different copies of the same part on S3
    String getUniqueId() const;

    /// UniqueMergeTree-only
    /// --------------------
    DeleteBitmapPtr getDeleteBitmap() const;

    UInt64 getDeleteVersion() const
    {
        return delete_version.load(std::memory_order_relaxed);
    }

    String getDeleteFilePathWithVersion(UInt64 version) const
    {
        return getFullPath() + delete_file_prefix + "." + std::to_string(version);
    }

    String getDeleteFilePath() const;

    void writeDeleteFileWithVersion(const DeleteBitmapPtr & bitmap, UInt64 version) const;
    DeleteBitmapPtr readDeleteFileWithVersion(UInt64 version, bool log_on_error = true) const;
    std::vector<UInt64> listDeleteFiles(UInt64 min_version) const;

    void writeDeleteFile(const DeleteBitmapPtr & bitmap, bool sync);
    DeleteBitmapPtr readDeleteFile(bool log_on_error = true) const;

    /// For unique table: when attach a new part into table, we need to allocate a new lsn
    /// and rename the delete file to this new version.
    void renameDeleteFileToVersion(UInt64 version) const;
    /// For unique table: remove unneeded delete files according to current table commit version.
    /// If nothing is performed, set `retry_next_time' to true and return 0.
    /// Otherwise set `retry_next_time' to false and return number of delete files removed.
    UInt32 clearOldDeleteFilesIfNeeded(UInt64 commit_version, bool & retry_next_time) const;
    /// For unique table: remove all unneeded delete files expect the current used one.
    UInt32 clearDeleteFilesExceptCurrentVersion() const;

    /// If current index contains too many deleted items or hasn't been used for a long time, do garbage collection to save memory.
    /// If force_unload is true, unload the index anyway.
    /// Return number of memory bytes freed.
    UInt64 gcUniqueIndexIfNeeded(const time_t * now = nullptr, bool force_unload = false);

    size_t getUniqueIndexSize() const { return unique_index_size.load(std::memory_order_relaxed); }

    size_t getUniqueIndexMemorySize() const { return unique_index_memory_size.load(std::memory_order_relaxed); }

    /// If uki type is "UNKNOWN", change it to either "MEMORY" or "DISK" depending on whether uki file exists.
    UniqueKeyIndexPtr getUniqueKeyIndex() const;

    /// If row store file exists, return row store. Otherwise, return nullptr.
    UniqueRowStorePtr tryGetUniqueRowStore() const;

    /// If row store meta file exists, return row store meta. Otherwise, return nullpter.
    UniqueRowStoreMetaPtr tryGetUniqueRowStoreMeta() const;

    void setUniqueRowStoreMeta(UniqueRowStoreMetaPtr row_store_meta);

    /// If `key' is found, return true and set its corresponding `rowid' and optional `version' and `is_offline'.
    /// Otherwise return false.
    bool getValueFromUniqueIndex(
        const UniqueKeyIndexPtr & unique_key_index,
        const String & key,
        UInt32 & rowid,
        UInt64 * version = nullptr,
        UInt8 * is_offline = nullptr) const;

    /// When storage.merging_params.partitionValueAsVersion() == true,
    /// use partition value as version for all rows in this part.
    /// should be accessed via getVersionFromPartition()
    mutable std::optional<UInt64> version_from_partition;
    /// Return version value from partition. Throws exception if the table didn't use partition as version
    UInt64 getVersionFromPartition() const;

    MemoryUniqueKeyIndexPtr memory_unique_index;
    std::atomic<size_t> unique_index_size{0};
    std::atomic<size_t> unique_index_memory_size{0};
    /// type of unique key index
    mutable UkiType uki_type = UkiType::UNKNOWN;

    // FIXME (UNIQUE KEY): Put this into metainfo leter
    mutable std::mutex unique_index_mutex;

    /// Stats used by GC thread
    struct DeleteFilesStat
    {
        std::atomic<time_t> last_add_time {0};
        /// the following fields are only used by single gc thread, thereby no synchronization is needed
        /// last time the GC thread scans part's dir for delete files
        time_t last_scan_dir_time {0};
        /// cached versions (in ascending order) of delete files in this part
        std::vector<UInt64> versions;
    };
    mutable DeleteFilesStat delete_files_stat;
    static constexpr auto delete_file_prefix = "delete";
    static constexpr UInt8 delete_file_format_version = 1;

    UInt64 deleteBitmapVersion() const { return delete_version.load(std::memory_order_acquire); }
    UInt32 numDeletedRows() const;
    size_t estimateSizeRemovingDeletes() const
    {
        auto num_deletes = numDeletedRows();
        if (num_deletes == 0)
            return bytes_on_disk;
        double ratio = 1.0 - num_deletes / static_cast<double>(rows_count);
        return static_cast<size_t>(bytes_on_disk * std::max(0.0, ratio));
    }
    UInt32 numRowsRemovingDeletes() const
    {
        auto num_deletes = numDeletedRows();
        if (rows_count < num_deletes)
            throw Exception("Part " + name + " has " + toString(num_deletes) + " deleted rows but only " + toString(rows_count) + " total rows", ErrorCodes::LOGICAL_ERROR);
        return rows_count - num_deletes;
    }

    /// Caller must hold DataPartsLock
    void setDeleteBitmapWithVersion(const DeleteBitmapPtr & new_delete, UInt64 version) const
    {
        delete_bitmap = new_delete;
        delete_rows = delete_bitmap ? delete_bitmap->cardinality() : 0;
        delete_version.store(version, std::memory_order_release);
        delete_files_stat.last_add_time.store(time(nullptr), std::memory_order_relaxed);
    }

    /// If Checksum has not been initialized, load it from filesystem(local/remote).
    // void loadChecksumsIfNeed();

    void loadMemoryUniqueIndex(const std::unique_lock<std::mutex> & unique_index_lock);

    /// Return disk unique key index (corresponding to unique_key.idx) if the part has unique key.
    DiskUniqueKeyIndexPtr loadDiskUniqueIndex();

    /// Return unique row store (corresponding to row_store.data) if the part has unique row store.
    UniqueRowStorePtr loadUniqueRowStore();

    String ALWAYS_INLINE getMemoryAddress() const
    {
        auto addr = reinterpret_cast<uintptr_t>(this);
        return String(reinterpret_cast<char *>(&addr), sizeof(addr));
    }

    bool containsExactly(const IMergeTreeDataPart & other) const
    {
        return info.partition_id == other.info.partition_id && info.min_block == other.info.min_block
            && info.max_block == other.info.max_block && info.level >= other.info.level && commit_time >= other.commit_time;
    }

    void setPreviousPart(IMergeTreeDataPartPtr part) const { prev_part = std::move(part); }
    const IMergeTreeDataPartPtr & tryGetPreviousPart() const { return prev_part; }
    IMergeTreeDataPartPtr getBasePart() const;

    bool isPartial() const { return info.hint_mutation; }

    /// FIXME: move to PartMetaEntry once metastore is added
    /// Used to prevent concurrent modification to a part.
    /// Can be removed once all data modification tasks (e.g, build bitmap index, recode) are
    /// implemented as mutation commands and parts become immutable.
    mutable std::mutex mutate_mutex;

    /// Delete bitmap is a bitmap containing the row numbers of deleted rows in this part.
    /// It's populated from the "delete file" on disk and is used to filter out deleted rows when reading from the part.
    ///
    /// Used by UniqueMergeTree to cache the latest version of delete bitmap.
    /// Changes should be done via `setDeleteBitmapWithVersion(..)' under the DataPartsLock.
    mutable DeleteBitmapPtr delete_bitmap;
    /// Cache the cardinality of delete bitmap so that
    /// 1) for UniqueMergeTree, caller can get numDeletedRows() without acquiring parts lock.
    /// 2) for ordinary MergeTree, we don't need to load the delete bitmap each time numDeleteRows() is called.
    mutable std::atomic<UInt32> delete_rows {0};
    /// FIXME (UNIQUE KEY): Put this into metastore later
    mutable std::atomic<UInt64> delete_version {0};
    /// Whether the part has inserted the delete bitmap into cache before.
    /// Used by ordinal MergeTree
    std::atomic<bool> is_delete_bitmap_cached = false;
    /// --------------------
    /// Total size on disk, not only columns. May not contain size of
    /// checksums.txt and columns.txt. 0 - if not counted;
    UInt64 bytes_on_disk{0};

    TxnTimestamp columns_commit_time;
    TxnTimestamp mutation_commit_time;

    UInt64 staging_txn_id = 0;

    /// Only for storage with UNIQUE KEY
    String min_unique_key = "";
    String max_unique_key = "";

    mutable UInt64 virtual_part_size = 0;

    /// secondary_txn_id > 0 mean this parts belong to an explicit transaction
    mutable TxnTimestamp secondary_txn_id {0};

    /// commit_time equals 0 means the data part is not committed.
    mutable TxnTimestamp commit_time {NOT_INITIALIZED_COMMIT_TIME};

    bool deleted = false;

    Int64 bucket_number = -1;               /// bucket_number > 0 if the part is assigned to bucket
    UInt64 table_definition_hash = 0;       // cluster by definition hash for data file

    /// Columns description. It could be shared between parts. This can help reduce memory usage during query execution.
    NamesAndTypesListPtr columns_ptr = std::make_shared<NamesAndTypesList>();

protected:
    friend class MergeTreeMetaBase;
    friend class MergeTreeData;
    friend class MergeScheduler;

    /// Total size of all columns, calculated once in calcuateColumnSizesOnDisk
    ColumnSize total_columns_size;

    /// Size for each column, calculated once in calcuateColumnSizesOnDisk
    ColumnSizeByName columns_sizes;

    /// Columns description. Cannot be changed, after part initialization.
    NamesAndTypesList columns;
    const Type part_type;

    /// Not null when it's a projection part.
    const IMergeTreeDataPart * parent_part;

    std::map<String, std::shared_ptr<IMergeTreeDataPart>> projection_parts;

    /// Protect checksums_ptr. FIXME:  May need more protection in getChecksums()
    /// to prevent checksums_ptr from being modified and corvered by multiple threads.
    mutable std::mutex checksums_mutex;
    mutable std::mutex index_mutex;

    void removeIfNeeded();

    virtual void checkConsistency(bool require_part_metadata) const;
    void checkConsistencyBase() const;

    /// Fill each_columns_size and total_size with sizes from columns files on
    /// disk using columns and checksums.
    virtual void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const = 0;

    String getRelativePathForDetachedPart(const String & prefix) const;

    std::optional<bool> keepSharedDataInDecoupledStorage() const;

    ColumnSize getMapColumnSizeNotKV(const IMergeTreeDataPart::ChecksumsPtr & checksums, const NameAndTypePair & column) const;

    IndexPtr loadIndexFromBuffer(ReadBuffer & index_file, const KeyDescription & primary_key) const;

private:
    /// In compact parts order of columns is necessary
    NameToNumber column_name_to_position;

    /// Reads part unique identifier (if exists) from uuid.txt
    void loadUUID();

    /// Reads columns names and types from columns.txt
    void loadColumns(bool require);

    /// If versions.txt exists, reads versions from it
    void loadVersions();

    /// If checksums.txt exists, reads file's checksums (and sizes) from it
    virtual ChecksumsPtr loadChecksums(bool require);

    /// Loads marks index granularity into memory
    virtual void loadIndexGranularity();

    /// Loads index file.
    virtual void loadIndex();

    /// Load rows count for this part from disk (for the newer storage format version).
    /// For the older format version calculates rows count from the size of a column with a fixed size.
    void loadRowsCount();

    /// Loads ttl infos in json format from file ttl.txt. If file doesn't exists assigns ttl infos with all zeros
    void loadTTLInfos();

    void loadPartitionAndMinMaxIndex();

    /// Load default compression codec from file default_compression_codec.txt
    /// if it not exists tries to deduce codec from compressed column without
    /// any specifial compression.
    void loadDefaultCompressionCodec();

    /// Found column without specific compression and return codec
    /// for this column with default parameters.
    CompressionCodecPtr detectDefaultCompressionCodec() const;

    mutable State state{State::Temporary};

    /// Used to prevent concurrent modification to row store mata.
    mutable std::mutex row_store_meta_mutex;
    /// Row store meta contains columns and removed columns info
    mutable UniqueRowStoreMetaPtr row_store_meta;

    mutable IMergeTreeDataPartPtr prev_part;

public:

    /// APIs for data parts serialization/deserialization
    void storePartitionAndMinMaxIndex(WriteBuffer & buf) const;
    void loadColumns(ReadBuffer & buf);
    void loadPartitionAndMinMaxIndex(ReadBuffer & buf);
    /// FIXME: old_meta_format is used to make it compatible with old part metadata. Remove it later.
    void loadTTLInfos(ReadBuffer & buf, bool old_meta_format = false);
    void loadDefaultCompressionCodec(const String & codec_str);
    virtual void loadIndexGranularity(const size_t marks_count, const std::vector<size_t> & index_granularities);

    /** ----------------------- COMPATIBLE CODE BEGIN-------------------------- */
    /*  compatible with old metastore. remove this later  */

    /// deserialize metadata into MergeTreeDataPart.
    /// @IMPORTANT Do not load checksums
    void deserializeMetaInfo(const String & metadata);
    void deserializeColumns(ReadBuffer & buffer);
    void deserializePartitionAndMinMaxIndex(ReadBuffer & buffer);

    /*  -----------------------  COMPATIBLE CODE END -------------------------- */
};

using MergeTreeDataPartState = IMergeTreeDataPart::State;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;

bool isCompactPart(const MergeTreeDataPartPtr & data_part);
bool isWidePart(const MergeTreeDataPartPtr & data_part);
bool isInMemoryPart(const MergeTreeDataPartPtr & data_part);
bool isCnchPart(const MergeTreeDataPartPtr & data_part);

}
