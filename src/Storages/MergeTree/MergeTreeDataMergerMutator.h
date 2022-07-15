#pragma once

#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MutationCommands.h>
#include <atomic>
#include <functional>
#include <Common/ActionBlocker.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>


namespace DB
{

class MergeProgressCallback;
class MergeScheduler;

enum class SelectPartsDecision
{
    SELECTED = 0,
    CANNOT_SELECT = 1,
    NOTHING_TO_MERGE = 2,
};

enum class RowStoreColumnsMatchType
{
    Mismatch = 0,
    PrefixMatch = 1,
    ExactMatch = 2,
};

struct RowStoreColumnsInfo
{
    RowStoreColumnsMatchType match_type;
    std::vector<size_t> column_indexes;
    std::vector<bool> column_hits;
    std::vector<SerializationPtr> column_serializations;
};

using RowStoreColumnsInfoList = std::vector<RowStoreColumnsInfo>;

/// Auxiliary struct holding metainformation for the future merged or mutated part.
struct FutureMergedMutatedPart
{
    String name;
    UUID uuid = UUIDHelpers::Nil;
    String path;
    MergeTreeDataPartType type;
    MergeTreePartInfo part_info;
    MergeTreeMetaBase::DataPartsVector parts;
    MergeType merge_type = MergeType::REGULAR;
    /// for unique table: hold snapshot of delete bitmap for each input part
    MergeTreeMetaBase::DataPartsDeleteSnapshot delete_snapshot;

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }

    FutureMergedMutatedPart() = default;
    explicit FutureMergedMutatedPart(MergeTreeMetaBase::DataPartsVector parts_)
    {
        assign(std::move(parts_));
    }

    FutureMergedMutatedPart(MergeTreeMetaBase::DataPartsVector parts_, MergeTreeDataPartType future_part_type)
    {
        assign(std::move(parts_), future_part_type);
    }

    void assign(MergeTreeMetaBase::DataPartsVector parts_);
    void assign(MergeTreeMetaBase::DataPartsVector parts_, MergeTreeDataPartType future_part_type);

    void updatePath(const MergeTreeMetaBase & storage, const ReservationPtr & reservation);
    DeleteBitmapPtr getDeleteBitmap(const MergeTreeMetaBase::DataPartPtr & part) const
    {
        if (!delete_snapshot.empty())
        {
            if (auto it = delete_snapshot.find(part); it != delete_snapshot.end())
                return it->second;
            return nullptr;
        }
        else
        {
            return part->getDeleteBitmap();
        }
    }
};


/** Can select parts for background processes and do them.
 * Currently helps with merges, mutations and moves
 */
class MergeTreeDataMergerMutator
{
public:
    using AllowedMergingPredicate = std::function<bool (const MergeTreeMetaBase::DataPartPtr &, const MergeTreeMetaBase::DataPartPtr &, String *)>;

    /// The i-th row in the merged part is from part PartIdMapping[i]
    using PartIdMapping = PODArray<UInt32, /*INITIAL_SIZE*/1024>;

    MergeTreeDataMergerMutator(MergeTreeMetaBase & data_, size_t background_pool_size);

    /** Get maximum total size of parts to do merge, at current moment of time.
      * It depends on number of free threads in background_pool and amount of free space in disk.
      */
    UInt64 getMaxSourcePartsSizeForMerge() const;

    /** For explicitly passed size of pool and number of used tasks.
      * This method could be used to calculate threshold depending on number of tasks in replication queue.
      */
    UInt64 getMaxSourcePartsSizeForMerge(size_t pool_size, size_t pool_used) const;

    /** Get maximum total size of parts to do mutation, at current moment of time.
      * It depends only on amount of free space in disk.
      */
    UInt64 getMaxSourcePartSizeForMutation() const;

    /** Selects which parts to merge. Uses a lot of heuristics.
      *
      * can_merge - a function that determines if it is possible to merge a pair of adjacent parts.
      *  This function must coordinate merge with inserts and other merges, ensuring that
      *  - Parts between which another part can still appear can not be merged. Refer to METR-7001.
      *  - A part that already merges with something in one place, you can not start to merge into something else in another place.
      */
    SelectPartsDecision selectPartsToMerge(
        FutureMergedMutatedPart & future_part,
        bool aggressive,
        size_t max_total_size_to_merge,
        const AllowedMergingPredicate & can_merge,
        bool merge_with_ttl_allowed,
        String * out_disable_reason = nullptr,
        MergeScheduler * merge_scheduler = nullptr);

    SelectPartsDecision selectPartsToMergeMulti(
        std::vector<FutureMergedMutatedPart> & future_parts,
        const MergeTreeMetaBase::DataPartsVector & data_parts,
        bool aggressive,
        size_t max_total_size_to_merge,
        const AllowedMergingPredicate & can_merge,
        bool merge_with_ttl_allowed,
        String * out_disable_reason = nullptr,
        MergeScheduler * merge_scheduler = nullptr,
        const bool enable_batch_select = false);

    /** Select all the parts in the specified partition for merge, if possible.
      * final - choose to merge even a single part - that is, allow to merge one part "with itself",
      * but if setting optimize_skip_merged_partitions is true than single part with level > 0
      * and without expired TTL won't be merged with itself.
      */
    SelectPartsDecision selectAllPartsToMergeWithinPartition(
        FutureMergedMutatedPart & future_part,
        UInt64 & available_disk_space,
        const AllowedMergingPredicate & can_merge,
        const String & partition_id,
        bool final,
        const StorageMetadataPtr & metadata_snapshot,
        String * out_disable_reason = nullptr,
        bool optimize_skip_merged_partitions = false,
        MergeScheduler * merge_scheduler = nullptr);

    /** Merge the parts.
      * If `reservation != nullptr`, now and then reduces the size of the reserved space
      *  is approximately proportional to the amount of data already written.
      *
      * Creates and returns a temporary part.
      * To end the merge, call the function renameMergedTemporaryPart.
      *
      * time_of_merge - the time when the merge was assigned.
      * Important when using ReplicatedGraphiteMergeTree to provide the same merge on replicas.
      */

    MergeTreeMetaBase::MutableDataPartPtr mergePartsToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const StorageMetadataPtr & metadata_snapshot,
        MergeListEntry & merge_entry,
        TableLockHolder & table_lock_holder,
        time_t time_of_merge,
        ContextPtr context,
        const ReservationPtr & space_reservation,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        const MergeTreeMetaBase::MergingParams & merging_params,
        const IMergeTreeDataPart * parent_part = nullptr,
        const String & prefix = "",
        const ActionBlocker * unique_table_blocker = nullptr);

    /// Mutate a single data part with the specified commands. Will create and return a temporary part.
    MergeTreeMetaBase::MutableDataPartPtr mutatePartToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MutationCommands & commands,
        MergeListEntry & merge_entry,
        time_t time_of_mutation,
        ContextPtr context,
        const ReservationPtr & space_reservation,
        TableLockHolder & table_lock_holder);

    MergeTreeMetaBase::DataPartPtr renameMergedTemporaryPart(
        MergeTreeMetaBase::MutableDataPartPtr & new_data_part,
        const MergeTreeMetaBase::DataPartsVector & parts,
        MergeTreeData::Transaction * out_transaction = nullptr);

    /// The approximate amount of disk space needed for merge or mutation. With a surplus.
    static size_t estimateNeededDiskSpace(const MergeTreeMetaBase::DataPartsVector & source_parts);

private:
    /*
     * Performance test result: https://bytedance.feishu.cn/docs/doccnilaBbofUvfnQ3zBuLQKjFe#o66usm
     * Based on the performance test result of writing row store, serialize value step is the most time-consuming, so it's necessary to use the serialized value in row store of old parts.
     *
     * Due to add/drop columns command, merge row store should handle three cases:
     * Case1: Exact Match
     *
     * Origin, there has 3 columns a,b,c and 2 part:
     * [Part 1]row store columns:{a   b   c}, removed column: {}
     * [Part 2]row store columns:{a   b   c}, removed column: {}
     * In this case, we can directly use origin serialized value of each row for two parts and contruct new row store.
     *
     * Case 2: Prefix Match
     *
     * Origin, there has 3 columns a,b,c and 1 part:
     * [Part 1]row store columns:{a   b   c}, removed column: {}
     * Then add columns d after c, and write a new part:
     * [Part 1]row store columns:{a   b   c}, removed column: {}
     * [Part 2]row store columns:{a   b   c   d}, removed column: {}
     * In this case, we can directly use origin serialized value of each row for part 2.
     * But for part 1, we need to append value using defaule value of column d for each row.
     *
     * Case 3: Mismatch
     * Origin, there has 3 columns a,b,c and 1 part:
     * [Part 1]row store columns:{a   b   c}, removed column: {}
     * Then add columns d before c, and write a new part:
     * [Part 1]row store columns:{a   b   c}, removed column: {}
     * [Part 2]row store columns:{a   b   d   c}, removed column: {}
     * In this case, we can directly use origin serialized value of each row for part 2.
     * But for part 1, we need to rewrite each row.
     *
     * In the fact that rewrite case need to take twice time to write row store, thus (Condition 1)if Mismatch row number is more than half of the total row number, it is better to generating row store from storage.
     *
     * We divide the process into several phases:
     * Phase 1: Get row store meta and row count of old parts. If any part doesn't have row store, return false to generate row store from storage.
     * Phase 2: Check metadata of columns. If it match Condition 1, return false to generate row store from storage.
     * Phase 3: Get row store iterator of old parts and new part.
     * Phase 4: Merge row store according to the match type.
     * Phase 5: Update checksums and write row store meta.
     */
    bool tryMergeRowStoreIntoNewPart(
        const FutureMergedMutatedPart & future_part,
        const PartIdMapping & part_id_mapping,
        const MergeTreeMetaBase::MutableDataPartPtr & new_part,
        MergeTreeMetaBase::DataPart::Checksums & checksums,
        bool need_sync);

    void generateRowStoreFromStorage(MergeTreeMetaBase::MutableDataPartPtr & new_part, bool need_sync);

    bool checkIfBuildRowStore();

    /** Select all parts belonging to the same partition.
      */
    MergeTreeMetaBase::DataPartsVector selectAllPartsFromPartition(const String & partition_id);

    /** Split mutation commands into two parts:
      * First part should be executed by mutations interpreter.
      * Other is just simple drop/renames, so they can be executed without interpreter.
      */
    static void splitMutationCommands(
        MergeTreeMetaBase::DataPartPtr part,
        const MutationCommands & commands,
        MutationCommands & for_interpreter,
        MutationCommands & for_file_renames);

    /// Apply commands to source_part i.e. remove and rename some columns in
    /// source_part and return set of files, that have to be removed or renamed
    /// from filesystem and in-memory checksums. Ordered result is important,
    /// because we can apply renames that affects each other: x -> z, y -> x.
    static NameToNameVector collectFilesForRenames(MergeTreeMetaBase::DataPartPtr source_part, const MutationCommands & commands_for_removes, const String & mrk_extension);

    /// Collect necessary implicit files for clear map key commands.
    /// If the part enables compact map data and all implicit keys of the map column has been removed, the compacted file need to remove too.
    static NameSet collectFilesForClearMapKey(MergeTreeMetaBase::DataPartPtr source_part, const MutationCommands & commands);

    /// Files, that we don't need to remove and don't need to hardlink, for example columns.txt and checksums.txt.
    /// Because we will generate new versions of them after we perform mutation.
    static NameSet collectFilesToSkip(
        const MergeTreeDataPartPtr & source_part,
        const Block & updated_header,
        const std::set<MergeTreeIndexPtr> & indices_to_recalc,
        const String & mrk_extension,
        const std::set<MergeTreeProjectionPtr> & projections_to_recalc,
        bool update_delete_bitmap);

    /// Get the columns list of the resulting part in the same order as storage_columns.
    static NamesAndTypesList getColumnsForNewDataPart(
        MergeTreeMetaBase::DataPartPtr source_part,
        const Block & updated_header,
        NamesAndTypesList storage_columns,
        const MutationCommands & commands_for_removes);

    /// Get skip indices, that should exists in the resulting data part.
    static MergeTreeIndices getIndicesForNewDataPart(
        const IndicesDescription & all_indices,
        const MutationCommands & commands_for_removes);

    static MergeTreeProjections getProjectionsForNewDataPart(
        const ProjectionsDescription & all_projections,
        const MutationCommands & commands_for_removes);

    static bool shouldExecuteTTL(
        const StorageMetadataPtr & metadata_snapshot, const ColumnDependencies & dependencies, const MutationCommands & commands);

    /// Return set of indices which should be recalculated during mutation also
    /// wraps input stream into additional expression stream
    static std::set<MergeTreeIndexPtr> getIndicesToRecalculate(
        BlockInputStreamPtr & input_stream,
        const NameSet & updated_columns,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        const NameSet & materialized_indices,
        const MergeTreeMetaBase::DataPartPtr & source_part);

    static std::set<MergeTreeProjectionPtr> getProjectionsToRecalculate(
        const NameSet & updated_columns,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & materialized_projections,
        const MergeTreeMetaBase::DataPartPtr & source_part);

    void writeWithProjections(
        MergeTreeMetaBase::MutableDataPartPtr new_data_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeProjections & projections_to_build,
        BlockInputStreamPtr mutating_stream,
        IMergedBlockOutputStream & out,
        time_t time_of_mutation,
        MergeListEntry & merge_entry,
        const ReservationPtr & space_reservation,
        TableLockHolder & holder,
        ContextPtr context,
        IMergeTreeDataPart::MinMaxIndex * minmax_idx = nullptr);

    /// Override all columns of new part using mutating_stream
    void mutateAllPartColumns(
        MergeTreeMetaBase::MutableDataPartPtr new_data_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeIndices & skip_indices,
        const MergeTreeProjections & projections_to_build,
        const MutationCommands & commands_for_removes,
        BlockInputStreamPtr mutating_stream,
        time_t time_of_mutation,
        const CompressionCodecPtr & compression_codec,
        MergeListEntry & merge_entry,
        bool need_remove_expired_values,
        bool need_sync,
        const ReservationPtr & space_reservation,
        TableLockHolder & holder,
        ContextPtr context);

    /// Mutate some columns of source part with mutation_stream
    void mutateSomePartColumns(
        const MergeTreeDataPartPtr & source_part,
        const StorageMetadataPtr & metadata_snapshot,
        const std::set<MergeTreeIndexPtr> & indices_to_recalc,
        const std::set<MergeTreeProjectionPtr> & projections_to_recalc,
        const Block & mutation_header,
        MergeTreeMetaBase::MutableDataPartPtr new_data_part,
        BlockInputStreamPtr mutating_stream,
        time_t time_of_mutation,
        const CompressionCodecPtr & compression_codec,
        MergeListEntry & merge_entry,
        bool need_remove_expired_values,
        bool need_sync,
        const ReservationPtr & space_reservation,
        TableLockHolder & holder,
        ContextPtr context);


public :
    /// Initialize and write to disk new part fields like checksums, columns,
    /// etc.
    static void finalizeMutatedPart(
        const MergeTreeDataPartPtr & source_part,
        MergeTreeMetaBase::MutableDataPartPtr new_data_part,
        bool need_remove_expired_values,
        const CompressionCodecPtr & codec);

    /** Is used to cancel all merges and mutations. On cancel() call all currently running actions will throw exception soon.
      * All new attempts to start a merge or mutation will throw an exception until all 'LockHolder' objects will be destroyed.
      */
    ActionBlocker merges_blocker;
    ActionBlocker ttl_merges_blocker;

private:

    MergeAlgorithm chooseMergeAlgorithm(
        const MergeTreeMetaBase::DataPartsVector & parts,
        size_t rows_upper_bound,
        const NamesAndTypesList & gathering_columns,
        bool deduplicate,
        bool need_remove_expired_values,
        const MergeTreeMetaBase::MergingParams & merging_params) const;

    bool checkOperationIsNotCanceled(const MergeListEntry & merge_entry) const;

    MergeTreeMetaBase & data;
    const size_t background_pool_size;

    Poco::Logger * log;

    /// When the last time you wrote to the log that the disk space was running out (not to write about this too often).
    time_t disk_space_warning_time = 0;

    /// Stores the next TTL delete merge due time for each partition (used only by TTLDeleteMergeSelector)
    ITTLMergeSelector::PartitionIdToTTLs next_delete_ttl_merge_times_by_partition;

    /// Stores the next TTL recompress merge due time for each partition (used only by TTLRecompressionMergeSelector)
    ITTLMergeSelector::PartitionIdToTTLs next_recompress_ttl_merge_times_by_partition;
    /// Performing TTL merges independently for each partition guarantees that
    /// there is only a limited number of TTL merges and no partition stores data, that is too stale
};


}
