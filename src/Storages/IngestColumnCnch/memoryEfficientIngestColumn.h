#pragma once

#include <Common/Logger.h>
#include <Storages/IngestColumnCnch/memoryEfficientIngestColumnHelper.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>
#include <Storages/StorageSnapshot.h>
#include <set>

namespace DB
{

class IngestColumnBlockInputStream;
class StorageCloudMergeTree;

class MemoryEfficientIngestColumn
{
public:
    MemoryEfficientIngestColumn(IngestColumnBlockInputStream & stream);
    void execute();
private:
    /// add new parts methods
    void addNewPartsForBucket(
        size_t bucket_num,
        size_t number_of_buckets
    );

    HashMapWithSavedHash<StringRef, IngestColumn::KeyInfo, StringRefHash> buildHashTableForSourceData(
        std::vector<Arena> & keys_pool_per_threads,
        size_t bucket_num,
        size_t number_of_buckets
    );

    void probeHashMapWithTargetData(
        HashMapWithSavedHash<StringRef, IngestColumn::KeyInfo, StringRefHash> & source_key_map,
        size_t bucket_num,
        size_t number_of_buckets);

    void insertNewData(
        const HashMapWithSavedHash<StringRef, IngestColumn::KeyInfo, StringRefHash> & source_key_map,
        size_t bucket_num,
        size_t number_of_buckets);

    /// create delta parts methods
    void updateTargetParts();

    MergeTreeMutableDataPartPtr updateTargetPartWithoutSourcePart(
        MergeTreeDataPartPtr target_part);

    MergeTreeMutableDataPartPtr updateTargetPart(
        MergeTreeDataPartPtr target_part,
        const std::set<UInt32> & source_part_ids,
        const Names & all_columns);

    IngestColumn::TargetPartData readTargetPartForUpdate(
        MergeTreeDataPartPtr target_part,
        const Names & all_columns);

    void updateTargetDataWithSourcePart(
        MergeTreeDataPartPtr source_part,
        const Names & all_columns,
        IngestColumn::TargetPartData & target_part_data);

    const IngestColumnBlockInputStream & stream;
    ContextPtr & context;
    const Settings & settings;
    StorageCloudMergeTree & target_cloud_merge_tree;
    const StorageCloudMergeTree & source_cloud_merge_tree;

    const StorageSnapshotPtr & target_storage_snapshot;
    const StorageSnapshotPtr & source_storage_snapshot;
    const MergeTreeDataPartsVector & visible_target_parts;
    const MergeTreeDataPartsVector & visible_source_parts;
    const size_t number_of_threads_for_read_source_parts;
    LoggerPtr log;

    /// Below is intermediate data to serve the algorithm
    /// Maping each part to an interger/ part_id
    const IngestColumn::PartMap source_part_map;
    const IngestColumn::PartMap target_part_map;
    /// which source part_ids contains the same key in a target part_id
    std::unordered_map<UInt32, std::set<UInt32>> target_to_source_part_index;
};
} /// end namespace DB
