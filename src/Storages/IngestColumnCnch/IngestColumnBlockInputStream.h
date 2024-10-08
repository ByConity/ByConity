#pragma once
#include <Common/Logger.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context_fwd.h>
#include "Storages/StorageSnapshot.h"

namespace DB
{

class StorageCloudMergeTree;
struct PartitionCommand;
class MemoryInefficientIngestColumn;
class MemoryEfficientIngestColumn;

class IngestColumnBlockInputStream : public IBlockInputStream
{
    friend MemoryEfficientIngestColumn;
public:
    IngestColumnBlockInputStream(
        StoragePtr target_storage,
        const PartitionCommand & command,
        ContextPtr local_context
    );

    String getName() const override { return "IngestColumnBlockInputStream"; }
    Block getHeader() const override { return {}; }
    IMergeTreeDataPartsVector & getCurrentVisibleTargetParts();
    IMergeTreeDataPartsVector & getCurrentVisibleSourceParts();
protected:
    Block readImpl() override;
private:
    void logIngestWithBucketStatus();

    StoragePtr target_storage;
    StoragePtr source_storage;
    StorageCloudMergeTree * target_cloud_merge_tree;
    StorageCloudMergeTree * source_cloud_merge_tree;
    StorageSnapshotPtr target_storage_snapshot;
    StorageSnapshotPtr source_storage_snapshot;
    Names ordered_key_names;
    Names ingest_column_names;
    String partition_id;
    MergeTreeDataPartsVector target_parts;
    MergeTreeDataPartsVector source_parts;
    MergeTreeDataPartsVector visible_target_parts;
    MergeTreeDataPartsVector visible_source_parts;
    std::vector<IMergeTreeDataPartsVector> visible_target_parts_with_bucket;
    std::vector<IMergeTreeDataPartsVector> visible_source_parts_with_bucket;
    Int64 cur_bucket_index = -1;    // if cur_bcuket_index = -1 use ordinary ingest
    std::vector<Int64> buckets_for_ingest;
    ContextPtr context;
    LoggerPtr log;
};

}
