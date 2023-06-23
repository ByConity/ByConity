#pragma once
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context_fwd.h>

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
protected:
    Block readImpl() override;
private:
    StoragePtr target_storage;
    StoragePtr source_storage;
    StorageCloudMergeTree * target_cloud_merge_tree;
    StorageCloudMergeTree * source_cloud_merge_tree;
    StorageMetadataPtr target_meta_data_ptr;
    StorageMetadataPtr source_meta_data_ptr;
    Names ordered_key_names;
    Names ingest_column_names;
    String partition_id;
    MergeTreeDataPartsVector target_parts;
    MergeTreeDataPartsVector source_parts;
    MergeTreeDataPartsVector visible_target_parts;
    MergeTreeDataPartsVector visible_source_parts;
    ContextPtr context;
    Poco::Logger * log;
};

}
