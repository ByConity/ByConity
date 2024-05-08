#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Disks/IDisk.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageCloudMergeTree.h>


namespace DB
{

class MergeTreeMetaBase;

void checkPartitionRows(ServerDataPartsVector & parts, const Settings & settings, const String & table_type);

void checkColumnStructure(const StorageInMemoryMetadata & target_data, const StorageInMemoryMetadata & src_data, const Names & names);

Names getOrderedKeys(const Names & names_to_order, const StorageInMemoryMetadata & meta_data);

void checkIngestColumns(const Strings & column_names, const StorageInMemoryMetadata & meta_data, bool & has_map_implicite_key);

int compare(Columns & target_key_cols, Columns & src_key_cols, size_t n, size_t m);

std::optional<NameAndTypePair> tryGetMapColumn(const StorageInMemoryMetadata & meta_data, const String & col_name);

MergeTreeMutableDataPartPtr createEmptyTempPart(
    MergeTreeMetaBase & data,
    const MergeTreeDataPartPtr & part,
    const Names & ingest_column_names,
    ReservationPtr& reserved_space,
    const ContextPtr & context);

size_t countRows(const MergeTreeDataPartsVector & parts);

void finalizeTempPart(
    const MergeTreeDataPartPtr & ingest_part,
    const MergeTreeMutableDataPartPtr & new_partial_part,
    const CompressionCodecPtr & codec);

void updateTempPartWithData(
    MergeTreeMutableDataPartPtr & new_partial_part,
    const MergeTreeDataPartPtr & target_part,
    const BlockInputStreamPtr & data_block_input_stream,
    const StorageMetadataPtr & target_meta_data_ptr);

// check ingest table on server
bool checkIngestWithBucketTable(
    const MergeTreeMetaBase & source_table,
    const MergeTreeMetaBase & target_table,
    const Names & ordered_key_names,
    const Names & ingest_column_names);

// split ingest buckets for worker on server
std::vector<std::vector<Int64>> splitBucketsForWorker(size_t total_worker_num, const std::vector<Int64> & buckets_for_ingest);

// split ingest parts with bucket on worker
std::vector<IMergeTreeDataPartsVector>
clusterDataPartWithBucketTable(const StorageCloudMergeTree & table, const IMergeTreeDataPartsVector & data_parts);

}
