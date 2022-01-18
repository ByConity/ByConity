#pragma once

#include <Client/Connection.h>
#include <Core/Types.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <roaring.hh>

namespace Poco { class Logger; }

namespace zkutil
{
class ZooKeeper;
using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageHaUniqueMergeTree;

class HaUniqueMergeTreeBlockOutputStreamV2 : public IBlockOutputStream
{
public:
    HaUniqueMergeTreeBlockOutputStreamV2(StorageHaUniqueMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_, size_t max_parts_per_block_);

    Block getHeader() const override;
    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    size_t removeDupKeys(Block & block, IColumn::Filter & filter);

    struct RowidPair
    {
        UInt32 part_rowid;
        UInt32 block_rowid;
    };
    using RowidPairs = std::vector<RowidPair>;
    using PartRowidPairs = std::vector<RowidPairs>;

    void readColumnsFromStorage(const MergeTreeData::DataPartPtr & part, RowidPairs & rowid_pairs,
        Block & to_block, PaddedPODArray<UInt32> & to_block_rowids);

    void readColumnsFromRowStore(const MergeTreeData::DataPartPtr & part, RowidPairs & rowid_pairs,
        Block & to_block, PaddedPODArray<UInt32> & to_block_rowids, const UniqueRowStorePtr & row_store);

    /// existing part -> rowid bitmap of new deletes
    using PartsWithDeleteRows = std::map<MergeTreeData::DataPartPtr, Roaring, MergeTreeData::LessDataPart>;
    /// merging part name -> (merge task state, pending deletes)
    using DeletesOnMergingParts = std::map<String, std::pair<MergeTreeData::UniqueMergeStatePtr, MergeTreeData::UniqueKeySet>>;

    /// Return whether block becomes empty after processing
    bool processPartitionBlock(
        BlockWithPartition & block_with_partition,
        const MergeTreeData::DataPartsVector & existing_parts,
        PartsWithDeleteRows & parts_with_deletes,
        DeletesOnMergingParts & deletes_on_merging_parts);

    StorageHaUniqueMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    size_t max_parts_per_block;
    bool allow_materialized;
    using Logger = Poco::Logger;
    Logger * log;
    bool need_forward;
    /// useful only in forward-to-leader mode (need_forward == true)
    String leader_name;
    ConnectionPtr remote_conn;
    BlockOutputStreamPtr remote_stream;
};

}
