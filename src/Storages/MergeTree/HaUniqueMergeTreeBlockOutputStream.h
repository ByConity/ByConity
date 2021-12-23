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

class HaUniqueMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    HaUniqueMergeTreeBlockOutputStream(StorageHaUniqueMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_, size_t max_parts_per_block_);

    Block getHeader() const override;
    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    size_t removeDupKeys(Block & block, ColumnWithTypeAndName * version_column, IColumn::Filter & filter);

    /// existing part -> rowid bitmap of new deletes
    using PartsWithDeleteRows = std::map<MergeTreeData::DataPartPtr, Roaring, MergeTreeData::LessDataPart>;
    /// merging part name -> (merge task state, pending deletes)
    using DeletesOnMergingParts = std::map<String, std::pair<MergeTreeData::UniqueMergeStatePtr, MergeTreeData::UniqueKeySet>>;

    /// Return whether block becomes empty after processing
    bool processPartitionBlock(
        BlockWithPartition & block_with_partition,
        bool preprocessed,
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
    Int64 saved_memory_used{0};
};

}
