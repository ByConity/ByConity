#pragma once

#include <Core/Block.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>

#include <MergeTreeCommon/MergeTreeMetaBase.h>


namespace DB
{

struct BlockWithPartition
{
    Block block;
    Row partition;

    BlockWithPartition(Block && block_, Row && partition_)
        : block(block_), partition(std::move(partition_))
    {
    }
};

using BlocksWithPartition = std::vector<BlockWithPartition>;

/** Writes new parts of data to the merge tree.
  */
class MergeTreeDataWriter
{
public:
    // use dest_policy_ to reserve space if specificed
    explicit MergeTreeDataWriter(MergeTreeMetaBase & data_, const StoragePolicyPtr& dest_policy_ = nullptr,
        const String& base_rel_path_ = ""):
            data(data_), destination_policy(dest_policy_), base_rel_path(base_rel_path_),
            log(&Poco::Logger::get(data.getLogName() + " (Writer)")) {}

    /** Split the block to blocks, each of them must be written as separate part.
      *  (split rows by partition)
      * Works deterministically: if same block was passed, function will return same result in same order.
      */
    static BlocksWithPartition splitBlockIntoParts(const Block & block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot, ContextPtr context);

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeMetaBase::MutableDataPartPtr writeTempPart(BlockWithPartition & block, const StorageMetadataPtr & metadata_snapshot, bool optimize_on_insert);

    MergeTreeMetaBase::MutableDataPartPtr
    writeTempPart(BlockWithPartition & block, const StorageMetadataPtr & metadata_snapshot, ContextPtr context);

    MergeTreeMetaBase::MutableDataPartPtr writeProjectionPart(
        Block block, const ProjectionDescription & projection, const IMergeTreeDataPart * parent_part);

    static MergeTreeMetaBase::MutableDataPartPtr writeTempProjectionPart(
        MergeTreeMetaBase & data,
        Poco::Logger * log,
        Block block,
        const ProjectionDescription & projection,
        const IMergeTreeDataPart * parent_part,
        size_t block_num,
        const StoragePolicyPtr& dest_policy = nullptr,
        const String& base_rel_path = "");

    Block mergeBlock(const Block & block, SortDescription sort_description, Names & partition_key_columns, IColumn::Permutation *& permutation);

private:
    static MergeTreeMetaBase::MutableDataPartPtr writeProjectionPartImpl(
        MergeTreeMetaBase & data,
        Poco::Logger * log,
        Block block,
        const StorageMetadataPtr & metadata_snapshot,
        MergeTreeMetaBase::MutableDataPartPtr && new_data_part);

    MergeTreeMetaBase & data;

    StoragePolicyPtr destination_policy;
    String base_rel_path;

    Poco::Logger * log;
};

}
