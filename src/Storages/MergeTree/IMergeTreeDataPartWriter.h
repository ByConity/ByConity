#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/ColumnBitmapIndex.h>
#include <Disks/IDisk.h>


namespace DB
{

Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation);

Block permuteBlockIfNeeded(const Block & block, const IColumn::Permutation * permutation);

/// Writes data part to disk in different formats.
/// Calculates and serializes primary and skip indices if needed.
class IMergeTreeDataPartWriter : private boost::noncopyable
{
public:
    IMergeTreeDataPartWriter(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeWriterSettings & settings_,
        const MergeTreeIndexGranularity & index_granularity_ = {});

    virtual ~IMergeTreeDataPartWriter();

    virtual void write(const Block & block, const IColumn::Permutation * permutation) = 0;

    void writeImplicitColumnForBitEngine(Block & block);

    virtual void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) = 0;
    virtual void updateWriterStream(const NameAndTypePair &pair);


    Columns releaseIndexColumns();
    const MergeTreeIndexGranularity & getIndexGranularity() const { return index_granularity; }

protected:

    const MergeTreeData::DataPartPtr data_part;
    const MergeTreeData & storage;
    const StorageMetadataPtr metadata_snapshot;
    const NamesAndTypesList columns_list;
    const MergeTreeWriterSettings settings;
    MergeTreeIndexGranularity index_granularity;
    const bool with_final_mark;

    MutableColumns index_columns;

    // added by bitmap index
    using ColumnBitmapIndexes = std::map<String, std::unique_ptr<ColumnBitmapIndex>>;
    using ColumnMarkBitmapIndexes = std::map<String, std::unique_ptr<ColumnMarkBitmapIndex>>;

    void addBitmapIndexes(const String & path, const String & name, const IDataType & type, const IndexParams & bitmap_params = IndexParams());
    void addMarkBitmapIndexes(const String & path, const String & name, const IDataType & type, const IndexParams & bitmap_params = IndexParams());

    ColumnBitmapIndexes column_bitmap_indexes;
    ColumnMarkBitmapIndexes column_mark_bitmap_indexes;
};

}
