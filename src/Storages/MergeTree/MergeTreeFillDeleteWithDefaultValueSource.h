#pragma once
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <memory>

namespace DB
{
/// TODO: doc
class MergeTreeFillDeleteWithDefaultValueSource : public SourceWithProgress
{
public:
    MergeTreeFillDeleteWithDefaultValueSource(
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        MergeTreeData::DataPartPtr data_part_,
        DeleteBitmapPtr delete_bitmap_,
        Names columns_to_read_);

    ~MergeTreeFillDeleteWithDefaultValueSource() override;

    String getName() const override { return "MergeTreeFillDeleteWithDefaultValueSource"; }

    size_t getCurrentMark() const { return current_mark; }

    size_t getCurrentRow() const { return current_row; }

protected:
    Chunk generate() override;

private:

    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;
    DeleteBitmapPtr delete_bitmap;

    /// Columns we have to read (each Block from read will contain them)
    Names columns_to_read;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeFillDeleteWithDefaultValueSource");

    std::shared_ptr<MarkCache> mark_cache;
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    MergeTreeReaderPtr reader;

    /// current mark at which we stop reading
    size_t current_mark = 0;

    /// current row at which we stop reading
    size_t current_row = 0;

private:
    /// Closes readers and unlock part locks
    void finish();
};


} // namespace DB
