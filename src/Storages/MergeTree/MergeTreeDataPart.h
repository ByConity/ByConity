#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Disks/IDisk.h>


namespace DB
{
class MergeTreeMetaBase;

/// Mock For MergeTreeDataDumper
class MergeTreeDataPart : public IMergeTreeDataPart
{
public:
    using Checksums = MergeTreeDataPartChecksums;
    // using ChecksumsPtr = MergeTreeDataPartChecksumsPtr;
    using Checksum = MergeTreeDataPartChecksums::Checksum;

    MergeTreeDataPart(
        const MergeTreeMetaBase & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr& volume_,
        const std::optional<String> & relative_path_,
        const IMergeTreeDataPart * parent_part_ = nullptr)
        : IMergeTreeDataPart(storage_, name_, info_, volume_, relative_path_, Type::WIDE, parent_part_)
        {}

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & /*columns_*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const MarkRanges & /*mark_ranges*/,
        UncompressedCache * /*uncompressed_cache*/,
        MarkCache * /*mark_cache*/,
        const MergeTreeReaderSettings & /*reader_settings_*/,
        MergeTreeBitMapIndexReader * /*bitmap_index_reader*/,
        const ValueSizeMap & /*avg_value_size_hints_*/,
        const ReadBufferFromFileBase::ProfileCallback & /*profile_callback_*/) const override
    {
        return nullptr;
    }

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & /*columns_list*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const std::vector<MergeTreeIndexPtr> & /*indices_to_recalc*/,
        const CompressionCodecPtr & /*default_codec_*/,
        const MergeTreeWriterSettings & /*writer_settings*/,
        const MergeTreeIndexGranularity & /*computed_index_granularity*/) const override
    {
        return nullptr;
    }

    bool isStoredOnDisk() const override { return true; }

    void calculateEachColumnSizes(ColumnSizeByName & /*each_columns_size*/, ColumnSize & /*total_size*/) const override
    {}

    String getFileNameForColumn(const NameAndTypePair & /*column*/) const override { return ""; }

};
}
