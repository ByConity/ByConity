#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>

namespace DB
{
class MergeTreeMetaBase;
/// Mock cnch part for Catalog usage.
class MergeTreeDataPartCNCH : public IMergeTreeDataPart
{
public:
    static constexpr auto DATA_FILE_NAME = "data";

    MergeTreeDataPartCNCH(
        const MergeTreeMetaBase & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {},
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeDataPartCNCH(
        MergeTreeMetaBase & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {},
        const IMergeTreeDataPart * parent_part_ = nullptr);

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns_to_read,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        MergeTreeBitMapIndexReader * bitmap_index_reader,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback) const override;

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity) const override;

    bool operator < (const MergeTreeDataPartCNCH & r) const;
    bool operator > (const MergeTreeDataPartCNCH & r) const;

    bool isStoredOnDisk() const override { return true; }

    bool supportsVerticalMerge() const override { return true; }

    String getFileNameForColumn(const NameAndTypePair & column) const override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    void loadIndexGranularity(const size_t marks_count, const std::vector<size_t> & index_granularities) override;

    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency) override;

private:

    bool isDeleted() const;
    String getFullDataPath() const;
    String getFullRelativeDataPath() const;

    void checkConsistency(bool require_part_metadata) const override;

    void loadIndex() override;

    MergeTreeDataPartChecksums::FileChecksums loadPartDataFooter() const;

    ChecksumsPtr loadChecksums(bool require) override;

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
};

}
