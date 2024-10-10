/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include "common/types.h"

namespace DB
{
class MergeTreeMetaBase;
/// Mock cnch part for Catalog usage.
class MergeTreeDataPartCNCH : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartCNCH(
        const MergeTreeMetaBase & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {},
        const IMergeTreeDataPart * parent_part_ = nullptr,
        const UUID & part_id = UUIDHelpers::Nil);

    MergeTreeDataPartCNCH(
        const MergeTreeMetaBase & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_ = {},
        const IMergeTreeDataPart * parent_part_ = nullptr,
        const UUID& part_id = UUIDHelpers::Nil);

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns_to_read,
        const StorageMetadataPtr & metadata_snapshot,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const MergeTreeReaderSettings & reader_settings_,
        MergeTreeIndexExecutor * index_executor,
        const ValueSizeMap & avg_value_size_hints,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback,
        const ProgressCallback & internal_progress_cb) const override;

    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity,
        const BitmapBuildInfo & bitmap_build_info) const override;

    bool operator < (const MergeTreeDataPartCNCH & r) const;
    bool operator > (const MergeTreeDataPartCNCH & r) const;

    /// for dump
    void fromLocalPart(const IMergeTreeDataPart & local_part);

    bool isStoredOnDisk() const override { return true; }

    bool isStoredOnRemoteDisk() const override { return true; }

    bool supportsVerticalMerge() const override { return true; }

    String getFileNameForColumn(const NameAndTypePair & column) const override;

    bool hasColumnFiles(const NameAndTypePair & column) const override;

    void loadIndexGranularity(size_t marks_count, const std::vector<size_t> & index_granularities) override;

    void loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency) override;

    void loadFromFileSystem();

    void copyToDetached(const String & prefix) const override;

    UniqueKeyIndexPtr getUniqueKeyIndex() const override;

    String getFullRelativePath() const override;

    String getFullPath() const override;

    String getRelativePathForDetachedPart(const String & prefix) const override;

    /// @param allow_null whether allow delete bitmap to be nullptr
    /// @attention make sure this method is thread-safe.
    /// There are following cases that allow delete bitmap to be nullptr:
    /// 1. For new part of unique table, it's valid if its delete_bitmap_metas is empty
    /// 2. Detach commands can force detach parts even if the delete bitmap of part is broken.
    /// 3. Repair part command
    /// DELETE mutation is supported by adding a implicit column _row_exists,
    /// and we combine the original delete bitmap and _row_exists when data processing.
    ImmutableDeleteBitmapPtr getDeleteBitmap(bool allow_null = false) const override;

    /// For partial update mode, load column generated during the write phase.
    /// If the corresponding checksum does not exist, a default column is generated.
    ColumnPtr loadDeleteFlag() const;
    ColumnPtr loadUpdateColumns() const;
    ColumnPtr loadDedupSort() const;

    /// it's a no-op because in CNCH, projection parts are uploaded to parent part's data file
    virtual void projectionRemove(const String &, bool) const override { }

    void preload(UInt64 preload_level, UInt64 submit_ts) const;
    void dropDiskCache(ThreadPool & pool, bool drop_vw_disk_cache = false) const;

    void setColumnsPtr(const NamesAndTypesListPtr & new_columns_ptr) override {columns_ptr = new_columns_ptr;}

private:
    /// See #getDeleteBitmap
    ImmutableDeleteBitmapPtr getCombinedDeleteBitmapForUniqueTable(bool allow_null = false) const;
    ImmutableDeleteBitmapPtr getCombinedDeleteBitmapForNormalTable(bool allow_null = false) const;

    void combineWithRowExists(DeleteBitmapPtr & bitmap) const;

    bool isDeleted() const;

    void checkConsistency(bool require_part_metadata) const override;

    void loadIndex() override;
    IndexPtr loadIndexFromStorage() const;

    MergeTreeDataPartChecksums::FileChecksums loadPartDataFooter(size_t & out_file_size) const;

    ChecksumsPtr loadChecksums(bool require) override;
    ChecksumsPtr loadChecksumsFromRemote(bool follow_part_chain);

    UniqueKeyIndexPtr loadUniqueKeyIndex() override;

    IndexFile::RemoteFileInfo getRemoteFileInfo();

    void getUniqueKeyIndexFilePosAndSize(const IMergeTreeDataPartPtr part, off_t & off, size_t & size);

    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    void loadMetaInfoFromBuffer(ReadBuffer & buffer);

    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;
    ColumnSize getColumnSizeImpl(const NameAndTypePair & column, const ChecksumsPtr & checksums, std::unordered_set<String> * processed_substreams) const;

    void loadProjections(bool require_columns_checksums, bool check_consistency) override;

    // for projection part
    void updateCommitTimeForProjection();

    void removeImpl(bool keep_shared_data) const override;

    void fillProjectionNamesFromChecksums(const MergeTreeDataPartChecksum & checksum_file);

    std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path, size_t file_size, const String & remote_read_context = {}) const;
};

}
