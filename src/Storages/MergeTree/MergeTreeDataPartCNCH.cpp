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

#include "MergeTreeDataPartCNCH.h"
#include <memory>
#include <unordered_map>

#include <common/types.h>
#include <Common/tests/gtest_global_context.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/Priority.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/RowExistsColumnInfo.h>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/MapHelpers.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/CnchSystemLog.h>
#include <IO/LimitReadBuffer.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/FileDiskCacheSegment.h>
#include <Storages/DiskCache/MetaFileDiskCacheSegment.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Storages/MergeTree/DeleteBitmapCache.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/MergeTreeReaderCNCH.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>
#include <Storages/UUIDAndPartName.h>
#include <Storages/UniqueKeyIndexCache.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/DiskCache/BitmapIndexDiskCacheSegment.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>


namespace ProfileEvents
{
    extern const Event LoadPrimaryIndexMicroseconds;
    extern const Event PrimaryIndexDiskCacheHits;
    extern const Event PrimaryIndexDiskCacheMisses;

    extern const Event LoadDataPartFooter;
    extern const Event LoadChecksums;
    extern const Event LoadRemoteChecksums;
    extern const Event LoadChecksumsMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_FILE_IN_DATA_PART;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int DISK_CACHE_NOT_USED;
}

static constexpr auto DATA_FILE = "data";

static LimitReadBuffer readPartFile(ReadBufferFromFileBase & in, off_t file_offset, size_t file_size)
{
    if (file_size == 0)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of file is zero");

    in.seek(file_offset);
    in.setReadUntilPosition(file_offset + file_size);
    return LimitReadBuffer(in, file_size, false);
}

static std::pair<off_t, size_t> getFileOffsetAndSize(const IMergeTreeDataPart & data_part, const String & file_name)
{
    auto checksums = data_part.getChecksums();
    if (auto it = checksums->files.find(file_name); it != checksums->files.end())
    {
        return {it->second.file_offset, it->second.file_size};
    }
    else
    {
        throw Exception(fmt::format("Cannot find file {} in part {}", file_name, data_part.name), ErrorCodes::NO_FILE_IN_DATA_PART);
    }
}

MergeTreeDataPartCNCH::MergeTreeDataPartCNCH(
    const MergeTreeMetaBase & storage_,
    const String & name_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    const IMergeTreeDataPart * parent_part_,
    const UUID & part_id_)
    : IMergeTreeDataPart(storage_, name_, volume_, relative_path_, Type::CNCH, parent_part_, IStorage::StorageLocation::MAIN, part_id_)
{
}

MergeTreeDataPartCNCH::MergeTreeDataPartCNCH(
    const MergeTreeMetaBase & storage_,
    const String & name_,
    const MergeTreePartInfo & info_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    const IMergeTreeDataPart * parent_part_,
    const UUID & part_id_)
    : IMergeTreeDataPart(
        storage_, name_, info_, volume_, relative_path_, Type::CNCH, parent_part_, IStorage::StorageLocation::MAIN, part_id_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartCNCH::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings_,
    MergeTreeIndexExecutor * index_executor,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    const ProgressCallback & internal_progress_cb) const
{
    auto new_settings = reader_settings_;
    new_settings.convert_nested_to_subcolumns = true;

    auto ptr = std::static_pointer_cast<const MergeTreeDataPartCNCH>(shared_from_this());
    return std::make_unique<MergeTreeReaderCNCH>(
        ptr,
        columns_to_read,
        metadata_snapshot,
        uncompressed_cache,
        mark_cache,
        mark_ranges,
        new_settings,
        index_executor,
        avg_value_size_hints,
        profile_callback,
        internal_progress_cb);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartCNCH::getWriter(
    [[maybe_unused]] const NamesAndTypesList & columns_list,
    [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot,
    [[maybe_unused]] const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    [[maybe_unused]] const CompressionCodecPtr & default_codec_,
    [[maybe_unused]] const MergeTreeWriterSettings & writer_settings,
    [[maybe_unused]] const MergeTreeIndexGranularity & computed_index_granularity,
    [[maybe_unused]] const BitmapBuildInfo & bitmap_build_info) const
{
    return {};
}

bool MergeTreeDataPartCNCH::operator<(const MergeTreeDataPartCNCH & r) const
{
    if (!info.mutation || !r.info.mutation)
        return name < r.name;

    if (name < r.name)
        return true;
    else if (name == r.name)
        return info.mutation < r.info.mutation;
    else
        return false;
}

bool MergeTreeDataPartCNCH::operator>(const MergeTreeDataPartCNCH & r) const
{
    if (!info.mutation || !r.info.mutation)
        return name > r.name;

    if (name > r.name)
        return true;
    else if (name == r.name)
        return info.mutation > r.info.mutation;
    else
        return false;
}

void MergeTreeDataPartCNCH::fromLocalPart(const IMergeTreeDataPart & local_part)
{
    partition.assign(local_part.partition);
    if (local_part.checksums_ptr)
    {
        checksums_ptr = std::make_shared<MergeTreeDataPartChecksums>(*local_part.checksums_ptr);
        checksums_ptr->storage_type = StorageType::ByteHDFS;
    }
    else
    {
        /// anywhy we need a checksums
        checksums_ptr = std::make_shared<MergeTreeDataPartChecksums>();
        checksums_ptr->storage_type = StorageType::ByteHDFS;
    }
    minmax_idx = local_part.minmax_idx;
    rows_count = local_part.rows_count;
    row_exists_count = local_part.row_exists_count;
    loadIndexGranularity(local_part.getMarksCount(), local_part.index_granularity.getIndexGranularities());
    setColumns(local_part.getColumns());
    index = local_part.index;
    has_bitmap = local_part.has_bitmap.load();
    deleted = local_part.deleted;
    bucket_number = local_part.bucket_number;
    table_definition_hash = storage.getTableHashForClusterBy().getDeterminHash();
    columns_commit_time = local_part.columns_commit_time;
    mutation_commit_time = local_part.mutation_commit_time;
    last_modification_time = local_part.last_modification_time;
    min_unique_key = local_part.min_unique_key;
    max_unique_key = local_part.max_unique_key;
    /// TODO:
    // setAesEncrypter(local_part.getAesEncrypter());
    secondary_txn_id = local_part.secondary_txn_id;
    covered_parts_count = local_part.covered_parts_count;
    covered_parts_size = local_part.covered_parts_size;
    covered_parts_rows = local_part.covered_parts_rows;
    delete_bitmap = local_part.delete_bitmap;
    delete_flag = local_part.delete_flag;
    low_priority = local_part.low_priority;
    projection_parts = local_part.getProjectionParts();
    projection_parts_names = local_part.getProjectionPartsNames();
    ttl_infos = local_part.ttl_infos;
    ttl_infos.evalPartFinished();
}

String MergeTreeDataPartCNCH::getFileNameForColumn(const NameAndTypePair & column) const
{
    String filename;
    auto serialization = column.type->getDefaultSerialization();
    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path) {
        if (filename.empty())
            filename = ISerialization::getFileNameForStream(column, substream_path);
    });
    return filename;
}

bool MergeTreeDataPartCNCH::hasColumnFiles(const NameAndTypePair & column) const
{
    if (hasOnlyOneCompactedMapColumnNotKV())
        return true;
    auto check_stream_exists = [this](const String & stream_name) {
        auto checksums = getChecksums();
        auto bin_checksum = checksums->files.find(stream_name + DATA_FILE_EXTENSION);
        auto mrk_checksum = checksums->files.find(stream_name + index_granularity_info.marks_file_extension);

        return bin_checksum != checksums->files.end() && mrk_checksum != checksums->files.end();
    };

    if (column.type->isByteMap())
    {
        for (auto & [file, _] : getChecksums()->files)
        {
            if (isMapImplicitFileNameOfSpecialMapName(file, column.name))
                return true;
        }
        return false;
    }
    else
    {
        bool res = true;
        auto serialization = IDataType::getSerialization(column, check_stream_exists);
        serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path) {
            String file_name = ISerialization::getFileNameForStream(column, substream_path);
            if (!check_stream_exists(file_name))
                res = false;
        });

        return res;
    }
};

void MergeTreeDataPartCNCH::loadIndexGranularity(size_t marks_count, [[maybe_unused]] const std::vector<size_t> & index_granularities)
{
    if (index_granularities.empty())
        index_granularity_info.setNonAdaptive();

    /// init once
    if (index_granularity.isInitialized())
        return;

    if (!parent_part && isPartial() && isEmpty())
    {
        auto base_part = getBasePart();
        if (!base_part->index_granularity.isInitialized())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Index granularity of base part must be ready before loading partial part index granularity");
        index_granularity = base_part->index_granularity;
        return;
    }

    if (index_granularity_info.is_adaptive)
    {
        // load from disk
        // usually we don't need to load index granularity from disk because
        // kv keeps this kind of information
        if (unlikely(index_granularities.empty()))
        {
            loadIndexGranularity();
            if (marks_count != index_granularity.getMarksCount())
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Expected marks count {}, loaded marks count {} from disk",
                    marks_count,
                    index_granularity.getMarksCount());
        }
        else
        {
            for (const auto & granularity : index_granularities)
                index_granularity.appendMark(granularity);
        }
    }
    else
    {
        index_granularity.resizeWithFixedGranularity(marks_count, index_granularity_info.fixed_index_granularity);
    }

    index_granularity.setInitialized();
};

void MergeTreeDataPartCNCH::loadColumnsChecksumsIndexes(
    [[maybe_unused]] bool require_columns_checksums, [[maybe_unused]] bool check_consistency)
{
    /// only load necessary stuff here
    assertOnDisk();
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);
    getChecksums();
    if (parent_part)
        loadFromFileSystem();
    loadIndexGranularity();
    calculateEachColumnSizes(columns_sizes, total_columns_size);
    if (!parent_part)
        loadProjections(require_columns_checksums, check_consistency);

    /// FIXME:
    default_codec = CompressionCodecFactory::instance().getDefaultCodec();
}

void MergeTreeDataPartCNCH::loadFromFileSystem(bool load_hint_mutation)
{
    if (parent_part && enableDiskCache())
    {
        try
        {
            MetaInfoDiskCacheSegment metainfo_segment(shared_from_this());
            auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();
            auto [cache_disk, segment_path] = disk_cache->get(metainfo_segment.getSegmentName());
            if (cache_disk && cache_disk->exists(segment_path))
            {
                auto reader = openForReading(cache_disk, segment_path, cache_disk->getFileSize(segment_path));
                loadMetaInfoFromBuffer(*reader, load_hint_mutation);

                return;
            }
        }
        catch (...)
        {
            tryLogCurrentException("Could not load meta infos from disk");
        }
    }

    MergeTreeDataPartChecksum meta_info_pos;
    auto checksums_ptr = loadChecksumsFromRemote(false);
    meta_info_pos = checksums_ptr->files["metainfo.txt"];

    String data_rel_path = fs::path(getFullRelativePath()) / DATA_FILE;
    DiskPtr disk = volume->getDisk();
    auto reader = openForReading(disk, data_rel_path, meta_info_pos.file_size, "metainfo.txt");
    LimitReadBuffer limit_reader = readPartFile(*reader, meta_info_pos.file_offset, meta_info_pos.file_size);
    loadMetaInfoFromBuffer(limit_reader, load_hint_mutation);

    // We should load the projection's name list from the disk when loading part from the file system, e.g., attach partion.
    if (!parent_part)
        fillProjectionNamesFromChecksums(checksums_ptr->files["checksums.txt"]);

    /// we only cache meta data info for projection part, because projection part is constructed in worker's side
    if (parent_part && enableDiskCache())
    {
        auto segment = std::make_shared<MetaInfoDiskCacheSegment>(shared_from_this());
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();
        disk_cache->cacheSegmentsToLocalDisk({std::move(segment)});
    }
}

UniqueKeyIndexPtr MergeTreeDataPartCNCH::getUniqueKeyIndex() const
{
    if (!storage.getInMemoryMetadataPtr()->hasUniqueKey())
        throw Exception(
            "getUniqueKeyIndex of " + storage.getStorageID().getNameForLogs() + " which doesn't have unique key",
            ErrorCodes::LOGICAL_ERROR);
    if (rows_count == 0)
        return std::make_shared<UniqueKeyIndex>(); /// return empty index for empty part

    if (storage.unique_key_index_cache)
    {
        UUIDAndPartName key(storage.getStorageUUID(), info.getBlockName());
        auto load_func = [this] { return const_cast<MergeTreeDataPartCNCH *>(this)->loadUniqueKeyIndex(); };
        return storage.unique_key_index_cache->getOrSet(std::move(key), std::move(load_func)).first;
    }
    else
        return const_cast<MergeTreeDataPartCNCH *>(this)->loadUniqueKeyIndex();
}

ImmutableDeleteBitmapPtr MergeTreeDataPartCNCH::getCombinedDeleteBitmapForUniqueTable(bool allow_null) const
{
    ImmutableDeleteBitmapPtr res;
    {
        DeleteBitmapReadLock rlock(delete_bitmap_mutex);
        res = delete_bitmap;
    }

    if (deleted)
    {
        if (res != nullptr)
            throw Exception("Delete bitmap for part " + name + " is not null", ErrorCodes::LOGICAL_ERROR);
        /// DropPart will return a nullptr.
        return res;
    }

    if (!res)
    {
        DeleteBitmapWriteLock wlock(delete_bitmap_mutex);
        /// bitmap hasn't been set, load it from cache and metas
        if (delete_bitmap_metas.empty())
        {
            if (allow_null)
                return res;
            throw Exception("No metadata for delete bitmap of part " + name, ErrorCodes::LOGICAL_ERROR);
        }

        if (!delete_bitmap)
        {
            bool has_row_exists_column = hasRowExistsImplicitColumn();
            Int64 row_exists_mutation = has_row_exists_column ? getMutationOfRowExists() : 0;
            Stopwatch watch;
            auto cache = storage.getContext()->getDeleteBitmapCache();
            String cache_key = DeleteBitmapCache::buildKey(
                storage.getStorageUUID(), info.partition_id, info.min_block, info.max_block, row_exists_mutation);
            ImmutableDeleteBitmapPtr cached_bitmap;
            UInt64 cached_version = 0; /// 0 is an invalid value and acts as a sentinel
            bool hit_cache = cache->lookup(cache_key, cached_version, cached_bitmap);

            UInt64 target_version = delete_bitmap_metas.front()->commit_time();
            UInt64 txn_id = delete_bitmap_metas.front()->txn_id();
            if (hit_cache && cached_version == target_version)
            {
                /// common case: got the exact version of bitmap from cache
                delete_bitmap = std::move(cached_bitmap);
            }
            else
            {
                DeleteBitmapPtr bitmap = std::make_shared<Roaring>();
                std::forward_list<DataModelDeleteBitmapPtr> to_reads; /// store meta in ascending order of commit time

                bool skip_cache = false;
                if (delete_flag && target_version == 0)
                {
                    /// case: insert with _delete_flag_ & sync dedup
                    skip_cache = true;
                }

                if (cached_version > target_version || skip_cache)
                {
                    /// case: querying an older version than the cached version
                    /// then cached bitmap can't be used and we need to build the bitmap from all metas
                    to_reads = delete_bitmap_metas;
                    to_reads.reverse();
                }
                else
                {
                    /// case: querying a newer version than the cached version
                    /// if all metas > cached version, build the bitmap from all metas.
                    /// otherwise build the bitmap from the cached bitmap and newer metas (whose version > cached version)
                    for (auto & meta : delete_bitmap_metas)
                    {
                        if (meta->commit_time() > cached_version)
                        {
                            to_reads.insert_after(to_reads.before_begin(), meta);
                        }
                        else if (hit_cache && meta->commit_time() == cached_version)
                        {
                            /// Make sure that cached bitmap is not nullptr
                            if (!cached_bitmap)
                                throw Exception(
                                    ErrorCodes::LOGICAL_ERROR, "Cached bitmap is nullptr at version: {}, part: {}", cached_version, name);

                            *bitmap = *cached_bitmap; /// copy the cached bitmap as the base
                            break;
                        }
                        else
                        {
                            if (auto unique_table_log = storage.getContext()->getCloudUniqueTableLog())
                            {
                                auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, storage.getCnchStorageID());
                                current_log.metric = ErrorCodes::LOGICAL_ERROR;
                                current_log.event_msg = "Part " + name + " doesn't contain delete bitmap meta at " + toString(cached_version) + ", request bitmap meta at " + toString(meta->commit_time());
                                unique_table_log->add(current_log);
                            }
                            throw Exception(
                                "Part " + name + " doesn't contain delete bitmap meta at " + toString(cached_version) + ", request bitmap meta at " + toString(meta->commit_time()),
                                ErrorCodes::LOGICAL_ERROR);
                        }
                    }
                }

                /// union to_reads into bitmap
                for (auto & meta : to_reads)
                    deserializeDeleteBitmapInfo(storage, meta, bitmap);

                if (has_row_exists_column)
                    combineWithRowExists(bitmap);

                delete_bitmap = std::move(bitmap);
                if (target_version > cached_version && !skip_cache)
                {
                    cache->insert(cache_key, target_version, delete_bitmap);
                }

                LOG_DEBUG(
                    storage.log,
                    "Loaded delete bitmap for unique table part {} in {} ms, commit_time: {}, bitmap cardinality: {}, generated in txn: {}{}",
                    name,
                    watch.elapsedMilliseconds(),
                    target_version,
                    delete_bitmap->cardinality(),
                    txn_id,
                    skip_cache ? ", skip cache": "");
            }
        }

        res = delete_bitmap;
    }

    if (res == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Delete bitmap for part {} is null", name);
    return res;
}

ImmutableDeleteBitmapPtr MergeTreeDataPartCNCH::getCombinedDeleteBitmapForNormalTable(bool /*allow_null*/) const
{
    bool has_row_exists_column = hasRowExistsImplicitColumn();
    ImmutableDeleteBitmapPtr res;
    {
        DeleteBitmapReadLock rlock(delete_bitmap_mutex);
        res = delete_bitmap;
    }

    /// If the normal part is deleted or has no _row_exists, then the delete_bitmap must be null.
    if (deleted || !has_row_exists_column)
    {
        if (res != nullptr)
            throw Exception("Delete bitmap for part " + name + " is not null", ErrorCodes::LOGICAL_ERROR);
        return res;
    }

    if (!res)
    {
        DeleteBitmapWriteLock wlock(delete_bitmap_mutex);
        if (!delete_bitmap)
        {
            /// If the normal part contains _row_exists, then load it as delete_bitmap.
            Int64 row_exists_mutation = getMutationOfRowExists();
            Stopwatch watch;
            auto cache = storage.getContext()->getDeleteBitmapCache();
            String cache_key = DeleteBitmapCache::buildKey(storage.getStorageUUID(), info.partition_id, info.min_block, info.max_block, row_exists_mutation);
            ImmutableDeleteBitmapPtr cached_bitmap;
            UInt64 cached_version = 0;
            bool hit_cache = cache->lookup(cache_key, cached_version, cached_bitmap);

            /// use max as the placeholder for normal part's cache version.
            UInt64 target_version = std::numeric_limits<std::int64_t>::max();
            if (hit_cache && cached_version == target_version)
            {
                delete_bitmap = std::move(cached_bitmap);
            }
            else
            {
                DeleteBitmapPtr bitmap = std::make_shared<Roaring>();
                combineWithRowExists(bitmap);
                delete_bitmap = std::move(bitmap);
                cache->insert(cache_key, target_version, delete_bitmap);

                LOG_DEBUG(storage.log,
                    "Loaded delete bitmap for normal part {} in {} ms, bitmap cardinality: {}, DELETE mutation task: {}",
                    name,
                    watch.elapsedMilliseconds(),
                    delete_bitmap->cardinality(),
                    row_exists_mutation
                );
            }
        }
        res = delete_bitmap;
    }

    if (res == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Delete bitmap for part {} is null", name);
    return res;
}

void MergeTreeDataPartCNCH::combineWithRowExists(DeleteBitmapPtr & bitmap) const
{
    if (!hasRowExistsImplicitColumn())
        return;

    NamesAndTypesList columns_to_read;
    columns_to_read.emplace_back(RowExistsColumn::ROW_EXISTS_COLUMN);

    auto reader = getReader(
        columns_to_read,
        storage.getInMemoryMetadataPtr(),
        MarkRanges{MarkRange(0, getMarksCount())},
        storage.getContext()->getUncompressedCache().get(),
        storage.getContext()->getMarkCache().get(),
        MergeTreeReaderSettings {
            .save_marks_in_cache = false,
        },
        /* index_executor */ {},
        /*avg_value_size_hints*/ {},
        /*profile_callback*/ {},
        /*progress_callback*/ {}
    );

    size_t deleted_count = 0;
    Columns columns(1);
    auto read_rows = reader->readRows(0, 0, rows_count, getMarksCount(), nullptr, columns);
    if (read_rows != rows_count)
    {
        throw Exception(
            fmt::format("Can't read all rows of _row_exists. rows_in_file:{}, read_rows:{}", rows_count, read_rows),
            ErrorCodes::LOGICAL_ERROR);
    }

    auto column = columns[0];
    for (size_t i = 0; i < rows_count; ++i)
    {
        auto row_exists = column->getBool(i);
        if (!row_exists)
        {
            bitmap->add(i);
            ++deleted_count;
        }
    }
    LOG_TRACE(storage.log, "Part {} has {} rows are marked as deleted by _row_exists.", name, deleted_count);
}

ImmutableDeleteBitmapPtr MergeTreeDataPartCNCH::getDeleteBitmap(bool allow_null) const
{
    if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
        return getCombinedDeleteBitmapForUniqueTable(allow_null);

    return getCombinedDeleteBitmapForNormalTable(allow_null);
}

MergeTreeDataPartChecksums::FileChecksums MergeTreeDataPartCNCH::loadPartDataFooter(size_t & out_file_size) const
{
    ProfileEvents::increment(ProfileEvents::LoadDataPartFooter);
    const String data_file_path = fs::path(getFullRelativePath()) / DATA_FILE;
    out_file_size = volume->getDisk()->getFileSize(data_file_path);

    auto data_file = openForReading(volume->getDisk(), data_file_path, MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE, "footer");

    if (!parent_part)
    {
        data_file->setReadUntilPosition(out_file_size);
        data_file->seek(out_file_size - MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE);
    }
    else
    {
        // for projection part
        auto [projection_offset, projection_size] = getFileOffsetAndSize(*parent_part, name + ".proj");
        data_file->setReadUntilPosition(projection_offset + projection_size);
        data_file->seek(projection_offset + projection_size - MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE);
    }

    MergeTreeDataPartChecksums::FileChecksums file_checksums;
    auto add_file_checksum = [this, &buf = *data_file, &file_checksums](const String & file_name) {
        Checksum file_checksum;
        file_checksum.mutation = info.mutation;
        readIntBinary(file_checksum.file_offset, buf);
        readIntBinary(file_checksum.file_size, buf);
        readIntBinary(file_checksum.file_hash, buf);
        file_checksums[file_name] = std::move(file_checksum);
    };

    add_file_checksum("primary.idx");
    add_file_checksum("checksums.txt");
    add_file_checksum("metainfo.txt");
    add_file_checksum("unique_key.idx");

    return file_checksums;
}

bool MergeTreeDataPartCNCH::isDeleted() const
{
    return deleted;
}

void MergeTreeDataPartCNCH::checkConsistency([[maybe_unused]] bool require_part_metadata) const
{
}

void MergeTreeDataPartCNCH::loadIndex()
{
    // each projection always has the primary index in the current part
    if (!parent_part && isPartial())
    {
        /// Partial parts may not have index; primary columns never get altered, so getting index from base parts
        auto base_part = getBasePart();
        index = base_part->getIndex();
        return;
    }

    if (auto cache = storage.getContext()->getPrimaryIndexCache())
    {
        UUIDAndPartName cache_key(storage.getStorageUUID(), getUniquePartName());
        auto load_func = [this] { return loadIndexFromStorage(); };
        index = cache->getOrSet(cache_key, std::move(load_func)).first;
    }
    else
    {
        index = loadIndexFromStorage();
    }
}

IMergeTreeDataPart::IndexPtr MergeTreeDataPartCNCH::loadIndexFromStorage() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::LoadPrimaryIndexMicroseconds);
    IndexPtr res = std::make_shared<Columns>();

    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    if (primary_key.column_names.empty())
        return res;

    /// first try to load index from local disk cache
    if (enableDiskCache())
    {
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();
        PrimaryIndexDiskCacheSegment segment(shared_from_this());
        auto [cache_disk, segment_path] = disk_cache->get(segment.getSegmentName());

        if (cache_disk && cache_disk->exists(segment_path))
        {
            try
            {
                LOG_DEBUG(storage.log, "has index disk cache {}", segment_path);
                auto cache_buf = openForReading(cache_disk, segment_path, cache_disk->getFileSize(segment_path));
                res = loadIndexFromBuffer(*cache_buf, primary_key);
                ProfileEvents::increment(ProfileEvents::PrimaryIndexDiskCacheHits);
                return res;
            }
            catch (...)
            {
                tryLogCurrentException("Could not load index from disk cache");
            }
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::PrimaryIndexDiskCacheMisses);
            if (disk_cache_mode == DiskCacheMode::FORCE_DISK_CACHE)
                throw Exception(
                    ErrorCodes::DISK_CACHE_NOT_USED,
                    "Index of part {} has no disk cache {} and 'FORCE_DISK_CACHE' is set",
                    name,
                    segment_path);
        }
    }

    /// load index from remote disk
    auto checksums = getChecksums();
    auto [file_offset, file_size] = getFileOffsetAndSize(*this, "primary.idx");
    String data_rel_path = fs::path(getFullRelativePath()) / DATA_FILE;
    auto data_file = openForReading(volume->getDisk(), data_rel_path, file_size, "primary.idx");
    LimitReadBuffer buf = readPartFile(*data_file, file_offset, file_size);
    res = loadIndexFromBuffer(buf, primary_key);
    if (enableDiskCache())
    {
        auto index_seg = std::make_shared<PrimaryIndexDiskCacheSegment>(shared_from_this());
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();
        disk_cache->cacheSegmentsToLocalDisk({std::move(index_seg)});
    }
    return res;
}

IMergeTreeDataPart::ChecksumsPtr MergeTreeDataPartCNCH::loadChecksums([[maybe_unused]] bool require)
{
    ProfileEvents::increment(ProfileEvents::LoadChecksums);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::LoadChecksumsMicroseconds);
    ChecksumsPtr checksums = std::make_shared<Checksums>();
    checksums->storage_type = StorageType::ByteHDFS;
    if ((!parent_part && deleted) || (parent_part && parent_part->deleted))
        return checksums;

    if (enableDiskCache())
    {
        ChecksumsDiskCacheSegment checksums_segment(shared_from_this());
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();
        auto [cache_disk, segment_path] = disk_cache->get(checksums_segment.getSegmentName());

        if (cache_disk && cache_disk->exists(segment_path))
        {
            try
            {
                auto cache_buf = openForReading(cache_disk, segment_path, cache_disk->getFileSize(segment_path));
                if (checksums->read(*cache_buf))
                    assertEOF(*cache_buf);
                return checksums;
            }
            catch (...)
            {
                tryLogCurrentException("Could not load checksums from disk");
            }
        }
        else if (disk_cache_mode == DiskCacheMode::FORCE_DISK_CACHE)
        {
            throw Exception(
                ErrorCodes::DISK_CACHE_NOT_USED,
                "Checksums of part {} has no disk cache {} and 'FORCE_DISK_CACHE' is set",
                name,
                segment_path);
        }
    }
    return loadChecksumsFromRemote(true);
}

IMergeTreeDataPart::ChecksumsPtr MergeTreeDataPartCNCH::loadChecksumsFromRemote(bool follow_part_chain)
{
    ProfileEvents::increment(ProfileEvents::LoadRemoteChecksums);
    ChecksumsPtr checksums = std::make_shared<Checksums>();
    checksums->storage_type = StorageType::ByteHDFS;
    if ((!parent_part && deleted) || (parent_part && parent_part->deleted))
        return checksums;

    String data_rel_path = fs::path(getFullRelativePath()) / DATA_FILE;
    size_t cnch_file_size = 0;
    auto data_footer = loadPartDataFooter(cnch_file_size);
    const auto & checksum_file = data_footer["checksums.txt"];

    if (checksum_file.file_size == 0 /* && isDeleted() */)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of checksums in part {} under path {} is zero", name, data_rel_path);

    auto data_file = openForReading(volume->getDisk(), data_rel_path, checksum_file.file_size, "checksums.txt");
    LimitReadBuffer buf = readPartFile(*data_file, checksum_file.file_offset, checksum_file.file_size);

    if (checksums->read(buf))
    {
        assertEOF(buf);
    }

    // merge with data footer
    data_footer.merge(checksums->files);
    checksums->files.swap(data_footer);

    // Update checksums base on current part's mutation, the mutation in hdfs's file
    // is not reliable, since when attach, part will have new mutation, but the mutation
    // and hint_mutation within part's checksums is untouched, so update it here
    for (auto & file : checksums->files)
    {
        file.second.mutation = parent_part ? parent_part->info.mutation : info.mutation;
    }

    // For projections, we collect the projections' checkums into the head part
    // If a projection/column is deleted, a partial part with the denoted deleted checksums for the projection/column will be generated
    if (!parent_part && isPartial() && follow_part_chain)
    {
        /// merge with previous checksums with current checksums
        const auto & prev_part = getPreviousPart();
        auto prev_checksums = prev_part->getChecksums();

        /// insert checksum files from previous part if it's not in current checksums
        for (const auto & [name, file] : prev_checksums->files)
        {
            checksums->files.emplace(name, file);
        }
    }

    // remove deleted files in checksums
    // this process should be done afther the above checksums collection process
    for (auto it = checksums->files.begin(); it != checksums->files.end();)
    {
        const auto & file = it->second;
        if (file.is_deleted)
            it = checksums->files.erase(it);
        else
            ++it;
    }

    /// store in disk cache
    if (enableDiskCache() && follow_part_chain)
    {
        auto segment = std::make_shared<ChecksumsDiskCacheSegment>(shared_from_this());
        auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache();
        disk_cache->cacheSegmentsToLocalDisk({std::move(segment)});
    }

    if (!bytes_on_disk)
        bytes_on_disk = cnch_file_size;

    return checksums;
}

void MergeTreeDataPartCNCH::loadProjections([[maybe_unused]] bool require_columns_checksums, [[maybe_unused]] bool check_consistency)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    for (const auto & projection : metadata_snapshot->projections)
    {
        if (auto it = projection_parts_names.find(projection.name); it != projection_parts_names.end())
        {
            auto part = storage.createPart(
                projection.name, MergeTreeDataPartType::CNCH, {"all", 0, 0, 0}, volume, projection.name + ".proj", this);
            part->loadColumnsChecksumsIndexes(require_columns_checksums, check_consistency);
            projection_parts.emplace(projection.name, std::move(part));
            LOG_TRACE(storage.log, "Loaded Projection Part {} for part {}", projection.name, name);
        }
        else
            LOG_TRACE(storage.log, "No find projection {} in current part {}", projection.name, name);
    }
}

UniqueKeyIndexPtr MergeTreeDataPartCNCH::loadUniqueKeyIndex()
{
    return std::make_shared<UniqueKeyIndex>(
        getRemoteFileInfo(), storage.getContext()->getUniqueKeyIndexFileCache(), storage.getContext()->getUniqueKeyIndexBlockCache());
}

IndexFile::RemoteFileInfo MergeTreeDataPartCNCH::getRemoteFileInfo()
{
    /// Get base part who contains unique key index
    IMergeTreeDataPartPtr base_part = getBasePart();

    String data_path = base_part->getFullPath() + "/data";
    off_t offset = 0;
    size_t size = 0;
    getUniqueKeyIndexFilePosAndSize(base_part, offset, size);

    IndexFile::RemoteFileInfo file;
    file.disk = volume->getDisk();
    file.rel_path = base_part->getFullRelativePath() + "/data";
    file.start_offset = offset;
    file.size = size;
    file.cache_key = toString(storage.getStorageUUID()) + "_" + info.getBlockName();
    return file;
}

void MergeTreeDataPartCNCH::getUniqueKeyIndexFilePosAndSize(const IMergeTreeDataPartPtr part, off_t & off, size_t & size)
{
    String data_rel_path = fs::path(part->getFullRelativePath()) / "data";
    String data_full_path = fs::path(part->getFullPath()) / "data";

    auto reader = openForReading(volume->getDisk(), data_rel_path, MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE, "footer");
    size_t data_file_size = volume->getDisk()->getFileSize(data_rel_path);
    reader->seek(data_file_size - MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE + 3 * (2 * sizeof(size_t) + sizeof(CityHash_v1_0_2::uint128)));
    readIntBinary(off, *reader);
    readIntBinary(size, *reader);
}

void MergeTreeDataPartCNCH::loadIndexGranularity()
{
    if (index_granularity.isInitialized())
        return;

    auto checksums = getChecksums();
    index_granularity_info.changeGranularityIfRequired(*checksums);

    String full_path = getFullRelativePath();
    if (columns_ptr->empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    /// We can use any column except for ByteMap column whose data file may not exist.
    std::string marks_file_name;
    for (auto & column : *columns_ptr)
    {
        if (column.type->isByteMap())
            continue;
        marks_file_name = index_granularity_info.getMarksFilePath(getFileNameForColumn(column));
        break;
    }
    size_t marks_file_size = checksums->files.at(marks_file_name).file_size;

    if (!index_granularity_info.is_adaptive)
    {
        size_t marks_count = marks_file_size / index_granularity_info.getMarkSizeInBytes();
        index_granularity.resizeWithFixedGranularity(marks_count, storage.getSettings()->index_granularity);
    }
    else
    {
        /// TODO: use cache
        auto [file_off, file_size] = getFileOffsetAndSize(*this, marks_file_name);
        String data_path = fs::path(getFullRelativePath()) / DATA_FILE;
        auto reader = openForReading(volume->getDisk(), data_path, file_size, marks_file_name + "[index granularity]");
        LimitReadBuffer buffer = readPartFile(*reader, file_off, file_size);
        while (!buffer.eof())
        {
            size_t discard = 0;
            readIntBinary(discard, buffer); /// skip offset_in_compressed file
            readIntBinary(discard, buffer); /// offset_in_decompressed_block
            size_t granularity = 0;
            readIntBinary(granularity, buffer);
            index_granularity.appendMark(granularity);
        }

        if (index_granularity.getMarksCount() * index_granularity_info.getMarkSizeInBytes() != marks_file_size)
            throw Exception("Cannot read all marks from file " + fullPath(volume->getDisk(), data_path), ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    index_granularity.setInitialized();
}

void MergeTreeDataPartCNCH::loadMetaInfoFromBuffer(ReadBuffer & buf, bool load_hint_mutation)
{
    assertString("CHPT", buf);
    UInt8 version{0};
    readIntBinary(version, buf);

    UInt8 flags;
    readIntBinary(flags, buf);
    if (flags & IMergeTreeDataPart::DELETED_FLAG)
        deleted = true;
    if (flags & IMergeTreeDataPart::LOW_PRIORITY_FLAG)
        low_priority = true;

    size_t skip_bytes_on_disk;
    readVarUInt(skip_bytes_on_disk, buf);
    readVarUInt(rows_count, buf);
    size_t marks_count = 0;
    readVarUInt(marks_count, buf);

    Int64 hint_mutation = 0;
    readVarUInt(hint_mutation, buf);
    if (load_hint_mutation)
    {
        info.hint_mutation = hint_mutation;
    }

    columns_ptr->readText(buf);
    if (parent_part)
    {
        setColumnsPtr(columns_ptr);
        loadTTLInfos(buf);
        // shall we sync commit time for projection parts
        updateCommitTimeForProjection();
    }

    if (!parent_part)
        deserializePartitionAndMinMaxIndex(buf);
    else
        minmax_idx.initialized = true;

    if (!parent_part)
    {
        readIntBinary(bucket_number, buf);
        readIntBinary(table_definition_hash, buf);
    }
    loadIndexGranularity(marks_count, {});
}

void MergeTreeDataPartCNCH::calculateEachColumnSizes(
    [[maybe_unused]] ColumnSizeByName & each_columns_size, [[maybe_unused]] ColumnSize & total_size) const
{
    auto checksums = getChecksums();
    std::unordered_set<String> processed_substreams;
    for (const NameAndTypePair & column : *columns_ptr)
    {
        ColumnSize size = getColumnSizeImpl(column, checksums, &processed_substreams);
        each_columns_size[column.name] = size;
        total_size.add(size);

#ifndef NDEBUG
        /// Most trivial types
        if (rows_count != 0 && size.data_uncompressed != 0 && column.type->isValueRepresentedByNumber() && !column.type->haveSubtypes())
        {
            size_t rows_in_column = size.data_uncompressed / column.type->getSizeOfValueInMemory();
            /// rows_in_column = 0 may indicate that the part don't contains the column
            /// (like running mutation task involved new columns on old parts).
            if (rows_in_column != 0 && rows_in_column != rows_count)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} has rows count {} according to size in memory "
                    "and size of single value, but data part {} has {} rows",
                    backQuote(column.name),
                    rows_in_column,
                    name,
                    rows_count);
            }
        }
#endif
    }
}

ColumnSize MergeTreeDataPartCNCH::getColumnSizeImpl(const NameAndTypePair & column, const ChecksumsPtr & checksums, std::unordered_set<String> * processed_substreams) const
{
    ColumnSize size;
    if (checksums->empty())
        return size;

    // Special handling flattened map type
    if (column.type->isByteMap())
    {
        if (storage.getSettings()->enable_calculate_columns_size_without_map)
            return size;
        else
            return getMapColumnSizeNotKV(checksums, column);
    }

    auto serialization = getSerializationForColumn(column);
    serialization->enumerateStreams(
        [&](const ISerialization::SubstreamPath & substream_path) {
            String file_name = ISerialization::getFileNameForStream(column, substream_path);

            if (processed_substreams && !processed_substreams->insert(file_name).second)
                return;

            auto bin_checksum = checksums->files.find(file_name + DATA_FILE_EXTENSION);
            if (bin_checksum != checksums->files.end())
            {
                size.data_compressed += bin_checksum->second.file_size;
                size.data_uncompressed += bin_checksum->second.uncompressed_size;
            }

            auto mrk_checksum = checksums->files.find(file_name + index_granularity_info.marks_file_extension);
            if (mrk_checksum != checksums->files.end())
                size.marks += mrk_checksum->second.file_size;
        });

    return size;
}

String MergeTreeDataPartCNCH::getFullRelativePath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);
    return fs::path(storage.getRelativeDataPath(location)) / (parent_part ? parent_part->relative_path : relative_path) / "";
}

String MergeTreeDataPartCNCH::getFullPath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return fs::path(storage.getFullPathOnDisk(location, volume->getDisk())) / (parent_part ? parent_part->relative_path : relative_path)
        / "";
}

String MergeTreeDataPartCNCH::getRelativePathForDetachedPart(const String & prefix) const
{
    /// no need to check file name conflict here because part name in CNCH is unique
    String part_dir = (prefix.empty() ? "" : prefix + "_") + info.getPartNameWithHintMutation();
    return fs::path("detached") / part_dir;
}

void MergeTreeDataPartCNCH::updateCommitTimeForProjection()
{
    if (parent_part)
    {
        columns_commit_time = parent_part->columns_commit_time;
        mutation_commit_time = parent_part->mutation_commit_time;
        commit_time = parent_part->commit_time;
    }
    else
        throw Exception("Parent part cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeDataPartCNCH::removeImpl(bool /*keep_shared_data*/) const
{
    auto disk = volume->getDisk();
    auto path_on_disk = fs::path(storage.getRelativeDataPath(location)) / relative_path;
    LOG_TRACE(storage.log, "Remove the part {} from {} disk.", fullPath(disk, path_on_disk), DiskType::toString(disk->getType()));
    disk->removePart(path_on_disk);
}

void MergeTreeDataPartCNCH::fillProjectionNamesFromChecksums(const MergeTreeDataPartChecksum & checksum_file)
{
    projection_parts_names.clear();
    String data_rel_path = fs::path(getFullRelativePath()) / DATA_FILE;
    if (checksum_file.file_size == 0 /* && isDeleted() */)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of checksums in part {} under path {} is zero", name, data_rel_path);

    auto data_file = openForReading(volume->getDisk(), data_rel_path, checksum_file.file_size, "checksums.txt");
    LimitReadBuffer buf = readPartFile(*data_file, checksum_file.file_offset, checksum_file.file_size);

    ChecksumsPtr checksums = std::make_shared<Checksums>();
    checksums->storage_type = StorageType::ByteHDFS;

    if (checksums->read(buf))
    {
        assertEOF(buf);
    }

    // remove deleted files in checksums
    for (auto it = checksums->files.begin(); it != checksums->files.end();)
    {
        const auto & name = it->first;
        const auto & file = it->second;
        if (endsWith(name, ".proj") && !file.is_deleted)
        {
            projection_parts_names.insert(name.substr(0, name.find(".proj")));
        }
        ++it;
    }
}

void MergeTreeDataPartCNCH::preload(UInt64 preload_level, UInt64 submit_ts) const
{
    Stopwatch watch;
    String full_path = getFullPath();

    String part_path = fs::path(getFullRelativePath()) / DATA_FILE;
    if (!volume->getDisk()->fileExists(part_path))
    {
        LOG_WARNING(storage.log, "Can't find {} when preload level: {} before caching", full_path + DATA_FILE, preload_level);
        return;
    }
    auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree);
    auto cache_strategy = disk_cache->getStrategy();

    MarkRanges all_mark_ranges{MarkRange(0, getMarksCount())};
    IDiskCacheSegmentsVector segments;

    MarkCachePtr mark_cache_holder = storage.getContext()->getMarkCache();
    auto add_segments = [&, this](const NameAndTypePair & real_column) {
        ISerialization::StreamCallback stream_callback = [&](const ISerialization::SubstreamPath & substream_path) {
            String stream_name = ISerialization::getFileNameForStream(real_column, substream_path);
            String file_name = stream_name;
            ChecksumsPtr checksums = getChecksums();
            if (!checksums->files.count(file_name + DATA_FILE_EXTENSION))
            {
                LOG_WARNING(
                    storage.log,
                    "Can't find {} in checksum info and skip cache it: column = {}, stream = {}",
                    real_column.name,
                    stream_name,
                    file_name + DATA_FILE_EXTENSION);
                return;
            }

            String mark_file_name = index_granularity_info.getMarksFilePath(stream_name);
            String data_file_name = stream_name + DATA_FILE_EXTENSION;

            IMergeTreeDataPartPtr source_data_part
                = isProjectionPart() ? shared_from_this() : getMvccDataPart(stream_name + DATA_FILE_EXTENSION);
            auto segs = cache_strategy->transferRangesToSegments<PartFileDiskCacheSegment>(
                all_mark_ranges,
                source_data_part,
                PartFileDiskCacheSegment::FileOffsetAndSize{getFileOffsetOrZero(mark_file_name), getFileSizeOrZero(mark_file_name)},
                getMarksCount(),
                mark_cache_holder.get(),
                disk_cache->getMetaCache().get(),
                stream_name,
                DATA_FILE_EXTENSION,
                PartFileDiskCacheSegment::FileOffsetAndSize{getFileOffsetOrZero(data_file_name), getFileSizeOrZero(data_file_name)},
                preload_level);

            for (auto & seg : segs)
            {
                segments.emplace_back(seg);
            }
        };

        auto serialization = getSerializationForColumn(real_column);
        serialization->enumerateStreams(stream_callback);
    };

    for (const NameAndTypePair & column : *columns_ptr)
    {
        if (column.type->isByteMap())
        {
            // Scan the directory to get all implicit columns(stream) for the map type
            const DataTypeMap & type_map = typeid_cast<const DataTypeMap &>(*column.type);
            for (auto & file : getChecksums()->files)
            {
                // Try to get keys, and form the stream, its bin file name looks like "NAME__xxxxx.bin"
                const String & file_name = file.first;
                if (isMapImplicitDataFileNameNotBaseOfSpecialMapName(file_name, column.name))
                {
                    auto key_name = parseKeyNameFromImplicitFileName(file_name, column.name);
                    String impl_key_name = getImplicitColNameForMapKey(column.name, key_name);
                    /// compact map is not supported in CNCH
                    add_segments({impl_key_name, type_map.getValueTypeForImplicitColumn()});
                }
            }
        }
        else if (column.name != "_part_row_number")
        {
            add_segments(column);
        }
    }

    /// cache checksums & pk
    /// ChecksumsCache and PrimaryIndexCache will be set during caching to disk
    if ((preload_level & PreloadLevelSettings::MetaPreload) == PreloadLevelSettings::MetaPreload)
    {
        segments.emplace_back(std::make_shared<ChecksumsDiskCacheSegment>(shared_from_this(), preload_level));
        segments.emplace_back(std::make_shared<PrimaryIndexDiskCacheSegment>(shared_from_this(), preload_level));
        segments.emplace_back(std::make_shared<MetaInfoDiskCacheSegment>(shared_from_this(), preload_level));
        // add skip_index segment
        if (storage.getContext()->getSettingsRef().enable_skip_index)
        {
            for (const auto & index : storage.getInMemoryMetadataPtr()->getSecondaryIndices())
            {
                auto index_helper = MergeTreeIndexFactory::instance().get(index);
                auto index_name = index_helper->getFileName();

                auto mvcc_full_path = fs::path(getMvccDataPart(index_name + INDEX_FILE_EXTENSION)->getFullRelativePath()) / DATA_FILE;

                // preload additional inverted index
                if (index_helper->isInvertedIndex())
                {
                    ChecksumsPtr checksums = getChecksums();
                    std::vector<String> file_names
                        = {index_name + GIN_SEGMENT_ID_FILE_EXTENSION,
                           index_name + GIN_SEGMENT_METADATA_FILE_EXTENSION,
                           index_name + GIN_DICTIONARY_FILE_EXTENSION,
                           index_name + GIN_POSTINGS_FILE_EXTENSION};
                    for (auto & file_name : file_names)
                    {
                        auto file_iter = checksums->files.find(file_name);
                        if (file_iter == checksums->files.end())
                        {
                            LOG_WARNING(
                                storage.log,
                                "Gin index file {} is not in part {} checksums when preload level: {}",
                                file_name,
                                full_path + DATA_FILE,
                                preload_level);
                            continue;
                        }

                        size_t offset = file_iter->second.file_offset;
                        size_t size = file_iter->second.file_size;

                        std::pair<size_t, size_t> data_range = {offset, offset + size};
                        segments.emplace_back(std::make_shared<FileDiskCacheSegment>(volume->getDisk(), mvcc_full_path, ReadSettings{}, data_range, SegmentType::GIN_INDEX, file_name));
                    }
                }

                // preload common secondary index
                {
                    MergeTreeDataPartPtr source_data_part = getMvccDataPart(index_name + INDEX_FILE_EXTENSION);
                    String mark_file_name = source_data_part->index_granularity_info.getMarksFilePath(index_name);

                    off_t data_file_offset = source_data_part->getFileOffsetOrZero(index_name + INDEX_FILE_EXTENSION);
                    size_t data_file_size = source_data_part->getFileSizeOrZero(index_name + INDEX_FILE_EXTENSION);

                    off_t mark_file_offset = source_data_part->getFileOffsetOrZero(mark_file_name);
                    size_t mark_file_size = source_data_part->getFileSizeOrZero(mark_file_name);

                    IDiskCacheSegmentsVector segs = cache_strategy->transferRangesToSegments<PartFileDiskCacheSegment>(
                            all_mark_ranges,
                            source_data_part,
                            PartFileDiskCacheSegment::FileOffsetAndSize{mark_file_offset, mark_file_size},
                            getMarksCount(),
                            mark_cache_holder.get(),
                            disk_cache->getMetaCache().get(),
                            index_name,
                            INDEX_FILE_EXTENSION,
                            PartFileDiskCacheSegment::FileOffsetAndSize{data_file_offset, data_file_size},
                            preload_level);

                    for (const auto & seg : segs)
                    {
                        segments.emplace_back(seg);
                    }
                }
            }
        }

        for (const NameAndTypePair & column : *columns_ptr)
        {
            if (column.type->isBitmapIndex())
            {
                MergeTreeDataPartPtr source_data_part;
                auto file_column_name = escapeForFileName(column.name);
                auto idx_pos = FileOffsetAndSize{
                    getFileOffsetOrZero(file_column_name + BITMAP_IDX_EXTENSION),
                    getFileSizeOrZero(file_column_name + BITMAP_IDX_EXTENSION)};
                auto irk_pos = FileOffsetAndSize{
                    getFileOffsetOrZero(file_column_name + BITMAP_IRK_EXTENSION),
                    getFileSizeOrZero(file_column_name + BITMAP_IRK_EXTENSION)};

                if ((idx_pos.file_offset != 0 && idx_pos.file_size != 0) && (irk_pos.file_offset != 0 && irk_pos.file_size != 0))
                    source_data_part = getMvccDataPart(file_column_name + BITMAP_IDX_EXTENSION);
                else
                    source_data_part = shared_from_this();
                std::shared_ptr<BitmapIndexDiskCacheSegment> seg
                    = std::make_shared<BitmapIndexDiskCacheSegment>(source_data_part, column.name, BITMAP_IDX_EXTENSION);
                segments.emplace_back(seg);
            }
        }
    }

    String last_exception{};
    std::unordered_map<String, UInt64> segments_map;
    int real_cache_segments_count = 0;

    auto meta_disk_cache = disk_cache->getMetaCache();
    auto data_disk_cache = disk_cache->getDataCache();
    for (const auto & segment : segments)
    {
        try
        {
            String mark_key = segment->getMarkName();
            String seg_key = segment->getSegmentName();
            SegmentType seg_type = segment->getSegmentType();
            if (seg_type > SegmentType::FILE_DATA)
            {
                if (!mark_key.empty() && meta_disk_cache->get(mark_key).second.empty())
                {
                    segment->cacheToDisk(*meta_disk_cache);
                    segments_map[SegmentTypeToString[seg_type]]++;
                    real_cache_segments_count++;
                }
                else if (meta_disk_cache->get(seg_key).second.empty())
                {
                    segment->cacheToDisk(*meta_disk_cache);
                    segments_map[SegmentTypeToString[seg_type]]++;
                    real_cache_segments_count++;
                }
            }
            else
            {
                if (preload_level == PreloadLevelSettings::MetaPreload)
                {
                    if (meta_disk_cache->get(mark_key).second.empty())
                    {
                        segment->cacheToDisk(*meta_disk_cache);
                        segments_map[SegmentTypeToString[seg_type]]++;
                        real_cache_segments_count++;
                    }
                }
                else if (preload_level == PreloadLevelSettings::DataPreload)
                {
                    if (data_disk_cache->get(seg_key).second.empty())
                    {
                        segment->cacheToDisk(*data_disk_cache);
                        segments_map[SegmentTypeToString[seg_type]]++;
                        real_cache_segments_count++;
                    }
                }
                else
                {
                    if (meta_disk_cache->get(seg_key).second.empty() || meta_disk_cache->get(mark_key).second.empty())
                    {
                        segment->cacheToDisk(*disk_cache);
                        segments_map[SegmentTypeToString[seg_type]]++;
                        real_cache_segments_count++;
                    }
                }
            }
        }
        catch (const Exception & e)
        {
            last_exception = e.message();
            /// no exception thrown
        }
    }

    /// Preload inverted index into memory
    ContextPtr ctx = storage.getContext();
    if (auto factory = ctx->getGinIndexStoreFactory();
        factory != nullptr && ctx->getSettings().enable_skip_index
        && (preload_level & PreloadLevelSettings::MetaPreload) == PreloadLevelSettings::MetaPreload)
    {
        for (const auto& idx : storage.getInMemoryMetadataPtr()->getSecondaryIndices())
        {
            auto index_helper = MergeTreeIndexFactory::instance().get(idx);
            if (!index_helper->isInvertedIndex())
            {
                continue;
            }

            std::unique_ptr<IGinDataPartHelper> part_helper = std::make_unique<GinDataCNCHPartHelper>(
                getMvccDataPart(index_helper->getFileName() + INDEX_FILE_EXTENSION),
                DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache(),
                DiskCacheMode::USE_DISK_CACHE);
            factory->get(index_helper->getFileName(), std::move(part_helper));
        }
    }

    auto part_log = storage.getContext()->getPartLog(storage.getDatabaseName());
    part_log->add(PartLog::createElement(
        PartLogElement::PRELOAD_PART, shared_from_this(), watch.elapsedNanoseconds(), last_exception, submit_ts, real_cache_segments_count, segments_map, preload_level));

    LOG_TRACE(
        storage.log,
        "Preloaded part: {}, marks_count: {}, total_segments: {}, cached_count: {}, time_ns: {}",
        name,
        getMarksCount(),
        segments.size(),
        real_cache_segments_count,
        watch.elapsedNanoseconds());
}

void MergeTreeDataPartCNCH::dropDiskCache(ThreadPool & pool, bool drop_vw_disk_cache) const
{
    String part_base_path;
    // get the target table part path if disable drop_vw_disk_cache
    if (!drop_vw_disk_cache)
    {
        WriteBufferFromString wb(part_base_path);
        writeString(UUIDHelpers::UUIDToString(storage.getStorageUUID()), wb);
        writeChar('/', wb);
        writeString(name, wb);
    }

    auto part_log = storage.getContext()->getPartLog(storage.getDatabaseName());
    auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree);
    auto cache_strategy = disk_cache->getStrategy();

    auto impl = [part_log, part = shared_from_this(), part_base_path, disk_cache] {
        auto log_elem = PartLog::createElement(PartLogElement::DROPCACHE_PART, part);
        log_elem.path_on_disk = part_base_path;

        Stopwatch watch;
        try
        {
            size_t dropped_size = disk_cache->drop(part_base_path);
            log_elem.bytes_compressed_on_disk = dropped_size;
        }
        catch (Exception & e)
        {
            log_elem.exception = e.message();
        }

        log_elem.duration_ms = watch.elapsedMilliseconds();

        if (part_log)
        {
            part_log->add(log_elem);
        }
    };

    pool.scheduleOrThrowOnError(impl);
}

std::unique_ptr<ReadBufferFromFileBase> MergeTreeDataPartCNCH::openForReading(
    const DiskPtr & disk, const String & path, size_t file_size, const String & remote_read_context) const
{
    ReadSettings settings = storage.getContext()->getReadSettings();
    settings.adjustBufferSize(file_size);
    if (settings.remote_read_log)
        settings.remote_read_context = remote_read_context;
    return disk->readFile(path, settings);
}

}
