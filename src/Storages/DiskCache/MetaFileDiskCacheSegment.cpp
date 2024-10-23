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

#include "MetaFileDiskCacheSegment.h"

#include <Core/UUID.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

#include "common/types.h"
#include <common/logger_useful.h>

namespace DB
{
ChecksumsDiskCacheSegment::ChecksumsDiskCacheSegment(IMergeTreeDataPartPtr data_part_, UInt64 preload_level_)
    : IDiskCacheSegment(0, 0, SegmentType::CHECKSUMS_DATA)
    , data_part(std::move(data_part_))
    , storage(data_part->storage.shared_from_this())
    , segment_name(formatSegmentName(
          UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->getUniquePartName(), "", segment_number, "checksums.txt"))
    , preload_level(preload_level_)
{
}

String ChecksumsDiskCacheSegment::getSegmentName() const
{
    return segment_name;
}

void ChecksumsDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache, bool)
{
    auto checksums = data_part->getChecksums();
    if (!checksums)
        return;

    MemoryWriteBuffer write_buffer;
    checksums->write(write_buffer);
    size_t file_size = write_buffer.count();
    if (auto read_buffer = write_buffer.tryGetReadBuffer())
    {
        disk_cache.set(getSegmentName(), *read_buffer, file_size, preload_level > 0);
        LOG_TRACE(disk_cache.getLogger(), "Cached checksums file: {}, preload_level: {}", getSegmentName(), preload_level);
    }
}

PrimaryIndexDiskCacheSegment::PrimaryIndexDiskCacheSegment(IMergeTreeDataPartPtr data_part_, UInt64 preload_level_)
    : IDiskCacheSegment(0, 0, SegmentType::PRIMARY_INDEX)
    , data_part(std::move(data_part_))
    , storage(data_part->storage.shared_from_this())
    , segment_name(formatSegmentName(
          UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->getUniquePartName(), "", segment_number, "primary.idx"))
    , preload_level(preload_level_)
{
}

String PrimaryIndexDiskCacheSegment::getSegmentName() const
{
    return segment_name;
}

void PrimaryIndexDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache, bool)
{
    auto metadata_snapshot = data_part->storage.getInMemoryMetadataPtr();
    if (data_part->isProjectionPart())
    {
        metadata_snapshot = metadata_snapshot->projections.get(data_part->name).metadata;
    }
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    size_t key_size = primary_key.column_names.size();

    if (key_size == 0)
        return;

    auto index = data_part->getIndex();
    size_t marks_count = data_part->getMarksCount();
    MemoryWriteBuffer write_buffer;

    Serializations serializations(key_size);
    for (size_t j = 0; j < key_size; ++j)
        serializations[j] = primary_key.data_types[j]->getDefaultSerialization();

    for (size_t i = 0; i < marks_count; ++i)
    {
        for (size_t j = 0; j < index->size(); ++j)
            serializations[j]->serializeBinary(*index->at(j), i, write_buffer);
    }

    size_t file_size = write_buffer.count();
    if (auto read_buffer = write_buffer.tryGetReadBuffer())
    {
        disk_cache.set(getSegmentName(), *read_buffer, file_size, preload_level > 0);
        LOG_TRACE(disk_cache.getLogger(), "Cached primary index file: {}, preload_level: {}", getSegmentName(), preload_level);
    }
}

MetaInfoDiskCacheSegment::MetaInfoDiskCacheSegment(IMergeTreeDataPartPtr data_part_, UInt64 preload_level_)
    : IDiskCacheSegment(0, 0, SegmentType::META_INFO)
    , data_part(std::move(data_part_))
    , storage(data_part->storage.shared_from_this())
    , segment_name(formatSegmentName(
          UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->getUniquePartName(), "", segment_number, "metainfo.txt"))
    , preload_level(preload_level_)
{
}

String MetaInfoDiskCacheSegment::getSegmentName() const
{
    return segment_name;
}

void MetaInfoDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache, bool)
{
    MemoryWriteBuffer write_buffer;
    if (data_part->isProjectionPart())
        writeProjectionBinary(*data_part, write_buffer);
    else
        writePartBinary(*data_part, write_buffer);

    size_t file_size = write_buffer.count();
    if (auto read_buffer = write_buffer.tryGetReadBuffer())
    {
        disk_cache.set(getSegmentName(), *read_buffer, file_size, preload_level > 0);
        LOG_TRACE(disk_cache.getLogger(), "Cached meta_info file: {}, preload_level: {}", getSegmentName(), preload_level);
    }
}

}
