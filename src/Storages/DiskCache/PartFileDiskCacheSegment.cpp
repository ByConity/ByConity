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

#include "PartFileDiskCacheSegment.h"
#include <vector>

#include <IO/LimitReadBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <sys/types.h>
#include "Common/Exception.h"
#include "Common/parseAddress.h"
#include "Common/HostWithPorts.h"
#include "common/types.h"
#include "Core/Settings.h"
#include "Core/SettingsEnums.h"
#include "Storages/DistributedDataClient.h"
#include "Storages/MergeTree/MergeTreeSettings.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

static MergeTreeReaderSettings getMergeTreeReaderSettings(const ContextPtr & context, const MergeTreeMetaBase & data)
{
    MergeTreeReaderSettings settings{
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = context->getSettingsRef().checksum_on_read,
        .read_source_bitmap = !context->getSettingsRef().use_encoded_bitmap,
    };
    
    settings.setDiskCacheSteaing(data.getSettings()->disk_cache_stealing_mode);
    return settings;
}

PartFileDiskCacheSegment::PartFileDiskCacheSegment(
    UInt32 segment_number_,
    UInt32 segment_size_,
    const IMergeTreeDataPartPtr & data_part_,
    const FileOffsetAndSize & mrk_file_pos_,
    size_t marks_count_,
    MarkCache * mark_mem_cache_,
    IDiskCache * mark_disk_cache_,
    const String & stream_name_,
    const String & extension_,
    const FileOffsetAndSize & stream_file_pos_,
    UInt64 preload_level_)
    : IDiskCacheSegment(segment_number_, segment_size_, extension_ == ".bin" ? SegmentType::PART_DATA : SegmentType::SENCONDARY_INDEX)
    , data_part(data_part_)
    , storage(data_part_->storage.shared_from_this()) /// Need to extend the lifetime of storage because disk cache can run async
    , mrk_file_pos(mrk_file_pos_)
    , marks_count(marks_count_)
    , mark_mem_cache(mark_mem_cache_)
    , merge_tree_reader_settings(getMergeTreeReaderSettings(data_part_->storage.getContext(), data_part_->storage))
    , stream_name(stream_name_)
    , extension(extension_)
    , stream_file_pos(stream_file_pos_)
    , preload_level(preload_level_)
    , marks_loader(
          data_part->volume->getDisk(),
          mark_mem_cache,
          data_part->getFullRelativePath() + "data",
          stream_name,
          marks_count,
          data_part->index_granularity_info,
          /*save_marks_in_cache*/ true,
          mrk_file_pos.file_offset,
          mrk_file_pos.file_size,
          merge_tree_reader_settings,
          1,
          mark_disk_cache_)
{
}

String PartFileDiskCacheSegment::getSegmentKey(
    const StorageID & storage_id, const String & part_name, const String & stream_name, UInt32 segment_index, const String & extension)
{
    return IDiskCacheSegment::formatSegmentName(
        UUIDHelpers::UUIDToString(storage_id.uuid), part_name, stream_name, segment_index, extension);
}

String PartFileDiskCacheSegment::getSegmentName() const
{
    return formatSegmentName(
        UUIDHelpers::UUIDToString(storage->getStorageUUID()), data_part->getUniquePartName(), stream_name, segment_number, extension);
}

String PartFileDiskCacheSegment::getMarkName() const
{
    return formatSegmentName(
        UUIDHelpers::UUIDToString(storage->getStorageUUID()), data_part->getUniquePartName(), stream_name, 0, data_part->getMarksFileExtension());
}

void PartFileDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache, bool throw_exception)
{
    LoggerPtr log = disk_cache.getLogger();

    try
    {
        size_t left_mark = std::min(segment_size * segment_number, marks_count);
        size_t right_mark = std::min(segment_size * (segment_number + 1), marks_count);
        if (left_mark >= right_mark)
            return;

        size_t cache_data_left_offset = marks_loader.getMark(left_mark).offset_in_compressed_file;
        size_t cache_data_right_offset = 0;
        if (right_mark >= marks_count)
        {
            cache_data_right_offset = stream_file_pos.file_size;
        }
        else if (marks_loader.getMark(right_mark).offset_in_decompressed_block == 0)
        {
            cache_data_right_offset = marks_loader.getMark(right_mark).offset_in_compressed_file;
        }
        else
        {
            /// if right_mark is inside the block, we will need to read the whole block;
            String data_path = data_part->getFullRelativePath() + "data";
            auto disk = data_part->volume->getDisk();
            auto source_buffer = std::make_unique<CompressedReadBufferFromFile>(
                disk->readFile(data_path, merge_tree_reader_settings.read_settings), false, stream_file_pos.file_offset,
                stream_file_pos.file_size, true);

            const auto & right_mark_pos = marks_loader.getMark(right_mark);
            cache_data_right_offset = right_mark_pos.offset_in_compressed_file;

            if (right_mark_pos.offset_in_decompressed_block > 0)
            {
                source_buffer->seek(stream_file_pos.file_offset + right_mark_pos.offset_in_compressed_file, 0);
                
                /// Need to trigger CompressedReadBufferFromFile::nextImpl to get current block size.
                /// As right_mark_pos.offset_in_decompressed_block > 0, should not get EOF here.
                if (source_buffer->eof())
                    throw Exception(
                        ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, 
                        "Failed to get last compressed block size for segment {}, encounter EOF", segment_number);

                cache_data_right_offset += source_buffer->getSizeCompressed();
            }
        }

        size_t cache_data_bytes = cache_data_right_offset - cache_data_left_offset;
        LOG_TRACE(
            log,
            "cache data file: `{}` mark range [{}, {}), offset: {}, bytes: {}, preload_level: {}",
            stream_name + extension,
            left_mark,
            right_mark,
            cache_data_left_offset,
            cache_data_bytes,
            preload_level);

        String data_path = data_part->getFullRelativePath() + "data";
        auto disk = data_part->volume->getDisk();
        auto data_file = disk->readFile(data_path, merge_tree_reader_settings.read_settings);

        /// cache data segment
        if (data_part->disk_cache_mode
            != DiskCacheMode::
                FORCE_STEAL_DISK_CACHE) // FORCE_STEAL_DISK_CACHE is used for testing, which only allow remote cache request so will skip local cache write
        {
            if (!preload_level || (preload_level & PreloadLevelSettings::DataPreload) == PreloadLevelSettings::DataPreload)
            {
                data_file->seek(stream_file_pos.file_offset + cache_data_left_offset);
                LimitReadBuffer segment_value(*data_file, cache_data_bytes, false);
                disk_cache.getDataCache()->set(getSegmentName(), segment_value, cache_data_bytes, preload_level > 0);
                LOG_TRACE(disk_cache.getLogger(), "Cached part{} data file: {}, preload_level: {}", extension, getSegmentName(), preload_level);
            }

            /// cache mark segment
            if (!preload_level || (preload_level & PreloadLevelSettings::MetaPreload) == PreloadLevelSettings::MetaPreload)
            {
                data_file->seek(mrk_file_pos.file_offset);
                LimitReadBuffer marks_value(*data_file, mrk_file_pos.file_size, false);
                String marks_key = getMarkName();
                disk_cache.getMetaCache()->set(marks_key, marks_value, mrk_file_pos.file_size, preload_level > 0);
                LOG_TRACE(disk_cache.getLogger(), "Cached part{} mark file: {}, preload_level: {}", extension, marks_key, preload_level);
            }

        }

        // cache into remote node
        std::optional<String> parsed_assign_compute_host;
        if (!data_part->assign_compute_host_port.empty())
            parsed_assign_compute_host = parseAddress(data_part->assign_compute_host_port, 0).first;
        std::optional<String> parsed_disk_cache_host;
        if (!data_part->disk_cache_host_port.empty())
            parsed_disk_cache_host = parseAddress(data_part->disk_cache_host_port, 0).first;

        if (data_part->disk_cache_mode == DiskCacheMode::FORCE_STEAL_DISK_CACHE
            || ((merge_tree_reader_settings.remote_disk_cache_stealing == StealingCacheMode::READ_WRITE
                 || merge_tree_reader_settings.remote_disk_cache_stealing == StealingCacheMode::WRITE_ONLY)
                && parsed_assign_compute_host.has_value() && parsed_disk_cache_host.has_value()
                && removeBracketsIfIpv6(parsed_assign_compute_host.value()) != removeBracketsIfIpv6(parsed_disk_cache_host.value())))
        {
            std::vector<WriteFile> files{
                {getMarkName(), data_path, static_cast<UInt64>(mrk_file_pos.file_offset), mrk_file_pos.file_size},
                {getSegmentName(), data_path, static_cast<UInt64>(stream_file_pos.file_offset + cache_data_left_offset), cache_data_bytes}};

            DistributedDataClientOption option{
                .max_request_rate = disk_cache.getSettings().stealing_max_request_rate,
                .connection_timeout_ms = disk_cache.getSettings().stealing_connection_timeout_ms,
                .read_timeout_ms = disk_cache.getSettings().stealing_read_timeout_ms,
                .max_retry_times = disk_cache.getSettings().stealing_max_retry_times,
                .retry_sleep_ms = disk_cache.getSettings().stealing_retry_sleep_ms,
                .max_queue_count = disk_cache.getSettings().stealing_max_queue_count,
            };
            auto remote_data_client = std::make_shared<DistributedDataClient>(data_part->disk_cache_host_port, "", option);
            remote_data_client->write(disk->getName(), files);
            LOG_TRACE(
                disk_cache.getLogger(),
                "Submit remote({}) cache file, mark_key: {}, data_key: {}, preload_level: {}",
                data_part->disk_cache_host_port,
                getMarkName(),
                getSegmentName(),
                preload_level);
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to cache segment to local disk.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        if (throw_exception)
            throw Exception(e);
    }
}
}
