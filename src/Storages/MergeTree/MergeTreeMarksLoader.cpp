/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Common/ProfileEvents.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <boost/algorithm/string/split.hpp>
#include "common/getFQDNOrHostName.h"
#include "Core/SettingsEnums.h"
#include "Common/parseAddress.h"
#include "Common/HostWithPorts.h"
#include "IO/ReadBufferFromRpcStreamFile.h"
#include "IO/ReadBufferFromRpcStreamFile.h"
#include "Storages/DistributedDataClient.h"
#include "Storages/MergeTree/IMergeTreeDataPart.h"

#include <utility>
#include <vector>

namespace ProfileEvents
{
    extern const Event CnchLoadMarksRequests;
    extern const Event CnchLoadMarksBytes;
    extern const Event CnchLoadMarksMicroseconds;
    extern const Event CnchLoadMarksFromDiskCacheRequests;
    extern const Event CnchLoadMarksFromDiskCacheBytes;
    extern const Event CnchLoadMarksFromDiskCacheMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

MergeTreeMarksLoader::MergeTreeMarksLoader(
    DiskPtr disk_,
    MarkCache * mark_cache_,
    const String & mrk_path_,
    const String & stream_name_,
    size_t marks_count_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    bool save_marks_in_cache_,
    off_t mark_file_offset_,
    size_t mark_file_size_,
    const MergeTreeReaderSettings & settings_,
    size_t columns_in_mark_,
    IDiskCache * disk_cache_,
    const PartHostInfo & part_host_,
    UUID storage_uuid_,
    const String & part_name_)
    : disk(std::move(disk_))
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , stream_name(stream_name_)
    , marks_count(marks_count_)
    , mark_file_offset(mark_file_offset_)
    , mark_file_size(mark_file_size_)
    , index_granularity_info(index_granularity_info_)
    , save_marks_in_cache(save_marks_in_cache_)
    , columns_in_mark(columns_in_mark_)
    , settings(settings_)
    , disk_cache(disk_cache_)
    , part_host(part_host_)
    , storage_uuid {storage_uuid_}
    , part_name(part_name_) {}

const MarkInCompressedFile & MergeTreeMarksLoader::getMark(size_t row_index, size_t column_index)
{
    if (!marks)
        loadMarks();

    if (column_index >= columns_in_mark)
        throw Exception("Column index: " + toString(column_index)
            + " is out of range [0, " + toString(columns_in_mark) + ")", ErrorCodes::LOGICAL_ERROR);

    return (*marks)[row_index * columns_in_mark + column_index];
}

MarkCache::MappedPtr MergeTreeMarksLoader::loadMarksImpl()
{
    ReadSettings load_mark_read_settings = settings.read_settings;
    load_mark_read_settings.adjustBufferSize(mark_file_size);
    if (load_mark_read_settings.remote_read_log)
        load_mark_read_settings.remote_read_context = index_granularity_info.getMarksFilePath(stream_name);

    enum class MetricType
    {
        Counts,
        Bytes,
        Microseconds,
    };

    const ProfileEvents::Event events_map[][3] = {
        {ProfileEvents::CnchLoadMarksRequests, ProfileEvents::CnchLoadMarksBytes, ProfileEvents::CnchLoadMarksMicroseconds},
        {ProfileEvents::CnchLoadMarksFromDiskCacheRequests, ProfileEvents::CnchLoadMarksFromDiskCacheBytes, ProfileEvents::CnchLoadMarksFromDiskCacheMicroseconds}
    };

    bool from_disk_cache = false;

    auto select_metric = [&](MetricType type)
    {
        return events_map[from_disk_cache][static_cast<int>(type)];
    };

    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker;

    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(select_metric(MetricType::Microseconds), watch.elapsedMicroseconds()); });

    size_t mark_size = index_granularity_info.getMarkSizeInBytes(columns_in_mark);
    size_t expected_file_size = mark_size * marks_count;

    if (expected_file_size != mark_file_size)
        throw Exception(
            "Bad size of marks file '" + fullPath(disk, mrk_path) + "' for stream '" + stream_name + "': " + std::to_string(mark_file_size) + ", must be: " + std::to_string(expected_file_size),
            ErrorCodes::CORRUPTED_DATA);

    auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns_in_mark);

    if (!index_granularity_info.is_adaptive)
    {
        std::optional<String> parsed_assign_compute_host;
        if (!part_host.assign_compute_host_port.empty())
            parsed_assign_compute_host = parseAddress(part_host.assign_compute_host_port, 0).first;
        std::optional<String> parsed_disk_cache_host;
        if (!part_host.disk_cache_host_port.empty())
            parsed_disk_cache_host = parseAddress(part_host.disk_cache_host_port, 0).first;

        LOG_TRACE(
            getLogger(__func__),
            "Current node host vs disk cache host: {} vs {}",
            parsed_assign_compute_host.has_value() ? removeBracketsIfIpv6(parsed_assign_compute_host.value()) : "",
            parsed_disk_cache_host.has_value() ? removeBracketsIfIpv6(parsed_disk_cache_host.value()) : "");
        auto buffer = [&, this]() -> std::unique_ptr<ReadBufferFromFileBase> {
            if (disk_cache)
            {
                try
                {
                    String mrk_seg_key = IDiskCacheSegment::formatSegmentName(
                        UUIDHelpers::UUIDToString(storage_uuid), part_name, stream_name, 0, index_granularity_info.marks_file_extension);
                    auto [local_cache_disk, local_cache_path] = disk_cache->get(mrk_seg_key);
                    if (local_cache_disk && local_cache_disk->exists(local_cache_path) && settings.read_settings.disk_cache_mode != DiskCacheMode::FORCE_STEAL_DISK_CACHE)
                    {
                        from_disk_cache = true;
                        LOG_TRACE(getLogger(__func__), "load from local disk cache {}, mrk_path {}", local_cache_disk->getPath(), local_cache_path);
                        size_t cached_mark_file_size = local_cache_disk->getFileSize(local_cache_path);
                        if (expected_file_size != cached_mark_file_size)
                            throw Exception(
                                "Bad size of marks file on disk cache'" + fullPath(local_cache_disk, local_cache_path) + "' for stream '"
                                    + stream_name + "': " + std::to_string(cached_mark_file_size)
                                    + ", must be: " + std::to_string(expected_file_size),
                                ErrorCodes::CORRUPTED_DATA);
                        return local_cache_disk->readFile(local_cache_path, load_mark_read_settings);
                    }
                    else if (
                        (parsed_assign_compute_host.has_value() && parsed_disk_cache_host.has_value()
                         && removeBracketsIfIpv6(parsed_assign_compute_host.value()) != removeBracketsIfIpv6(parsed_disk_cache_host.value())
                         && (settings.remote_disk_cache_stealing == StealingCacheMode::READ_WRITE
                             || settings.remote_disk_cache_stealing == StealingCacheMode::READ_ONLY))
                        || settings.read_settings.disk_cache_mode == DiskCacheMode::FORCE_STEAL_DISK_CACHE)
                    {
                        DistributedDataClientOption option{
                            .max_request_rate = disk_cache->getSettings().stealing_max_request_rate,
                            .connection_timeout_ms = disk_cache->getSettings().stealing_connection_timeout_ms,
                            .read_timeout_ms = disk_cache->getSettings().stealing_read_timeout_ms,
                            .max_retry_times = disk_cache->getSettings().stealing_max_retry_times,
                            .retry_sleep_ms = disk_cache->getSettings().stealing_retry_sleep_ms,
                            .max_queue_count = disk_cache->getSettings().stealing_max_queue_count,
                        };
                        auto remote_data_client = std::make_shared<DistributedDataClient>(part_host.disk_cache_host_port, mrk_seg_key, option);
                        auto remote_cache_file = std::make_unique<ReadBufferFromRpcStreamFile>(remote_data_client, mark_file_size);
                        if (remote_cache_file->getFileName().empty())
                        {
                            LOG_TRACE(getLogger(__func__), "load from remote filesystem mrk_path {} since remote disk cache is empty", mrk_path);
                            auto buf = disk->readFile(mrk_path, load_mark_read_settings);
                            if (buf->seek(mark_file_offset) != mark_file_offset)
                                throw Exception(
                                    "Cannot seek to mark file  " + mrk_path + " for stream " + stream_name,
                                    ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
                            return buf;
                        }

                        LOG_TRACE(
                            getLogger(__func__),
                            "load from remote disk cache mrk_path {}/{}, size = {}",
                            part_host.disk_cache_host_port,
                            remote_cache_file->getFileName(), remote_cache_file->getFileSize());

                        if (expected_file_size != remote_cache_file->getFileSize())
                            throw Exception(
                                "Bad size of marks file on remote disk cache'" + fullPath(local_cache_disk, local_cache_path)
                                    + "' for stream '" + stream_name + "': " + std::to_string(remote_cache_file->getFileSize())
                                    + ", must be: " + std::to_string(expected_file_size),
                                ErrorCodes::CORRUPTED_DATA);
                        return remote_cache_file;
                    }
                }
                catch (...)
                {
                    tryLogCurrentException("Could not load marks from disk cache");
                }
            }

            LOG_TRACE(getLogger(__func__), "load from remote filesystem mrk_path {}", mrk_path);
            auto buf = disk->readFile(mrk_path, load_mark_read_settings);
            if (buf->seek(mark_file_offset) != mark_file_offset)
                throw Exception("Cannot seek to mark file  " + mrk_path + " for stream " + stream_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
            // prefetch mark file
            buf->setReadUntilPosition(mark_file_offset + mark_file_size);
            return buf;
        }();

        try
        {
            buffer->readStrict(reinterpret_cast<char *>(res->data()), mark_file_size);
        }
        catch (DB::Exception & e)
        {
            e.addMessage(
                "while loading marks from file: {}, file size: {}({}), remote file offset: {}",
                buffer->getFileName(),
                mark_file_size,
                buffer->getFileSize(),
                mark_file_offset);

            throw;
        }
    }
    else
    {
        auto buffer = disk->readFile(mrk_path, load_mark_read_settings);
        if (buffer->seek(mark_file_offset) != mark_file_offset)
            throw Exception("Cannot seek to mark file  " + mrk_path + " for stream " + stream_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        size_t i = 0;
        off_t limit_offset_in_file = mark_file_offset + mark_file_size;
        while (buffer->getPosition() < limit_offset_in_file)
        {
            res->read(*buffer, i * columns_in_mark, columns_in_mark);
            buffer->seek(sizeof(size_t), SEEK_CUR);
            ++i;
        }

        if (i * mark_size != mark_file_size)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all marks from file {}, marks expected {} (bytes size {}), marks read {} (bytes size{})",
                mrk_path, marks_count, mark_file_size, i, buffer->count());
    }
    res->protect();

    ProfileEvents::increment(select_metric(MetricType::Counts));
    ProfileEvents::increment(select_metric(MetricType::Bytes), mark_file_size);
    return res;
}

void MergeTreeMarksLoader::loadMarks()
{
    String mrk_name = index_granularity_info.getMarksFilePath(stream_name);
    if (mark_cache && !part_name.empty())
    {
        auto key = mark_cache->hash(mrk_path + part_name + mrk_name);
        if (save_marks_in_cache)
        {
            auto callback = [this]{ return loadMarksImpl(); };
            marks = mark_cache->getOrSet(key, callback);
        }
        else
        {
            marks = mark_cache->get(key);
            if (!marks)
                marks = loadMarksImpl();
        }
    }
    else
        marks = loadMarksImpl();

    if (!marks)
        throw Exception("Failed to load marks: " + mrk_name + " from path:" + mrk_path, ErrorCodes::LOGICAL_ERROR);
}

}
