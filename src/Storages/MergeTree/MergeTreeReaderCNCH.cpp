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

#include <Storages/MergeTree/MergeTreeReaderCNCH.h>

#include <unordered_map>
#include <utility>
#include <Columns/ColumnArray.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/DiskCache_fwd.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeReaderStreamWithSegmentCache.h>
#include <bits/types/clockid_t.h>
#include <Poco/Logger.h>
#include <common/getFQDNOrHostName.h>
#include <Common/Priority.h>
#include <common/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <Common/ProfileEventsTimer.h>
#include <Common/typeid_cast.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace ProfileEvents
{
    extern const Event CnchReadRowsFromDiskCache;
    extern const Event CnchReadRowsFromRemote;
    extern const Event CnchReadDataMicroSeconds;
    extern const Event CnchAddStreamsElapsedMilliseconds;
    extern const Event CnchAddStreamsParallelTasks;
    extern const Event CnchAddStreamsParallelElapsedMilliseconds;
    extern const Event CnchAddStreamsSequentialTasks;
    extern const Event CnchAddStreamsSequentialElapsedMilliseconds;
    extern const Event SkipRowsTimeMicro;
    extern const Event ReadRowsTimeMicro;
}

namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
}

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeReaderCNCH::MergeTreeReaderCNCH(
    const DataPartCNCHPtr & data_part_,
    const NamesAndTypesList & columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    const MarkRanges & mark_ranges_,
    const MergeTreeReaderSettings & settings_,
    MergeTreeIndexExecutor* index_executor_,
    const ValueSizeMap & avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    const ProgressCallback & internal_progress_cb_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_, columns_, metadata_snapshot_, uncompressed_cache_,
        mark_cache_, mark_ranges_, settings_, avg_value_size_hints_, index_executor_)
    , segment_cache_strategy(nullptr)
    , segment_cache(nullptr)
    , log(getLogger("MergeTreeReaderCNCH(" + data_part_->get_name() + ")"))
    , reader_id(UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()))

{
    if (data_part->enableDiskCache())
    {
        segment_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree);
        segment_cache_strategy = segment_cache->getStrategy();
    }

    initializeStreams(profile_callback_, internal_progress_cb_, clock_type_);

    if (settings.read_settings.remote_read_log)
        LOG_TRACE(log, "Created reader {} with {} columns: {}", reader_id, columns.size(), fmt::join(columns.getNames(), ","));
}

size_t MergeTreeReaderCNCH::readRows(size_t from_mark, size_t from_row,
    size_t max_rows_to_read, size_t current_task_last_mark, const UInt8* filter,
    Columns& res_columns)
{
    try
    {
        size_t num_bitmap_columns = hasBitmapIndexReader() ? getBitmapOutputColumns().size() : 0;
        checkNumberOfColumns(res_columns.size() - num_bitmap_columns);
        size_t num_columns = columns.size();

        std::unordered_map<String, size_t> res_col_to_idx;
        auto column_it = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++column_it)
        {
            const auto & [name, type] = getColumnFromPart(*column_it);
            res_col_to_idx[name] = i;
        }

        // If MAP implicit column and MAP column co-exist in columns, implicit column should
        // only read only once.
        // We sort columns, so that put the result of implicit column into MAP column directly.
        auto sort_columns = columns;
        if (!dup_implicit_keys.empty())
            sort_columns.sort([](const auto & lhs, const auto & rhs) { return (!lhs.type->isMap()) && rhs.type->isMap(); });

        size_t from_mark_start_row = data_part->index_granularity.getMarkStartingRow(from_mark);
        size_t starting_row = from_mark_start_row + from_row;
        size_t init_row_number = next_row_number_to_read;

        bool adjacent_reading = next_row_number_to_read >= from_mark_start_row
            && starting_row >= next_row_number_to_read;
        size_t rows_to_skip = adjacent_reading ? starting_row - next_row_number_to_read : from_row;

        if (!adjacent_reading)
        {
            next_row_number_to_read = from_mark_start_row;
        }

        {
            std::unordered_map<String, ISerialization::SubstreamsCache> caches;
            prefetchForAllColumns(Priority{1}, sort_columns, from_mark,
                current_task_last_mark, adjacent_reading, caches);
        }

        size_t skipped_rows = 0;
        if (rows_to_skip > 0)
        {
            Columns tmp_columns(sort_columns.size());
            skipped_rows = readBatch(sort_columns, num_columns, from_mark,
                adjacent_reading, rows_to_skip, current_task_last_mark, res_col_to_idx,
                filter, tmp_columns);
            next_row_number_to_read += skipped_rows;
        }

        size_t read_rows = 0;
        if (skipped_rows >= rows_to_skip)
        {
            adjacent_reading = rows_to_skip > 0 || init_row_number == starting_row;
            read_rows = readBatch(sort_columns, num_columns, from_mark, adjacent_reading,
                max_rows_to_read, current_task_last_mark, res_col_to_idx, filter,
                res_columns);
            next_row_number_to_read += read_rows;
        }

        prefetched_streams.clear();
        if (settings.read_settings.remote_read_log)
            LOG_TRACE(log, "reader {} reads {} rows into {} columns from mark {} offset {}", reader_id, read_rows, num_columns, from_mark, from_row);

        return read_rows;
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            storage.reportBrokenPart(data_part->name);

        /// Better diagnostics.
        e.addMessage("(worker node: " + getPodOrHostName() +  " while reading from part " + data_part->getFullPath() + " "
                     "from mark " + toString(from_mark) + " "
                     "with max_rows_to_read = " + toString(max_rows_to_read) + ")");
        throw;
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);

        throw;
    }
}

void MergeTreeReaderCNCH::initializeStreams(const ReadBufferFromFileBase::ProfileCallback& profile_callback, const ProgressCallback & internal_progress_cb, clockid_t clock_type)
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchAddStreamsElapsedMilliseconds, watch.elapsedMilliseconds()); });

    try
    {
        FileStreamBuilders stream_builders;

        for (const NameAndTypePair& column : columns)
        {
            initializeStreamForColumnIfNoBurden(column, profile_callback, internal_progress_cb, clock_type, &stream_builders);
        }

        executeFileStreamBuilders(stream_builders);
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}

void MergeTreeReaderCNCH::initializeStreamForColumnIfNoBurden(
    const NameAndTypePair & column,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    const ProgressCallback & internal_progress_cb,
    clockid_t clock_type,
    FileStreamBuilders * stream_builders)
{
    auto column_from_part = getColumnFromPart(column);
    if (column_from_part.type->isByteMap())
    {
        /// Scan the directory to get all implicit columns(stream) for the map type
        const DataTypeMap & type_map = typeid_cast<const DataTypeMap &>(*column_from_part.type);

        auto checksums = data_part->getChecksums();
        for (auto & file : checksums->files)
        {
            /// Need to use column_from_part to read the correct implicit column when renaming columns.
            /// Try to get keys, and form the stream, its bin file name looks like "NAME__xxxxx.bin"
            const String & file_name = file.first;
            if (isMapImplicitDataFileNameNotBaseOfSpecialMapName(file_name, column_from_part.name))
            {
                auto key_name = parseKeyNameFromImplicitFileName(file_name, column_from_part.name);
                auto impl_key_name = getImplicitColNameForMapKey(column_from_part.name, key_name);
                /// Special handing if implicit key is referenced too
                if (columns.contains(impl_key_name))
                {
                    dup_implicit_keys.insert(impl_key_name);
                }

                /// compact map is not supported in CNCH
                addStreamsIfNoBurden(
                    {impl_key_name, type_map.getValueTypeForImplicitColumn()},
                    profile_callback, internal_progress_cb, clock_type, stream_builders
                );

                map_column_keys.insert({column_from_part.name, key_name});
            }
        }
    }
    else if (column.name != "_part_row_number")
    {
        addStreamsIfNoBurden(column_from_part,
            profile_callback, internal_progress_cb, clock_type, stream_builders
        );
    }
}

void MergeTreeReaderCNCH::executeFileStreamBuilders(FileStreamBuilders& stream_builders)
{
    if (stream_builders.size() <= 1)
    {
        ProfileEvents::increment(ProfileEvents::CnchAddStreamsSequentialTasks,
            stream_builders.size());
        Stopwatch watch_seq;
        SCOPE_EXIT({
            ProfileEvents::increment(
                ProfileEvents::CnchAddStreamsSequentialElapsedMilliseconds,
                watch_seq.elapsedMilliseconds());
        });

        for (const auto & [stream_name, builder] : stream_builders)
            streams.emplace(std::move(stream_name), builder());
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::CnchAddStreamsParallelTasks, stream_builders.size());
        Stopwatch watch_pl;
        SCOPE_EXIT({
            ProfileEvents::increment(
                ProfileEvents::CnchAddStreamsParallelElapsedMilliseconds,
                watch_pl.elapsedMilliseconds());
        });

        ThreadPool pool(std::min(stream_builders.size(), 16UL));
        auto thread_group = CurrentThread::getGroup();
        for (const auto & [stream_name, builder] : stream_builders)
        {
            // placeholder
            auto it = streams.emplace(std::move(stream_name), nullptr).first;
            pool.scheduleOrThrowOnError(
                [it, thread_group, builder = std::move(builder)] {
                    setThreadName("AddStreamsThr");
                    if (thread_group)
                        CurrentThread::attachToIfDetached(thread_group);

                    it->second = builder();
                }
            );
        }
        pool.wait();
    }
}

void MergeTreeReaderCNCH::addStreamsIfNoBurden(
    const NameAndTypePair & name_and_type,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    const ProgressCallback & internal_progress_cb,
    clockid_t clock_type, FileStreamBuilders * stream_builders)
{
    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath& substream_path) {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.count(stream_name))
            return;

        String file_name = stream_name;

        auto check_validity
            = [&](String & stream_name_) -> bool { return data_part->getChecksums()->files.count(stream_name_ + DATA_FILE_EXTENSION); };

        if ((!name_and_type.type->isKVMap() && !check_validity(stream_name))
            || (name_and_type.type->isKVMap() && !tryConvertToValidKVStreamName(stream_name, check_validity)))
            return;

        // no need to do mvcc when read column in projection part
        IMergeTreeDataPartPtr source_data_part = data_part->isProjectionPart() ? data_part : data_part->getMvccDataPart(stream_name + DATA_FILE_EXTENSION);
        String mark_file_name = source_data_part->index_granularity_info.getMarksFilePath(stream_name);

        /// data file
        String data_path = source_data_part->getFullRelativePath() + "data";
        off_t data_file_offset = source_data_part->getFileOffsetOrZero(stream_name + DATA_FILE_EXTENSION);
        size_t data_file_size = source_data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION);

        /// mark file
        String mark_path = source_data_part->getFullRelativePath() + "data";
        off_t mark_file_offset = source_data_part->getFileOffsetOrZero(mark_file_name);
        size_t mark_file_size = source_data_part->getFileSizeOrZero(mark_file_name);


        if (segment_cache_strategy)
        {
            // Cache segment if necessary
            IDiskCacheSegmentsVector segments
                = segment_cache_strategy->getCacheSegments(segment_cache, segment_cache_strategy->transferRangesToSegments<PartFileDiskCacheSegment>(
                    all_mark_ranges,
                    source_data_part,
                    PartFileDiskCacheSegment::FileOffsetAndSize{mark_file_offset, mark_file_size},
                    source_data_part->getMarksCount(),
                    mark_cache,
                    segment_cache->getMetaCache().get(),
                    stream_name,
                    DATA_FILE_EXTENSION,
                    PartFileDiskCacheSegment::FileOffsetAndSize{data_file_offset, data_file_size}));
            segment_cache->cacheSegmentsToLocalDisk(segments);
        }

        bool is_lc_dict = isLowCardinalityDictionary(substream_path);
        PartHostInfo part_host{
            .disk_cache_host_port = source_data_part->disk_cache_host_port,
            .assign_compute_host_port = source_data_part->assign_compute_host_port};

        std::function<MergeTreeReaderStreamUniquePtr()> stream_builder = [=, this]() {
            return std::make_unique<MergeTreeReaderStreamWithSegmentCache>(
                source_data_part->storage.getStorageID(),
                source_data_part->getUniquePartName(),
                stream_name,
                source_data_part->volume->getDisk(),
                source_data_part->getMarksCount(),
                data_path,
                data_file_offset,
                data_file_size,
                mark_path,
                mark_file_offset,
                mark_file_size,
                all_mark_ranges,
                settings,
                mark_cache,
                uncompressed_cache,
                segment_cache.get(),
                segment_cache_strategy == nullptr ? 1 : segment_cache_strategy->getSegmentSize(),
                part_host,
                &(source_data_part->index_granularity_info),
                profile_callback,
                internal_progress_cb,
                clock_type,
                is_lc_dict
            );
            // TODO: here we can use the pointer to source_data_part's index_granularity_info, because *source_data_part will not be destoryed
        };

        // Check if mark is present
        auto mark_cache_key = mark_cache->hash(mark_path + source_data_part->getUniquePartName() + source_data_part->index_granularity_info.getMarksFilePath(stream_name));
        if (mark_cache->get(mark_cache_key))
        {
            ProfileEvents::increment(ProfileEvents::CnchAddStreamsSequentialTasks);
            Stopwatch watch_seq;
            SCOPE_EXIT({
                ProfileEvents::increment(ProfileEvents::CnchAddStreamsSequentialElapsedMilliseconds,
                watch_seq.elapsedMilliseconds());
            });

            /// able to get marks from mark cache
            streams.emplace(std::move(stream_name), stream_builder());
        }
        else
        {
            /// prepare for loading marks parallel
            stream_builders->emplace(std::move(stream_name), std::move(stream_builder));
        }
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}

size_t MergeTreeReaderCNCH::readBatch(const NamesAndTypesList& sort_columns, size_t num_columns,
    size_t from_mark, bool continue_reading, size_t rows_to_read,
    size_t current_task_last_mark, std::unordered_map<String, size_t>& res_col_to_idx,
    const UInt8* filter, Columns& res_columns)
{
    if (rows_to_read <= 0)
        return 0;

    ProfileEventsTimer timer(ProfileEvents::ReadRowsTimeMicro);

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;

    size_t processed_rows = 0;
    int row_number_column_pos = -1;
    auto name_and_type = sort_columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++name_and_type)
    {
        auto column_from_part = getColumnFromPart(*name_and_type);
        const auto& [name, type] = column_from_part;
        size_t pos = res_col_to_idx[name];

        if (!res_columns[pos])
        {
            type->enable_zero_cpy_read = this->settings.read_settings.zero_copy_read_from_cache;
            res_columns[pos] = type->createColumn();
        }

        /// row number column will be populated at last after `read_rows` is set
        if (name == "_part_row_number")
        {
            row_number_column_pos = pos;
            continue;
        }

        auto& column = res_columns[pos];
        try
        {
            auto & cache = caches[column_from_part.getNameInStorage()];
            size_t col_processed_rows = 0;
            if (type->isByteMap())
                col_processed_rows = readMapDataNotKV(column_from_part, column,
                    from_mark, continue_reading, current_task_last_mark, rows_to_read,
                    caches, res_col_to_idx, filter, res_columns);
            else
                col_processed_rows = readData(column_from_part, column, from_mark,
                    continue_reading, current_task_last_mark, rows_to_read, filter,
                    cache);

            /// For elements of Nested, column_size_before_reading may be greater than column size
            ///  if offsets are not empty and were already read, but elements are empty.
            if (!column->empty())
                processed_rows = std::max(processed_rows, col_processed_rows);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + name + ")");
            throw;
        }

        if (column->empty())
            res_columns[pos] = nullptr;
    }

    /// Populate _part_row_number column if requested
    if (row_number_column_pos >= 0)
    {
        /// update `read_rows` if no physical columns are read (only _part_row_number is requested)
        if (columns.size() == 1)
        {
            processed_rows = std::min(rows_to_read, data_part->rows_count - next_row_number_to_read);
        }

        if (processed_rows)
        {
            auto mutable_column = res_columns[row_number_column_pos]->assumeMutable();
            ColumnUInt64 & column = typeid_cast<ColumnUInt64 &>(*mutable_column);
            for (size_t i = 0, row_number = next_row_number_to_read; i < processed_rows; ++i, ++row_number)
                if (filter == nullptr || *(filter + i) != 0)
                    column.insertValue(row_number);
            res_columns[row_number_column_pos] = std::move(mutable_column);
        }
        else
        {
            res_columns[row_number_column_pos] = nullptr;
        }
    }

    if (index_executor && index_executor->valid())
    {
        Columns res_bitmap_columns;
        for (size_t i = num_columns; i < res_columns.size(); ++i)
            res_bitmap_columns.emplace_back(std::move(res_columns[i]));

        size_t bitmap_rows_read = readIndexColumns(from_mark, continue_reading,
            rows_to_read, res_bitmap_columns);

        if (filter != nullptr)
        {
            PaddedPODArray<UInt8> idx_filter(filter, filter + rows_to_read);
            for (auto & col : res_bitmap_columns)
                col = col->filter(idx_filter, col->size());
        }

        // If there is only bitmap_index columns, rows_read may be zero.
        if (processed_rows == 0)
            processed_rows = bitmap_rows_read;

        if (processed_rows != bitmap_rows_read)
        {
            throw Exception("Mismatch rows read from index_executor: " + toString(processed_rows) + " : " + toString(bitmap_rows_read), ErrorCodes::LOGICAL_ERROR);
        }
        for (size_t i = num_columns; i < res_columns.size(); ++i)
            res_columns[i] = std::move(res_bitmap_columns[i - num_columns]);
    }

    return processed_rows;
}

size_t MergeTreeReaderCNCH::readIndexColumns(size_t from_mark, bool continue_reading,
    size_t max_rows, Columns& res_bitmap_columns)
{
    size_t bitmap_rows_read = index_executor->read(from_mark, continue_reading,
            max_rows, res_bitmap_columns);

#ifdef NDEBUG
        String output_names;
        for (const auto & output_name: getBitmapOutputColumns())
            output_names += " " + output_name;
        LOG_TRACE(getLogger("index_executor"), "read bitmap index file:{} for part:{}", output_names, this->data_part->name);
#endif

    return bitmap_rows_read;
}

}
