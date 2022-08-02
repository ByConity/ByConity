#include <Storages/MergeTree/MergeTreeReaderCNCH.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeByteMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Storages/DiskCache/DiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeBitMapIndexReader.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreeReaderStreamWithSegmentCache.h>
#include <bits/types/clockid_t.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/DiskCache/DiskCache_fwd.h>
#include <Poco/Logger.h>
#include <utility>

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

MergeTreeReaderCNCH::MergeTreeReaderCNCH(const DataPartCNCHPtr& data_part_,
    const NamesAndTypesList& columns_, const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache* uncompressed_cache_, MarkCache * mark_cache_,
    const MarkRanges& mark_ranges_, const MergeTreeReaderSettings& settings_,
    MergeTreeBitMapIndexReader* bitmap_index_reader_,
    const ValueSizeMap& avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_):
        IMergeTreeReader(data_part_, columns_, metadata_snapshot_, uncompressed_cache_,
            mark_cache_, mark_ranges_, settings_, avg_value_size_hints_,
            bitmap_index_reader_),
        segment_cache_strategy(nullptr), segment_cache(nullptr)
{
    if (data_part->storage.getSettings()->enable_local_disk_cache)
    {
        auto [cache, cache_strategy] = DiskCacheFactory::instance().getDefault();

        segment_cache_strategy = std::move(cache_strategy);
        segment_cache = std::move(cache);
    }

    initializeStreams(profile_callback_, clock_type_);
}

size_t MergeTreeReaderCNCH::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (!continue_reading)
        next_row_number_to_read = data_part->index_granularity.getMarkStartingRow(from_mark);
    LOG_DEBUG(&Poco::Logger::get("MergeTreeDataPartCNCH"), "Start reading from mark {}, row {}\n", from_mark, next_row_number_to_read);

    size_t read_rows = 0;
    try
    {
        size_t num_columns = columns.size();
        checkNumberOfColumns(num_columns);

        std::unordered_map<String, size_t> res_col_to_idx;
        auto column_it = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++column_it)
        {
            const auto & [name, type] = getColumnFromPart(*column_it);
            res_col_to_idx[name] = i;
        }

        auto sort_columns = columns;
        if (!dup_implicit_keys.empty())
            sort_columns.sort([](const auto & lhs, const auto & rhs) { return (!lhs.type->isMap()) && rhs.type->isMap(); });

        /// Pointers to offset columns that are common to the nested data structure columns.
        /// If append is true, then the value will be equal to nullptr and will be used only to
        /// check that the offsets column has been already read.
        OffsetColumns offset_columns;
        std::unordered_map<String, ISerialization::SubstreamsCache> caches;

        int row_number_column_pos = -1;
        auto name_and_type = sort_columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++name_and_type)
        {
            auto column_from_part = getColumnFromPart(*name_and_type);
            const auto & [name, type] = column_from_part;
            size_t pos = res_col_to_idx[name];

            /// The column is already present in the block so we will append the values to the end.
            bool append = res_columns[pos] != nullptr;
            if (!append)
                res_columns[pos] = type->createColumn();

            /// row number column will be populated at last after `read_rows` is set
            if (name == "_part_row_number")
            {
                row_number_column_pos = pos;
                continue;
            }

            auto & column = res_columns[pos];
            try
            {
                size_t column_size_before_reading = column->size();
                auto & cache = caches[column_from_part.getNameInStorage()];
                if (type->isMap() && !type->isMapKVStore())
                    readMapDataNotKV(
                        column_from_part, column, from_mark, continue_reading, max_rows_to_read, caches, res_col_to_idx, res_columns);
                else
                    readData(column_from_part, column, from_mark, continue_reading, max_rows_to_read, cache);

                /// For elements of Nested, column_size_before_reading may be greater than column size
                ///  if offsets are not empty and were already read, but elements are empty.
                if (!column->empty())
                    read_rows = std::max(read_rows, column->size() - column_size_before_reading);
                LOG_DEBUG(&Poco::Logger::get("MergeTreeDataPartCNCH"), "Read {} rows for column {} - {}\n", read_rows, name, type->getName());
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
                read_rows = std::min(max_rows_to_read, data_part->rows_count - next_row_number_to_read);
            }

            if (read_rows)
            {
                auto mutable_column = res_columns[row_number_column_pos]->assumeMutable();
                ColumnUInt64 & column = assert_cast<ColumnUInt64 &>(*mutable_column);
                for (size_t i = 0, row_number = next_row_number_to_read; i < read_rows; ++i)
                    column.insertValue(row_number++);
                res_columns[row_number_column_pos] = std::move(mutable_column);
            }
            else
            {
                res_columns[row_number_column_pos] = nullptr;
            }
        }

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.

        if (bitmap_index_reader && bitmap_index_reader->validIndexReader())
        {
            size_t bitmap_rows_read = bitmap_index_reader->read(from_mark, continue_reading, max_rows_to_read, res_columns);
#ifndef NDEBUG
            String output_names = "";
            for (const auto & output_name: bitmap_index_reader->getOutputColumnNames())
                output_names += " " + output_name;
            LOG_TRACE(&Poco::Logger::get("bitmap_index_reader"), "read bitmap index file:{} for part:{}", output_names, this->data_part->name);
#endif
            // If there is only bitmap_index columns, rows_read may be zero.
            if (read_rows == 0)
                read_rows = bitmap_rows_read;

            if (read_rows != bitmap_rows_read)
            {
                throw Exception("Mismatch rows read from bitmap_index_reader: " + toString(read_rows) + " : " + toString(bitmap_rows_read), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            storage.reportBrokenPart(data_part->name);

        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + " "
                     "from mark " + toString(from_mark) + " "
                     "with max_rows_to_read = " + toString(max_rows_to_read) + ")");
        throw;
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);

        throw;
    }

    next_row_number_to_read += read_rows;
    return read_rows;
}

void MergeTreeReaderCNCH::initializeStreams(const ReadBufferFromFileBase::ProfileCallback& profile_callback,
    clockid_t clock_type)
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchAddStreamsElapsedMilliseconds, watch.elapsedMilliseconds()); });

    try
    {
        FileStreamBuilders stream_builders;

        for (const NameAndTypePair& column : columns)
        {
            initializeStreamForColumnIfNoBurden(column, profile_callback,
                clock_type, &stream_builders);
        }

        executeFileStreamBuilders(stream_builders);
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}

void MergeTreeReaderCNCH::initializeStreamForColumnIfNoBurden(const NameAndTypePair& column,
    const ReadBufferFromFileBase::ProfileCallback& profile_callback,
    clockid_t clock_type, FileStreamBuilders* stream_builders)
{
    auto column_from_part = getColumnFromPart(column);
    LOG_DEBUG(&Poco::Logger::get("MergeTreeDataPartCNCH"), "Initilize stream for columns {}\n", column_from_part.name);
    if (column_from_part.type->isMap() && !column_from_part.type->isMapKVStore())
    {
        // Scan the directory to get all implicit columns(stream) for the map type
        const DataTypeByteMap & type_map = typeid_cast<const DataTypeByteMap &>(*column_from_part.type);

        String key_name;
        String impl_key_name;
        {
            for (auto & file : data_part->getChecksums()->files)
            {
                // Try to get keys, and form the stream, its bin file name looks like "NAME__xxxxx.bin"
                const String & file_name = file.first;
                if (isMapImplicitDataFileNameNotBaseOfSpecialMapName(file_name, column.name))
                {
                    key_name = parseKeyNameFromImplicitFileName(file_name, column.name);
                    impl_key_name = getImplicitColNameForMapKey(column.name, key_name);
                    // Special handing if implicit key is referenced too
                    if (columns.contains(impl_key_name))
                    {
                        dup_implicit_keys.insert(impl_key_name);
                    }

                    addStreamsIfNoBurden(
                        {impl_key_name, type_map.getValueTypeForImplicitColumn()},
                        [map_col_name = column.name, this](const String& stream_name,
                                const ISerialization::SubstreamPath& substream_path) -> String {
                            String data_file_name = stream_name;
                            if (data_part->versions->enable_compact_map_data)
                            {
                                data_file_name = ISerialization::getFileNameForStream(
                                    map_col_name, substream_path);
                            }
                            return data_file_name;
                        },
                        profile_callback, clock_type, stream_builders
                    );

                    map_column_keys.insert({column.name, key_name});
                }
            }
        }
    }
    else if (isMapImplicitKeyNotKV(column.name)) // check if it's an implicit key and not KV
    {
        String map_col_name = parseMapNameFromImplicitColName(column.name);
        addStreamsIfNoBurden(
            {column.name, column.type},
            [map_col_name, this](const String& stream_name,
                    const ISerialization::SubstreamPath& substream_path) {
                String data_file_name = stream_name;
                if (data_part->versions->enable_compact_map_data)
                {
                    data_file_name = ISerialization::getFileNameForStream(
                        map_col_name, substream_path);
                }
                return data_file_name;
            },
            profile_callback, clock_type, stream_builders
        );
    }
    else if (column.name != "_part_row_number")
    {
        addStreamsIfNoBurden(column_from_part,
            [](const String& stream_name, [[maybe_unused]]const ISerialization::SubstreamPath& substream_path) {
                return stream_name;
            },
            profile_callback, clock_type, stream_builders
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

void MergeTreeReaderCNCH::addStreamsIfNoBurden(const NameAndTypePair& name_and_type,
    const std::function<String(const String&, const ISerialization::SubstreamPath&)>& file_name_getter,
    const ReadBufferFromFileBase::ProfileCallback& profile_callback,
    clockid_t clock_type, FileStreamBuilders* stream_builders)
{
    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath& substream_path) {
        String stream_name = ISerialization::getFileNameForStream(name_and_type,
            substream_path);

        if (streams.count(stream_name))
            return;

        String file_name = file_name_getter(stream_name, substream_path);
        LOG_DEBUG(&Poco::Logger::get("MergeTreeReaderCNCH"), "File name is: {}\n", file_name);
        bool data_file_exists = data_part->getChecksums()->files.count(file_name + DATA_FILE_EXTENSION);

        if (!data_file_exists)
            return;

        std::function<MergeTreeReaderStreamUniquePtr()> stream_builder = [=, this]() {
            size_t cache_segment_size = 1;
            if (segment_cache_strategy != nullptr)
            {
                // Cache segment if necessary
                IDiskCacheSegmentsVector segments = segment_cache_strategy->transferRangesToSegments<DiskCacheSegment>(
                    all_mark_ranges, data_part, stream_name, DATA_FILE_EXTENSION);
                segment_cache->cacheSegmentsToLocalDisk(segments);
            }

            String source_data_rel_path = data_part->getFullRelativePath() + "data";
            LOG_DEBUG(&Poco::Logger::get("MergeTreeReaderCNCH"), "Adding stream for reading {}\n", source_data_rel_path);
            LOG_DEBUG(&Poco::Logger::get("MergeTreeReaderCNCH"), "The disk is {}\n", data_part->volume->getDisk()->getName()); 
            String mark_file_name = data_part->index_granularity_info.getMarksFilePath(stream_name);
            LOG_DEBUG(&Poco::Logger::get("MergeTreeReaderCNCH"), mark_file_name); 
            return std::make_unique<MergeTreeReaderStreamWithSegmentCache>(
                data_part->storage.getStorageID(), data_part->get_name(),
                stream_name, data_part->volume->getDisk(), data_part->getMarksCount(),
                source_data_rel_path,
                data_part->getFileOffsetOrZero(stream_name + DATA_FILE_EXTENSION),
                data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
                source_data_rel_path,
                data_part->getFileOffsetOrZero(mark_file_name),
                data_part->getFileSizeOrZero(mark_file_name),
                all_mark_ranges, settings, mark_cache, uncompressed_cache,
                segment_cache.get(), cache_segment_size, &(data_part->index_granularity_info),
                profile_callback, clock_type
            );
        };

        // Check if mark is present
        auto mark_cache_key = mark_cache->hash(fullPath(data_part->volume->getDisk(),
            data_part->getFullRelativePath() + "data") + ":" + stream_name);
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
            /// prepare for laoding marks parallelly
            stream_builders->emplace(std::move(stream_name), std::move(stream_builder));
        }
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}

}
