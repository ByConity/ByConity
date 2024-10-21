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

#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/MapHelpers.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedDataIndex.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Common/typeid_cast.h>
#include <Storages/MergeTree/Index/MergeTreeBitmapIndexReader.h>


namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
}
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::unique_ptr<CompressedDataIndex> getCompressedIndex(
    const MergeTreeDataPartPtr& data_part, const NameAndTypePair& name_and_type,
    const ISerialization::SubstreamPath& substream_path)
{
    String stream_name = ISerialization::getFileNameForStream(name_and_type,
        substream_path);

    String stream_index_file = stream_name + DATA_FILE_EXTENSION
        + COMPRESSED_DATA_INDEX_EXTENSION;

    if (!data_part->getChecksums()->files.count(stream_index_file))
    {
        return nullptr;
    }

    DiskPtr disk = data_part->volume->getDisk();
    String rel_path = data_part->getFullRelativePath() + stream_index_file;

    return CompressedDataIndex::openForRead(disk->readFile(rel_path).get());
}

IMergeTreeReader::IMergeTreeReader(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    const MarkRanges & all_mark_ranges_,
    const MergeTreeReaderSettings & settings_,
    const ValueSizeMap & avg_value_size_hints_,
    MergeTreeIndexExecutor * index_executor_)
    : data_part(data_part_)
    , avg_value_size_hints(avg_value_size_hints_)
    , columns(columns_)
    , part_columns(data_part->getColumns())
    , uncompressed_cache(uncompressed_cache_)
    , mark_cache(mark_cache_)
    , settings(settings_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , all_mark_ranges(all_mark_ranges_)
    , index_executor(index_executor_)
    , alter_conversions(storage.getAlterConversionsForPart(data_part))
{
    if (settings.convert_nested_to_subcolumns)
    {
        columns = Nested::convertToSubcolumns(columns);
        part_columns = Nested::collect(part_columns);
    }

    columns_from_part.set_empty_key(StringRef());

    for (const auto & column_from_part : part_columns)
        columns_from_part.emplace(column_from_part.name, &column_from_part.type);

    // auto requested_columns = data_part_->getType() == MergeTreeDataPartType::WIDE ? Nested::convertToSubcolumns(columns_) : columns_;

    // columns_to_read.reserve(requested_columns.size());

    // for (const auto & column : requested_columns)
    // {
    //     columns_to_read.emplace_back(getColumnFromPart(column));
    // }
}

IMergeTreeReader::~IMergeTreeReader() = default;


const IMergeTreeReader::ValueSizeMap & IMergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}

void IMergeTreeReader::fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows, bool /* check_column_size */) const
{
    try
    {
        size_t num_bitmap_columns = hasBitmapIndexReader() ? getBitmapOutputColumns().size() : 0;

        auto mutable_this = const_cast<IMergeTreeReader *>(this);
        DB::fillMissingColumns(res_columns, num_rows, columns, metadata_snapshot, num_bitmap_columns, &(mutable_this->filled_missing_column_types));
        should_evaluate_missing_defaults = std::any_of(
            res_columns.begin(), res_columns.end(), [](const auto & column) { return column == nullptr; });
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + ")");
        throw;
    }
}

void IMergeTreeReader::evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const
{
    try
    {
        size_t num_columns = columns.size();
        size_t num_bitmap_columns = hasBitmapIndexReader() ? getBitmapOutputColumns().size() : 0;
        if (res_columns.size() != num_columns + num_bitmap_columns)
            throw Exception("invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected " + toString(num_columns + num_bitmap_columns) + ", "
                            "got " + toString(res_columns.size()), ErrorCodes::LOGICAL_ERROR);

        /// Convert columns list to block.
        /// TODO: rewrite with columns interface. It will be possible after changes in ExpressionActions.
        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            additional_columns.insert({res_columns[pos], name_and_type->type, name_and_type->name});
        }

        auto dag = DB::evaluateMissingDefaults(
                additional_columns, columns, metadata_snapshot->getColumns(), storage.getContext());
        if (dag)
        {
            auto actions = std::make_shared<
                ExpressionActions>(std::move(dag),
                ExpressionActionsSettings::fromSettings(storage.getContext()->getSettingsRef()));
            actions->execute(additional_columns);
        }

        /// Move columns from block.
        name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            res_columns[pos] = std::move(additional_columns.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + ")");
        throw;
    }
}


NameAndTypePair IMergeTreeReader::getColumnFromPart(const NameAndTypePair & required_column)
{
    if (auto iter = columns_type_cache.find(required_column.name);
        iter != columns_type_cache.end())
    {
        return iter->second;
    }

    NameAndTypePair type = columnTypeFromPart(required_column);
    columns_type_cache[required_column.name] = type;
    return type;
}

void IMergeTreeReader::performRequiredConversions(Columns & res_columns, bool /* check_column_size */)
{
    try
    {
        size_t num_columns = columns.size();
        size_t num_bitmap_columns = hasBitmapIndexReader() ? getBitmapOutputColumns().size() : 0;
        if (res_columns.size() != num_columns + num_bitmap_columns)
        {
            throw Exception(
                "Invalid number of columns passed to MergeTreeReader::performRequiredConversions. "
                "Expected "
                    + toString(num_columns + num_bitmap_columns)
                    + ", "
                      "got "
                    + toString(res_columns.size()),
                ErrorCodes::LOGICAL_ERROR);
        }

        Block copy_block;
        auto name_and_type = columns.begin();

        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            /// If some column is filled by fillMissingColumns, we should use the same type.
            auto it = filled_missing_column_types.find(name_and_type->name);
            if (it != filled_missing_column_types.end())
                copy_block.insert({res_columns[pos], it->second, name_and_type->name});
            else
                copy_block.insert({res_columns[pos], getColumnFromPart(*name_and_type).type, name_and_type->name});
        }

        DB::performRequiredConversions(copy_block, columns, storage.getContext());

        /// Move columns from block.
        name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            res_columns[pos] = std::move(copy_block.getByName(name_and_type->name).column);
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + ")");
        throw;
    }
}

IMergeTreeReader::ColumnPosition IMergeTreeReader::findColumnForOffsets(const String & column_name) const
{
    String table_name = Nested::extractTableName(column_name);
    for (const auto & part_column : data_part->getColumns())
    {
        if (typeid_cast<const DataTypeArray *>(part_column.type.get()))
        {
            auto position = data_part->getColumnPosition(part_column.name);
            if (position && Nested::extractTableName(part_column.name) == table_name)
                return position;
        }
    }

    return {};
}

void IMergeTreeReader::checkNumberOfColumns(size_t num_columns_to_read) const
{
    if (num_columns_to_read != columns.size())
        throw Exception("invalid number of columns passed to MergeTreeReader::readRows. "
                        "Expected " + toString(columns.size()) + ", "
                        "got " + toString(num_columns_to_read), ErrorCodes::LOGICAL_ERROR);
}

bool IMergeTreeReader::isLowCardinalityDictionary(const ISerialization::SubstreamPath & substream_path)
{
    // TODO: Check it's right or not in CNCH
    return substream_path.size() > 1 && substream_path[substream_path.size() - 2].type == ISerialization::Substream::Type::DictionaryKeys;
}

void IMergeTreeReader::addByteMapStreams(
    const NameAndTypePair & name_and_type,
    const String & col_name,
	const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    clockid_t clock_type)
{
    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path) {
        String implicit_stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.count(implicit_stream_name))
            return;

        String col_stream_name = implicit_stream_name;
        if (data_part->versions->enable_compact_map_data)
            col_stream_name = ISerialization::getFileNameForStream(col_name, substream_path);

        bool data_file_exists = data_part->getChecksums()->files.count(implicit_stream_name + DATA_FILE_EXTENSION);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        bool is_lc_dict = isLowCardinalityDictionary(substream_path);
        streams.emplace(
            implicit_stream_name,
            std::make_unique<MergeTreeReaderStream>(
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = data_part->volume->getDisk(),
                    .rel_path = data_part->getFullRelativePath() + col_stream_name + DATA_FILE_EXTENSION,
                    .offset = data_part->getFileOffsetOrZero(implicit_stream_name + DATA_FILE_EXTENSION),
                    .size = data_part->getFileSizeOrZero(implicit_stream_name + DATA_FILE_EXTENSION)
                },
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = data_part->volume->getDisk(),
                    .rel_path = data_part->index_granularity_info.getMarksFilePath(data_part->getFullRelativePath() + col_stream_name),
                    .offset = data_part->getFileOffsetOrZero(data_part->index_granularity_info.getMarksFilePath(implicit_stream_name)),
                    .size = data_part->getFileSizeOrZero(data_part->index_granularity_info.getMarksFilePath(implicit_stream_name)),
                },
                implicit_stream_name,
                data_part->getMarksCount(),
                all_mark_ranges,
                settings,
                mark_cache,
                uncompressed_cache,
                &data_part->index_granularity_info,
                profile_callback,
                clock_type,
                is_lc_dict
            )
        );
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}

static ReadBuffer * getStream(
    bool seek_to_start,
    const ISerialization::SubstreamPath & substream_path,
    IMergeTreeReader::FileStreams & streams,
    const NameAndTypePair & name_and_type,
    size_t from_mark, bool seek_to_mark,
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache)
{
    /// If substream have already been read.
    if (cache.contains(ISerialization::getSubcolumnNameForStream(substream_path)))
        return nullptr;

    String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

    auto check_vadility = [&](String & stream_name_) -> bool { return streams.find(stream_name_) != streams.end(); };

    if ((!name_and_type.type->isKVMap() && !check_vadility(stream_name))
        || (name_and_type.type->isKVMap() && !tryConvertToValidKVStreamName(stream_name, check_vadility)))
        return nullptr;

    IMergeTreeReaderStream & stream = *streams[stream_name];

    // Adjust right mark MUST be executed before seek(), otherwise seek position may over until_postion
    stream.adjustRightMark(current_task_last_mark);

    if (seek_to_start)
        stream.seekToStart();
    else if (seek_to_mark)
        stream.seekToMark(from_mark);

    return stream.getDataBuffer();
}

void IMergeTreeReader::deserializePrefix(
    const SerializationPtr & serialization,
    const NameAndTypePair & name_and_type,
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache)
{
    const auto & name = name_and_type.name;
    if (!deserialize_binary_bulk_state_map.contains(name))
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            return getStream(/* seek_to_start = */true, substream_path, streams, name_and_type, 0, /* seek_to_mark = */false, current_task_last_mark, cache);
        };
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_binary_bulk_state_map[name]);
    }
}


void IMergeTreeReader::prefetchForAllColumns(
    Priority priority, NamesAndTypesList & prefetch_columns, size_t from_mark, size_t current_task_last_mark, bool continue_reading)
{
    bool do_prefetch = data_part->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_prefetch
        : settings.read_settings.local_fs_prefetch;

    if (!do_prefetch)
        return;

    /// Request reading of data in advance,
    /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
    auto name_and_type = prefetch_columns.begin();
    for (size_t pos = 0; pos < prefetch_columns.size(); ++pos, ++name_and_type)
    {
        auto column_from_part = getColumnFromPart(*name_and_type);
        const auto & [name, type] = column_from_part;

        if (name == "_part_row_number")
            continue;
        try
        {
            auto & cache = caches[column_from_part.getNameInStorage()];
            if (type->isByteMap())
                prefetchForMapColumn(priority, column_from_part, from_mark, continue_reading, current_task_last_mark);
            else
                prefetchForColumn(
                    priority, column_from_part, serializations[name], from_mark, continue_reading,
                    current_task_last_mark, cache);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + name + ")");
            throw;
        }
    }
}

void IMergeTreeReader::prefetchForMapColumn(
    Priority priority,
    const NameAndTypePair & name_and_type,
    size_t from_mark,
    bool continue_reading,
    size_t current_task_last_mark)
{
    const auto & [name, type] = name_and_type;
    // collect all the substreams based on map column's name and its keys substream.
    // and somehow construct runtime two implicit columns(key&value) representation.
    auto keys_iter = map_column_keys.equal_range(name);
    const DataTypeMap & type_map = typeid_cast<const DataTypeMap &>(*type);
    DataTypePtr impl_value_type = type_map.getValueTypeForImplicitColumn();

    for (auto kit = keys_iter.first; kit != keys_iter.second; ++kit)
    {
        String impl_key_name = getImplicitColNameForMapKey(name, kit->second);
        NameAndTypePair implicit_key_name_and_type{impl_key_name, impl_value_type};
        auto cache = caches[implicit_key_name_and_type.getNameInStorage()];

        // If MAP implicit column and MAP column co-exist in columns, implicit column should
        // only read only once.
        if (dup_implicit_keys.count(impl_key_name) != 0)
            continue;

        const SerializationPtr & serialization = serializations[impl_key_name];
        prefetchForColumn(priority, implicit_key_name_and_type, serialization, from_mark,
            continue_reading, current_task_last_mark, cache);
    }
}

void IMergeTreeReader::prefetchForColumn(
    Priority priority,
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    size_t from_mark,
    bool continue_reading,
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache)
{
    deserializePrefix(serialization, name_and_type, current_task_last_mark, cache);

    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (!prefetched_streams.contains(stream_name))
        {
            bool seek_to_mark = !continue_reading;
            if (ReadBuffer * buf = getStream(false, substream_path, streams, name_and_type, from_mark, seek_to_mark, current_task_last_mark, cache))
            {
                buf->prefetch(priority);
                prefetched_streams.insert(stream_name);
            }
        }
    });
}

void IMergeTreeReader::readMapDataNotKV(
    const NameAndTypePair & name_and_type, ColumnPtr & column,
    size_t from_mark, bool continue_reading, size_t current_task_last_mark, size_t max_rows_to_read,
    std::unordered_map<String, size_t> & res_col_to_idx, Columns & res_columns)
{
    size_t column_size_before_reading = column->size();
    const auto & [name, type] = name_and_type;

    // collect all the substreams based on map column's name and its keys substream.
    // and somehow construct runtime two implicit columns(key&value) representation.
    auto keys_iter = map_column_keys.equal_range(name);
    const DataTypeMap & type_map = typeid_cast<const DataTypeMap &>(*type);
    DataTypePtr impl_value_type = type_map.getValueTypeForImplicitColumn();

    std::map<String, std::pair<size_t, const IColumn *>> impl_key_values;
    std::list<ColumnPtr> impl_key_col_holder;
    String impl_key_name;
    for (auto kit = keys_iter.first; kit != keys_iter.second; ++kit)
    {
        impl_key_name = getImplicitColNameForMapKey(name, kit->second);
        NameAndTypePair implicit_key_name_and_type{impl_key_name, impl_value_type};
        auto cache = caches[implicit_key_name_and_type.getNameInStorage()];

        // If MAP implicit column and MAP column co-exist in columns, implicit column should
        // only read only once.

        if (dup_implicit_keys.count(impl_key_name) != 0)
        {
            auto idx = res_col_to_idx[impl_key_name];
            /// Duplicated implicit key column may be droped if empty
            if (res_columns[idx])
                impl_key_values[kit->second] = std::pair<size_t, const IColumn *>(column_size_before_reading, res_columns[idx].get());
            continue;
        }


        impl_key_col_holder.push_back(impl_value_type->createColumn());
        auto & impl_value_column = impl_key_col_holder.back();
        readData(implicit_key_name_and_type, impl_value_column, from_mark, continue_reading,
            current_task_last_mark, max_rows_to_read, cache);
        impl_key_values[kit->second] = {0, &(*impl_value_column)};
    }

    // after reading all implicit values columns based files(built by keys), it's time to
    // construct runtime ColumnMap(key_column, value_column).
    dynamic_cast<ColumnMap *>(const_cast<IColumn *>(column.get()))
        ->fillByExpandedColumns(type_map, impl_key_values, std::min(max_rows_to_read, data_part->rows_count - next_row_number_to_read));
}

size_t IMergeTreeReader::skipMapDataNotKV(const NameAndTypePair& name_and_type,
    size_t from_mark, bool continue_reading, size_t current_task_last_mark,
    size_t max_rows_to_skip)
{
    const auto & [name, type] = name_and_type;

    // collect all the substreams based on map column's name and its keys substream.
    // and somehow construct runtime two implicit columns(key&value) representation.
    auto keys_iter = map_column_keys.equal_range(name);
    const DataTypeMap & type_map = typeid_cast<const DataTypeMap &>(*type);
    DataTypePtr impl_value_type = type_map.getValueTypeForImplicitColumn();

    String impl_key_name;
    for (auto kit = keys_iter.first; kit != keys_iter.second; ++kit)
    {
        impl_key_name = getImplicitColNameForMapKey(name, kit->second);
        NameAndTypePair implicit_key_name_and_type{impl_key_name, impl_value_type};
        auto cache = caches[implicit_key_name_and_type.getNameInStorage()];

        if (dup_implicit_keys.count(impl_key_name) != 0)
        {
            continue;
        }

        skipData(implicit_key_name_and_type, from_mark, continue_reading,
            current_task_last_mark, max_rows_to_skip, cache);
    }

    return std::min(max_rows_to_skip, data_part->rows_count - next_row_number_to_read);
}

void IMergeTreeReader::readData(
    const NameAndTypePair & name_and_type, ColumnPtr & column,
    size_t from_mark, bool continue_reading, size_t current_task_last_mark,
    size_t max_rows_to_read, ISerialization::SubstreamsCache & cache)
{
    double & avg_value_size_hint = avg_value_size_hints[name_and_type.name];
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hint;
    deserialize_settings.compressed_idx_getter = [&](const ISerialization::SubstreamPath& substream_path) -> std::unique_ptr<CompressedDataIndex>
    {
        return getCompressedIndex(data_part, name_and_type, substream_path);
    };

    deserialize_settings.zero_copy_read_from_cache = settings.read_settings.zero_copy_read_from_cache;

    const auto & name = name_and_type.name;
    const SerializationPtr & serialization = serializations[name];
    deserializePrefix(serialization, name_and_type, current_task_last_mark, cache);

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath& substream_path)
    {
        bool seek_to_mark = !continue_reading;

        return getStream(false, substream_path, streams, name_and_type, from_mark,
            seek_to_mark, current_task_last_mark, cache);
    };
    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name];

    serialization->deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, deserialize_settings, deserialize_state, &cache);
    IDataType::updateAvgValueSizeHint(*column, avg_value_size_hint);
}

size_t IMergeTreeReader::skipData(const NameAndTypePair & name_and_type,
    size_t from_mark, bool continue_reading, size_t current_task_last_mark,
    size_t max_rows_to_skip, ISerialization::SubstreamsCache & cache)
{
    double & avg_value_size_hint = avg_value_size_hints[name_and_type.name];
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hint;
    deserialize_settings.compressed_idx_getter = [&](const ISerialization::SubstreamPath& substream_path) -> std::unique_ptr<CompressedDataIndex>
    {
        return getCompressedIndex(data_part, name_and_type, substream_path);
    };

    const auto & name = name_and_type.name;
    const SerializationPtr & serialization = serializations[name];
    deserializePrefix(serialization, name_and_type, current_task_last_mark, cache);

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath& substream_path)
    {
        bool seek_to_mark = !continue_reading;

        return getStream(false, substream_path, streams, name_and_type, from_mark,
            seek_to_mark, current_task_last_mark, cache);
    };
    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name];

    return serialization->skipBinaryBulkWithMultipleStreams(name_and_type,
        max_rows_to_skip, deserialize_settings, deserialize_state, &cache);
}

bool IMergeTreeReader::hasBitmapIndexReader() const
{
    if (index_executor && index_executor->valid())
    {
        if (auto index_reader = index_executor->getReader(MergeTreeIndexInfo::Type::BITMAP))
        {
            if (auto * bitmap_reader = dynamic_cast<MergeTreeBitmapIndexReader *>(index_reader.get()); bitmap_reader && bitmap_reader->validIndexReader())
                return true;
        }
    }
    return false;
}

const NameOrderedSet & IMergeTreeReader::getBitmapOutputColumns() const
{
    if (index_executor)
    {
        if (auto index_reader = index_executor->getReader(MergeTreeIndexInfo::Type::BITMAP))
        {
            if (auto * bitmap_index_reader = dynamic_cast<MergeTreeBitmapIndexReader *>(index_reader.get()))
                return bitmap_index_reader->getOutputColumnNames();
        }
    }
    throw Exception("Bitmap index reader is nullptr", ErrorCodes::LOGICAL_ERROR);
}

const NamesAndTypesList & IMergeTreeReader::getBitmapColumns() const
{
    if (index_executor)
    {
        if (auto index_reader = index_executor->getReader(MergeTreeIndexInfo::Type::BITMAP))
        {
            if (auto * bitmap_index_reader = dynamic_cast<MergeTreeBitmapIndexReader *>(index_reader.get()))
                return bitmap_index_reader->getOutputColumns();
        }
    }
    throw Exception("Bitmap index reader is nullptr", ErrorCodes::LOGICAL_ERROR);
}

NameAndTypePair IMergeTreeReader::columnTypeFromPart(const NameAndTypePair & required_column)
{
    auto name_in_storage = required_column.getNameInStorage();
    if (alter_conversions.isColumnRenamed(name_in_storage))
        name_in_storage = alter_conversions.getColumnOldName(name_in_storage);

    auto it = columns_from_part.find(name_in_storage);
    if (it == columns_from_part.end())
    {
        return required_column;
    }

    const auto & type = *it->second;
    if (required_column.isSubcolumn())
    {
        auto subcolumn_name = required_column.getSubcolumnName();
        auto subcolumn_type = type->tryGetSubcolumnType(subcolumn_name);

        if (!subcolumn_type)
            return required_column;

        return {String{it->first}, subcolumn_name, type, subcolumn_type};
    }

    return {String{it->first}, type};
}

}
