#include <Storages/MergeTree/MergeTreeReaderWide.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnByteMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeByteMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <utility>

namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
    //constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeReaderWide::MergeTreeReaderWide(
    DataPartWidePtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    IMergeTreeDataPart::ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        std::move(data_part_),
        std::move(columns_),
        metadata_snapshot_,
        uncompressed_cache_,
        std::move(mark_cache_),
        std::move(mark_ranges_),
        std::move(settings_),
        std::move(avg_value_size_hints_))
{
    try
    {
        for (const NameAndTypePair & column : columns)
        {
            auto column_from_part = getColumnFromPart(column);
            if (column_from_part.type->isMap() && !column_from_part.type->isMapKVStore())
            {
                // Scan the directory to get all implicit columns(stream) for the map type
                const DataTypeByteMap & type_map = typeid_cast<const DataTypeByteMap&>(*column_from_part.type);
                //auto& keyType = type_map.getKeyType();
                auto& valueType = type_map.getValueType();

                String keyName;
                String implKeyName;
                {
                    //auto data_lock = data_part->getColumnsReadLock();
                    for (auto & file : data_part->checksums.files)
                    {
                        //Try to get keys, and form the stream, its bin file name looks like "NAME__xxxxx.bin"
                        const String & fileName = file.first;
                        bool parseSuccess = parseKeyFromImplicitFileName(column.name, fileName, keyName);

                        if (parseSuccess)
                        {
                            implKeyName = getImplicitFileNameForMapKey(column.name, keyName);
                            // Special handing if implicit key is referenced too
                            if (columns.contains(implKeyName))
                            {
                                dupImplicitKeys.insert(implKeyName);
                            }

                            if (type_map.valueTypeIsLC())
                                addByteMapStreams({implKeyName, valueType}, column.name, profile_callback_, clock_type_);
                            else
                                addByteMapStreams({implKeyName, makeNullable(valueType)}, column.name, profile_callback_, clock_type_);

                            mapColumnKeys.insert({column.name, keyName});
                        }

                        //TBD: it's possible that no relevant streams for MAP type column. Do we need any special handling??
                    }
                }
            }
            else if (isMapImplicitKeyNotKV(column.name)) // check if it's an implicit key and not KV
            {
                addByteMapStreams({column.name, column.type}, parseColNameFromImplicitName(column.name), profile_callback_, clock_type_);
            }
            else
            {
                // TODO
                //String column_name = column_from_part.name;
                //if (checkBitEngineColumn(column_from_part))
                //	column_name = column_from_part.name + BITENGINE_COLUMN_EXTENSION;
                //addStreams(column_name, *column_from_part.type, profile_callback, clock_type);
                addStreams(column_from_part, profile_callback_, clock_type_);
            } 
        }

        if (!dupImplicitKeys.empty()) names = columns.getNames();
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}

size_t MergeTreeReaderWide::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
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
        if (!dupImplicitKeys.empty())
            sort_columns.sort([](const auto & lhs, const auto & rhs) { return (!lhs.type->isMap()) && rhs.type->isMap(); });

        /// Pointers to offset columns that are common to the nested data structure columns.
        /// If append is true, then the value will be equal to nullptr and will be used only to
        /// check that the offsets column has been already read.
        OffsetColumns offset_columns;
        std::unordered_map<String, ISerialization::SubstreamsCache> caches;

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

            auto & column = res_columns[pos];
            try
            {
                size_t column_size_before_reading = column->size();
                auto & cache = caches[column_from_part.getNameInStorage()];
				if (type->isMap() && !type->isMapKVStore())
                {
                    // collect all the substreams based on map column's name and its keys substream.
                    // and somehow construct runtime two implicit columns(key&value) representation.
                    auto keysIter = mapColumnKeys.equal_range(name);
                    const DataTypeByteMap & type_map = typeid_cast<const DataTypeByteMap&>(*type);
                    auto & valueType = type_map.getValueType();

                    DataTypePtr implValueType;
                    if (type_map.valueTypeIsLC())
                        implValueType = valueType;
                    else
                        implValueType = makeNullable(valueType);

                    std::map<String, std::pair<size_t, const IColumn *>> implKeyValues;
                    std::list<ColumnPtr> implKeyColHolder;
                    String implKeyName;
                    for (auto kit = keysIter.first; kit != keysIter.second; ++kit)
                    {
                        implKeyName = getImplicitFileNameForMapKey(name, kit->second);
                        NameAndTypePair implicit_key_name_and_type{implKeyName, implValueType};
                        cache = caches[implicit_key_name_and_type.getNameInStorage()];

                        // If MAP implicit column and MAP column co-exist in columns, implicit column should
                        // only read only once.

                        if (dupImplicitKeys.count(implKeyName) !=0)
                        {
                            auto idx = res_col_to_idx[implKeyName];
                            implKeyValues[kit->second] = std::pair<size_t, const IColumn*>(column_size_before_reading, res_columns[idx].get());
                            continue;
                        }


                        implKeyColHolder.push_back(implValueType->createColumn());
                        auto& implValueColumn = implKeyColHolder.back();
                        readData(implicit_key_name_and_type, 
								 implValueColumn, from_mark, continue_reading, 
							     max_rows_to_read, cache);
                        implKeyValues[kit->second] = {0, &(*implValueColumn)};
                    }

                    // after reading all implicit values columns based files(built by keys), it's time to
                    // construct runtime ColumnMap(keyColumn, valueColumn).
                    dynamic_cast<ColumnByteMap*>(const_cast<IColumn *>(column.get()))->fillByExpandedColumns(type_map, implKeyValues);

                }
				else
				{
                	readData(column_from_part, column, from_mark, continue_reading, max_rows_to_read, cache);
				}
                /// For elements of Nested, column_size_before_reading may be greater than column size
                ///  if offsets are not empty and were already read, but elements are empty.
                if (!column->empty())
                    read_rows = std::max(read_rows, column->size() - column_size_before_reading);
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

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
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

    return read_rows;
}

void MergeTreeReaderWide::addStreams(const NameAndTypePair & name_and_type,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
{
    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path) {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.count(stream_name))
            return;

        bool data_file_exists = data_part->checksums.files.count(stream_name + DATA_FILE_EXTENSION);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        streams.emplace(
            stream_name,
            std::make_unique<MergeTreeReaderStream>(
                data_part->volume->getDisk(),
                data_part->getFullRelativePath() + stream_name,
                stream_name,
                DATA_FILE_EXTENSION,
                data_part->getMarksCount(),
                all_mark_ranges,
                settings,
                mark_cache,
                uncompressed_cache,
                &data_part->index_granularity_info,
                profile_callback,
                clock_type,
                data_part->getFileOffsetOrZero(stream_name + DATA_FILE_EXTENSION),
                data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
                data_part->getFileOffsetOrZero(data_part->index_granularity_info.getMarksFilePath(stream_name)),
                data_part->getFileSizeOrZero(data_part->index_granularity_info.getMarksFilePath(stream_name))));
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}

}
