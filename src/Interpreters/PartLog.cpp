#include <unordered_map>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>

#include <common/types.h>
#include <Common/CurrentThread.h>
#include <Common/time.h>
#include <Columns/ColumnMap.h>

namespace DB
{

void dumpToMapColumn(const std::unordered_map<String, UInt64> & map, DB::IColumn * column)
{
    auto * column_map = column ? &typeid_cast<DB::ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getOffsets();
    auto & key_column = column_map->getKey();
    auto & value_column = column_map->getValue();

    size_t size = 0;
    for (auto & entry : map)
    {
        UInt64 value = entry.second;

        key_column.insertData(entry.first.c_str(), strlen(entry.first.c_str()));
        value_column.insert(value);
        size++;
    }

    offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size);
}

NamesAndTypesList PartLogElement::getNamesAndTypes()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"NewPart",       static_cast<Int8>(NEW_PART)},
            {"MergeParts",    static_cast<Int8>(MERGE_PARTS)},
            {"DownloadPart",  static_cast<Int8>(DOWNLOAD_PART)},
            {"RemovePart",    static_cast<Int8>(REMOVE_PART)},
            {"MutatePart",    static_cast<Int8>(MUTATE_PART)},
            {"MovePart",      static_cast<Int8>(MOVE_PART)},
            {"PreloadPart",   static_cast<Int8>(PRELOAD_PART)},
            {"DROPCACHE_PART", static_cast<Int8>(DROPCACHE_PART)},
        }
    );

    ColumnsWithTypeAndName columns_with_type_and_name;

    return {
        {"query_id", std::make_shared<DataTypeString>()},
        {"event_type", std::move(event_type_datatype)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"start_time", std::make_shared<DataTypeDateTime>()},

        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"duration_ms", std::make_shared<DataTypeUInt64>()},

        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"part_name", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"partition", std::make_shared<DataTypeString>()},
        {"path_on_disk", std::make_shared<DataTypeString>()},

        {"rows", std::make_shared<DataTypeUInt64>()},
        {"segments", std::make_shared<DataTypeUInt64>()},
        {"segments_map", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>())},
        {"preload_level", std::make_shared<DataTypeUInt64>()},
        {"size_in_bytes", std::make_shared<DataTypeUInt64>()}, // On disk

        /// Merge-specific info
        {"merged_from", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"bytes_uncompressed", std::make_shared<DataTypeUInt64>()}, // Result bytes
        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"peak_memory_usage", std::make_shared<DataTypeUInt64>()},

        /// Is there an error during the execution or commit
        {"error", std::make_shared<DataTypeUInt16>()},
        {"exception", std::make_shared<DataTypeString>()},
    };
}

void PartLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(query_id);
    columns[i++]->insert(event_type);
    columns[i++]->insert(DateLUT::serverTimezoneInstance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(start_time);
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(duration_ms);

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(part_name);
    columns[i++]->insert(partition_id);
    columns[i++]->insert(partition);
    columns[i++]->insert(path_on_disk);

    columns[i++]->insert(rows);
    columns[i++]->insert(segments_count);

    auto * column = columns[i++].get();
    if (segments.size() > 0)
        dumpToMapColumn(segments, column);
    else
        column->insertDefault();

    columns[i++]->insert(preload_level);
    columns[i++]->insert(bytes_compressed_on_disk);

    Array source_part_names_array;
    source_part_names_array.reserve(source_part_names.size());
    for (const auto & name : source_part_names)
        source_part_names_array.push_back(name);

    columns[i++]->insert(source_part_names_array);

    columns[i++]->insert(bytes_uncompressed);
    columns[i++]->insert(rows_read);
    columns[i++]->insert(bytes_read_uncompressed);
    columns[i++]->insert(peak_memory_usage);

    columns[i++]->insert(error);
    columns[i++]->insert(exception);
}


bool PartLog::addNewPart(
    ContextPtr current_context, const MutableDataPartPtr & part, UInt64 elapsed_ns, const ExecutionStatus & execution_status)
{
    return addNewParts(current_context, {part}, elapsed_ns, execution_status);
}

bool PartLog::addNewParts(
    ContextPtr current_context, const PartLog::MutableDataPartsVector & parts, UInt64 elapsed_ns, const ExecutionStatus & execution_status)
{
    if (parts.empty())
        return true;

    std::shared_ptr<PartLog> part_log;

    try
    {
        auto table_id = parts.front()->storage.getStorageID();
        part_log = current_context->getPartLog(table_id.database_name); // assume parts belong to the same table
        if (!part_log)
            return false;

        auto query_id = CurrentThread::getQueryId();

        for (const auto & part : parts)
        {
            PartLogElement elem;

            if (query_id.data && query_id.size)
                elem.query_id.insert(0, query_id.data, query_id.size);

            elem.event_type = PartLogElement::NEW_PART; //-V1048

            // construct event_time and event_time_microseconds using the same time point
            // so that the two times will always be equal up to a precision of a second.
            const auto time_now = std::chrono::system_clock::now();
            elem.event_time = time_in_seconds(time_now);
            elem.event_time_microseconds = time_in_microseconds(time_now);
            elem.duration_ms = elapsed_ns / 1000000;

            elem.database_name = table_id.database_name;
            elem.table_name = table_id.table_name;
            elem.partition_id = part->info.partition_id;
            {
                WriteBufferFromString out(elem.partition);
                part->partition.serializeText(part->storage, out, {});
            }
            elem.part_name = part->name;
            elem.path_on_disk = part->getFullPath();

            elem.bytes_compressed_on_disk = part->getBytesOnDisk();
            elem.rows = part->rows_count;

            elem.error = static_cast<UInt16>(execution_status.code);
            elem.exception = execution_status.message;

            part_log->add(elem);
        }
    }
    catch (...)
    {
        tryLogCurrentException(part_log ? part_log->log : getRawLogger("PartLog"), __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

PartLogElement PartLog::createElement(PartLogElement::Type event_type, const IMergeTreeDataPartPtr & part, UInt64 elapsed_ns, const String & exception, UInt64 submit_ts, UInt64 segments_count, std::unordered_map<String, UInt64> segments, UInt64 preload_level)
{
    PartLogElement elem;

    elem.event_type = event_type;
    elem.start_time = submit_ts;
    elem.event_time = time(nullptr);
    elem.duration_ms = elapsed_ns / 1000000;

    elem.database_name = part->storage.getDatabaseName();
    elem.table_name = part->storage.getTableName();
    elem.partition_id = part->info.partition_id;
    elem.part_name = part->name;

    elem.rows = part->rows_count;
    elem.segments_count = segments_count;
    elem.segments = segments;
    elem.preload_level = preload_level;
    elem.bytes_compressed_on_disk = part->bytes_on_disk;

    elem.exception = exception;

    return elem;
}

}
