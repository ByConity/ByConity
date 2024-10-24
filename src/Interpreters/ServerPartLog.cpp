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

#include <Interpreters/ServerPartLog.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>

#include <Common/CurrentThread.h>


namespace DB
{
NamesAndTypesList ServerPartLogElement::getNamesAndTypes()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"InsertPart", static_cast<Int8>(INSERT_PART)},
        {"MergeParts", static_cast<Int8>(MERGE_PARTS)},
        {"MutatePart", static_cast<Int8>(MUTATE_PART)},
        {"DropRange", static_cast<Int8>(DROP_RANGE)},
        {"RemovePart", static_cast<Int8>(REMOVE_PART)},
        {"DeleteParts", static_cast<Int8>(DELETE_PARTS)},
    });

    return {
        {"event_type", std::move(event_type_datatype)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"txn_id", std::make_shared<DataTypeUInt64>()},

        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},

        {"part_name", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"part_id", std::make_shared<DataTypeString>()},
        {"is_staged", std::make_shared<DataTypeUInt8>()},
        {"rows", std::make_shared<DataTypeUInt64>()},
        {"bytes", std::make_shared<DataTypeUInt64>()},
        {"commit_ts", std::make_shared<DataTypeUInt64>()},
        {"end_ts", std::make_shared<DataTypeUInt64>()},
        {"source_part_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},

        {"duration_ms", std::make_shared<DataTypeUInt64>()},
        {"peak_memory_usage", std::make_shared<DataTypeUInt64>()},

        {"from_attach", std::make_shared<DataTypeUInt8>()},

        {"error", std::make_shared<DataTypeUInt8>()},
        {"exception", std::make_shared<DataTypeString>()},
    };
}

NamesAndAliases ServerPartLogElement::getNamesAndAliases()
{
    return {
        {"num_source_parts", std::make_shared<DataTypeUInt64>(), "length(source_part_names)"}
    };
}


void ServerPartLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(static_cast<UInt64>(event_type));
    columns[i++]->insert(DateLUT::serverTimezoneInstance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(txn_id);

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(uuid);

    columns[i++]->insert(part_name);
    columns[i++]->insert(partition_id);
    columns[i++]->insert(part_id);
    columns[i++]->insert(is_staged_part);
    columns[i++]->insert(rows);
    columns[i++]->insert(bytes);
    columns[i++]->insert(commit_ts);
    columns[i++]->insert(end_ts);

    Array source_part_names_array;
    source_part_names_array.reserve(source_part_names.size());
    for (const auto & name : source_part_names)
        source_part_names_array.push_back(name);
    columns[i++]->insert(source_part_names_array);

    columns[i++]->insert(duration_ms);
    columns[i++]->insert(peak_memory_usage);

    columns[i++]->insert(from_attach);
    columns[i++]->insert(error);
    columns[i++]->insert(exception);
}

static String UUIDToStringIfNotNil(const UUID & uuid)
{
    return uuid != UUIDHelpers::Nil ? UUIDHelpers::UUIDToString(uuid) : "";
}

bool ServerPartLog::addNewParts(
    const ContextPtr & local_context,
    StorageID storage_id,
    ServerPartLogElement::Type type,
    const MutableMergeTreeDataPartsCNCHVector & parts,
    const MutableMergeTreeDataPartsCNCHVector & staged_parts,
    UInt64 txn_id,
    UInt8 error,
    const Strings & source_part_names,
    UInt64 duration_ns,
    UInt64 peak_memory_usage,
    bool from_attach)
{
    std::shared_ptr<ServerPartLog> server_part_log = local_context->getServerPartLog();
    if (!server_part_log)
        return false;

    auto event_time = time(nullptr);
    std::unordered_map<String, std::pair<UInt64, UInt64> > parts_and_bytes_by_partition;

    auto add = [&](const MutableMergeTreeDataPartCNCHPtr & part, bool is_staged)
    {
        ServerPartLogElement elem;
        elem.event_type = type;
        elem.event_time = event_time;
        elem.txn_id = txn_id;

        elem.database_name = storage_id.getDatabaseName();
        elem.table_name = storage_id.getTableName();
        elem.uuid = storage_id.uuid;

        elem.part_name = part->name;
        elem.partition_id = part->info.partition_id;
        elem.part_id = UUIDToStringIfNotNil(part->get_uuid());
        elem.is_staged_part = is_staged;
        elem.rows = part->rows_count;
        elem.bytes = part->bytes_on_disk;
        elem.commit_ts = part->get_commit_time();
        elem.end_ts = 0;
        elem.source_part_names = source_part_names;

        elem.duration_ms = duration_ns / 1000000;
        elem.peak_memory_usage = peak_memory_usage;


        elem.from_attach = from_attach;
        elem.error = error;

        server_part_log->add(elem);

        if (!is_staged)
        {
            parts_and_bytes_by_partition[elem.partition_id].first++;
            parts_and_bytes_by_partition[elem.partition_id].second += part->bytes_on_disk;
        }
    };

    try
    {
        for (const auto & part : parts)
            add(part, false);
        for (const auto & part : staged_parts)
            add(part, true);

        if (auto bg_task_stats = MergeTreeBgTaskStatisticsInitializer::instance().getOrCreateTableStats(storage_id))
        {
            if (type == ServerPartLogElement::INSERT_PART && !parts.empty())
            {
                for (const auto & [partition_id, parts_bytes] : parts_and_bytes_by_partition)
                    bg_task_stats->addInsertedParts(partition_id, parts_bytes.first, parts_bytes.second, event_time);
            }

            if (type == ServerPartLogElement::MERGE_PARTS && !parts.empty())
            {
                /// Assume only have one merged part
                bg_task_stats->addMergedParts(parts.front()->info.partition_id, source_part_names.size(), parts.front()->bytes_on_disk, event_time);
            }

            if (type == ServerPartLogElement::DROP_RANGE && !parts.empty())
            {
                for (const auto & [partition_id, _] : parts_and_bytes_by_partition)
                    bg_task_stats->dropPartition(partition_id);
            }
        }

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(server_part_log->log, __PRETTY_FUNCTION__);
        return false;
    }
}

bool ServerPartLog::addRemoveParts(const ContextPtr & local_context, StorageID storage_id, const ServerDataPartsVector & parts, bool is_staged)
{
    std::shared_ptr<ServerPartLog> server_part_log = local_context->getServerPartLog();
    if (parts.empty() || !server_part_log)
        return false;

    try
    {
        auto now = time(nullptr);

        std::unordered_map<String, size_t> count_by_partition;

        for (const auto & part: parts)
        {
            ServerPartLogElement elem;
            elem.event_type = ServerPartLogElement::REMOVE_PART;
            elem.event_time = now;
            elem.database_name = storage_id.getDatabaseName();
            elem.table_name = storage_id.getTableName();
            elem.uuid = storage_id.uuid;
            elem.part_name = part->name();
            elem.partition_id = part->info().partition_id;
            elem.part_id = UUIDToStringIfNotNil(part->get_uuid());
            elem.is_staged_part = is_staged;
            elem.rows = part->rowsCount();
            elem.bytes = part->size();
            elem.commit_ts = part->getCommitTime();
            elem.end_ts = part->getEndTime();

            server_part_log->add(elem);
            count_by_partition[elem.partition_id] += static_cast<int>(elem.rows > 0);
        }

        if (auto bg_task_stats = MergeTreeBgTaskStatisticsInitializer::instance().getOrCreateTableStats(storage_id))
        {
            for (const auto & [partition_id, count] : count_by_partition)
                bg_task_stats->addRemovedParts(partition_id, count);
        }

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(server_part_log->log, __PRETTY_FUNCTION__);
        return false;
    }

}

bool ServerPartLog::addDeleteParts(const ContextPtr & local_context, StorageID storage_id, const ServerDataPartsVector & parts)
{
    std::shared_ptr<ServerPartLog> server_part_log = local_context->getServerPartLog();
    if (parts.empty() || !server_part_log)
        return false;

    try
    {
        auto now = time(nullptr);
        ServerPartLogElement elem;
        size_t commit_ts = 0;
        size_t end_ts = 0;
        size_t rows = 0;
        size_t bytes = 0;
        for (const auto & part : parts)
        {
            rows += part->rowsCount();
            bytes += part->size();
            commit_ts = std::max(commit_ts, part->getCommitTime());
            end_ts = std::max(end_ts, part->getEndTime());
        }

        elem.event_type = ServerPartLogElement::DELETE_PARTS;
        elem.event_time = now;
        elem.database_name = storage_id.getDatabaseName();
        elem.table_name = storage_id.getTableName();
        elem.uuid = storage_id.uuid;
        elem.is_staged_part = false;
        elem.rows = rows;
        elem.bytes = bytes;
        elem.commit_ts = commit_ts;
        elem.end_ts = end_ts;

        server_part_log->add(elem);
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(server_part_log->log, __PRETTY_FUNCTION__);
        return false;
    }
}

void ServerPartLog::prepareTable()
{
    SystemLog::prepareTable();
}

}
