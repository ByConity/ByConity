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

#include <Access/ContextAccess.h>
#include <map>
#include <Catalog/Catalog.h>
#include <CloudServices/CnchPartsHelper.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemCnchCommon.h>
#include <Storages/System/StorageSystemCnchParts.h>
#include <Storages/VirtualColumnUtils.h>
#include <TSO/TSOClient.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}

NamesAndTypesList StorageSystemCnchParts::getNamesAndTypes()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"VisiblePart", static_cast<Int8>(PartType::VisiblePart)},
        {"InvisiblePart", static_cast<Int8>(PartType::InvisiblePart)},
        {"Tombstone", static_cast<Int8>(PartType::Tombstone)},
        {"DroppedPart", static_cast<Int8>(PartType::DroppedPart)},
    });

    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"partition", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"rows_count", std::make_shared<DataTypeUInt64>()},
        {"delete_rows", std::make_shared<DataTypeUInt64>()},
        {"columns", std::make_shared<DataTypeString>()},
        {"marks_count", std::make_shared<DataTypeUInt64>()},
        {"index_granularity", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"commit_time", std::make_shared<DataTypeDateTime>()},
        {"kv_commit_time", std::make_shared<DataTypeDateTime>()},
        {"columns_commit_time", std::make_shared<DataTypeDateTime>()},
        {"mutation_commit_time", std::make_shared<DataTypeDateTime>()},
        {"previous_version", std::make_shared<DataTypeUInt64>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"bucket_number", std::make_shared<DataTypeInt64>()},
        {"table_definition_hash", std::make_shared<DataTypeUInt64>()},
        {"outdated", std::make_shared<DataTypeUInt8>()},    /// parts that should be deleted by GCThread
        {"visible", std::make_shared<DataTypeUInt8>()},
        {"part_type", std::move(type_enum)},
        {"part_id", std::make_shared<DataTypeUUID>()},
        /// useful for getting raw value for debug
        {"commit_ts", std::make_shared<DataTypeUInt64>()},
        {"end_ts", std::make_shared<DataTypeUInt64>()},
        {"last_modification_time", std::make_shared<DataTypeDateTime>()},
    };
}

NamesAndAliases StorageSystemCnchParts::getNamesAndAliases()
{
    return
    {
        {"active", {std::make_shared<DataTypeUInt8>()}, "visible"},
        {"bytes", {std::make_shared<DataTypeUInt64>()}, "bytes_on_disk"},
        {"rows", {std::make_shared<DataTypeUInt64>()}, "rows_count"}
    };
}

void StorageSystemCnchParts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    auto cnch_catalog = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_parts only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    std::vector<std::tuple<String, String, String>> tables;

    ASTPtr where_expression = query_info.query->as<ASTSelectQuery &>().where();

    const std::vector<std::map<String,Field>> value_by_column_names = collectWhereORClausePredicate(where_expression, context);
    bool enable_filter_by_table = false;
    bool enable_filter_by_partition = false;
    String only_selected_db;
    String only_selected_table;
    String only_selected_partition_id;

    if (value_by_column_names.size() == 1)
    {
        const auto & value_by_column_name = value_by_column_names.at(0);
        auto db_it = value_by_column_name.find("database");
        auto table_it = value_by_column_name.find("table");
        auto partition_it = value_by_column_name.find("partition_id");
        if ((db_it != value_by_column_name.end()) && (table_it != value_by_column_name.end()))
        {
            only_selected_db = db_it->second.getType() == Field::Types::String ? db_it->second.get<String>() : "";
            only_selected_table = table_it->second.getType() == Field::Types::String ? table_it->second.get<String>() : "";
            enable_filter_by_table = true;

            LOG_TRACE(getLogger("StorageSystemCnchParts"),
                    "filtering from catalog by table with db name {} and table name {}",
                    only_selected_db, only_selected_table);
        }

        if (partition_it != value_by_column_name.end())
        {
            only_selected_partition_id = partition_it->second.getType() == Field::Types::String ? partition_it->second.get<String>() : "";
            enable_filter_by_partition = true;

            LOG_TRACE(getLogger("StorageSystemCnchParts"),
                    "filtering from catalog by partition with partition name {}",
                    only_selected_partition_id);
        }
    }

    if (!(enable_filter_by_partition || enable_filter_by_table))
        LOG_TRACE(getLogger("StorageSystemCnchParts"), "No explicitly table and partition provided in where expression");

    // check for required structure of WHERE clause for cnch_parts
    if (!enable_filter_by_table)
    {
        if (!context->getSettingsRef().enable_multiple_tables_for_cnch_parts)
            throw Exception(
                "You should specify database and table in where cluster or set enable_multiple_tables_for_cnch_parts to enable visit "
                "multiple "
                "tables",
                ErrorCodes::BAD_ARGUMENTS);
        tables = filterTables(context, query_info);
    }
    else
    {
        const String & tenant_id = context->getTenantId();
        String only_selected_db_full = only_selected_db;
        if (!tenant_id.empty())
        {
            if (!DB::DatabaseCatalog::isDefaultVisibleSystemDatabase(only_selected_db))
            {
                only_selected_db_full = tenant_id + "." + only_selected_db;
            }
        }
        tables.emplace_back(
                only_selected_db_full,
                only_selected_db,
                only_selected_table
            );
    }

    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & [database_fullname, database_name, table_name] : tables)
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_fullname);
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_fullname, table_name))
            continue;

        auto table = cnch_catalog->tryGetTable(*context, database_fullname, table_name, start_time);

        /// Skip not exist table
        if (!table)
            continue;

        auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(table.get());
        if (!cnch_merge_tree)
        {
            if (context->getSettingsRef().enable_skip_non_cnch_tables_for_cnch_parts)
                continue;
            else if (enable_filter_by_table)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Table system.cnch_parts only support CnchMergeTree engine, but got `{}`. "
                    "Consider enable `enable_skip_non_cnch_tables_for_cnch_parts` to skip non CnchMergeTree engine.",
                    table ? table->getName() : "unknown engine");
            else
                continue;
        }

        /// use committed visibility to include dropped parts (and exclude intermediates) in system table
        auto [all_parts, all_bitmaps] = enable_filter_by_partition
            ? cnch_catalog->getServerDataPartsInPartitionsWithDBM(table, {only_selected_partition_id}, start_time, nullptr, Catalog::VisibilityLevel::Committed)
            : cnch_catalog->getAllServerDataPartsWithDBM(table, start_time, nullptr, Catalog::VisibilityLevel::Committed);

        ServerDataPartsVector visible_parts;
        CnchPartsHelper::calcPartsForGC(all_parts, nullptr, &visible_parts);

        if (visible_parts.empty())
            continue;

        if (cnch_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
        {
            cnch_merge_tree->getDeleteBitmapMetaForServerParts(visible_parts, all_bitmaps, /*force_found*/false);
        }

        const FormatSettings format_settings;

        for (auto & part : visible_parts)
        {
            bool latest_in_mvcc = true;
            for (auto curr_part = part; curr_part; curr_part = curr_part->tryGetPreviousPart())
            {
                size_t col_num = 0;
                res_columns[col_num++]->insert(database_name);
                res_columns[col_num++]->insert(table_name);
                res_columns[col_num++]->insert(cnch_merge_tree->getStorageUUID());

                {
                    WriteBufferFromOwnString out;
                    curr_part->partition().serializeText(*cnch_merge_tree, out, format_settings);
                    res_columns[col_num++]->insert(out.str());
                }
                res_columns[col_num++]->insert(curr_part->name());
                res_columns[col_num++]->insert(curr_part->part_model().size());
                auto delete_rows = curr_part->deletedRowsCount(*cnch_merge_tree, /*ignore_error*/true);
                res_columns[col_num++]->insert(curr_part->part_model().rows_count() - delete_rows);
                res_columns[col_num++]->insert(delete_rows);
                res_columns[col_num++]->insert(curr_part->part_model().columns());
                res_columns[col_num++]->insert(curr_part->part_model().marks_count());
                Array index_granularity;
                index_granularity.reserve(curr_part->part_model().index_granularities_size());
                for (const auto & granularity : curr_part->part_model().index_granularities())
                    index_granularity.push_back(granularity);
                res_columns[col_num++]->insert(index_granularity);

                res_columns[col_num++]->insert(TxnTimestamp(curr_part->getCommitTime()).toSecond());
                res_columns[col_num++]->insert(TxnTimestamp(curr_part->part_model().commit_time()).toSecond());
                res_columns[col_num++]->insert(TxnTimestamp(curr_part->part_model().columns_commit_time()).toSecond());
                res_columns[col_num++]->insert(TxnTimestamp(curr_part->part_model().mutation_commit_time()).toSecond());

                res_columns[col_num++]->insert(curr_part->info().hint_mutation);
                res_columns[col_num++]->insert(curr_part->info().partition_id);
                res_columns[col_num++]->insert(curr_part->part_model().bucket_number());
                res_columns[col_num++]->insert(curr_part->part_model().table_definition_hash());

                bool outdated = curr_part->getEndTime() > 0;
                bool visible = false;
                PartType type = PartType::VisiblePart;
                if (curr_part->get_deleted())
                    type = PartType::Tombstone;
                else if (outdated)
                    type = PartType::DroppedPart;
                else if (!latest_in_mvcc)
                    type = PartType::InvisiblePart;
                else
                    visible = true;

                res_columns[col_num++]->insert(outdated);
                res_columns[col_num++]->insert(visible);
                res_columns[col_num++]->insert(static_cast<Int8>(type));

                res_columns[col_num++]->insert(curr_part->get_uuid());
                res_columns[col_num++]->insert(curr_part->getCommitTime());
                res_columns[col_num++]->insert(curr_part->getEndTime());
                res_columns[col_num++]->insert(TxnTimestamp(curr_part->getLastModificationTime()).toSecond());

                if (type == PartType::VisiblePart)
                    type = PartType::InvisiblePart;
                latest_in_mvcc = false;
            }
        }
    }
}
}
