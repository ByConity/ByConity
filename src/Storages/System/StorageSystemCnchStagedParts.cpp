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

#include <Storages/System/StorageSystemCnchStagedParts.h>

#include <Catalog/Catalog.h>
#include <CloudServices/CnchPartsHelper.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/CollectWhereClausePredicate.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int UNSUPPORTED_PARAMETER;
}
NamesAndTypesList StorageSystemCnchStagedParts::getNamesAndTypes()
{
    return {
        {"partition", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"to_publish", std::make_shared<DataTypeUInt8>()},
        {"to_gc", std::make_shared<DataTypeUInt8>()},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"rows_count", std::make_shared<DataTypeUInt64>()},
        {"columns", std::make_shared<DataTypeString>()},
        {"marks_count", std::make_shared<DataTypeUInt64>()},
        {"ttl", std::make_shared<DataTypeString>()},
        {"commit_time", std::make_shared<DataTypeDateTime>()},
        {"columns_commit_time", std::make_shared<DataTypeDateTime>()},
        {"previous_version", std::make_shared<DataTypeUInt64>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"bucket_number", std::make_shared<DataTypeInt64>()},
        {"hdfs_path", std::make_shared<DataTypeString>()},
        {"part_id", std::make_shared<DataTypeUUID>()},
    };
}

NamesAndAliases StorageSystemCnchStagedParts::getNamesAndAliases()
{
    return
    {
        {"bytes", {std::make_shared<DataTypeUInt64>()}, "bytes_on_disk"},
        {"rows", {std::make_shared<DataTypeUInt64>()}, "rows_count"}
    };
}

void StorageSystemCnchStagedParts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    auto cnch_catalog = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_staged_parts only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    // check for required structure of WHERE clause for cnch_staged_parts
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    const std::vector<std::map<String,Field>> value_by_column_names = collectWhereORClausePredicate(where_expression, context);

    String only_selected_db;
    String only_selected_table;
    bool enable_filter_by_table = false;

    if (value_by_column_names.size() == 1)
    {
        const auto value_by_column_name = value_by_column_names.at(0);
        auto db_it = value_by_column_name.find("database");
        auto table_it = value_by_column_name.find("table");
        if ((db_it != value_by_column_name.end()) &&
            (table_it != value_by_column_name.end()))
        {
            only_selected_db = db_it->second.getType() == Field::Types::String ? db_it->second.get<String>() : "";
            only_selected_table = table_it->second.getType() == Field::Types::String ? table_it->second.get<String>() : "";
            enable_filter_by_table = true;
            LOG_TRACE(getLogger("StorageSystemCnchStagedParts"),
                    "filtering from catalog by table with db name {} and table name {}",
                    only_selected_db, only_selected_table);
        }
    }

    if (!enable_filter_by_table)
        throw Exception(
            "Please check if where condition follow this form "
            "system.cnch_staged_parts WHERE database = 'some_name' AND table = 'another_name'",
            ErrorCodes::INCORRECT_QUERY);
    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    DB::StoragePtr table = cnch_catalog->tryGetTable(*context, only_selected_db, only_selected_table, start_time);
    if (!table)
        return;

    auto * data = dynamic_cast<StorageCnchMergeTree *>(table.get());

    if (!data || !data->getInMemoryMetadataPtr()->hasUniqueKey())
        throw Exception("Table system.cnch_staged_parts only support CnchMergeTree unique engine", ErrorCodes::UNSUPPORTED_PARAMETER);

    auto all_parts = CnchPartsHelper::toIMergeTreeDataPartsVector(cnch_catalog->getStagedParts(table, start_time));
    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    auto current_visible = visible_parts.cbegin();

    //get CNCH staged parts
    for (const auto & part : all_parts)
    {
        if (part->deleted)
            continue;
        size_t col_num = 0;
        {
            WriteBufferFromOwnString out;
            part->partition.serializeText(*data, out, format_settings);
            res_columns[col_num++]->insert(out.str());
        }
        res_columns[col_num++]->insert(part->name);
        res_columns[col_num++]->insert(only_selected_db);
        res_columns[col_num++]->insert(only_selected_table);

        /// all_parts and visible_parts are both ordered by info.
        /// For each part within all_parts, we find the first part greater than or equal to it in visible_parts,
        /// which means the part is visible if they are equal to each other
        while (current_visible != visible_parts.cend() && (*current_visible)->info < part->info)
            ++current_visible;
        bool to_publish = current_visible != visible_parts.cend() && part->info == (*current_visible)->info;
        res_columns[col_num++]->insert(to_publish);
        res_columns[col_num++]->insert(!to_publish);

        res_columns[col_num++]->insert(part->getBytesOnDisk());
        res_columns[col_num++]->insert(part->rows_count);
        res_columns[col_num++]->insert(part->getColumnsPtr()->toString());
        res_columns[col_num++]->insert(part->getMarksCount());

        String ttl = std::to_string(part->ttl_infos.table_ttl.min) + "," + std::to_string(part->ttl_infos.table_ttl.max);
        res_columns[col_num++]->insert(ttl);
        res_columns[col_num++]->insert(part->commit_time.toSecond());
        res_columns[col_num++]->insert(part->columns_commit_time.toSecond());
        res_columns[col_num++]->insert(part->info.hint_mutation);
        res_columns[col_num++]->insert(part->info.partition_id);
        res_columns[col_num++]->insert(part->bucket_number);
        res_columns[col_num++]->insert(part->getFullPath());
        res_columns[col_num++]->insert(part->get_uuid());
    }
}
}
