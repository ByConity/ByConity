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

#include <Catalog/Catalog.h>
#include <Parsers/queryToString.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemCnchTableHost.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <common/logger_useful.h>
#include <Protos/DataModelHelpers.h>

namespace DB
{
NamesAndTypesList StorageSystemCnchTableHost::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeString>()},
        {"server_vw_name", std::make_shared<DataTypeString>()},
        {"host", std::make_shared<DataTypeString>()},
        {"tcp_port", std::make_shared<DataTypeUInt16>()},
        {"http_port", std::make_shared<DataTypeUInt16>()},
        {"rpc_port", std::make_shared<DataTypeUInt16>()},
    };
}

void StorageSystemCnchTableHost::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    std::map<String, String> columnToValue;

    const std::vector<std::map<String,Field>> value_by_column_names = DB::collectWhereORClausePredicate(where_expression, context);
    bool enable_filter_by_db = false;
    bool enable_filter_by_database_and_table = false;
    String only_selected_database;
    String only_selected_table;

    if (value_by_column_names.size() == 1)
    {
        const auto value_by_column_name = value_by_column_names.at(0);
        auto db_it = value_by_column_name.find("database");
        auto table_it = value_by_column_name.find("name");
        if ((db_it != value_by_column_name.end()) &&
            (table_it != value_by_column_name.end())
            )
        {
            only_selected_database = db_it->second.getType() == Field::Types::String ? db_it->second.get<String>() : "";
            only_selected_table = table_it->second.getType() == Field::Types::String ? table_it->second.get<String>() : "";
            enable_filter_by_database_and_table = true;
            LOG_TRACE(getLogger("StorageSystemCnchTableHost"),
                    "filtering by db and table with db name {} and table name {}",
                    only_selected_database, only_selected_table);
        }
        else if (db_it != value_by_column_name.end())
        {
            only_selected_database = db_it->second.getType() == Field::Types::String ? db_it->second.get<String>() : "";
            enable_filter_by_db = true;
            LOG_TRACE(getLogger("StorageSystemCnchTableHost"),
                    "filtering by db with db name {}", only_selected_database);
        }
        else
            LOG_TRACE(getLogger("StorageSystemCnchTableHost"), "doesn't do any filtering");
    }

    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();

    std::vector<std::shared_ptr<Protos::TableIdentifier>> table_ids;

    if (enable_filter_by_database_and_table)
        table_ids.push_back(
            cnch_catalog->getTableIDByName(
                only_selected_database,
                only_selected_table
            )
        );
    else if (enable_filter_by_db)
        table_ids = cnch_catalog->getAllTablesID(only_selected_database);
    else
        table_ids = cnch_catalog->getAllTablesID();

    UInt64 ts = 0;
    static constexpr size_t TSO_BATCH_SIZE = 100;
    for (size_t i = 0; i < table_ids.size(); ++i)
    {
        /// Get latest timestamp from TSO every TSO_BATCH_SIZE to avoid ts is too old when there are too many table_ids
        if ((i % TSO_BATCH_SIZE) == 0)
            ts = context->tryGetTimestamp(__PRETTY_FUNCTION__);
        const auto & table_id = table_ids[i];
        if (table_id)
        {
            size_t col_num = 0;
            auto server_vw_name = getServerVwNameFrom(*table_id);
            res_columns[col_num++]->insert(table_id->database());
            res_columns[col_num++]->insert(table_id->name());
            res_columns[col_num++]->insert(table_id->uuid());
            res_columns[col_num++]->insert(server_vw_name);
            auto target_server = context->getCnchTopologyMaster()->getTargetServer(table_id->uuid(), server_vw_name, ts, true, true);
            res_columns[col_num++]->insert(target_server.getHost());
            res_columns[col_num++]->insert(target_server.getTCPPort());
            res_columns[col_num++]->insert(target_server.getHTTPPort());
            res_columns[col_num++]->insert(target_server.getRPCPort());
        }
    }
}
}
