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
#include <Storages/System/StorageSystemCnchColumns.h>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Common/Status.h>
#include <Parsers/queryToString.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/Logger.h>
#include <Storages/System/CollectWhereClausePredicate.h>

namespace DB
{

#define GET_ALL_TABLES_LIMIT (10)

NamesAndTypesList StorageSystemCnchColumns::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"type", std::make_shared<DataTypeString>()},
        {"flags", std::make_shared<DataTypeString>()},
        {"default_kind", std::make_shared<DataTypeString>()},
        {"default_expression", std::make_shared<DataTypeString>()},
        {"data_compressed_bytes", std::make_shared<DataTypeUInt64>()},
        {"data_uncompressed_bytes", std::make_shared<DataTypeUInt64>()},
        {"marks_bytes", std::make_shared<DataTypeUInt64>()},
        {"comment", std::make_shared<DataTypeString>()},
        {"is_in_partition_key", std::make_shared<DataTypeUInt8>()},
        {"is_in_sorting_key", std::make_shared<DataTypeUInt8>()},
        {"is_in_primary_key", std::make_shared<DataTypeUInt8>()},
        {"is_in_sampling_key", std::make_shared<DataTypeUInt8>()},
        {"compression_codec", std::make_shared<DataTypeString>()}};
}

static std::optional<std::vector<std::map<String, String>>> parsePredicatesFromWhere(const SelectQueryInfo & query_info, const ContextPtr & context)
{
    ASTPtr where_expression = query_info.query->as<const ASTSelectQuery &>().where();
    std::vector<std::map<String,Field>> predicates;
    predicates = collectWhereORClausePredicate(where_expression, context, true);

    std::optional<std::vector<std::map<String, String>>> res;
    std::vector<std::map<String, String>> tmp_res;
    for (auto & item: predicates)
    {
        std::map<String, String> tmp_map;
        if (item.count("database") && item["database"].getType() == Field::Types::String)
        {
            tmp_map["database"] = item["database"].get<String>();
        }

        if (item.count("table") && item["table"].getType() == Field::Types::String)
        {
            tmp_map["table"] = item["table"].get<String>();
        }

        tmp_res.push_back(tmp_map);
    }

    if (!tmp_res.empty())
        res = tmp_res;

    return res;
}

static bool getDBTablesFromPredicates(const std::optional<std::vector<std::map<String, String>>> & predicates,
        std::vector<std::pair<String, String>> & db_table_pairs)
{
    if (!predicates)
        return false;

    for (const auto & item : predicates.value())
    {
        if (!item.count("database") || !item.count("table"))
            return false;

        db_table_pairs.push_back(std::make_pair(item.at("database"), item.at("table")));
    }

    return true;
}

static bool matchAnyPredicate(const std::optional<std::vector<std::map<String, String>>> & predicates,
        const Protos::DataModelTable & table_model)
{
    if (!predicates)
        return true;

    for (const auto & item : predicates.value())
    {
        if ((!item.count("database") || (item.at("database") == table_model.database()))
             && (!item.count("table") ||(item.at("table") == table_model.name())))
        {
            return true;
        }
    }

    return false;
}

void StorageSystemCnchColumns::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();

    if (context->getServerType() == ServerType::cnch_server && cnch_catalog)
    {
        Stopwatch stop_watch;
        stop_watch.start();

        const String & tenant_id = context->getTenantId();
        Catalog::Catalog::DataModelTables table_models;

        std::vector<std::pair<String, String>> db_table_pairs;
        auto predicates = parsePredicatesFromWhere(query_info, context);
        bool get_db_tables_ok = getDBTablesFromPredicates(predicates, db_table_pairs);
        if (get_db_tables_ok && (db_table_pairs.size() <= GET_ALL_TABLES_LIMIT))
        {
            if (!tenant_id.empty())
            {
                for (auto & pair : db_table_pairs)
                    pair.first = formatTenantDatabaseNameWithTenantId(pair.first, tenant_id);
            }

            auto table_ids = cnch_catalog->getTableIDsByNames(db_table_pairs);
            if (table_ids)
                table_models = cnch_catalog->getTablesByIDs(*table_ids);
        }
        else
        {
            table_models = cnch_catalog->getAllTables();
        }

        std::vector<String> full_db_name;
        for (auto it = table_models.begin(); it != table_models.end();)
        {
            String non_stripped_db_name = it->database();
            if (!tenant_id.empty())
            {
                if (startsWith(non_stripped_db_name, tenant_id + "."))
                {
                    it->set_database(getOriginalDatabaseName(non_stripped_db_name, tenant_id));
                }
                else
                {
                    // Will skip database of other tenants and default user (without tenantid prefix)
                    if (non_stripped_db_name.find(".") != std::string::npos)
                    {
                        it = table_models.erase(it);
                        continue;
                    }

                    if (!DatabaseCatalog::isDefaultVisibleSystemDatabase(non_stripped_db_name))
                    {
                        it = table_models.erase(it);
                        continue;
                    }
                }
            }
            full_db_name.push_back(non_stripped_db_name);
            it++;
        }

        UInt64 time_pass_ms = stop_watch.elapsedMilliseconds();
        if (time_pass_ms > 2000)
            LOG_INFO(getLogger("StorageSystemCnchColumns"),
                "cnch_catalog->getAllTables() took {} ms", time_pass_ms);

        const auto access = context->getAccess();
        const bool check_access_for_tables = !access->isGranted(AccessType::SHOW_COLUMNS);

        ContextMutablePtr mutable_context = Context::createCopy(context);
        for (size_t i = 0, size = table_models.size(); i != size; ++i)
        {
            if (!Status::isDeleted(table_models[i].status()) && matchAnyPredicate(predicates, table_models[i]))
            {

                String db = table_models[i].database();
                String table_name = table_models[i].name();
                auto table_uuid = RPCHelpers::createUUID(table_models[i].uuid());
                String create_query = table_models[i].definition();

                ColumnsDescription columns;
                Names cols_required_for_partition_key;
                Names cols_required_for_sorting_key;
                Names cols_required_for_primary_key;
                Names cols_required_for_sampling;
                MergeTreeData::ColumnSizeByName column_sizes;

                {
                    StoragePtr storage
                        = Catalog::CatalogFactory::getTableByDefinition(mutable_context, formatTenantDatabaseNameWithTenantId(db, tenant_id), table_name, create_query);

                    StorageMetadataPtr metadata_snapshot = storage->getInMemoryMetadataPtr();

                    columns = metadata_snapshot->getColumns();

                    cols_required_for_partition_key = metadata_snapshot->getColumnsRequiredForPartitionKey();
                    cols_required_for_sorting_key = metadata_snapshot->getColumnsRequiredForSortingKey();
                    cols_required_for_primary_key = metadata_snapshot->getColumnsRequiredForPrimaryKey();
                    cols_required_for_sampling = metadata_snapshot->getColumnsRequiredForSampling();
                    column_sizes = storage->getColumnSizes();
                }

                bool check_access_for_columns = check_access_for_tables && !access->isGranted(AccessType::SHOW_COLUMNS, full_db_name[i], table_models[i].name());

                for (const auto & column : columns)
                {
                    if (check_access_for_columns && !access->isGranted(AccessType::SHOW_COLUMNS, full_db_name[i], table_models[i].name(), column.name))
                        continue;

                    size_t res_index = 0;
                    res_columns[res_index++]->insert(db);
                    res_columns[res_index++]->insert(table_name);
                    res_columns[res_index++]->insert(column.name);
                    res_columns[res_index++]->insert(table_uuid);
                    res_columns[res_index++]->insert(column.type->getName());
                    res_columns[res_index++]->insert(String("")); /// what do bloom set mean here , taken from clickhouse system.columns

                    if (column.default_desc.expression)
                    {
                        res_columns[res_index++]->insert(toString(column.default_desc.kind));
                        res_columns[res_index++]->insert(queryToString(column.default_desc.expression));
                    }
                    else
                    {
                        res_columns[res_index++]->insertDefault();
                        res_columns[res_index++]->insertDefault();
                    }

                    {
                        const auto it = column_sizes.find(column.name);
                        if (it == std::end(column_sizes))
                        {
                            res_columns[res_index++]->insertDefault();
                            res_columns[res_index++]->insertDefault();
                            res_columns[res_index++]->insertDefault();
                        }
                        else
                        {
                            res_columns[res_index++]->insert(it->second.data_compressed);
                            res_columns[res_index++]->insert(it->second.data_uncompressed);
                            res_columns[res_index++]->insert(it->second.marks);
                        }
                    }

                    res_columns[res_index++]->insert(column.comment);

                    {
                        auto find_in_vector = [&key = column.name](const Names & names) {
                            return std::find(names.cbegin(), names.cend(), key) != names.end();
                        };

                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_partition_key));
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sorting_key));
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_primary_key));
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sampling));
                    }
                    if (column.codec)
                        res_columns[res_index++]->insert(queryToString(column.codec));
                    else
                        res_columns[res_index++]->insertDefault();
                }
            }
        }
    }
}
}
