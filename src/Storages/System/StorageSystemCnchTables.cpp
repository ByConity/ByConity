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
#include <Catalog/Catalog.h>
#include <Parsers/queryToString.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTClusterByElement.h>
#include <Parsers/ASTCreateQuery.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/StorageSystemCnchTables.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Status.h>
#include <common/logger_useful.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#define GET_ALL_TABLES_LIMIT (10)

StorageSystemCnchTables::StorageSystemCnchTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            {"database", std::make_shared<DataTypeString>()},
            {"name", std::make_shared<DataTypeString>()},
            {"uuid", std::make_shared<DataTypeUUID>()},
            {"vw_name", std::make_shared<DataTypeString>()},
            {"dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
            {"definition", std::make_shared<DataTypeString>()},
            {"txn_id", std::make_shared<DataTypeUInt64>()},
            {"previous_version", std::make_shared<DataTypeUInt64>()},
            {"current_version", std::make_shared<DataTypeUInt64>()},
            {"modification_time", std::make_shared<DataTypeDateTime>()},
            {"is_preallocated", std::make_shared<DataTypeUInt8>()},
            {"is_detached", std::make_shared<DataTypeUInt8>()},
            {"partition_key", std::make_shared<DataTypeString>()},
            {"sorting_key", std::make_shared<DataTypeString>()},
            {"primary_key", std::make_shared<DataTypeString>()},
            {"unique_key", std::make_shared<DataTypeString>()},
            {"sampling_key", std::make_shared<DataTypeString>()},
            {"cluster_key", std::make_shared<DataTypeString>()},
            {"split_number", std::make_shared<DataTypeInt64>()},
            {"with_range", std::make_shared<DataTypeUInt8>()},
            {"table_definition_hash", std::make_shared<DataTypeString>()},
            {"engine", std::make_shared<DataTypeString>()},
        }));
    setInMemoryMetadata(storage_metadata);
}

static std::unordered_set<String> key_columns =
{
    "partition_key",
    "sorting_key",
    "primary_key",
    "unique_key",
    "sampling_key",
    "cluster_key",
    "split_number",
    "with_range",
    "engine" /// extract engine from AST
};

static std::unordered_set<std::string> columns_require_storage =
{
    "dependencies_database",
    "dependencies_table",
    "table_definition_hash"
};

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

        if (item.count("name") && item["name"].getType() == Field::Types::String)
        {
            tmp_map["name"] = item["name"].get<String>();
        }

        tmp_res.push_back(std::move(tmp_map));
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
        if (!item.count("database") || !item.count("name"))
            return false;

        db_table_pairs.push_back(std::make_pair(item.at("database"), item.at("name")));
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
             && (!item.count("name") ||(item.at("name") == table_model.name())))
        {
            return true;
        }
    }

    return false;
}

Pipe StorageSystemCnchTables::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();

    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_tables only support cnch_server", ErrorCodes::LOGICAL_ERROR);

    bool require_key_columns = false;
    bool require_storage = false;

    for (auto it=column_names.begin(); it!=column_names.end(); ++it)
    {
        if (columns_require_storage.count(*it))
            require_storage = true;
        if (key_columns.count(*it))
        {
            /// looks very strange
            require_storage = true;
            require_key_columns = true;
        }
    }

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    Block header;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    Catalog::Catalog::DataModelTables table_models;
    const String & tenant_id = context->getTenantId();

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
        table_models = cnch_catalog->getAllTables();

    Block block_to_filter;

    /// Add `database` column.
    MutableColumnPtr db_column_mut = ColumnString::create();
    /// Add `name` column.
    MutableColumnPtr name_column_mut = ColumnString::create();
    /// Add `uuid` column
    MutableColumnPtr uuid_column_mut = ColumnUUID::create();
    /// ADD 'index' column
    MutableColumnPtr index_column_mut = ColumnUInt64::create();

    for (size_t i=0; i<table_models.size(); i++)
    {
        const String& non_stripped_db_name = table_models[i].database();
        if (!tenant_id.empty())
        {
            if (startsWith(non_stripped_db_name, tenant_id + "."))
            {
                db_column_mut->insert(getOriginalDatabaseName(non_stripped_db_name, tenant_id));
            }
            else
            {
                // Will skip database of other tenants and default user (without tenantid prefix)
                if (non_stripped_db_name.find('.') != std::string::npos)
                    continue;

                if (!DatabaseCatalog::isDefaultVisibleSystemDatabase(non_stripped_db_name))
                    continue;

                db_column_mut->insert(non_stripped_db_name);
            }
        }
        else
        {
            db_column_mut->insert(non_stripped_db_name);
        }

        name_column_mut->insert(table_models[i].name());
        uuid_column_mut->insert(RPCHelpers::createUUID(table_models[i].uuid()));
        index_column_mut->insert(i);
    }

    block_to_filter.insert(ColumnWithTypeAndName(std::move(db_column_mut), std::make_shared<DataTypeString>(), "database"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(name_column_mut), std::make_shared<DataTypeString>(), "name"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(uuid_column_mut), std::make_shared<DataTypeUUID>(), "uuid"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(index_column_mut), std::make_shared<DataTypeUInt64>(), "index"));

    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return Pipe(std::make_shared<NullSource>(std::move(header)));

    ColumnPtr filtered_index_column = block_to_filter.getByName("index").column;

    MutableColumns res_columns = header.cloneEmptyColumns();

    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    for (size_t i = 0; i<filtered_index_column->size(); i++)
    {
        auto table_model = table_models[(*filtered_index_column)[i].get<UInt64>()];

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, table_model.database());
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, table_model.database(), table_model.name()))
            continue;

        table_model.set_database(getOriginalDatabaseName(table_model.database(), tenant_id));

        if (Status::isDeleted(table_model.status()) || !matchAnyPredicate(predicates, table_model))
            continue;

        StoragePtr storage;

        if (require_storage)
        {
            try
            {
                storage = cnch_catalog->tryGetTableByUUID(*context, UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table_model.uuid())), TxnTimestamp::maxTS());
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("StorageSystemCnchTables"));
            }
            if (!storage)
                continue;
        }

        Array dependencies_table_name_array;
        Array dependencies_database_name_array;

        size_t col_num = 0;
        size_t src_index = 0;
        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.database());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.name());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(RPCHelpers::createUUID(table_model.uuid()));

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.vw_name());

        if (columns_mask[src_index] || columns_mask[src_index + 1])
        {
            std::vector<StoragePtr> dependencies = {};
            if (storage)
            {
                dependencies = cnch_catalog->getAllViewsOn(*context, storage, TxnTimestamp::maxTS());
            }
            if (!dependencies.empty())
            {
                dependencies_table_name_array.reserve(dependencies.size());
                dependencies_database_name_array.reserve(dependencies.size());
                for (const auto & dependency : dependencies)
                {
                    dependencies_table_name_array.push_back(dependency->getTableName());
                    dependencies_database_name_array.push_back(dependency->getDatabaseName());
                }
            }
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(dependencies_database_name_array);

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(dependencies_table_name_array);

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.definition());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.txnid());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.previous_version());

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(table_model.commit_time());

        if (columns_mask[src_index++])
        {
            auto modification_time = (table_model.commit_time() >> 18) ;  // first 48 bits represent times
            res_columns[col_num++]->insert(modification_time/1000) ; // convert to seconds
        }

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(!table_model.vw_name().empty()); // is_preallocated should be 1 when vw_name exists

        if (columns_mask[src_index++])
            res_columns[col_num++]->insert(Status::isDetached(table_model.status())) ;

        String engine;
        if (require_key_columns)
        {
            /// I just update what i need
            auto metadata = storage->getInMemoryMetadataPtr();
            auto ast_partition_by = metadata->getPartitionKeyAST();
            auto ast_primary_key = metadata->getPrimaryKeyAST();
            auto ast_unique_key = metadata->getUniqueKeyAST();
            auto ast_order_by = metadata->getSortingKeyAST();
            auto ast_sample_by = metadata->getSamplingKeyAST();
            auto ast_cluster_by = metadata->getClusterByKeyAST();

            if (columns_mask[src_index++])
                ast_partition_by ? (res_columns[col_num++]->insert(queryToString(*ast_partition_by))) : (res_columns[col_num++]->insertDefault());

            if (columns_mask[src_index++])
                ast_order_by ? (res_columns[col_num++]->insert(queryToString(*ast_order_by))) : (res_columns[col_num++]->insertDefault());

            if (columns_mask[src_index++])
                ast_primary_key ? (res_columns[col_num++]->insert(queryToString(*ast_primary_key))) : (res_columns[col_num++]->insertDefault());

            if (columns_mask[src_index++])
                ast_unique_key ? (res_columns[col_num++]->insert(queryToString(*ast_unique_key))) : (res_columns[col_num++]->insertDefault());

            if (columns_mask[src_index++])
                ast_sample_by ? (res_columns[col_num++]->insert(queryToString(*ast_sample_by))) : (res_columns[col_num++]->insertDefault());

            if(ast_cluster_by)
            {
                auto * cluster_by = ast_cluster_by->as<ASTClusterByElement>();
                if (columns_mask[src_index++])
                    res_columns[col_num++]->insert(queryToString(*ast_cluster_by));
                if (columns_mask[src_index++])
                    res_columns[col_num++]->insert(cluster_by->split_number);
                if (columns_mask[src_index++])
                    res_columns[col_num++]->insert(cluster_by->is_with_range);
            }
            else
            {
                if (columns_mask[src_index++])
                    res_columns[col_num++]->insertDefault();
                if (columns_mask[src_index++])
                    res_columns[col_num++]->insert(-1); // shard ratio is not available
                if (columns_mask[src_index++])
                    res_columns[col_num++]->insert(0); // with range is not available
            }
        }
        else
            src_index += 7;

        if (columns_mask[src_index++])
        {
            if (storage && storage->isBucketTable())
                res_columns[col_num++]->insert(storage->getTableHashForClusterBy().toString());
            else
                res_columns[col_num++]->insertDefault();
        }

        if (columns_mask[src_index++])
        {
            res_columns[col_num++]->insert(engine);
        }
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
