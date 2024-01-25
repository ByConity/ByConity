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

#include <memory>
#include <CloudServices/CnchWorkerResource.h>

#include <Core/Names.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>
#include <Poco/Logger.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/ForeignKeysDescription.h>
#include <Storages/UniqueNotEnforcedDescription.h>
#include <Storages/IStorage.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/StorageFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int INCORRECT_QUERY;
    extern const int TABLE_ALREADY_EXISTS;
}

void CnchWorkerResource::executeCreateQuery(ContextMutablePtr context, const String & create_query, bool skip_if_exists, const ColumnsDescription & object_columns)
{
    LOG_DEBUG(&Poco::Logger::get("WorkerResource"), "start create cloud table {}", create_query);
    const char * begin = create_query.data();
    const char * end = create_query.data() + create_query.size();
    ParserQueryWithOutput parser{end};
    const auto & settings = context->getSettingsRef();
    ASTPtr ast_query = parseQuery(parser, begin, end, "CreateCloudTable", settings.max_query_size, settings.max_parser_depth);
    auto & ast_create_query = ast_query->as<ASTCreateQuery &>();

    /// set query settings
    if (ast_create_query.settings_ast)
        InterpreterSetQuery(ast_create_query.settings_ast, context).executeForCurrentContext();

    const auto & database_name = ast_create_query.database; // not empty.
    const auto & table_name = ast_create_query.table;
    String tenant_db = formatTenantDatabaseName(database_name);
    {
        auto lock = getLock();
        if (cloud_tables.find({tenant_db, table_name}) != cloud_tables.end())
        {
            if (ast_create_query.if_not_exists || skip_if_exists)
                return;
            else
                throw Exception("Table " + tenant_db + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
        }
    }

    ColumnsDescription columns;
    IndicesDescription indices;
    ConstraintsDescription constraints;
    ForeignKeysDescription foreign_keys;
    UniqueNotEnforcedDescription unique_not_enforced;

    if (ast_create_query.columns_list)
    {
        if (ast_create_query.columns_list->columns)
        {
            // Set attach = true to avoid making columns nullable due to ANSI settings, because the dialect change
            // should NOT affect existing tables.
            columns = InterpreterCreateQuery::getColumnsDescription(*ast_create_query.columns_list->columns, context, /* attach= */ true);
        }

        if (ast_create_query.columns_list->indices)
            for (const auto & index : ast_create_query.columns_list->indices->children)
                indices.push_back(IndexDescription::getIndexFromAST(index->clone(), columns, context));

        if (ast_create_query.columns_list->constraints)
            for (const auto & constraint : ast_create_query.columns_list->constraints->children)
                constraints.constraints.push_back(std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint->clone()));

        if (ast_create_query.columns_list->foreign_keys)
            for (const auto & foreign_key : ast_create_query.columns_list->foreign_keys->children)
                foreign_keys.foreign_keys.push_back(std::dynamic_pointer_cast<ASTForeignKeyDeclaration>(foreign_key->clone()));

        if (ast_create_query.columns_list->unique)
            for (const auto & unique : ast_create_query.columns_list->unique->children)
                unique_not_enforced.unique.push_back(std::dynamic_pointer_cast<ASTUniqueNotEnforcedDeclaration>(unique->clone()));
    }
    else
        throw Exception("Incorrect CREATE query: required list of column descriptions or AS section or SELECT.", ErrorCodes::INCORRECT_QUERY);

    /// Even if query has list of columns, canonicalize it (unfold Nested columns).
    ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns, ParserSettings::valueOf(context->getSettingsRef()));
    ASTPtr new_indices = InterpreterCreateQuery::formatIndices(indices);
    ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(constraints);
    ASTPtr new_foreign_keys = InterpreterCreateQuery::formatForeignKeys(foreign_keys);
    ASTPtr new_unique_not_enforced = InterpreterCreateQuery::formatUnique(unique_not_enforced);

    if (ast_create_query.columns_list->columns)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);

    if (ast_create_query.columns_list->indices)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->indices, new_indices);

    if (ast_create_query.columns_list->constraints)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->constraints, new_constraints);

    if (ast_create_query.columns_list->foreign_keys)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->foreign_keys, new_foreign_keys);

    if (ast_create_query.columns_list->unique)
        ast_create_query.columns_list->replace(ast_create_query.columns_list->unique, new_unique_not_enforced);

    /// Check for duplicates
    std::set<String> all_columns;
    for (const auto & column : columns)
    {
        if (!all_columns.emplace(column.name).second)
            throw Exception("Column " + backQuoteIfNeed(column.name) + " already exists", ErrorCodes::DUPLICATE_COLUMN);
    }

    /// Table constructing
    StoragePtr res = StorageFactory::instance().get(ast_create_query, "", context, context->getGlobalContext(), columns, constraints, foreign_keys, unique_not_enforced, false);
    res->startup();

    if (auto cloud_table = std::dynamic_pointer_cast<StorageCloudMergeTree>(res))
        cloud_table->resetObjectColumns(object_columns);

    {
        auto lock = getLock();
        cloud_tables.emplace(std::make_pair(tenant_db, table_name), res);
        auto it = memory_databases.find(tenant_db);
        if (it == memory_databases.end())
        {
            DatabasePtr database = std::make_shared<DatabaseMemory>(tenant_db, context->getGlobalContext());
            memory_databases.insert(std::make_pair(tenant_db, std::move(database)));
        }
    }

    LOG_DEBUG(&Poco::Logger::get("WorkerResource"), "Successfully create cloud table {} and database {}", res->getStorageID().getNameForLogs(), database_name);
}

StoragePtr CnchWorkerResource::getTable(const StorageID & table_id) const
{
    String tenant_db = formatTenantDatabaseName(table_id.getDatabaseName());
    auto lock = getLock();

    auto it = cloud_tables.find({tenant_db, table_id.getTableName()});
    if (it != cloud_tables.end())
    {
        return it->second;
    }

    return {};
}

DatabasePtr CnchWorkerResource::getDatabase(const String & database_name) const
{
    String tenant_db = formatTenantDatabaseName(database_name);
    auto lock = getLock();

    auto it = memory_databases.find(tenant_db);
    if (it != memory_databases.end())
        return it->second;

    return {};
}

bool CnchWorkerResource::isCnchTableInWorker(const StorageID & table_id) const
{
    String tenant_db = formatTenantDatabaseName(table_id.getDatabaseName());
    auto lock = getLock();
    return cnch_tables.find({tenant_db, table_id.getTableName()}) != cnch_tables.end();
}

void CnchWorkerResource::clearResource()
{
    auto lock = getLock();
    for (const auto & table : cloud_tables)
        table.second->shutdown();
    cloud_tables.clear();
    memory_databases.clear();
    cnch_tables.clear();
    worker_table_names.clear();
}

}
