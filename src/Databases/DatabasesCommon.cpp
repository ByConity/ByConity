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

#include <Databases/DatabasesCommon.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Catalog/Catalog.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
}

String getTableDefinitionFromCreateQuery(const ASTPtr & query, bool attach)
{
    ASTPtr query_clone = query->clone();
    auto & create = query_clone->as<ASTCreateQuery &>();

    /// We remove everything that is not needed for ATTACH from the query.
    create.attach = attach;
    create.as_database.clear();
    create.as_table.clear();
    create.if_not_exists = false;
    create.is_populate = false;
    create.replace_view = false;

    /// For views it is necessary to save the SELECT query itself, for the rest - on the contrary
    if (!create.is_ordinary_view && !create.is_materialized_view)
        create.select = nullptr;

    create.format = nullptr;
    create.out_file = nullptr;

    WriteBufferFromOwnString statement_stream;
    formatAST(create, statement_stream, false);
    statement_stream << '\n';
    return statement_stream.str();
}

ASTPtr getCreateQueryFromStorage(const StoragePtr & storage, const ASTPtr & ast_storage, bool only_ordinary, uint32_t max_parser_depth, bool throw_on_error)
{
    auto table_id = storage->getStorageID();
    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    if (metadata_ptr == nullptr)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Cannot get metadata of {}.{}", backQuote(table_id.database_name), backQuote(table_id.table_name));
        else
            return nullptr;
    }

    auto create_table_query = std::make_shared<ASTCreateQuery>();
    create_table_query->attach = false;
    create_table_query->table = table_id.table_name;
    create_table_query->database = table_id.database_name;
    create_table_query->set(create_table_query->storage, ast_storage);

    /// setup create table query columns info.
    {
        auto ast_columns_list = std::make_shared<ASTColumns>();
        auto ast_expression_list = std::make_shared<ASTExpressionList>();
        NamesAndTypesList columns;
        if (only_ordinary)
            columns = metadata_ptr->columns.getOrdinary();
        else
            columns = metadata_ptr->columns.getAll();
        for (const auto & column_name_and_type: columns)
        {
            const auto & ast_column_declaration = std::make_shared<ASTColumnDeclaration>();
            ast_column_declaration->name = column_name_and_type.name;
            /// parser typename
            {
                ASTPtr ast_type;
                auto type_name = column_name_and_type.type->getName();
                const auto * string_end = type_name.c_str() + type_name.length();
                Expected expected;
                expected.max_parsed_pos = string_end;
                Tokens tokens(type_name.c_str(), string_end);
                IParser::Pos pos(tokens, max_parser_depth);
                ParserDataType parser;
                if (!parser.parse(pos, ast_type, expected))
                {
                    if (throw_on_error)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parser metadata of {}.{}", backQuote(table_id.database_name), backQuote(table_id.table_name));
                    else
                        return nullptr;
                }
                ast_column_declaration->type = ast_type;
            }
            ast_expression_list->children.emplace_back(ast_column_declaration);
        }

        ast_columns_list->set(ast_columns_list->columns, ast_expression_list);
        create_table_query->set(create_table_query->columns_list, ast_columns_list);
    }
    return create_table_query;
}

DatabaseWithOwnTablesBase::DatabaseWithOwnTablesBase(const String & name_, const String & logger, ContextPtr context_)
        : IDatabase(name_), WithContext(context_->getGlobalContext()), log(&Poco::Logger::get(logger))
{
}

bool DatabaseWithOwnTablesBase::isTableExist(const String & table_name, ContextPtr) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(const String & table_name, ContextPtr) const
{
    std::lock_guard lock(mutex);
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    return {};
}

DatabaseTablesIteratorPtr DatabaseWithOwnTablesBase::getTablesIterator(ContextPtr, const FilterByNameFunction & filter_by_table_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_table_name)
        return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(filtered_tables), database_name);
}

bool DatabaseWithOwnTablesBase::empty() const
{
    std::lock_guard lock(mutex);
    return tables.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    std::unique_lock lock(mutex);
    return detachTableUnlocked(table_name, lock);
}

StoragePtr DatabaseWithOwnTablesBase::detachTableUnlocked(const String & table_name, std::unique_lock<std::mutex> &)
{
    StoragePtr res;

    auto it = tables.find(table_name);
    if (it == tables.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                        backQuote(database_name), backQuote(table_name));
    res = it->second;
    tables.erase(it);

    auto table_id = res->getStorageID();
    if (table_id.hasUUID())
    {
        [[maybe_unused]] bool is_cloud = dynamic_cast<StorageCloudMergeTree *>(res.get()) != nullptr;
        assert(database_name == DatabaseCatalog::TEMPORARY_DATABASE || getUUID() != UUIDHelpers::Nil || (is_cloud && (getEngineName() == "Memory")));
        DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
    }

    return res;
}

void DatabaseWithOwnTablesBase::attachTable(const String & table_name, const StoragePtr & table, const String &)
{
    std::unique_lock lock(mutex);
    attachTableUnlocked(table_name, table, lock);
}

void DatabaseWithOwnTablesBase::attachTableUnlocked(const String & table_name, const StoragePtr & table, std::unique_lock<std::mutex> &)
{
    auto table_id = table->getStorageID();
    if (table_id.database_name != database_name)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                        database_name, table_id.database_name);

    if (table_id.hasUUID())
    {
        // assert(database_name == DatabaseCatalog::TEMPORARY_DATABASE || getUUID() != UUIDHelpers::Nil);
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid, shared_from_this(), table);
    }

    if (!tables.emplace(table_name, table).second)
    {
        if (table_id.hasUUID())
            DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists.", table_id.getFullTableName());
    }
}

void DatabaseWithOwnTablesBase::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        kv.second->flush();
    }

    for (const auto & kv : tables_snapshot)
    {
        auto table_id = kv.second->getStorageID();
        kv.second->flushAndShutdown();
        if (table_id.hasUUID())
        {
            assert(getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE || getUUID() != UUIDHelpers::Nil);
            DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
        }
    }

    std::lock_guard lock(mutex);
    tables.clear();
}

std::map<String, String> DatabaseWithOwnTablesBase::getBrokenTables()
{
    std::lock_guard lock(mutex);
    return brokenTables;
}

void DatabaseWithOwnTablesBase::clearBrokenTables()
{
    std::lock_guard lock(mutex);
    brokenTables.clear();
}

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        DatabaseWithOwnTablesBase::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

StoragePtr DatabaseWithOwnTablesBase::getTableUnlocked(const String & table_name, std::unique_lock<std::mutex> &) const
{
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuote(database_name), backQuote(table_name));
}

std::vector<StoragePtr> getViews(const StorageID & storage_id, const ContextPtr & context)
{
    std::vector<StoragePtr> views;
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    auto start_time = context->getTimestamp();
    auto catalog_client = context->getCnchCatalog();
    if (!catalog_client)
        throw Exception("get catalog client failed", ErrorCodes::LOGICAL_ERROR);
    auto all_views_from_catalog = catalog_client->getAllViewsOn(*context, storage, start_time);
    for (auto & dependence : all_views_from_catalog)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(
            dependence->getStorageID(), context);
        if (table)
        {
            if (dynamic_cast<StorageMaterializedView*>(table.get()))
                views.emplace_back(table);
        }
    }
    return views;
}

String generateInnerTableName(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return ".inner_id." + toString(view_id.uuid);
    return ".inner." + view_id.getTableName();
}

}
