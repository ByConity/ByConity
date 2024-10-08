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

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>

#include <Common/typeid_cast.h>
#include <DataTypes/MapHelpers.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableIdentifier & identifier, const String & current_database)
{
    alias = identifier.tryGetAlias();

    auto table_id = identifier.getTableId();
    std::tie(database, table, uuid) = std::tie(table_id.database_name, table_id.table_name, table_id.uuid);
    LOG_TRACE(&Poco::Logger::get("DatabaseAndTableWithAlias"), fmt::format("got {}.{}", database, table));
    if (database.empty())
        database = current_database;
}


StorageID DatabaseAndTableWithAlias::getStorageID() const
{
    return StorageID(database, table, uuid);
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTPtr & node, const String & current_database)
{
    const auto * identifier = node->as<ASTTableIdentifier>();
    if (!identifier)
        throw Exception("Logical error: table identifier expected", ErrorCodes::LOGICAL_ERROR);

    *this = DatabaseAndTableWithAlias(*identifier, current_database);
}

DatabaseAndTableWithAlias::DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database)
{
    if (table_expression.database_and_table_name)
        *this = DatabaseAndTableWithAlias(table_expression.database_and_table_name, current_database);
    else if (table_expression.table_function)
        alias = table_expression.table_function->tryGetAlias();
    else if (table_expression.subquery)
    {
        const auto & database_of_view = table_expression.subquery->as<const ASTSubquery &>().database_of_view;
        const auto & cte_name = table_expression.subquery->as<const ASTSubquery &>().cte_name;
        if (!cte_name.empty())
        {
            database = !database_of_view.empty() ? database_of_view : current_database;
            table = cte_name;
        }
        alias = table_expression.subquery->tryGetAlias();
    }
    else
        throw Exception("Logical error: no known elements in ASTTableExpression", ErrorCodes::LOGICAL_ERROR);
}

bool DatabaseAndTableWithAlias::satisfies(const DatabaseAndTableWithAlias & db_table, bool table_may_be_an_alias) const
{
    /// table.*, alias.* or database.table.*

    if (database.empty())
    {
        if (!db_table.table.empty() && table == db_table.table)
            return true;

        if (!db_table.alias.empty())
            return (alias == db_table.alias) || (table_may_be_an_alias && table == db_table.alias);
    }

    return database == db_table.database && table == db_table.table;
}

String DatabaseAndTableWithAlias::getQualifiedNamePrefix(bool with_dot) const
{
    if (alias.empty() && table.empty())
        return "";
    return (!alias.empty() ? alias : table) + (with_dot ? "." : "");
}

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database)
{
    std::vector<const ASTTableExpression *> tables_expression = getTableExpressions(select_query);

    std::vector<DatabaseAndTableWithAlias> database_and_table_with_aliases;
    database_and_table_with_aliases.reserve(tables_expression.size());

    for (const auto & table_expression : tables_expression)
        database_and_table_with_aliases.emplace_back(DatabaseAndTableWithAlias(*table_expression, current_database));

    return database_and_table_with_aliases;
}

std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number)
{
    const ASTTableExpression * table_expression = getTableExpression(select, table_number);
    if (!table_expression)
        return {};

    ASTPtr database_and_table_name = table_expression->database_and_table_name;
    if (!database_and_table_name || !database_and_table_name->as<ASTTableIdentifier>())
        return {};

    return DatabaseAndTableWithAlias(database_and_table_name);
}

bool TableWithColumnNamesAndTypes::hasColumn(const String & name) const 
{  
    bool exists = names.contains(name);
    if (!exists && isMapImplicitKey(name))
    {
        return names.count(parseMapNameFromImplicitColName(name));
    }
    return exists;
}

}
