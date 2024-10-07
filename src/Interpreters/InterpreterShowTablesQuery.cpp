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

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Poco/Logger.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include "IO/WriteBufferFromString.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


InterpreterShowTablesQuery::InterpreterShowTablesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}

static String rewriteShowDatabaseForExternal(const ASTShowTablesQuery & query, const String & catalog_name);
static String rewriteShowTableForExternal(const ASTShowTablesQuery & query, const String & catalog_name, const String & database_name);
static String rewriteShowDatabaseForExternal(const ASTShowTablesQuery & query, const String & catalog_name);

String InterpreterShowTablesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();
    if (query.external)
        return getRewrittenQueryForExternalCatalogImpl();
    else
        return getRewrittenQueryImpl();
}

String InterpreterShowTablesQuery::getRewrittenQueryImpl()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();
    /// SHOW DATABASES
    if (query.databases)
    {
        if (auto current_catalog = getContext()->getCurrentCatalog(); !current_catalog.empty())
        {
            return rewriteShowDatabaseForExternal(query, current_catalog);
        }
        WriteBufferFromOwnString rewritten_query;

        if (query.history)
            rewritten_query << "SELECT name, uuid, delete_time FROM system.cnch_databases_history";
        else
            rewritten_query << "SELECT name FROM system.databases";

        if (!query.like.empty())
        {
            rewritten_query << " WHERE " << (query.history ? "system.cnch_databases_history.name " : "system.databases.name ")
                            << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ") << DB::quote
                            << query.like;
        }

        if (query.limit_length)
            rewritten_query << " LIMIT " << query.limit_length;

        return rewritten_query.str();
    }

    /// SHOW CLUSTER/CLUSTERS
    if (query.clusters)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT DISTINCT cluster FROM system.clusters";

        if (!query.like.empty())
        {
            rewritten_query << " WHERE cluster " << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                            << DB::quote << query.like;
        }

        if (query.limit_length)
            rewritten_query << " LIMIT " << query.limit_length;

        return rewritten_query.str();
    }
    else if (query.cluster)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT * FROM system.clusters";

        rewritten_query << " WHERE cluster = " << DB::quote << query.cluster_str;

        return rewritten_query.str();
    }

    /// SHOW SETTINGS
    if (query.m_settings)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT name, type, value FROM system.settings";

        if (query.changed)
            rewritten_query << " WHERE changed = 1";

        if (!query.like.empty())
        {
            rewritten_query << (query.changed ? " AND name " : " WHERE name ") << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                            << DB::quote << query.like;
        }

        return rewritten_query.str();
    }

    if (query.temporary && !query.from.empty())
        throw Exception("The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`", ErrorCodes::SYNTAX_ERROR);

    String database = getContext()->resolveDatabase(query.from);
    DatabaseCatalog::instance().assertDatabaseExists(database, getContext());

    auto [catalog_opt, database_opt] = getCatalogNameAndDatabaseName(database);
    if (catalog_opt.has_value() && database_opt.has_value())
    {
        return rewriteShowTableForExternal(query, catalog_opt.value(), database_opt.value());
    }

    WriteBufferFromOwnString rewritten_query;
    bool is_mysql = getContext()->getSettingsRef().dialect_type == DialectType::MYSQL;
    if (is_mysql)
        rewritten_query << "SELECT Tables_in_" << getOriginalDatabaseName(database);
    else
        rewritten_query << "SELECT name";

    if (query.full)
    {
        if (query.dictionaries)
            throw Exception("FULL is not allowed for dictionaries.", ErrorCodes::SYNTAX_ERROR);
        else if (query.snapshots)
            throw Exception("FULL is not allowed for snapshots.", ErrorCodes::SYNTAX_ERROR);
        if (query.history)
            throw Exception("FULL TABLE is not compatible with HISTORY.", ErrorCodes::SYNTAX_ERROR);
        rewritten_query <<
            ", multiIf("
                "database = 'INFORMATION_SCHEMA' OR database = 'information_schema' OR database = 'mysql' OR database = 'MYSQL', 'SYSTEM VIEW',"
                "engine = 'View', 'VIEW',"
                "'BASE TABLE'"
            ") AS ";

        if (is_mysql)
            rewritten_query << "Table_type";
        else
            rewritten_query << "table_type";
    }

    rewritten_query << " FROM ";

    if (query.dictionaries)
        rewritten_query << "system.dictionaries ";
    else if (query.snapshots)
    {
        rewritten_query.restart();
        rewritten_query << "SELECT name, table_uuid, creation_time, ttl_in_days FROM system.cnch_snapshots ";
    }
    else
    {
        if (query.history)
        {
            rewritten_query.restart();
            rewritten_query << "SELECT name, uuid, delete_time FROM system.cnch_tables_history ";
        }
        else
        {
            String tables_in_db = "Tables_in_" + getOriginalDatabaseName(database);
            rewritten_query << "(SELECT *, name AS " << backQuote(tables_in_db) << " FROM system.tables) ";
        }
    }

    rewritten_query << "WHERE ";

    if (query.temporary)
    {
        if (query.dictionaries)
            throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);
        rewritten_query << "is_temporary";
    }
    else
        rewritten_query << "database = " << DB::quote << ((query.snapshots) ? database : getOriginalDatabaseName(database));

    if (!query.like.empty())
        rewritten_query << " AND name " << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ") << DB::quote
                        << query.like;
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression << ")";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

    return rewritten_query.str();
}

static String rewriteShowDatabaseForExternal(const ASTShowTablesQuery & query, const String & catalog_name)
{
    WriteBufferFromOwnString rewritten_query;
    rewritten_query << "SELECT database FROM system.external_databases";

    rewritten_query << " WHERE "
                    << "catalog_name"
                    << " = " << DB::quote << catalog_name;
    if (!query.like.empty())
    {
        rewritten_query << " AND database " << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                        << DB::quote << query.like;
    }
    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

    return rewritten_query.str();
}

static String rewriteShowTableForExternal(const ASTShowTablesQuery & query, const String & catalog_name, const String & database_name)
{
    WriteBufferFromOwnString rewritten_query;
    rewritten_query << "SELECT table FROM system.external_tables "
                    << " WHERE "
                    << "catalog_name "
                    << " = " << DB::quote << catalog_name << " AND database "
                    << " = " << DB::quote << database_name;

    if (!query.like.empty())
        rewritten_query << " AND table " << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                        << DB::quote << query.like;
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression << ")";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;
    return rewritten_query.str();
}

static String rewriteShowCatalogForExternal(const ASTShowTablesQuery & query, const String & tenant_id)
{
    WriteBufferFromOwnString rewritten_query;
    if (!tenant_id.empty())
    {
        rewritten_query << "SELECT arrayStringConcat(arraySlice(splitByChar('.',catalog_name), 2),'.') AS catalog_name FROM "
                           "system.external_catalogs";
    }
    else
    {
        rewritten_query << "SELECT catalog_name FROM system.external_catalogs";
    }

    if (!query.like.empty())
    {
        rewritten_query << " WHERE "
                        << (tenant_id.empty() ? "catalog_name " : "  arrayStringConcat(arraySlice(splitByChar('.',catalog_name), 2),'.') ")
                        << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ") << DB::quote << query.like;
    }
    if (!tenant_id.empty())
    {
        rewritten_query << (!query.like.empty() ? " AND " : " WHERE ") << " system.external_catalogs.catalog_name "
                        << "LIKE '" << tenant_id << ".%'";
    }

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;
    LOG_TRACE(getLogger("getRewrittenQueryForExternalCatalogImpl"), rewritten_query.str());

    return rewritten_query.str();
}

String InterpreterShowTablesQuery::getRewrittenQueryForExternalCatalogImpl()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();

    if (query.catalog) // SHOW EXTERNAL CATALOGS
    {
        const auto & tenant_id = getContext()->getTenantId();
        return rewriteShowCatalogForExternal(query, tenant_id);
    }

    if (query.databases) // SHOW DATABASES
    {
        return rewriteShowDatabaseForExternal(query, query.from_catalog);
    }
    return rewriteShowTableForExternal(query, query.from_catalog, query.from);
}


BlockIO InterpreterShowTablesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}
