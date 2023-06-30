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
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Common/typeid_cast.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


InterpreterShowTablesQuery::InterpreterShowTablesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


String InterpreterShowTablesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();

    /// SHOW DATABASES
    if (query.databases)
    {
        WriteBufferFromOwnString rewritten_query;
        const auto& tenant_id = getContext()->getTenantId();
        if (!tenant_id.empty())
        {
            if (query.history)
                rewritten_query << "SELECT arrayStringConcat(arraySlice(splitByChar('.',name), 2),'.') AS name, uuid, delete_time FROM system.cnch_databases_history";
            else
                rewritten_query << "SELECT arrayStringConcat(arraySlice(splitByChar('.',name), 2),'.') AS name FROM system.databases";
        }
        else
        {
            if (query.history)
                rewritten_query << "SELECT name, uuid, delete_time FROM system.cnch_databases_history";
            else
                rewritten_query << "SELECT name FROM system.databases";
        }
        

        if (!query.like.empty())
        {
            rewritten_query
                << " WHERE "
                << (query.history ? "system.cnch_databases_history.name " : "system.databases.name ")
                << (query.not_like ? "NOT " : "")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                << DB::quote << query.like;
        }
        if (!tenant_id.empty())
        {
            rewritten_query
                << (!query.like.empty() ? " AND " : " WHERE ")
                << (query.history ? "system.cnch_databases_history.name " : "system.databases.name ")
                << "LIKE '" << tenant_id << ".%'";
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
            rewritten_query
                << " WHERE cluster "
                << (query.not_like ? "NOT " : "")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
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
            rewritten_query
                << (query.changed ? " AND name " : " WHERE name ")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                << DB::quote << query.like;
        }

        return rewritten_query.str();
    }

    if (query.temporary && !query.from.empty())
        throw Exception("The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`", ErrorCodes::SYNTAX_ERROR);

    String database = getContext()->resolveDatabase(query.from);
    DatabaseCatalog::instance().assertDatabaseExists(database);

    WriteBufferFromOwnString rewritten_query;
    rewritten_query << "SELECT name FROM system.";

    if (query.dictionaries)
        rewritten_query << "dictionaries ";
    else
    {
        if (query.history)
        {
            rewritten_query.restart();
            rewritten_query << "SELECT name, uuid, delete_time FROM system.cnch_tables_history ";
        }
        else
            rewritten_query << "tables ";
    }

    rewritten_query << "WHERE ";

    if (query.temporary)
    {
        if (query.dictionaries)
            throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);
        rewritten_query << "is_temporary";
    }
    else
        rewritten_query << "database = " << DB::quote << database;

    if (!query.like.empty())
        rewritten_query
            << " AND name "
            << (query.not_like ? "NOT " : "")
            << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
            << DB::quote << query.like;
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression << ")";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

    return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}


}
