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

#include <memory>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowTablesQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include "Parsers/formatTenantDatabaseName.h"

namespace DB
{


bool ParserShowTablesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_external_catalog("EXTERNAL CATALOGS");
    ParserKeyword s_tables("TABLES");
    ParserKeyword s_databases("DATABASES");
    ParserKeyword s_history("HISTORY");
    ParserKeyword s_clusters("CLUSTERS");
    ParserKeyword s_cluster("CLUSTER");
    ParserKeyword s_dictionaries("DICTIONARIES");
    ParserKeyword s_settings("SETTINGS");
    ParserKeyword s_changed("CHANGED");
    ParserKeyword s_from("FROM");
    ParserKeyword s_in("IN");
    ParserKeyword s_not("NOT");
    ParserKeyword s_like("LIKE");
    ParserKeyword s_ilike("ILIKE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_limit("LIMIT");
    ParserStringLiteral like_p;
    ParserIdentifier name_p;
    ParserToken s_dot(TokenType::Dot);
    ParserExpressionWithOptionalAlias exp_elem(false, dt);

    ASTPtr like;
    ASTPtr database;
    ASTPtr catalog;
    auto query = std::make_shared<ASTShowTablesQuery>();

    if (!s_show.ignore(pos, expected))
        return false;

    if (s_external_catalog.ignore(pos, expected)) // show external catalog
    {
        query->catalog = true;
        query->external = true;
        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
        tryRewriteHiveCatalogName(database, pos.getContext());
    }
    else if (s_databases.ignore(pos, expected)) // show databases
    {
        query->databases = true;

        if (s_history.ignore(pos, expected))
            query->history = true;

        if (s_from.ignore(pos, expected))
        {
            query->external = true;
            if (!name_p.parse(pos, catalog, expected))
                return false;
            tryRewriteHiveCatalogName(catalog, pos.getContext());
            tryGetIdentifierNameInto(catalog, query->from_catalog);
        }
        // else
        // {
        //     // rewrite current database into query.
        //     auto current_catalog = getCurrentCatalog();
        //     if (!current_catalog.empty())
        //     {
        //         query->external = true;
        //         catalog = std::make_shared<ASTIdentifier>(current_catalog);
        //         tryRewriteHiveCatalogName(catalog, pos.getContext());
        //         tryGetIdentifierNameInto(catalog, query->from_catalog);
        //     }
        // }


        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }
    else if (s_clusters.ignore(pos, expected)) // show clusters
    {
        query->clusters = true;

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }
    else if (s_cluster.ignore(pos, expected)) // show cluster
    {
        query->cluster = true;

        String cluster_str;
        if (!parseIdentifierOrStringLiteral(pos, expected, cluster_str))
            return false;

        query->cluster_str = std::move(cluster_str);
    }
    else if (bool changed = s_changed.ignore(pos, expected); changed || s_settings.ignore(pos, expected)) // show changed settings
    {
        query->m_settings = true;

        if (changed)
        {
            query->changed = true;
            if (!s_settings.ignore(pos, expected))
                return false;
        }

        /// Not expected due to "SHOW SETTINGS PROFILES"
        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else
            return false;
    }
    else // show tables
    {
        if (s_temporary.ignore(pos))
            query->temporary = true;

        if (!s_tables.ignore(pos, expected))
        {
            if (s_dictionaries.ignore(pos, expected))
                query->dictionaries = true;
            else
                return false;
        }

        if (s_history.ignore(pos, expected))
            query->history = true;

        if (s_from.ignore(pos, expected) || s_in.ignore(pos, expected))
        {
            // parse [catalog.]db
            if (!name_p.parse(pos, database, expected))
            {
                return false;
            }
            if (s_dot.ignore(pos, expected))
            {
                // get catalog from query
                catalog = database;
                tryRewriteHiveCatalogName(catalog, pos.getContext());
                if (!name_p.parse(pos, database, expected))
                {
                    return false;
                }
                query->external = true;
                tryGetIdentifierNameInto(catalog, query->from_catalog);
            }
            // else if (auto current_catalog = getCurrentCatalog(); !current_catalog.empty())
            // {
            //     // get catalog from query context
            //     catalog = std::make_shared<ASTIdentifier>(current_catalog);
            //     tryRewriteHiveCatalogName(catalog, pos.getContext());
            //     query->external = true;
            //     tryGetIdentifierNameInto(catalog, query->from_catalog);
            // }
            else
            {
                // no external catalog.
                tryRewriteCnchDatabaseName(database, pos.getContext());
            }
            tryGetIdentifierNameInto(database, query->from);
        }

        if (s_not.ignore(pos, expected))
            query->not_like = true;

        if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
        {
            if (insensitive)
                query->case_insensitive_like = true;

            if (!like_p.parse(pos, like, expected))
                return false;
        }
        else if (query->not_like)
            return false;
        else if (s_where.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->where_expression, expected))
                return false;
        }

        if (s_limit.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, query->limit_length, expected))
                return false;
        }
    }


    if (like)
        query->like = safeGet<const String &>(like->as<ASTLiteral &>().value);

    node = query;

    return true;
}


}
