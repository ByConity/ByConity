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

#include <Parsers/ParserReproduceQuery.h>

#include <Parsers/ASTReproduceQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{
bool ParserReproduceQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_reproduce("REPRODUCE");
    ParserKeyword s_verbose("VERBOSE");

    ParserKeyword s_ddl("DDL");
    ParserKeyword s_on("ON");
    ParserKeyword s_cluster("CLUSTER");

    ParserKeyword s_explain("EXPLAIN");
    ParserKeyword s_execute("EXECUTE");

    ParserKeyword s_query("QUERY");
    ParserKeyword s_id("ID");
    ParserKeyword s_source("SOURCE");
    ParserKeyword s_settings("SETTINGS");

    ParserSelectWithUnionQuery select_p(dt);
    ParserLiteral literal_p(dt);
    ParserIdentifier identifier_p;

    ASTReproduceQuery::Mode mode = ASTReproduceQuery::Mode::EXECUTE;
    bool is_verbose = false;
    ASTPtr subquery;
    ASTPtr query_id;
    ASTPtr reproduce_path;
    ASTPtr settings;
    ASTPtr cluster_name;

    /// REPRODUCE
    if (!s_reproduce.ignore(pos, expected))
        return false;

    /// DDL
    if (s_ddl.ignore(pos, expected))
    {
        mode = ASTReproduceQuery::Mode::DDL;
        /// [ON CLUSTER cluster_name]
        if (s_on.ignore(pos, expected))
        {
            if (!s_cluster.ignore(pos, expected))
                return false;

            if (!identifier_p.parse(pos, cluster_name, expected))
                return false;
        }
    }
    else
    {
        /// [VERBOSE]
        if (s_verbose.ignore(pos, expected))
            is_verbose = true;
        /// [EXPLAIN | EXECUTE], default to EXECUTE
        if (s_explain.ignore(pos, expected))
        {
            mode = ASTReproduceQuery::Mode::EXPLAIN;
        } else if (s_execute.ignore(pos, expected))
        {
            mode = ASTReproduceQuery::Mode::EXECUTE;
        }

        /// [QUERY (subquery) | ID 'query_id']
        if (s_query.ignore(pos, expected))
        {
            if (!select_p.parse(pos, subquery, expected))
                return false;
        }
        else if (s_id.ignore(pos, expected))
        {
            if (!literal_p.parse(pos, query_id, expected))
                return false;
        }
    }

    /// SOURCE 'query.zip'
    if (!s_source.ignore(pos, expected))
        return false;
    if (!literal_p.parse(pos, reproduce_path, expected))
        return false;

    /// SETTINGS key1 = value1, key2 = value2, ...
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, settings, expected))
            return false;
    }

    auto reproduce_query = std::make_shared<ASTReproduceQuery>();
    reproduce_query->mode = mode;
    reproduce_query->is_verbose = is_verbose;
    reproduce_query->setExpression(ASTReproduceQuery::Expression::SUBQUERY, std::move(subquery));
    reproduce_query->setExpression(ASTReproduceQuery::Expression::QUERY_ID, std::move(query_id));
    reproduce_query->setExpression(ASTReproduceQuery::Expression::REPRODUCE_PATH, std::move(reproduce_path));
    reproduce_query->setExpression(ASTReproduceQuery::Expression::SETTINGS_CHANGES, std::move(settings));
    reproduce_query->setExpression(ASTReproduceQuery::Expression::CLUSTER, std::move(cluster_name));

    node = reproduce_query;
    return true;
}

}
