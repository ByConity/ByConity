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

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserShowAccessEntitiesQuery.h>
#include <Parsers/ParserShowAccessQuery.h>
#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ParserShowPrivilegesQuery.h>
#include <Parsers/ParserExplainQuery.h>
#include <Parsers/QueryWithOutputSettingsPushDownVisitor.h>
#include <Parsers/ParserRefreshQuery.h>
#include <Parsers/ParserStatsQuery.h>
#include <Parsers/ParserDumpQuery.h>
#include <Parsers/ParserReproduceQuery.h>
#include <Parsers/ParserUndropQuery.h>
#include <Parsers/ParserAlterDiskCacheQuery.h>
#include <Parsers/ParserTransaction.h>

namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserShowTablesQuery show_tables_p(dt);
    ParserSelectWithUnionQuery select_p(dt);
    ParserTablePropertiesQuery table_p;
    ParserDescribeTableQuery describe_table_p(dt);
    ParserShowProcesslistQuery show_processlist_p;
    ParserCreateQuery create_p(dt);
    ParserAlterQuery alter_p(dt);
    ParserRenameQuery rename_p;
    ParserDropQuery drop_p;
    ParserUndropQuery undrop_p;
    ParserCheckQuery check_p(dt);
    ParserAlterDiskCacheQuery alter_disk_cache_p;
    ParserOptimizeQuery optimize_p(dt);
    ParserKillQueryQuery kill_query_p(dt);
    ParserWatchQuery watch_p;
    ParserShowAccessQuery show_access_p;
    ParserShowAccessEntitiesQuery show_access_entities_p;
    ParserShowCreateAccessEntityQuery show_create_access_entity_p;
    ParserShowGrantsQuery show_grants_p;
    ParserShowPrivilegesQuery show_privileges_p;
    ParserExplainQuery explain_p(end, dt);
    ParserDumpQuery dump_p;
    ParserReproduceQuery reproduce_p(end);
    ParserRefreshQuery refresh_p(dt);
    ParserCreateStatsQuery create_stats_p;
    ParserShowStatsQuery show_stats_p;
    ParserDropStatsQuery drop_stats_p;
    ParserShowStatementsQuery show_statements_p;
    ParserBeginTransactionQuery begin_transaction_p;
    ParserBeginQuery begin_p;
    ParserCommitQuery commit_p;
    ParserRollbackQuery rollback_p;

    ASTPtr query;

    bool parsed =
           explain_p.parse(pos, query, expected)
        || reproduce_p.parse(pos, query, expected)
        || dump_p.parse(pos, query, expected)
        || select_p.parse(pos, query, expected)
        || show_create_access_entity_p.parse(pos, query, expected) /// should be before `show_tables_p`
        || show_tables_p.parse(pos, query, expected)
        || table_p.parse(pos, query, expected)
        || describe_table_p.parse(pos, query, expected)
        || show_processlist_p.parse(pos, query, expected)
        || create_p.parse(pos, query, expected)
        || alter_p.parse(pos, query, expected)
        || rename_p.parse(pos, query, expected)
        || drop_p.parse(pos, query, expected)
        || undrop_p.parse(pos, query, expected)
        || check_p.parse(pos, query, expected)
        || alter_disk_cache_p.parse(pos, query, expected)
        || kill_query_p.parse(pos, query, expected)
        || optimize_p.parse(pos, query, expected)
        || watch_p.parse(pos, query, expected)
        || show_access_p.parse(pos, query, expected)
        || show_access_entities_p.parse(pos, query, expected)
        || show_grants_p.parse(pos, query, expected)
        || show_privileges_p.parse(pos, query, expected)
        || refresh_p.parse(pos, query, expected)
        || create_stats_p.parse(pos, query, expected)
        || show_stats_p.parse(pos, query, expected)
        || drop_stats_p.parse(pos, query, expected)
        || show_statements_p.parse(pos, query, expected)
        || begin_transaction_p.parse(pos, query, expected)
        || begin_p.parse(pos, query, expected)
        || commit_p.parse(pos, query, expected)
        || rollback_p.parse(pos, query, expected);

    if (!parsed)
        return false;

    /// FIXME: try to prettify this cast using `as<>()`
    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserKeyword s_into_outfile("INTO OUTFILE");
    if (s_into_outfile.ignore(pos, expected))
    {
        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, query_with_output.out_file, expected))
            return false;

        query_with_output.children.push_back(query_with_output.out_file);
    }

    auto parse_format = [&]()
    {
        ParserKeyword s_format("FORMAT");

        if (s_format.ignore(pos, expected))
        {
            ParserIdentifier format_p;

            if (!format_p.parse(pos, query_with_output.format, expected))
                return false;
            setIdentifierSpecial(query_with_output.format);

            query_with_output.children.push_back(query_with_output.format);
        }

        return true;
    };

    if (!parse_format())
        return false;

    // [ COMPRESSION "compression_method_vaule" [LEVEL compression_level_value] ]
    ParserKeyword s_compression_method("COMPRESSION");
    if (s_compression_method.ignore(pos, expected))
    {
        ParserStringLiteral compression_method_p;
        if (!compression_method_p.parse(pos, query_with_output.compression_method, expected))
            return false;
        query_with_output.children.push_back(query_with_output.compression_method);

        ParserKeyword s_compression_level("LEVEL");
        if (s_compression_level.ignore(pos, expected))
        {
            ParserNumber compression_level_p;
            if (!compression_level_p.parse(pos, query_with_output.compression_level, expected))
                return false;
            query_with_output.children.push_back(query_with_output.compression_level);
        }
    }

    // SETTINGS key1 = value1, key2 = value2, ...
    ParserKeyword s_settings("SETTINGS");
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, query_with_output.settings_ast, expected))
            return false;
        query_with_output.children.push_back(query_with_output.settings_ast);

        // SETTINGS after FORMAT is not parsed by the SELECT parser (ParserSelectQuery)
        // Pass them manually, to apply in InterpreterSelectQuery::initSettings()
        if (query->as<ASTSelectWithUnionQuery>())
        {
            QueryWithOutputSettingsPushDownVisitor::Data data{query_with_output.settings_ast};
            QueryWithOutputSettingsPushDownVisitor(data).visit(query);
        }
    }

    /**
     * if no format before, try parse format after settings, i.g. (select 1) settings xxxxx format xxxxx;
     */
    if (!query_with_output.format)
    {
        if (!parse_format())
            return false;
    }

    node = std::move(query);
    return true;
}

}
