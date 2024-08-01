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

#include <Parsers/ParserExplainQuery.h>

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

bool ParserExplainQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTExplainQuery::ExplainKind kind;

    ParserKeyword s_ast("AST");
    ParserKeyword s_explain("EXPLAIN");
    ParserKeyword s_syntax("SYNTAX");
    ParserKeyword s_pipeline("PIPELINE");
    ParserKeyword s_plan("PLAN");
    ParserKeyword s_element("ELEMENT");
    ParserKeyword s_plansegment("PLANSEGMENT");
    ParserKeyword s_opt_plan("OPT_PLAN");
    ParserKeyword s_distributed("DISTRIBUTED");
    ParserKeyword s_analyze("ANALYZE");
    ParserKeyword s_trace("TRACE_OPT");
    ParserKeyword s_rule("RULE");
    ParserKeyword s_metadata("METADATA");


    if (s_explain.ignore(pos, expected))
    {
        kind = ASTExplainQuery::QueryPlan;
        if (s_ast.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::ParsedAST;
        else if (s_syntax.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::AnalyzedSyntax;
        else if (s_pipeline.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPipeline;
        else if (s_plan.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPlan; //-V1048
        else if (s_element.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryElement;
        else if (s_plansegment.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::PlanSegment;
        else if (s_opt_plan.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::OptimizerPlan;
        else if (s_distributed.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::Distributed;
        else if (s_analyze.ignore(pos, expected))
        {
            if (s_distributed.ignore(pos, expected))
                kind = ASTExplainQuery::ExplainKind::DistributedAnalyze;
            else if (s_pipeline.ignore(pos, expected))
                kind = ASTExplainQuery::ExplainKind::PipelineAnalyze;
            else
                kind = ASTExplainQuery::ExplainKind::LogicalAnalyze;
        }
        else if (s_trace.ignore(pos, expected))
        {
            if (s_rule.ignore(pos, expected))
                kind = ASTExplainQuery::ExplainKind::TraceOptimizerRule;
            else
                kind = ASTExplainQuery::ExplainKind::TraceOptimizer;
        }
        else if (s_metadata.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::MetaData;
    }
    else
        return false;

    auto explain_query = std::make_shared<ASTExplainQuery>(kind);

    {
        ASTPtr settings;
        ParserSetQuery parser_settings(true);

        auto begin = pos;
        if (parser_settings.parse(pos, settings, expected))
        {
            auto settings_ast = settings->as<ASTSetQuery &>();
            auto * is_json = settings_ast.changes.tryGet("json");
            if (kind == ASTExplainQuery::ExplainKind::MetaData && is_json && is_json->toString() == "1")
            {

                explain_query->format = std::make_shared<ASTIdentifier>("JSON");
                setIdentifierSpecial(explain_query->format);
            }

            auto * ignore_format = settings_ast.changes.tryGet("ignore_format");
            if (ignore_format && ignore_format->toString() == "1")
                explain_query->ignore_format = true;
            explain_query->setSettings(std::move(settings));
        }
        else
            pos = begin;
    }

    ParserCreateTableQuery create_p(dt);
    ParserSelectWithUnionQuery select_p(dt);
    ParserInsertQuery insert_p(end, dt);
    ASTPtr query;
    if (kind == ASTExplainQuery::ExplainKind::ParsedAST)
    {
        ParserQuery p(end, dt);
        if (p.parse(pos, query, expected))
            explain_query->setExplainedQuery(std::move(query));
        else
            return false;
    }
    else if (select_p.parse(pos, query, expected) ||
             create_p.parse(pos, query, expected) ||
             insert_p.parse(pos, query, expected))
        explain_query->setExplainedQuery(std::move(query));
    else
        return false;

    node = std::move(explain_query);
    return true;
}

}
