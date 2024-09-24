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

#include <Interpreters/InterpreterExplainQuery.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/InterpreterDistributedStages.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/predicateExpressionsUtils.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/QueryUseOptimizerChecker.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/printPipeline.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/QueryPlan.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageView.h>
#include <google/protobuf/util/json_util.h>
#include <Common/JSONBuilder.h>
#include "Parsers/ASTExplainQuery.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
    extern const int LOGICAL_ERROR;
}

namespace
{
    struct ExplainAnalyzedSyntaxMatcher
    {
        struct Data : public WithContext
        {
            explicit Data(ContextPtr context_) : WithContext(context_) { }
        };

        static bool needChildVisit(ASTPtr & node, ASTPtr &) { return !node->as<ASTSelectQuery>(); }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                visit(*select, ast, data);
        }

        static void visit(ASTSelectQuery & select, ASTPtr & node, Data & data)
        {
            InterpreterSelectQuery interpreter(
                node, data.getContext(), SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());

            const SelectQueryInfo & query_info = interpreter.getQueryInfo();
            if (query_info.view_query)
            {
                ASTPtr tmp;
                StorageView::replaceWithSubquery(select, query_info.view_query->clone(), tmp);
            }
        }
    };

    using ExplainAnalyzedSyntaxVisitor = InDepthNodeVisitor<ExplainAnalyzedSyntaxMatcher, true>;

}

BlockIO InterpreterExplainQuery::execute()
{
    BlockIO res;

    const auto & ast = query->as<ASTExplainQuery &>();
    if ((ast.getKind() == ASTExplainQuery::DistributedAnalyze || ast.getKind() == ASTExplainQuery::LogicalAnalyze || ast.getKind() == ASTExplainQuery::PipelineAnalyze)
        && QueryUseOptimizerChecker::check(query, getContext(), true))
    {
        return explainAnalyze();
    }
    // Explain in bsp mode makes no sense.
    getContext()->setSetting("bsp_mode", false);

    res.in = executeImpl();
    return res;
}


Block InterpreterExplainQuery::getSampleBlock()
{
    Block block;
    const auto & ast = query->as<ASTExplainQuery &>();
    if (ast.getKind() == ASTExplainQuery::MaterializedView)
    {
        auto view_table_column = ColumnString::create();
        auto hit_view = ColumnUInt8::create();
        auto match_cost = ColumnInt64::create();
        auto read_cost = ColumnUInt64::create();
        auto view_inner_query = ColumnString::create();
        auto miss_match_reason = ColumnString::create();
        block.insert({std::move(view_table_column), std::make_shared<DataTypeString>(), "view_table"});
        block.insert({std::move(hit_view), std::make_shared<DataTypeUInt8>(), "match_result"});
        block.insert({std::move(match_cost), std::make_shared<DataTypeInt64>(), "match_cost"});
        block.insert({std::move(read_cost), std::make_shared<DataTypeUInt64>(), "read_cost"});
        block.insert({std::move(view_inner_query), std::make_shared<DataTypeString>(), "view_query"});
        block.insert({std::move(miss_match_reason), std::make_shared<DataTypeString>(), "miss_reason"});
    }
    else
    {
        ColumnWithTypeAndName col;
        col.name = "explain";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        block.insert(col);
    }
    return block;
}

/// Split str by line feed and write as separate row to ColumnString.
void InterpreterExplainQuery::fillColumn(IColumn & column, const std::string & str)
{
    size_t start = 0;
    size_t end = 0;
    size_t size = str.size();

    while (end < size)
    {
        if (str[end] == '\n')
        {
            column.insertData(str.data() + start, end - start);
            start = end + 1;
        }

        ++end;
    }

    if (start < end)
        column.insertData(str.data() + start, end - start);
}

void InterpreterExplainQuery::rewriteDistributedToLocal(ASTPtr & ast)
{
    if (!ast)
        return;

    if (ASTSelectWithUnionQuery * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union->list_of_selects->children)
            rewriteDistributedToLocal(child);
    }
    else if (ASTTableExpression * table = ast->as<ASTTableExpression>())
    {
        if (table->subquery)
            rewriteDistributedToLocal(table->subquery);
        else if (table->database_and_table_name)
        {
            auto target_table_id = getContext()->resolveStorageID(table->database_and_table_name);
            auto storage = DatabaseCatalog::instance().getTable(target_table_id, getContext());
            auto * distributed = dynamic_cast<StorageDistributed *>(storage.get());

            if (!distributed)
                return;

            String table_alias = table->database_and_table_name->tryGetAlias();
            auto old_table_ast = std::find(table->children.begin(), table->children.end(), table->database_and_table_name);
            table->database_and_table_name
                = std::make_shared<ASTTableIdentifier>(distributed->getRemoteDatabaseName(), distributed->getRemoteTableName());
            *old_table_ast = table->database_and_table_name;

            if (!table_alias.empty())
                table->database_and_table_name->setAlias(table_alias);
        }
    }
    else
    {
        for (auto & child : ast->children)
            rewriteDistributedToLocal(child);
    }
}

BlockInputStreamPtr InterpreterExplainQuery::executeImpl()
{
    const auto & ast = query->as<ASTExplainQuery &>();

    // if settings.enable_optimizer = true && query is supported by optimizer, print plan with optimizer.
    // if query is not supported by optimizer, settings `settings.enable_optimizer` in context will be disabled.
    if (ast.getKind() == ASTExplainQuery::MetaData)
    {
        return explainMetaData();
    }
    else if (getContext()->getSettingsRef().enable_optimizer
        && QueryUseOptimizerChecker::check(query, getContext(), !getContext()->getSettingsRef().enable_optimizer_fallback))
    {
        return explainUsingOptimizer();
    }
    else
    {
        return explain();
    }
}

BlockInputStreamPtr InterpreterExplainQuery::explain()
{
    const auto & ast = query->as<ASTExplainQuery &>();

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    WriteBufferFromOwnString buf;
    bool single_line = false;

    // if settings.enable_optimizer = true && query is supported by optimizer, print plan with optimizer.
    // if query is not supported by optimizer, settings `settings.enable_optimizer` in context will be disabled.
    if (ast.getKind() == ASTExplainQuery::ParsedAST)
    {
        if (ast.getSettings())
            throw Exception("Settings are not supported for EXPLAIN AST query.", ErrorCodes::UNKNOWN_SETTING);

        dumpAST(*ast.getExplainedQuery(), buf);
    }
    else if (ast.getKind() == ASTExplainQuery::AnalyzedSyntax)
    {
        if (ast.getSettings())
            throw Exception("Settings are not supported for EXPLAIN SYNTAX query.", ErrorCodes::UNKNOWN_SETTING);

        ExplainAnalyzedSyntaxVisitor::Data data(getContext());
        ExplainAnalyzedSyntaxVisitor(data).visit(query);

        ast.getExplainedQuery()->format(IAST::FormatSettings(buf, false));
    }
    else if (ast.getKind() == ASTExplainQuery::QueryPlan)
    {
        if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            throw Exception("Only SELECT is supported for EXPLAIN query", ErrorCodes::INCORRECT_QUERY);

        auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
        QueryPlan plan;

        InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), SelectQueryOptions());
        interpreter.buildQueryPlan(plan);

        if (settings.optimize)
            plan.optimize(QueryPlanOptimizationSettings::fromContext(getContext()));

        if (settings.json)
        {
            /// Add extra layers to make plan look more like from postgres.
            auto plan_map = std::make_unique<JSONBuilder::JSONMap>();
            plan_map->add("Plan", plan.explainPlan(settings.query_plan_options));
            auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
            plan_array->add(std::move(plan_map));

            auto format_settings = getFormatSettings(getContext());
            format_settings.json.quote_64bit_integers = false;

            JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
            JSONBuilder::FormatContext format_context{.out = buf};

            plan_array->format(json_format_settings, format_context);

            single_line = true;
        }
        else
            plan.explainPlan(buf, settings.query_plan_options);
    }
    else if (ast.getKind() == ASTExplainQuery::QueryPipeline)
    {
        if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            throw Exception("Only SELECT is supported for EXPLAIN query", ErrorCodes::INCORRECT_QUERY);

        auto settings = checkAndGetSettings<QueryPipelineSettings>(ast.getSettings());
        QueryPlan plan;

        InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), SelectQueryOptions());
        interpreter.buildQueryPlan(plan);
        auto pipeline = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(getContext()), BuildQueryPipelineSettings::fromContext(getContext()));

        if (settings.graph)
        {
            /// Pipe holds QueryPlan, should not go out-of-scope
            auto pipe = QueryPipeline::getPipe(std::move(*pipeline));
            const auto & processors = pipe.getProcessors();

            if (settings.compact)
                printPipelineCompact(processors, buf, settings.query_pipeline_options.header);
            else
                printPipeline(processors, buf);
        }
        else
        {
            plan.explainPipeline(buf, settings.query_pipeline_options);
        }
    }
    else if (ast.getKind() == ASTExplainQuery::ExplainKind::QueryElement)
    {
        ASTPtr explain_query = ast.getExplainedQuery()->clone();
        rewriteDistributedToLocal(explain_query);
        InterpreterSelectWithUnionQuery interpreter(
            explain_query, getContext(), SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());
        const auto & ast_ptr = interpreter.getQuery()->children.at(0)->as<ASTExpressionList &>();
        if (ast_ptr.children.size() != 1)
            throw Exception("Element query not support multiple select query", ErrorCodes::LOGICAL_ERROR);
        const auto & select_query = ast_ptr.children.at(0)->as<ASTSelectQuery &>();
        ASTs where_expressions = {select_query.prewhere(), select_query.where()};
        where_expressions.erase(
            std::remove_if(where_expressions.begin(), where_expressions.end(), [](const auto & q) { return !q; }), where_expressions.end());
        ASTPtr where = composeAnd(where_expressions);

        buf << "{";
        elementDatabaseAndTable(select_query, where, buf);
        elementDimensions(select_query.select(), buf);
        elementMetrics(select_query.select(), buf);
        elementWhere(where, buf);
        elementGroupBy(select_query.groupBy(), buf);
        buf << "}";
    }
    else if (ast.getKind() == ASTExplainQuery::PlanSegment)
    {
        if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            throw Exception("Only SELECT is supported for EXPLAIN query", ErrorCodes::INCORRECT_QUERY);

        auto interpreter = std::make_unique<InterpreterDistributedStages>(ast.getExplainedQuery(), Context::createCopy(getContext()));
        auto * plan_segment_tree = interpreter->getPlanSegmentTree();
        if (plan_segment_tree)
            buf << plan_segment_tree->toString();
    }
    else
        throw Exception(
            "This explain syntax is not supported, you can try to open optimizer(enable_optimizer=1).", ErrorCodes::INCORRECT_QUERY);

    if (single_line)
        res_columns[0]->insertData(buf.str().data(), buf.str().size());
    else
        fillColumn(*res_columns[0], buf.str());

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}


void InterpreterExplainQuery::elementDatabaseAndTable(const ASTSelectQuery & select_query, const ASTPtr & where, WriteBuffer & buffer)
{
    if (!select_query.tables())
        throw Exception("Can not get database and table in element query", ErrorCodes::LOGICAL_ERROR);

    if (select_query.tables()->children.size() != 1)
        throw Exception("Element query not support multiple table query, such as join, etc", ErrorCodes::LOGICAL_ERROR);

    auto database_and_table = getDatabaseAndTable(select_query, 0);
    if (database_and_table)
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable({database_and_table->database, database_and_table->table}, getContext());
        buffer << "\"database\": \"" << storage->getStorageID().getDatabaseName() << "\", ";
        buffer << "\"table\": \"" << storage->getStorageID().getTableName() << "\", ";
        auto dependencies = DatabaseCatalog::instance().getDependencies(storage->getStorageID());
        buffer << "\"dependencies\": [";
        for (size_t i = 0, size = dependencies.size(); i < size; ++i)
            buffer << "\"" << dependencies[i].getDatabaseName() << "." << dependencies[i].getTableName() << "\""
                   << (i + 1 != size ? ", " : "");
        buffer << "], ";

        listPartitionKeys(storage, buffer);
        listRowsOfOnePartition(storage, select_query.groupBy(), where, buffer);
    }
    else
        throw Exception("Can not get database and table in element query", ErrorCodes::LOGICAL_ERROR);
}

void InterpreterExplainQuery::listPartitionKeys(StoragePtr & storage, WriteBuffer & buffer)
{
    buffer << "\"partition_keys\": [";
    if (auto partition_key = storage->getInMemoryMetadataPtr()->getPartitionKey().expression_list_ast)
    {
        const auto & partition_expr_list = partition_key->as<ASTExpressionList &>();
        if (!partition_expr_list.children.empty())
        {
            for (size_t i = 0, size = partition_expr_list.children.size(); i < size; ++i)
            {
                buffer << "\"" << partition_expr_list.children[i]->getColumnName() << "\"" << (i + 1 != size ? ", " : "");
            }
        }
    }
    buffer << "], ";
}

/**
 * Select one partition to estimate view table definition is suitable with ratio of view rows and base table rows.
 */
void InterpreterExplainQuery::listRowsOfOnePartition(
    StoragePtr & storage, const ASTPtr & group_by, const ASTPtr & where, WriteBuffer & buffer)
{
    UInt64 base_rows = 0;
    UInt64 view_rows = 0;
    if (getContext()->getSettingsRef().enable_element_mv_rows)
    {
        auto partition_condition = getActivePartCondition(storage);
        if (partition_condition)
        {
            {
                WriteBufferFromOwnString query_buffer;
                query_buffer << "select count() as count from " << storage->getStorageID().getFullTableName() << " where "
                             << *partition_condition;

                const String query_str = query_buffer.str();
                const char * begin = query_str.data();
                const char * end = query_str.data() + query_str.size();

                ParserQuery parser(end, ParserSettings::valueOf(getContext()->getSettingsRef()));
                auto query_ast = parseQuery(parser, begin, end, "", 0, 0);

                InterpreterSelectWithUnionQuery select(query_ast, getContext(), SelectQueryOptions());
                BlockInputStreamPtr in = select.execute().getInputStream();

                in->readPrefix();
                Block block = in->read();
                in->readSuffix();

                auto & column = block.getByName("count").column;
                if (column->size() == 1)
                    base_rows = column->getUInt(0);
            }

            {
                WriteBufferFromOwnString query_ss;
                query_ss << "select";
                if (group_by)
                {
                    query_ss << " uniq(";
                    auto & expression_list = group_by->as<ASTExpressionList &>();
                    for (size_t i = 0, size = expression_list.children.size(); i < size; ++i)
                    {
                        expression_list.children[i]->format(IAST::FormatSettings(query_ss, true, true));
                        query_ss << (i + 1 != size ? ", " : ")");
                    }
                    query_ss << " as count";
                }
                else
                {
                    query_ss << " count() as count";
                }

                query_ss << " from " << backQuoteIfNeed(storage->getStorageID().getDatabaseName()) << "."
                         << backQuoteIfNeed(storage->getStorageID().getTableName()) << " where " << *partition_condition;

                if (where)
                {
                    query_ss << " and (";
                    where->format(IAST::FormatSettings(query_ss, true));
                    query_ss << ")";
                }

                String query_str = query_ss.str();

                LOG_DEBUG(log, "element view rows query-{}", query_str);
                const char * begin = query_str.data();
                const char * end = query_str.data() + query_str.size();

                ParserQuery parser(end, ParserSettings::valueOf(getContext()->getSettingsRef()));
                auto query_ast = parseQuery(parser, begin, end, "", 0, 0);


                InterpreterSelectWithUnionQuery select(query_ast, getContext(), SelectQueryOptions());
                BlockInputStreamPtr in = select.execute().getInputStream();

                in->readPrefix();
                Block block = in->read();
                in->readSuffix();

                auto & column = block.getByName("count").column;
                if (column->size() == 1)
                {
                    if (isColumnNullable(*column))
                    {
                        const auto * nullable_column = static_cast<const ColumnNullable *>(column.get());
                        view_rows = nullable_column->isNullAt(0) ? 0 : nullable_column->getNestedColumnPtr()->getUInt(0);
                    }
                    else
                    {
                        view_rows = column->getUInt(0);
                    }
                    LOG_DEBUG(log, "view query count column-{}, result-{}", column->dumpStructure(), view_rows);
                }
            }
        }
    }
    buffer << "\"base_rows\": " << base_rows << ", "
           << "\"view_rows\": " << view_rows << ", ";
}


std::optional<String> InterpreterExplainQuery::getActivePartCondition(StoragePtr & storage)
{
    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());
    if (!merge_tree)
        throw Exception("Unknown engine: " + storage->getName(), ErrorCodes::LOGICAL_ERROR);

    auto parts = merge_tree->getDataPartsVector();
    if (!parts.empty())
    {
        return "_partition_id = '" + parts[0]->partition.getID(*merge_tree) + "'";
    }

    return {};
}

void InterpreterExplainQuery::elementWhere(const ASTPtr & where, WriteBuffer & buffer)
{
    buffer << "\"where\": "
           << "\"";
    if (where)
        where->format(IAST::FormatSettings(buffer, true));
    buffer << "\", ";
}

void InterpreterExplainQuery::elementMetrics(const ASTPtr & select, WriteBuffer & buffer)
{
    buffer << "\"metrics\": [";
    std::vector<String> metric_aliases;
    if (select)
    {
        auto & expression_list = select->as<ASTExpressionList &>();
        bool first_metric = true;
        for (auto & child : expression_list.children)
        {
            if (child->as<ASTFunction>())
            {
                GetAggregatesVisitor::Data data = {};
                GetAggregatesVisitor(data).visit(child);
                if (!data.aggregates.empty())
                {
                    if (!first_metric)
                        buffer << ", ";
                    buffer << "\"";
                    child->format(IAST::FormatSettings(buffer, true, true));
                    buffer << "\"";
                    metric_aliases.push_back(child->tryGetAlias());
                    first_metric = false;
                }
            }
        }
    }

    buffer << "], ";
    buffer << "\"metric_aliases\": [";
    for (size_t i = 0, size = metric_aliases.size(); i < size; ++i)
        buffer << "\"" << metric_aliases[i] << "\"" << (i + 1 != size ? ", " : "");
    buffer << "], ";
}

void InterpreterExplainQuery::elementDimensions(const ASTPtr & select, WriteBuffer & buffer)
{
    buffer << "\"dimensions\": [";
    std::vector<String> dimension_aliases;
    if (select)
    {
        auto & expression_list = select->as<ASTExpressionList &>();
        bool first_dimension = true;
        for (auto & child : expression_list.children)
        {
            bool not_aggregate_func = false;
            if (child->as<ASTFunction>())
            {
                GetAggregatesVisitor::Data data = {};
                GetAggregatesVisitor(data).visit(child);
                not_aggregate_func = data.aggregates.empty();
            }
            auto * identifier = child->as<ASTIdentifier>();
            if (identifier || not_aggregate_func)
            {
                if (!first_dimension)
                    buffer << ", ";
                buffer << "\"";
                child->format(IAST::FormatSettings(buffer, true, true));
                buffer << "\"";
                dimension_aliases.push_back(child->tryGetAlias());
                first_dimension = false;
            }
        }
    }

    buffer << "], ";
    buffer << "\"dimension_aliases\": [";
    for (size_t i = 0, size = dimension_aliases.size(); i < size; ++i)
        buffer << "\"" << dimension_aliases[i] << "\"" << (i + 1 != size ? ", " : "");
    buffer << "], ";
}

void InterpreterExplainQuery::elementGroupBy(const ASTPtr & group_by, WriteBuffer & buffer)
{
    buffer << "\"group_by\": [";
    if (group_by)
    {
        auto & expression_list = group_by->as<ASTExpressionList &>();
        bool first_group_expression = true;
        for (auto & child : expression_list.children)
        {
            if (child->as<ASTFunction>() || child->as<ASTIdentifier>())
            {
                if (!first_group_expression)
                    buffer << ", ";
                buffer << "\"";
                child->format(IAST::FormatSettings(buffer, true, true));
                buffer << "\"";
                first_group_expression = false;
            }
        }
    }
    buffer << "]";
}

BlockInputStreamPtr InterpreterExplainQuery::explainUsingOptimizer()
{
    WriteBufferFromOwnString buffer;
    bool single_line = false;
    const auto & explain = query->as<ASTExplainQuery &>();
    auto context = getContext();

    if (explain.getKind() == ASTExplainQuery::AnalyzedSyntax)
    {
        if (explain.getSettings())
            throw Exception("Settings are not supported for EXPLAIN SYNTAX query.", ErrorCodes::UNKNOWN_SETTING);

        auto query_ptr = explain.getExplainedQuery();
        query_ptr = QueryRewriter().rewrite(query_ptr, context);
        query_ptr->format(IAST::FormatSettings(buffer, false));
    }
    else if (explain.getKind() == ASTExplainQuery::TraceOptimizer || explain.getKind() == ASTExplainQuery::TraceOptimizerRule)
    {
        if (explain.getSettings())
            throw Exception("Settings are not supported for EXPLAIN TRACE OPTIMIZER query.", ErrorCodes::UNKNOWN_SETTING);

        context->initOptimizerProfile();
        try
        {
            InterpreterSelectQueryUseOptimizer interpreter(explain.getExplainedQuery(), context, SelectQueryOptions());
            interpreter.getPlanSegment();
        }
        catch (...)
        {
            context->clearOptimizerProfile();
            throw;
        }

        if (explain.getKind() == ASTExplainQuery::TraceOptimizerRule)
            buffer << context->getOptimizerProfile(true);
        else
            buffer << context->getOptimizerProfile();
    }
    else
    {
        InterpreterSelectQueryUseOptimizer interpreter(explain.getExplainedQuery(), context, SelectQueryOptions());
        auto query_plan = interpreter.getQueryPlan();
        if (explain.getKind() == ASTExplainQuery::ExplainKind::OptimizerPlan
            || explain.getKind() == ASTExplainQuery::ExplainKind::QueryPlan)
        {
            explainPlanWithOptimizer(explain, *query_plan, buffer, context, single_line);
        }
        else if (explain.getKind() == ASTExplainQuery::ExplainKind::Distributed)
        {
            explainDistributedWithOptimizer(explain, *query_plan, buffer, context);
        }
        else if (explain.getKind() == ASTExplainQuery::ExplainKind::QueryPipeline)
        {
            explainPipelineWithOptimizer(explain, *query_plan, buffer, context);
        }
    }

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    if (single_line)
        res_columns[0]->insertData(buffer.str().data(), buffer.str().size());
    else
        fillColumn(*res_columns[0], buffer.str());

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

BlockIO InterpreterExplainQuery::explainAnalyze()
{
    BlockIO res;

    auto context_ptr = getContext();
    context_ptr->setSetting("report_segment_profiles", true);
    context_ptr->setSetting("log_explain_analyze_type", Field("NONE"));
    context_ptr->setIsExplainQuery(true);
    try
    {
        auto interpreter = std::make_unique<InterpreterSelectQueryUseOptimizer>(query, context_ptr, options);
        res = interpreter->execute();
    }
    catch (...)
    {
        throw;
    }

    return res;
}

void InterpreterExplainQuery::explainPlanWithOptimizer(
    const ASTExplainQuery & explain_ast, QueryPlan & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr, bool & /*single_line*/)
{
    auto settings = checkAndGetSettings<QueryPlanSettings>(explain_ast.getSettings());
    CardinalityEstimator::estimate(plan, context_ptr);
    PlanCostMap costs = CostCalculator::calculate(plan, *context_ptr);
    if (settings.json)
    {
        auto plan_cost = CostCalculator::calculatePlanCost(plan, *context_ptr);
        buffer << PlanPrinter::jsonLogicalPlan(plan, plan_cost, CostModel(*context_ptr), {}, costs, settings);
    }
    else if (settings.pb_json)
    {
        Protos::QueryPlan plan_pb;
        plan.toProto(plan_pb);
        String json_msg;
        google::protobuf::util::JsonPrintOptions pb_options;
        pb_options.preserve_proto_field_names = true;
        pb_options.always_print_primitive_fields = true;
        pb_options.add_whitespace = settings.add_whitespace;

        google::protobuf::util::MessageToJsonString(plan_pb, &json_msg, pb_options);
        buffer << json_msg;
    }
    else
        buffer << PlanPrinter::textLogicalPlan(plan, context_ptr, costs, {}, settings);
}

void InterpreterExplainQuery::explainDistributedWithOptimizer(const ASTExplainQuery & explain_ast, QueryPlan & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr)
{
    auto settings = checkAndGetSettings<QueryPlanSettings>(explain_ast.getSettings());
    QueryPlan query_plan = PlanNodeToNodeVisitor::convert(plan);
    PlanSegmentTreePtr plan_segment_tree = std::make_unique<PlanSegmentTree>();
    // select health worker before split
    if (context_ptr->getSettingsRef().scheduler_mode != SchedulerMode::SKIP && context_ptr->tryGetCurrentWorkerGroup())
    {
        context_ptr->adaptiveSelectWorkers(context_ptr->getSettingsRef().scheduler_mode);
        auto wg_status = context_ptr->getWorkerGroupStatusPtr();
        if (wg_status && wg_status->getWorkerGroupHealth() == GroupHealthType::Critical)
            throw Exception("No worker available", ErrorCodes::LOGICAL_ERROR);
    }

    ClusterInfoContext cluster_info_context{.query_plan = query_plan, .context = context_ptr, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(plan, cluster_info_context);

    PlanSegmentSplitter::split(query_plan, plan_segment_context);
    GraphvizPrinter::printPlanSegment(plan_segment_tree, context_ptr);

    PlanCostMap costs = CostCalculator::calculate(plan, *context_ptr);

    PlanSegmentDescriptions plan_segment_descriptions;
    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
        plan_segment_descriptions.emplace_back(PlanSegmentDescription::getPlanSegmentDescription(node.plan_segment, settings.json));

    if (settings.json)
        buffer << PlanPrinter::jsonDistributedPlan(plan_segment_descriptions, {});
    else
        buffer << PlanPrinter::textDistributedPlan(plan_segment_descriptions, context_ptr, costs, {}, plan, settings);
}

BlockInputStreamPtr InterpreterExplainQuery::explainMetaData()
{
    const auto & explain = query->as<ASTExplainQuery &>();
    auto context = Context::createCopy(getContext());
    auto query_ptr = explain.getExplainedQuery();
    auto contxt = getContext();
    auto metadata_settings = checkAndGetSettings<QueryMetadataSettings>(explain.getSettings());
    AnalysisPtr analysis;
    QueryPlanPtr query_plan;

    if (metadata_settings.lineage || metadata_settings.lineage_use_optimizer)
    {
        try
        {
            InterpreterSelectQueryUseOptimizer interpreter(query_ptr, contxt, SelectQueryOptions());
            interpreter.buildQueryPlan(query_plan, analysis, !metadata_settings.lineage_use_optimizer);
            query_ptr = interpreter.getQuery();
        }
        catch (...)
        {
            tryLogWarningCurrentException(getLogger("InterpreterExplainQuery::explainMetaData"), "build plan failed.");
        }
    }

    if (!analysis)
    {
        query_ptr = QueryRewriter().rewrite(query_ptr, contxt);
        analysis = QueryAnalyzer::analyze(query_ptr, contxt);
    }

    if (metadata_settings.format_json)
    {
        String res = PlanPrinter::jsonMetaData(query_ptr, analysis, contxt, query_plan, metadata_settings);
        Block sample_block = getSampleBlock();
        MutableColumns res_columns = sample_block.cloneEmptyColumns();
        fillColumn(*res_columns[0], res);
        return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
    }


    // get used tables, databases, columns_list
    auto column_tables = ColumnArray::create(ColumnString::create());
    auto column_databases = ColumnArray::create(ColumnString::create());
    auto column_columns_list = ColumnArray::create(ColumnString::create());

    auto column_lists_off = ColumnUInt64::create();
    auto & columns_list_offset_data = column_lists_off->getData();

    Array tables_array;
    Array databases_array;
    size_t array_off_size = 0;

    const auto & used_columns_map = analysis->getUsedColumns();
    for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
    {
        Array columns_array;
        tables_array.push_back(storage_analysis.table);
        databases_array.push_back(storage_analysis.database);
        array_off_size++;
        if (auto it = used_columns_map.find(storage_analysis.storage->getStorageID()); it != used_columns_map.end())
        {
            for (const auto & column : it->second)
                columns_array.push_back(column);
        }
        column_columns_list->insert(columns_array);
    }
    columns_list_offset_data.push_back(array_off_size);

    //get used functions
    auto column_functions = ColumnArray::create(ColumnString::create());
    Array functions_array;
    for (const auto & func_name : analysis->getUsedFunctions())
        functions_array.push_back(func_name);

    // get settings
    SettingsChanges settings_changes = InterpreterSetQuery::extractSettingsFromQuery(query, contxt);

    auto key_column = ColumnString::create();
    auto value_column = ColumnString::create();
    auto settings_offset_column = ColumnVector<UInt64>::create();
    size_t offest_size = 0;
    if (!settings_changes.empty())
    {
        for (const auto & setting : settings_changes)
        {
            offest_size++;
            key_column->insert(setting.name);
            value_column->insert(setting.value.toString());
        }
    }
    settings_offset_column->insert(offest_size);

    auto insert_list = ColumnArray::create(ColumnString::create());
    auto insert_offset = ColumnUInt64::create();
    auto & insert_offset_data = insert_offset->getData();
    size_t insert_offest_size = 0;
    if (analysis->getInsert())
    {
        insert_offest_size += 3;
        auto & insert_info = analysis->getInsert().value();
        Array database_array;
        database_array.push_back(insert_info.storage_id.getDatabaseName());
        insert_list->insert(database_array);
        Array table_array;
        table_array.push_back(insert_info.storage_id.getTableName());
        insert_list->insert(table_array);
        Array columns;
        for (auto & column_info : insert_info.columns)
            columns.push_back(column_info.name);
        insert_list->insert(columns);
    }
    insert_offset_data.push_back(insert_offest_size);

    auto func_column = ColumnString::create();
    auto args_column = ColumnArray::create(ColumnString::create());
    auto func_args_offset_column = ColumnVector<UInt64>::create();

    auto function_arguments = analysis->function_arguments;
    size_t func_args_offest_size = 0;
    if (!function_arguments.empty())
    {
        for (const auto & func_args : function_arguments)
        {
            Array arg_array;
            func_args_offest_size++;
            func_column->insert(func_args.first);
            for (const auto & arg : func_args.second)
                arg_array.push_back(arg);
            args_column->insert(arg_array);
        }
    }
    func_args_offset_column->insert(func_args_offest_size);

    column_tables->insert(tables_array);
    column_databases->insert(databases_array);
    column_functions->insert(functions_array);
    auto col_arr_arr = ColumnArray::create(std::move(column_columns_list), std::move(column_lists_off));
    ColumnPtr settings_column = ColumnMap::create(ColumnArray::create(
            ColumnTuple::create(Columns{std::move(key_column), std::move(value_column)}),
            std::move(settings_offset_column)));
    ColumnPtr func_args_column = ColumnMap::create(ColumnArray::create(
            ColumnTuple::create(Columns{std::move(func_column), std::move(args_column)}),
            std::move(func_args_offset_column)));
    auto insert_info_arr = ColumnArray::create(std::move(insert_list), std::move(insert_offset));

    Block block;
    block.insert({std::move(column_tables), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "tables"});
    block.insert({std::move(column_databases), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "databases"});
    block.insert(
        {std::move(col_arr_arr),
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())),
         "columns_lists"});
    block.insert({std::move(column_functions), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "used_function_names"});
    block.insert({std::move(settings_column), std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "settings"});
    block.insert({std::move(insert_info_arr), std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())), "insert_info"});
    block.insert({std::move(func_args_column), std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())), "function_const_arguments"});

    return std::make_shared<OneBlockInputStream>(block);
}

void InterpreterExplainQuery::explainPipelineWithOptimizer(const ASTExplainQuery & explain_ast, QueryPlan & plan, WriteBuffer & buffer, ContextMutablePtr & context_ptr)
{
    auto settings = checkAndGetSettings<QueryPipelineSettings>(explain_ast.getSettings());
    QueryPlan query_plan = PlanNodeToNodeVisitor::convert(plan);
    PlanSegmentTreePtr plan_segment_tree = std::make_unique<PlanSegmentTree>();

    ClusterInfoContext cluster_info_context{.query_plan = query_plan, .context = context_ptr, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(plan, cluster_info_context);
    // query_plan.allocateLocalTable(context_ptr);
    PlanSegmentSplitter::split(query_plan, plan_segment_context);
    auto & plan_segments = plan_segment_tree->getNodes();
    GraphvizPrinter::printPlanSegment(plan_segment_tree, context_ptr);

//    PlanSegmentsStatusPtr scheduler_status;
//    if (plan_segment_tree->getNodes().size() > 1)
//    {
//        RuntimeFilterManager::getInstance().registerQuery(context_ptr->getCurrentQueryId(), *plan_segment_tree);
//        scheduler_status = context_ptr->getSegmentScheduler()->insertPlanSegments(context_ptr->getCurrentQueryId(), plan_segment_tree.get(), context_ptr);
//    }
//    else
//    {
//        scheduler_status = context_ptr->getSegmentScheduler()->insertPlanSegments(context_ptr->getCurrentQueryId(), plan_segment_tree.get(), context_ptr);
//    }
//    if (!scheduler_status)
//    {
//        RuntimeFilterManager::getInstance().removeQuery(context_ptr->getCurrentQueryId());
//        throw Exception("Cannot get scheduler status from segment scheduler", ErrorCodes::LOGICAL_ERROR);
//    }

    for (auto it = plan_segments.begin(); it != plan_segments.end(); ++it)
    {
        auto * segment = it->getPlanSegment();
        segment->update(context_ptr);
        buffer << "\nSegment[ " << std::to_string(segment->getPlanSegmentId()) <<" ] :\n" ;
        auto & segment_plan = segment->getQueryPlan();
        auto pipeline = segment_plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context_ptr),
            BuildQueryPipelineSettings::fromPlanSegment(
                segment, {.execution_address = segment->getCoordinatorAddress()}, context_ptr, true));
        if (pipeline)
        {
            if (settings.graph)
            {
                /// Pipe holds QueryPlan, should not go out-of-scope
                auto pipe = QueryPipeline::getPipe(std::move(*pipeline));
                const auto & processors = pipe.getProcessors();
                if (settings.compact)
                    printPipelineCompact(processors, buffer, settings.query_pipeline_options.header);
                else
                    printPipeline(processors, buffer);
            }
            else
            {
                segment_plan.explainPipeline(buffer, settings.query_pipeline_options);
            }
            buffer << "\n------------------------------------------\n";
        }
    }
}

void ExplainConsumer::consume(ProcessorProfileLogElement & element)
{
    store_vector.emplace_back(element);
}

}
