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

#include <QueryPlan/QueryPlanner.h>

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <Analyzers/ExpressionVisitor.h>
#include <Analyzers/analyze_common.h>
#include <Columns/Collator.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/WindowDescription.h>
#include <Interpreters/getTableExpressions.h>
#include <Optimizer/ExpressionExtractor.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rewriter/ColumnPruning.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Optimizer/makeCastFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ApplyStep.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/EnforceSingleRowStep.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/ExtremesStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/IntersectStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/LimitByStep.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/OutfileFinishStep.h>
#include <QueryPlan/OutfileWriteStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <QueryPlan/PlanBuilder.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SortingStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/UnionStep.h>
#include <QueryPlan/WindowStep.h>
#include <QueryPlan/planning_common.h>
#include <Common/FieldVisitors.h>

#include <algorithm>
#include <memory>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int PLAN_BUILD_ERROR;
    extern const int EXPECTED_ALL_OR_ANY;
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
}

#define PRINT_PLAN(plan, NAME) \
    do \
    { \
        if (context->getSettingsRef().print_graphviz_planner) \
            GraphvizPrinter::printLogicalPlan(*(plan), context, std::to_string(context->getAndIncStepId()) + "_" + #NAME); \
    } while (false)

class QueryPlannerVisitor : public ASTVisitor<RelationPlan, const Void>
{
public:
    QueryPlannerVisitor(ContextMutablePtr context_, CTERelationPlans & cte_plans_, Analysis & analysis_, TranslationMapPtr outer_context_)
        : ASTVisitor(context_->getSettingsRef().max_ast_depth)
        , context(std::move(context_))
        , cte_plans(cte_plans_)
        , analysis(analysis_)
        , outer_context(std::move(outer_context_))
        , use_ansi_semantic(context->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
        , enable_implicit_type_conversion(context->getSettingsRef().enable_implicit_type_conversion)
        , enable_subcolumn_optimization_through_union(context->getSettingsRef().enable_subcolumn_optimization_through_union)
    {
    }

    RelationPlan visitASTInsertQuery(ASTPtr & node, const Void &) override;
    RelationPlan visitASTSelectIntersectExceptQuery(ASTPtr & node, const Void &) override;
    RelationPlan visitASTSelectWithUnionQuery(ASTPtr & node, const Void &) override;
    RelationPlan visitASTSelectQuery(ASTPtr & node, const Void &) override;
    RelationPlan visitASTSubquery(ASTPtr & node, const Void &) override;
    RelationPlan visitASTExplainQuery(ASTPtr & node, const Void &) override;
    RelationPlan visitASTCreatePreparedStatementQuery(ASTPtr & node, const Void &) override
    {
        auto & prepare = node->as<ASTCreatePreparedStatementQuery &>();
        auto query = prepare.getQuery();
        return process(query);
    }

    RelationPlan process(ASTPtr & node) { return ASTVisitorUtil::accept(node, *this, {}); }

private:
    ContextMutablePtr context;
    CTERelationPlans & cte_plans;
    Analysis & analysis;
    TranslationMapPtr outer_context;
    const bool use_ansi_semantic;
    const bool enable_implicit_type_conversion;
    const bool enable_subcolumn_optimization_through_union;

    /// plan FROM
    PlanBuilder planFrom(ASTSelectQuery &);
    PlanBuilder planWithoutTables(ASTSelectQuery & select_query);
    PlanBuilder planTables(ASTTablesInSelectQuery & tables_in_select, ASTSelectQuery & select_query);
    PlanBuilder planTableExpression(ASTTableExpression & table_expression, ASTSelectQuery & select_query);
    PlanBuilder planTable(ASTTableIdentifier & db_and_table, ASTSelectQuery & select_query, SqlHints & hints);
    PlanBuilder planTableFunction(ASTFunction & table_function, ASTSelectQuery & select_query);
    PlanBuilder planTableSubquery(ASTSubquery & subquery, ASTPtr & node, SqlHints & hints);

    /// plan join
    /// 1. join node will be planned in the left builder, so no return value is needed
    /// 2. join prepare methods are used to retrieve join keys, and do necessary projection for join keys
    void planJoin(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);
    void planCrossJoin(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);
    void planJoinUsing(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);
    void planJoinOn(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);
    std::pair<Names, Names> prepareJoinUsingKeys(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);
    std::pair<Names, Names> prepareJoinOnKeys(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);
    static DataStream getJoinOutputStream(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder);

    RelationPlan planReadFromStorage(IAST & table_ast, ScopePtr table_scope, ASTSelectQuery & origin_query, SqlHints & hints, bool is_table_function = false);
    // static MergeTreeReadPlannerPtr getReadPlanner(ASTSelectQuery & select_query);
    // static MergeTreeBitMapSchedulerPtr getBitMapScheduler(ASTSelectQuery & select_query, const StoragePtr & storage, size_t max_streams);

    void planArrayJoin(ASTArrayJoin & array_join, PlanBuilder & builder, ASTSelectQuery & select_query);
    void planFilter(PlanBuilder & builder, ASTSelectQuery & select_query, const ASTPtr & filter);
    void planAggregate(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planWindow(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planSelect(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planDistinct(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planOrderBy(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planWithFill(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planLimitBy(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planTotalsAndHaving(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planLimitAndOffset(PlanBuilder & builder, ASTSelectQuery & select_query);
    void planSampling(PlanBuilder & builder, ASTSelectQuery & select_query);

    RelationPlan planFinalSelect(PlanBuilder & builder, ASTSelectQuery & select_query);

    // the routine to plan expressions in most scenarios, which handle non-deterministic function & subqueries within expressions
    template <typename T>
    void planExpression(PlanBuilder & builder, ASTSelectQuery & select_query, const T & expressions);
    template <typename T>
    void planNonDeterministicFunction(PlanBuilder & builder, ASTSelectQuery & select_query, const T & expressions);

    // plan subquery expressions, only subquery expressions under parent_expression will be planned
    // return apply nodes in order to setOuterColumns
    template <typename T>
    PlanNodes planSubqueryExpression(PlanBuilder & builder, ASTSelectQuery & select_query, const T & expressions);
    void planScalarSubquery(PlanBuilder & builder, const ASTPtr & scalar_subquery);
    void planInSubquery(PlanBuilder & builder, const ASTPtr & node, ASTSelectQuery & select_query);
    void planExistsSubquery(PlanBuilder & builder, const ASTPtr & node);
    void planQuantifiedComparisonSubquery(PlanBuilder & builder, const ASTPtr & node, ASTSelectQuery & select_query);
    RelationPlan combineSubqueryOutputsToTuple(const RelationPlan & plan, const ASTPtr & subquery);

    /// plan UNION/INTERSECT/EXCEPT
    using FieldSubColumnID = std::pair<size_t, SubColumnID>;
    using FieldSubColumnIDs = std::vector<FieldSubColumnID>;
    RelationPlan projectFieldSymbols(const RelationPlan & plan, const FieldSubColumnIDs & sub_column_positions);
    RelationPlan planSetOperation(ASTs & selects, ASTSelectWithUnionQuery::Mode union_mode);

    /// type coercion
    // coerce types for a set of symbols
    PlanWithSymbolMappings coerceTypesForSymbols(const PlanNodePtr & node, const NameToType & symbol_and_types, bool replace_symbol);
    NameToNameMap coerceTypesForSymbols(PlanBuilder & builder, const NameToType & symbol_and_types, bool replace_symbol);
    // coerce types for the first output column of a subquery plan
    void coerceTypeForSubquery(RelationPlan & plan, const DataTypePtr & type);

    /// utils
    SizeLimits extractDistinctSizeLimits();
    std::pair<UInt64, UInt64> getLimitLengthAndOffset(ASTSelectQuery & query);
    PlanBuilder toPlanBuilder(const RelationPlan & plan, ScopePtr scope);

    void processSubqueryArgs(
        PlanBuilder & builder,
        ASTs & children,
        String & rhs_symbol,
        String & lhs_symbol,
        RelationPlan & rhs_plan,
        ASTSelectQuery & select_query);
    void addPlanHint(const QueryPlanStepPtr & step, SqlHints & sql_hints, bool check_step_type = false);
    bool needAggregateOverflowRow(ASTSelectQuery & select_query) const;
};

namespace
{
    PlanNodePtr planOutfile(PlanNodePtr output_root, Analysis & analysis, ContextMutablePtr context)
    {
        auto & outfile_info = analysis.getOutfileInfo();
        if (context->getSettingsRef().enable_distributed_output && outfile_info)
        {
            OutfileTargetPtr outfile_target = std::make_shared<OutfileTarget>(
                context, outfile_info->out_file, outfile_info->format, outfile_info->compression_method, outfile_info->compression_level);
            auto outfile_step = std::make_shared<OutfileWriteStep>(output_root->getCurrentDataStream(), outfile_target);
            auto outfile_root = output_root->addStep(context->nextNodeId(), std::move(outfile_step));

            auto outfile_finish_step = std::make_shared<OutfileFinishStep>(output_root->getCurrentDataStream());
            auto outfile_finish_root = outfile_root->addStep(context->nextNodeId(), std::move(outfile_finish_step));

            return outfile_finish_root;
        }
        return output_root;
    }

    PlanNodePtr planOutput(const RelationPlan & plan, ASTPtr & query, Analysis & analysis, ContextMutablePtr context)
    {
        const auto & output_desc = analysis.getOutputDescription(*query);
        const auto & field_symbol_infos = plan.getFieldSymbolInfos();
        const auto old_root = plan.getRoot();

        Assignments assignments;
        NameToType input_types = old_root->getOutputNamesToTypes();
        NameToType output_types;
        NameSet output_names;
        std::unordered_map<String, UInt64> output_name_counter;

        auto get_uniq_output_name = [&](const auto & output_name) {
            String uniq_name = output_name;
            while (true)
            {
                auto cur_id = output_name_counter[output_name]++;
                // when current_id == 0, hide it
                uniq_name = cur_id == 0 ? output_name : fmt::format("{}_{}", output_name, cur_id);
                // it is possible to have conflicts so do check
                // merely redo is ok to avoid O(N^2) total complexity
                // since conflicting names won't be retried with the same cur_id
                // with the help of output_name_counter
                if (!output_names.count(uniq_name))
                {
                    output_names.insert(uniq_name);
                    return uniq_name;
                }
            }
        };

        assert(output_desc.size() == field_symbol_infos.size());

        for (size_t i = 0; i < output_desc.size(); ++i)
        {
            String input_column = field_symbol_infos[i].getPrimarySymbol();
            String output_name = get_uniq_output_name(output_desc[i].name);
            assignments.emplace_back(output_name, toSymbolRef(input_column));
            output_types[output_name] = input_types[input_column];
        }

        auto output_step = std::make_shared<ProjectionStep>(old_root->getCurrentDataStream(), assignments, output_types, true);
        auto output_root = old_root->addStep(context->nextNodeId(), std::move(output_step));

        auto new_root = planOutfile(output_root, analysis, context);
        PRINT_PLAN(new_root, plan_output);
        return new_root;
    }

    void planExtremes(RelationPlan & plan, ContextMutablePtr context)
    {
        if (context->getSettingsRef().extremes)
        {
            auto extremes_step = std::make_shared<ExtremesStep>(plan.getRoot()->getCurrentDataStream());
            auto extremes = plan.getRoot()->addStep(context->nextNodeId(), std::move(extremes_step));
            plan.withNewRoot(extremes);
        }
    }
}

QueryPlanPtr QueryPlanner::plan(ASTPtr & query, Analysis & analysis, ContextMutablePtr context)
{
    context->setStepId(GraphvizPrinter::PRINT_PLAN_BUILD_INDEX);

    CTERelationPlans cte_plans;
    RelationPlan relation_plan = planQuery(query, nullptr, analysis, context, cte_plans);
    planExtremes(relation_plan, context);
    PlanNodePtr plan_root = planOutput(relation_plan, query, analysis, context);
    CTEInfo cte_info;
    for (const auto & cte_plan : cte_plans)
        cte_info.add(cte_plan.first, cte_plan.second.getRoot());
    return std::make_unique<QueryPlan>(plan_root, cte_info, context->getPlanNodeIdAllocator());
}

RelationPlan QueryPlanner::planQuery(
    ASTPtr query, TranslationMapPtr outer_query_context, Analysis & analysis, ContextMutablePtr context, CTERelationPlans & cte_plans)
{
    QueryPlannerVisitor visitor{context, cte_plans, analysis, outer_query_context};
    return visitor.process(query);
}

RelationPlan QueryPlannerVisitor::visitASTInsertQuery(ASTPtr & node, const Void &)
{
    auto & insert_query = node->as<ASTInsertQuery &>();

    auto & insert = *analysis.getInsert();
    auto select_plan = process(insert_query.select);
    select_plan.withNewRoot(planOutput(select_plan, insert_query.select, analysis, context));
    auto insert_select_with_profiles = context->getSettingsRef().insert_select_with_profiles;

    auto target = std::make_shared<TableWriteStep::InsertTarget>(insert.storage, insert.storage_id, insert.columns, node);

    auto insert_node = select_plan.getRoot()->addStep(
        context->nextNodeId(),
        std::make_shared<TableWriteStep>(select_plan.getRoot()->getCurrentDataStream(), target, insert_select_with_profiles),
        {select_plan.getRoot()});

    auto total_affected_row_count_symbol = context->getSymbolAllocator()->newSymbol("rows");
    // plan = PlanNodeBase::createPlanNode(
    //     context.nextNodeId(),
    //     std::make_shared<TableFinishStep>(plan->getCurrentDataStream(), target, total_affected_row_count_symbol),
    //     {plan});

    auto return_node = PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_shared<TableFinishStep>(insert_node->getCurrentDataStream(), target, total_affected_row_count_symbol, node, insert_select_with_profiles),
        {insert_node});

    PRINT_PLAN(return_node, plan_insert);
    return {return_node, {}};
}


RelationPlan QueryPlannerVisitor::visitASTSelectIntersectExceptQuery(ASTPtr & node, const Void &)
{
    auto & intersect_or_except = node->as<ASTSelectIntersectExceptQuery &>();
    auto selects = intersect_or_except.getListOfSelects();
    auto operator_to_union_mode = [](ASTSelectIntersectExceptQuery::Operator op) -> ASTSelectWithUnionQuery::Mode {
        switch (op)
        {
            case ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL:
                return ASTSelectWithUnionQuery::Mode::INTERSECT_ALL;
            case ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT:
                return ASTSelectWithUnionQuery::Mode::INTERSECT_DISTINCT;
            case ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL:
                return ASTSelectWithUnionQuery::Mode::EXCEPT_ALL;
            case ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT:
                return ASTSelectWithUnionQuery::Mode::EXCEPT_DISTINCT;
            default:
                throw Exception("Unrecognized set operator found", ErrorCodes::LOGICAL_ERROR);
        }
    };

    return planSetOperation(selects, operator_to_union_mode(intersect_or_except.final_operator));
}

RelationPlan QueryPlannerVisitor::visitASTSelectWithUnionQuery(ASTPtr & node, const Void &)
{
    auto & select_with_union = node->as<ASTSelectWithUnionQuery &>();
    return planSetOperation(select_with_union.list_of_selects->children, select_with_union.union_mode);
}

RelationPlan QueryPlannerVisitor::visitASTSelectQuery(ASTPtr & node, const Void &)
{
    auto & select_query = node->as<ASTSelectQuery &>();

    PlanBuilder builder = planFrom(select_query);
    PRINT_PLAN(builder.plan, plan_from);

    planFilter(builder, select_query, select_query.where());
    PRINT_PLAN(builder.plan, plan_where);

    planAggregate(builder, select_query);

    if (select_query.group_by_with_totals)
        planTotalsAndHaving(builder, select_query);
    else
        planFilter(builder, select_query, select_query.having());
    PRINT_PLAN(builder.plan, plan_having);

    planWindow(builder, select_query);

    planSelect(builder, select_query);
    PRINT_PLAN(builder.plan, plan_select);

    planDistinct(builder, select_query);

    planOrderBy(builder, select_query);

    planLimitBy(builder, select_query);

    planWithFill(builder, select_query);

    planLimitAndOffset(builder, select_query);

    planSampling(builder, select_query);

    return planFinalSelect(builder, select_query);
}

RelationPlan QueryPlannerVisitor::visitASTSubquery(ASTPtr & node, const Void &)
{
    auto & subquery = node->as<ASTSubquery &>();
    if (auto cte_analysis = analysis.tryGetCTEAnalysis(subquery))
    {
        if (cte_analysis->isSharable() || context->getSettingsRef().cte_mode == CTEMode::ENFORCED)
        {
            auto cte_id = cte_analysis->id;
            RelationPlan cte_ref;
            if (!cte_plans.contains(cte_id))
            {
                assert(&subquery == cte_analysis->representative);
                cte_ref = process(node->children.front());
                cte_plans.emplace(cte_id, cte_ref);
            }
            else
                cte_ref = cte_plans.at(cte_id);

            std::unordered_map<String, String> output_columns;
            NamesAndTypes mapped_name_and_types;
            NameToNameMap old_name_to_new_name;
            for (const auto & name_and_type : cte_ref.getRoot()->getCurrentDataStream().header.getNamesAndTypes())
            {
                auto new_name = context->getSymbolAllocator()->newSymbol(name_and_type.name);
                output_columns.emplace(new_name, name_and_type.name);
                mapped_name_and_types.emplace_back(new_name, name_and_type.type);
                old_name_to_new_name.emplace(name_and_type.name, new_name);
            }

            PlanNodePtr plan = PlanNodeBase::createPlanNode(
                context->nextNodeId(), std::make_shared<CTERefStep>(DataStream{mapped_name_and_types}, cte_id, output_columns, false));
            PRINT_PLAN(plan, plan_cte);

            FieldSymbolInfos mapped_field_symbol_infos = cte_ref.getFieldSymbolInfos();
            mapFieldSymbolInfos(mapped_field_symbol_infos, old_name_to_new_name, true);

            return RelationPlan{plan, mapped_field_symbol_infos};
        }
    }

    return process(node->children.front());
}

RelationPlan QueryPlannerVisitor::visitASTExplainQuery(ASTPtr & node, const Void &)
{
    auto & query = node->as<ASTExplainQuery &>();
    auto plan = process(query.getExplainedQuery());
    auto settings = checkAndGetSettings<QueryPlanSettings>(query.getSettings());
    auto output = context->getSymbolAllocator()->newSymbol("Explain Analyze");
    auto analyze_node = PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_shared<ExplainAnalyzeStep>(plan.getRoot()->getCurrentDataStream(), output, query.getKind(), context, nullptr, settings),
        {plan.getRoot()});

    return {analyze_node, {{output}}};
}

PlanBuilder QueryPlannerVisitor::planWithoutTables(ASTSelectQuery & select_query)
{
    PlanNodePtr node;
    auto symbol = context->getSymbolAllocator()->newSymbol("dummy");

    {
        NamesAndTypes header;
        Fields data;

        header.emplace_back(symbol, std::make_shared<DataTypeUInt8>());
        data.emplace_back(0U);

        auto values_step = std::make_shared<ValuesStep>(header, data);
        node = PlanNodeBase::createPlanNode(context->nextNodeId(), values_step);
    }

    RelationPlan plan{node, FieldSymbolInfos{{symbol}}};
    return toPlanBuilder(plan, analysis.getQueryWithoutFromScope(select_query));
}

PlanBuilder QueryPlannerVisitor::planTables(ASTTablesInSelectQuery & tables_in_select, ASTSelectQuery & select_query)
{
    auto & first_table_elem = tables_in_select.children[0]->as<ASTTablesInSelectQueryElement &>();
    auto builder = planTableExpression(first_table_elem.table_expression->as<ASTTableExpression &>(), select_query);

    for (size_t idx = 1; idx < tables_in_select.children.size(); ++idx)
    {
        auto & table_element = tables_in_select.children[idx]->as<ASTTablesInSelectQueryElement &>();

        if (table_element.table_expression)
        {
            auto * table_expression = table_element.table_expression->as<ASTTableExpression>();
            if (!table_expression)
                throw Exception("Invalid Table Expression", ErrorCodes::LOGICAL_ERROR);
            auto right_builder = planTableExpression(*table_expression, select_query);
            planJoin(table_element.table_join->as<ASTTableJoin &>(), builder, right_builder);
        }
        else if (table_element.array_join)
        {
            planArrayJoin(table_element.array_join->as<ASTArrayJoin &>(), builder, select_query);
        }
    }

    addPlanHint(builder.plan->getStep(), tables_in_select.hints);
    return builder;
}

PlanBuilder QueryPlannerVisitor::planTableExpression(ASTTableExpression & table_expression, ASTSelectQuery & select_query)
{
    if (table_expression.database_and_table_name)
        return planTable(table_expression.database_and_table_name->as<ASTTableIdentifier &>(), select_query, table_expression.hints);
    if (table_expression.subquery)
        return planTableSubquery(table_expression.subquery->as<ASTSubquery &>(), table_expression.subquery, table_expression.hints);
    if (table_expression.table_function)
        return planTableFunction(table_expression.table_function->as<ASTFunction &>(), select_query);

    __builtin_unreachable();
}

PlanBuilder QueryPlannerVisitor::planTable(ASTTableIdentifier & db_and_table, ASTSelectQuery & select_query, SqlHints & hints)
{
    // Reading a table consists of 3 steps:
    //  1. Read ordinary columns from storage
    //  2. Calculate alias columns
    //  3. Add mask for sensitive columns(FGAC)

    // read ordinary columns
    const auto * storage_scope = analysis.getTableStorageScope(db_and_table);
    auto relation_plan = planReadFromStorage(db_and_table, storage_scope, select_query, hints);
    auto builder = toPlanBuilder(relation_plan, storage_scope);
    PRINT_PLAN(builder.plan, plan_table);

    // append alias columns
    FieldSymbolInfos field_symbol_infos = builder.getFieldSymbolInfos();
    if (auto & alias_columns = analysis.getTableAliasColumns(db_and_table); !alias_columns.empty())
    {
        Assignments assignments;
        NameToType types;
        putIdentities(builder.getOutputNamesAndTypes(), assignments, types);

        for (auto & alias_column : alias_columns)
        {
            auto alias_symbol = context->getSymbolAllocator()->newSymbol(alias_column->tryGetAlias());
            assignments.emplace_back(alias_symbol, builder.translate(alias_column));
            types[alias_symbol] = analysis.getExpressionType(alias_column);
            field_symbol_infos.emplace_back(alias_symbol);
        }

        auto project_alias_columns = std::make_shared<ProjectionStep>(builder.getCurrentDataStream(), assignments, types);
        builder.addStep(std::move(project_alias_columns));
        builder.withScope(analysis.getScope(db_and_table), field_symbol_infos);
        PRINT_PLAN(builder.plan, plan_add_alias);
    }

    builder.withScope(analysis.getScope(db_and_table), std::move(field_symbol_infos));
    return builder;
}

PlanBuilder QueryPlannerVisitor::planTableFunction(ASTFunction & table_function, ASTSelectQuery & select_query)
{
    const auto * scope = analysis.getScope(table_function);
    auto relation_plan = planReadFromStorage(table_function, scope, select_query, table_function.hints, true);
    auto builder = toPlanBuilder(relation_plan, scope);
    PRINT_PLAN(builder.plan, plan_table_function);
    return builder;
}

PlanBuilder QueryPlannerVisitor::planTableSubquery(ASTSubquery & subquery, ASTPtr & node, SqlHints & hints)
{
    auto plan = process(node);
    auto builder = toPlanBuilder(plan, analysis.getScope(subquery));

    //set hints
    addPlanHint(builder.plan->getStep(), hints);

    PRINT_PLAN(builder.plan, plan_table_subquery);
    return builder;
}

void QueryPlannerVisitor::planJoin(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    if (table_join.strictness == ASTTableJoin::Strictness::Any)
        if (table_join.kind == ASTTableJoin::Kind::Full)
            throw Exception("ANY FULL JOINs are not implemented.", ErrorCodes::NOT_IMPLEMENTED);

    if (isCrossJoin(table_join))
        planCrossJoin(table_join, left_builder, right_builder);
    else if (table_join.using_expression_list)
        planJoinUsing(table_join, left_builder, right_builder);
    else if (table_join.on_expression)
        planJoinOn(table_join, left_builder, right_builder);
    else
        throw Exception("Unrecognized join criteria found", ErrorCodes::PLAN_BUILD_ERROR);

    PRINT_PLAN(left_builder.plan, plan_join);
}

void QueryPlannerVisitor::planCrossJoin(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    // build join node
    {
        NamesAndTypes output_header;
        append(output_header, left_builder.getOutputNamesAndTypes());
        append(output_header, right_builder.getOutputNamesAndTypes());

        auto join_step = std::make_shared<JoinStep>(
            DataStreams{left_builder.getCurrentDataStream(), right_builder.getCurrentDataStream()},
            DataStream{.header = output_header},
            ASTTableJoin::Kind::Cross,
            ASTTableJoin::Strictness::Unspecified,
            context->getSettingsRef().max_threads,
            context->getSettingsRef().optimize_read_in_order);

        addPlanHint(join_step, table_join.hints);
        left_builder.addStep(std::move(join_step), {left_builder.getRoot(), right_builder.getRoot()});
    }

    // update scope
    {
        FieldSymbolInfos field_symbol_infos;
        append(field_symbol_infos, left_builder.getFieldSymbolInfos());
        append(field_symbol_infos, right_builder.getFieldSymbolInfos());
        left_builder.withScope(analysis.getScope(table_join), std::move(field_symbol_infos));
    }
}

void QueryPlannerVisitor::planJoinUsing(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    if (use_ansi_semantic && table_join.kind != ASTTableJoin::Kind::Inner)
    {
        throw Exception(
            "USING is banned for outer joins in ANSI mode, to avoid unexpected different behaviour with CLICKHOUSE",
            ErrorCodes::SYNTAX_ERROR);
    }

    auto & join_analysis = analysis.getJoinUsingAnalysis(table_join);

    // 1. prepare join keys
    auto [left_keys, right_keys] = prepareJoinUsingKeys(table_join, left_builder, right_builder);

    // 2. build join node
    auto join_step = std::make_shared<JoinStep>(
        DataStreams{left_builder.getCurrentDataStream(), right_builder.getCurrentDataStream()},
        getJoinOutputStream(table_join, left_builder, right_builder),
        table_join.kind,
        table_join.strictness,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        left_keys,
        right_keys,
        PredicateConst::TRUE_VALUE,
        true,
        use_ansi_semantic ? std::nullopt : std::make_optional(join_analysis.require_right_keys));

    addPlanHint(join_step, table_join.hints);
    left_builder.addStep(std::move(join_step), {left_builder.getRoot(), right_builder.getRoot()});
    // left_builder.setWithNonJoinStreamIfNecessary(table_join);

    // 3. update translation map
    {
        FieldSymbolInfos output_symbols;

        if (use_ansi_semantic)
        {
            for (auto & field_id : join_analysis.left_join_fields)
            {
                output_symbols.emplace_back(left_builder.getFieldSymbol(field_id));
            }

            auto add_non_join_fields = [&](const FieldSymbolInfos & source_fields, std::vector<size_t> & join_fields_list) {
                std::unordered_set<size_t> join_fields{join_fields_list.begin(), join_fields_list.end()};

                for (size_t i = 0; i < source_fields.size(); ++i)
                {
                    if (join_fields.find(i) == join_fields.end())
                        output_symbols.push_back(source_fields[i]);
                }
            };

            add_non_join_fields(left_builder.getFieldSymbolInfos(), join_analysis.left_join_fields);
            add_non_join_fields(right_builder.getFieldSymbolInfos(), join_analysis.right_join_fields);
        }
        else
        {
            // process left
            auto & left_join_field_reverse_map = join_analysis.left_join_field_reverse_map;
            const auto & left_symbols = left_builder.getFieldSymbolInfos();

            for (size_t i = 0; i < left_symbols.size(); ++i)
            {
                if (left_join_field_reverse_map.count(i))
                {
                    output_symbols.emplace_back(left_keys[left_join_field_reverse_map[i]]);
                }
                else
                {
                    output_symbols.push_back(left_symbols[i]);
                }
            }

            // process right
            auto & require_right_keys = join_analysis.require_right_keys;
            auto & right_join_field_reverse_map = join_analysis.right_join_field_reverse_map;
            const auto & right_field_symbols = right_builder.getFieldSymbolInfos();

            for (size_t i = 0; i < right_field_symbols.size(); ++i)
            {
                if (!right_join_field_reverse_map.count(i))
                {
                    output_symbols.push_back(right_field_symbols[i]);
                }
                else if (require_right_keys[right_join_field_reverse_map[i]])
                {
                    output_symbols.emplace_back(right_field_symbols[i].getPrimarySymbol());
                }
            }
        }
        left_builder.withScope(analysis.getScope(table_join), std::move(output_symbols));
    }
}

void QueryPlannerVisitor::planJoinOn(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    // 1. update translation map(for Semi/Anti join, we need to keep the left side's scope & symbols, as they will be used for output)
    FieldSymbolInfos left_field_symbols = left_builder.getFieldSymbolInfos();
    ScopePtr joined_scope = analysis.getScope(table_join);
    FieldSymbolInfos joined_field_symbols;
    append(joined_field_symbols, left_builder.getFieldSymbolInfos());
    append(joined_field_symbols, right_builder.getFieldSymbolInfos());

    left_builder.withScope(joined_scope, joined_field_symbols);
    right_builder.withScope(joined_scope, joined_field_symbols);

    // 2. prepare join keys
    auto [left_keys, right_keys] = prepareJoinOnKeys(table_join, left_builder, right_builder);

    // 3. build join filter
    ASTPtr join_filter = PredicateConst::TRUE_VALUE;
    {
        auto & join_analysis = analysis.getJoinOnAnalysis(table_join);
        // use a new translation map to avoid any calculated expression
        auto translation = std::make_shared<TranslationMap>(outer_context, joined_scope, joined_field_symbols, analysis, context);
        ASTs conjuncts;

        if (!isAsofJoin(table_join))
        {
            for (auto & cond : join_analysis.inequality_conditions)
            {
                auto left_side = translation->translate(cond.left_ast);
                auto right_side = translation->translate(cond.right_ast);
                conjuncts.push_back(makeASTFunction(getFunctionForInequality(cond.inequality), left_side, right_side));
            }
        }

        append(conjuncts, join_analysis.complex_expressions, [&](auto & expr) { return translation->translate(expr); });

        join_filter = cnfToExpression(conjuncts);
    }

    // 4. join inequality
    ASOF::Inequality asof_inequality = ASOF::Inequality::GreaterOrEquals;

    if (isAsofJoin(table_join))
        asof_inequality = analysis.getJoinOnAnalysis(table_join).getAsofInequality();

    // 5. build join node
    auto join_step = std::make_shared<JoinStep>(
        DataStreams{left_builder.getCurrentDataStream(), right_builder.getCurrentDataStream()},
        getJoinOutputStream(table_join, left_builder, right_builder),
        table_join.kind,
        table_join.strictness,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        left_keys,
        right_keys,
        isNormalInnerJoin(table_join) ? PredicateConst::TRUE_VALUE : join_filter,
        false,
        std::nullopt,
        asof_inequality);
    addPlanHint(join_step, table_join.hints);
    left_builder.addStep(std::move(join_step), {left_builder.getRoot(), right_builder.getRoot()});
    // left_builder.setWithNonJoinStreamIfNecessary(table_join);

    // 6. for inner join, build a FilterNode for join filter
    if (isNormalInnerJoin(table_join) && !ASTEquality::compareTree(join_filter, PredicateConst::TRUE_VALUE))
    {
        auto filter_step = std::make_shared<FilterStep>(left_builder.getCurrentDataStream(), join_filter);
        left_builder.addStep(filter_step);
    }

    left_builder.removeMappings();
}

std::pair<Names, Names>
QueryPlannerVisitor::prepareJoinUsingKeys(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    auto & join_using_analysis = analysis.getJoinUsingAnalysis(table_join);

    auto prepare_join_keys = [&](std::vector<size_t> & join_key_indices, DataTypes & coercion, PlanBuilder & builder) {
        Names join_key_symbols;
        join_key_symbols.reserve(join_key_indices.size());

        auto make_join_key_symbols = [&]() {
            join_key_symbols.clear();
            for (auto & join_key_index : join_key_indices)
                join_key_symbols.push_back(builder.getFieldSymbol(join_key_index));
        };

        make_join_key_symbols();
        assert(join_key_symbols.size() == coercion.size());
        NameToType name_to_type;

        for (size_t i = 0; i < join_key_symbols.size(); ++i)
            if (coercion[i])
                name_to_type.emplace(join_key_symbols[i], coercion[i]);

        if (!name_to_type.empty())
        {
            auto name_mapping = coerceTypesForSymbols(builder, name_to_type, true);
            builder.mapSymbols(name_mapping);
            make_join_key_symbols();
        }

        return join_key_symbols;
    };


    Names left_keys, right_keys;

    if (use_ansi_semantic)
    {
        left_keys = prepare_join_keys(join_using_analysis.left_join_fields, join_using_analysis.left_coercions, left_builder);
        right_keys = prepare_join_keys(join_using_analysis.right_join_fields, join_using_analysis.right_coercions, right_builder);
    }
    else
    {
        ExpressionsAndTypes expressions_and_types;
        assert(join_using_analysis.join_key_asts.size() == join_using_analysis.left_coercions.size());

        for (size_t i = 0; i < join_using_analysis.join_key_asts.size(); ++i)
            expressions_and_types.emplace_back(join_using_analysis.join_key_asts[i], join_using_analysis.left_coercions[i]);
        left_keys = left_builder.projectExpressionsWithCoercion(expressions_and_types);
        right_keys = prepare_join_keys(join_using_analysis.right_join_fields, join_using_analysis.right_coercions, right_builder);
    }

    return {left_keys, right_keys};
}

std::pair<Names, Names>
QueryPlannerVisitor::prepareJoinOnKeys(ASTTableJoin & table_join, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    auto & join_analysis = analysis.getJoinOnAnalysis(table_join);

    ExpressionsAndTypes left_conditions;
    ExpressionsAndTypes right_conditions;

    // for asof join, equality exprs & inequality exprs forms the join keys
    // for other joins, equality exprs forms the join keys, inequality exprs & complex exprs forms the join filter
    for (const auto & condition : join_analysis.equality_conditions)
    {
        left_conditions.emplace_back(condition.left_ast, condition.left_coercion);
        right_conditions.emplace_back(condition.right_ast, condition.right_coercion);
    }

    if (isAsofJoin(table_join))
    {
        for (const auto & condition : join_analysis.inequality_conditions)
        {
            left_conditions.emplace_back(condition.left_ast, condition.left_coercion);
            right_conditions.emplace_back(condition.right_ast, condition.right_coercion);
        }
    }

    // translate join keys to symbols
    Names left_symbols = left_builder.projectExpressionsWithCoercion(left_conditions);
    Names right_symbols = right_builder.projectExpressionsWithCoercion(right_conditions);

    return {left_symbols, right_symbols};
}

DataStream QueryPlannerVisitor::getJoinOutputStream(ASTTableJoin &, PlanBuilder & left_builder, PlanBuilder & right_builder)
{
    DataStream output_stream;

    // columns will be pruned further in optimizing phase
    for (auto & x : left_builder.getOutputNamesAndTypes())
        output_stream.header.insert({x.type, x.name});

    for (auto & x : right_builder.getOutputNamesAndTypes())
        output_stream.header.insert({x.type, x.name});

    return output_stream;
}

RelationPlan
QueryPlannerVisitor::planReadFromStorage(IAST & table_ast, ScopePtr table_scope, ASTSelectQuery & origin_query, SqlHints & hints, bool is_table_function)
{
    const auto & storage_analysis = analysis.getStorageAnalysis(table_ast);
    const auto & storage = storage_analysis.storage;
    const auto & alias = table_ast.tryGetAlias();

    auto primary_column_size = table_scope->size();
    const auto & used_columns = analysis.getReadColumns(table_ast);
    const auto & used_sub_columns = analysis.getReadSubColumns(table_ast);
    NamesWithAliases columns_with_aliases;
    // note primary column and sub columns may overlap, we use a map to keep allocated columns
    // e.g. SELECT m{'foo'}, __m__foo from t;
    NameToNameMap columns_to_aliases;
    FieldSymbolInfos field_symbols;

    field_symbols.reserve(primary_column_size);
    columns_with_aliases.reserve(primary_column_size); // may be larger

    assert(used_sub_columns.size() <= primary_column_size);

    for (size_t i = 0; i < primary_column_size; ++i)
    {
        const auto & primary_column_name = table_scope->at(i).getOriginColumnName();
        String primary_column_symbol;
        FieldSymbolInfo::SubColumnToSymbol sub_column_symbols;

        // allocate primary columns, not allocate if it is not used
        if (used_columns.count(i) || (i < used_sub_columns.size() && !used_sub_columns[i].empty()))
        {
            if (!columns_to_aliases.count(primary_column_name))
            {
                primary_column_symbol = context->getSymbolAllocator()->newSymbol(primary_column_name);
                columns_with_aliases.emplace_back(primary_column_name, primary_column_symbol);
                columns_to_aliases.emplace(primary_column_name, primary_column_symbol);
            }
            else
            {
                primary_column_symbol = columns_to_aliases.at(primary_column_name);
            }
        }

        // allocate sub columns
        if (i < used_sub_columns.size())
        {
            for (const auto & sub_column_id : used_sub_columns[i])
            {
                auto sub_column_name = sub_column_id.getSubColumnName(primary_column_name);
                if (!columns_to_aliases.count(sub_column_name))
                {
                    auto sub_column_symbol = context->getSymbolAllocator()->newSymbol(sub_column_name);
                    columns_with_aliases.emplace_back(sub_column_name, sub_column_symbol);
                    columns_to_aliases.emplace(sub_column_name, sub_column_symbol);
                    sub_column_symbols.emplace(sub_column_id, sub_column_symbol);
                }
                else
                {
                    sub_column_symbols.emplace(sub_column_id, columns_to_aliases.at(sub_column_name));
                }
            }
        }

        field_symbols.emplace_back(primary_column_symbol, sub_column_symbols);
    }

    // if no columns allocated, select a column with minimum size to allocate
    if (columns_with_aliases.empty())
    {
        NamesAndTypesList source_columns;
        std::unordered_map<String, size_t> name_to_index_map;

        for (size_t i = 0; i < primary_column_size; ++i)
        {
            source_columns.emplace_back(table_scope->at(i).getOriginColumnName(), table_scope->at(i).type);
            name_to_index_map.emplace(table_scope->at(i).getOriginColumnName(), i);
        }

        auto column_name = ColumnPruningVisitor::selectColumnWithMinSize(std::move(source_columns), storage);
        auto column_symbol = context->getSymbolAllocator()->newSymbol(column_name);
        columns_with_aliases.emplace_back(column_name, column_symbol);
        field_symbols[name_to_index_map.at(column_name)] = FieldSymbolInfo(column_symbol);
    }

    /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
    const auto generated_query = std::make_shared<ASTSelectQuery>();
    generated_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    const auto select_expression_list = generated_query->select();
    ASTPtr rewritten_prewhere = analysis.tryGetPrewhere(origin_query);
    if (rewritten_prewhere)
    {
        // translate PREWHERE for subcolumn optimization
        TranslationMap translation{outer_context, table_scope, field_symbols, analysis, context};
        rewritten_prewhere = translation.translate(rewritten_prewhere);

        // now change the symbol name back to column name
        std::unordered_map<String, String> name_mapping;
        for (const auto & [column, symbol] : columns_with_aliases)
            name_mapping.emplace(symbol, column);
        auto symbol_mapper = SymbolMapper::simpleMapper(name_mapping);
        generated_query->setExpression(ASTSelectQuery::Expression::PREWHERE, symbol_mapper.map(rewritten_prewhere));
    }
    /*
    if (origin_query.implicitWhere())
        generated_query->setExpression(ASTSelectQuery::Expression::IMPLICITWHERE, origin_query.implicitWhere()->clone());
    if (origin_query.encodedWhere())
        generated_query->setExpression(ASTSelectQuery::Expression::ENCODEDWHERE, origin_query.encodedWhere()->clone());
    */

    NamesAndTypesList columns;
    columns = storage->getInMemoryMetadataPtr()->getColumns().getOrdinary();
    if (is_table_function)
    {
        auto table_func = table_ast.shared_from_this();
        generated_query->addTableFunction(table_func);
    }
    else
        generated_query->replaceDatabaseAndTable(storage->getStorageID());

    // set sampling size
    if (origin_query.sampleSize())
    {
        auto & new_tables_in_select_query = generated_query->tables()->as<ASTTablesInSelectQuery &>();
        auto & new_tables_element = new_tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
        auto & new_table_expr = new_tables_element.table_expression->as<ASTTableExpression &>();
        new_table_expr.sample_size = origin_query.sampleSize();

        if (origin_query.sampleOffset())
            new_table_expr.sample_offset = origin_query.sampleOffset();
    }

    select_expression_list->children.reserve(columns.size());
    /// manually substitute column names in place of asterisk
    for (const auto & column : columns)
        select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

    SelectQueryInfo query_info;
    query_info.query = generated_query;

    const Settings & settings = context->getSettingsRef();
    UInt64 max_block_size = settings.max_block_size;
    if (!max_block_size)
        throw Exception("Setting 'max_block_size' cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    /*
    bool is_remote = false;
    if (storage && storage->isRemote())
    {
        is_remote = true;
    }

    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;
    if (max_streams == 0)
        throw Exception("Logical error: zero number of streams requested", ErrorCodes::LOGICAL_ERROR);

    /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads.
    if (max_streams > 1 && !is_remote)
        max_streams *= settings.max_streams_to_max_threads_ratio;

    MergeTreeReadPlannerPtr read_planner = getReadPlanner(origin_query);
    MergeTreeBitMapSchedulerPtr bitmap_scheduler = getBitMapScheduler(origin_query, storage, max_streams);

    if (read_planner || bitmap_scheduler)
    {
        auto * merge_tree_data = dynamic_cast<MergeTreeData *>(storage.get());
        auto * cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        BlockInputStreams streams;

        if (merge_tree_data)
            streams = merge_tree_data->read(
                required_columns, query_info, context, processing_stage, max_block_size, max_streams, read_planner, bitmap_scheduler);
        else if (cloud_merge_tree)
            streams = cloud_merge_tree->read(
                required_columns, query_info, context, processing_stage, max_block_size, max_streams, nullptr, bitmap_scheduler);
        else
            //throw Exception("Read order not supported by non MergeTree engine", ErrorCodes::NOT_IMPLEMENTED);
            streams = storage->read(required_columns, query_info, context, processing_stage, max_block_size, max_streams);

        if (!streams.empty())
        {
            auto table_step = std::make_shared<ReadFromStorageStep>(
                context,
                storage,
                database,
                table,
                columns_with_aliases,
                origin_query.prewhere(),
                query_info,
                processing_stage,
                max_block_size);
            return table_step;
        }
    }
    else
    {
        auto * cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (cloud_merge_tree)
        {
            const_cast<Context *>(&context)->setNameNode();
        }

        auto table_step = std::make_unique<TableScanStep>(
            context,
            storage->getStorageID(),
            columns_with_aliases,
            query_info,
            QueryProcessingStage::Enum::FetchColumns,
            max_block_size);
        return table_step;
    }
    */

    auto table_step
        = std::make_shared<TableScanStep>(context, storage->getStorageID(), columns_with_aliases, query_info, max_block_size, alias);

    addPlanHint(table_step, hints);
    auto plan_node = PlanNodeBase::createPlanNode(context->nextNodeId(), table_step);
    if (rewritten_prewhere)
        plan_node = PlanNodeBase::createPlanNode(
            context->nextNodeId(), std::make_shared<FilterStep>(plan_node->getCurrentDataStream(), rewritten_prewhere, true), {plan_node});
    return {plan_node, field_symbols};
}

/*
MergeTreeReadPlannerPtr QueryPlannerVisitor::getReadPlanner(ASTSelectQuery & query)
{
    if (!query.stepExecute())
        return nullptr;

    ASTStepExecute & step_execute = typeid_cast<ASTStepExecute &>(*(query.refStepExecute()));

    if (!step_execute.order_expression_list || !step_execute.step_number || !step_execute.batch_size)
        return nullptr;

    SortDescription order_desc;
    order_desc.reserve(step_execute.order_expression_list->children.size());
    for (const auto & elem : step_execute.order_expression_list->children)
    {
        String name = elem->children.front()->getColumnName();
        const ASTOrderByElement & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

        std::shared_ptr<Collator> collator;
        if (order_by_elem.collation)
            collator = std::make_shared<Collator>(typeid_cast<const ASTLiteral &>(*order_by_elem.collation).value.get<String>());

        order_desc.emplace_back(name, order_by_elem.direction, order_by_elem.nulls_direction, collator);
    }
    if (order_desc.empty())
        return nullptr;

    size_t step_number = 0;
    size_t step_size = 1;
    size_t batch_size = 0;
    step_number = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*(step_execute.step_number)).value);
    if (step_execute.step_size)
        step_size = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*(step_execute.step_size)).value);
    batch_size = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*(step_execute.batch_size)).value);

    return std::make_shared<MergeTreeReadPlanner>(std::move(order_desc), step_number, step_size, batch_size);
}

MergeTreeBitMapSchedulerPtr QueryPlannerVisitor::getBitMapScheduler(ASTSelectQuery & query, const StoragePtr & storage, size_t max_streams)
{
    if (!query.bitmapExecute())
        return nullptr;

    ASTBitMapExecute & bitmap_execute = typeid_cast<ASTBitMapExecute &>(*(query.refBitmapExecute()));

    if (!bitmap_execute.split_key)
        return nullptr;

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage.get());

    if (!merge_tree)
        return nullptr;

    auto is_partition_key = [&](auto & data) -> size_t {
        ASTPtr partition_expr_ast = data.getPartitionKeyList();
        if (!partition_expr_ast)
            return 0;

        ASTs children = partition_expr_ast->children;

        size_t res_idx = 0;

        for (size_t idx = 0; idx < children.size(); ++idx)
        {
            ASTPtr child = children.at(idx);
            if (child->getAliasOrColumnName() == bitmap_execute.split_key->getAliasOrColumnName())
            {
                res_idx = idx;
                break;
            }
        }

        NamesAndTypesList partition_keys = data.partition_key_sample.getNamesAndTypesList();
        DataTypePtr split_type = partition_keys.getTypes().at(res_idx);
        WhichDataType to_type(split_type);
        if (to_type.isUInt() || to_type.isInt())
            return res_idx;
        else
            throw Exception("Only numeric type can be used to be BITMAPEXECUTE by key", ErrorCodes::LOGICAL_ERROR);

        return 0;
    };

    size_t valid_split_by = is_partition_key(*merge_tree);

    // return nullptr if we want split by the first partition key
    if (valid_split_by == 0)
        return nullptr;

    return std::make_shared<MergeTreeBitMapScheduler>(max_streams, valid_split_by);
}
*/

PlanBuilder QueryPlannerVisitor::planFrom(ASTSelectQuery & select_query)
{
    if (select_query.tables())
            return planTables(select_query.refTables()->as<ASTTablesInSelectQuery &>(), select_query);
        else
            return planWithoutTables(select_query);
}

void QueryPlannerVisitor::planArrayJoin(ASTArrayJoin & array_join, PlanBuilder & builder, ASTSelectQuery & select_query)
{
    const auto & array_join_analysis = analysis.getArrayJoinAnalysis(select_query);
    const auto & array_join_descs = array_join_analysis.descriptions;
    ASTs array_join_exprs;
    array_join_exprs.reserve(array_join_descs.size());

    for (const auto & desc : array_join_descs)
        array_join_exprs.push_back(desc.expr);

    auto symbols = builder.applyProjection(array_join_exprs);
    auto array_join_action
        = std::make_shared<ArrayJoinAction>(NameSet{symbols.begin(), symbols.end()}, array_join_analysis.is_left_array_join, context);
    auto array_join_step = std::make_shared<ArrayJoinStep>(builder.getCurrentDataStream(), array_join_action);
    builder.addStep(array_join_step);

    FieldSymbolInfos new_symbol_infos = builder.getFieldSymbolInfos();
    for (size_t i = 0; i < array_join_descs.size(); ++i)
    {
        const auto & desc = array_join_descs[i];
        const auto & symbol = symbols[i];

        if (!desc.create_new_field)
        {
            auto col_ref = analysis.tryGetColumnReference(desc.expr);
            assert(col_ref.has_value());
            new_symbol_infos[col_ref->hierarchy_index] = FieldSymbolInfo(symbol);
        }
        else
            new_symbol_infos.emplace_back(symbol);
    }

    builder.withScope(analysis.getScope(array_join), new_symbol_infos);
    PRINT_PLAN(builder.plan, plan_array_join);
}

void QueryPlannerVisitor::planFilter(PlanBuilder & builder, ASTSelectQuery & select_query, const ASTPtr & filter)
{
    if (!filter || ASTEquality::compareTree(filter, PredicateConst::TRUE_VALUE))
        return;

    auto scalar_apply = planSubqueryExpression(builder, select_query, filter);
    planNonDeterministicFunction(builder, select_query, filter);
    auto filter_step = std::make_shared<FilterStep>(builder.getCurrentDataStream(), builder.translate(filter));
    for (const auto & scalar : scalar_apply)
    {
        auto * apply_step = dynamic_cast<ApplyStep *>(scalar->getStep().get());
        if (apply_step->getSubqueryType() == ApplyStep::SubqueryType::SCALAR)
        {
            NameSet outer_columns;
            for (const auto & conjunct : PredicateUtils::extractConjuncts(filter_step->getFilter()))
            {
                auto symbols = SymbolsExtractor::extract(conjunct);
                if (symbols.contains(apply_step->getAssignment().first))
                {
                    symbols.erase(apply_step->getAssignment().first);
                    outer_columns.insert(symbols.begin(), symbols.end());
                }
            }
            apply_step->setOuterColumns(outer_columns);
        }
    }
    builder.addStep(std::move(filter_step));
}

void QueryPlannerVisitor::planAggregate(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (!analysis.needAggregate(select_query))
    {
        if (select_query.group_by_with_totals || select_query.group_by_with_rollup || select_query.group_by_with_cube)
            throw Exception("WITH TOTALS, ROLLUP or CUBE are not supported without aggregation", ErrorCodes::NOT_IMPLEMENTED);
        return;
    }

    auto & group_by_analysis = analysis.getGroupByAnalysis(select_query);
    auto & aggregate_analysis = analysis.getAggregateAnalysis(select_query);

    // add projections for aggregation function params, group by keys
    {
        ASTs aggregate_inputs;

        append(aggregate_inputs, group_by_analysis.grouping_expressions);

        for (auto & agg_item : aggregate_analysis)
            append(aggregate_inputs, agg_item.expression->arguments->children);

        planExpression(builder, select_query, aggregate_inputs);
        PRINT_PLAN(builder.plan, plan_prepare_aggregate);
    }

    // build aggregation descriptions
    AstToSymbol mappings_for_aggregate = createScopeAwaredASTMap<String>(analysis, builder.getScope());
    AggregateDescriptions aggregate_descriptions;

    auto uniq_aggs = deduplicateByAst(aggregate_analysis, builder.getScope(), analysis, std::mem_fn(&AggregateAnalysis::expression));
    for (auto & agg_item : uniq_aggs)
    {
        AggregateDescription agg_desc;

        agg_desc.function = agg_item.function;
        agg_desc.parameters = agg_item.parameters;
        agg_desc.argument_names = builder.translateToSymbols(agg_item.expression->arguments->children);
        agg_desc.column_name = context->getSymbolAllocator()->newSymbol(agg_item.expression);

        aggregate_descriptions.push_back(agg_desc);
        mappings_for_aggregate.emplace(agg_item.expression, agg_desc.column_name);
    }

    // build grouping operations
    // TODO: grouping function only work, when group by with rollup?
    GroupingDescriptions grouping_operations_descs;
    for (auto & grouping_op : analysis.getGroupingOperations(select_query))
        if (!mappings_for_aggregate.count(grouping_op))
        {
            GroupingDescription description;

            for (const auto & argument : grouping_op->arguments->children)
                description.argument_names.emplace_back(builder.translateToSymbol(argument));

            description.output_name = context->getSymbolAllocator()->newSymbol(grouping_op);

            mappings_for_aggregate.emplace(grouping_op, description.output_name);
            grouping_operations_descs.emplace_back(std::move(description));
        }

    // collect group by keys & prune invisible columns
    Names keys_for_all_group;
    NameSet key_set_for_all_group;
    GroupingSetsParamsList grouping_sets_params;
    FieldSymbolInfos visible_fields(builder.getFieldSymbolInfos().size());
    AstToSymbol complex_expressions = createScopeAwaredASTMap<String>(analysis, builder.getScope());

    auto process_grouping_set = [&](const ASTs & grouping_set) {
        Names keys_for_this_group;
        NameSet key_set_for_this_group;

        for (const auto & grouping_expr : grouping_set)
        {
            auto symbol = builder.translateToSymbol(grouping_expr);
            bool new_global_key = false;

            if (!key_set_for_all_group.count(symbol))
            {
                keys_for_all_group.push_back(symbol);
                key_set_for_all_group.insert(symbol);
                new_global_key = true;
            }

            if (!key_set_for_this_group.count(symbol))
            {
                keys_for_this_group.push_back(symbol);
                key_set_for_this_group.insert(symbol);
            }

            if (new_global_key)
            {
                if (auto col_ref = analysis.tryGetColumnReference(grouping_expr); col_ref && builder.isLocalScope(col_ref->scope))
                {
                    assert(symbol == builder.getFieldSymbolInfo(col_ref->hierarchy_index).getPrimarySymbol());
                    visible_fields[col_ref->hierarchy_index].primary_symbol = symbol;
                }
                else if (auto sub_col_ref = analysis.tryGetSubColumnReference(grouping_expr); sub_col_ref
                         && builder.isLocalScope(sub_col_ref->getScope())
                         && builder.getFieldSymbolInfo(sub_col_ref->getFieldHierarchyIndex())
                                .tryGetSubColumnSymbol(sub_col_ref->getColumnID())) // Note: sub column symbol may be invalidated
                {
                    auto sub_column_symbol = *builder.getFieldSymbolInfo(sub_col_ref->getFieldHierarchyIndex())
                                                  .tryGetSubColumnSymbol(sub_col_ref->getColumnID());
                    assert(symbol == sub_column_symbol);
                    visible_fields[sub_col_ref->getFieldHierarchyIndex()].sub_column_symbols.emplace(sub_col_ref->getColumnID(), symbol);
                }
                else if (builder.isCalculatedExpression(grouping_expr))
                {
                    complex_expressions.emplace(grouping_expr, symbol);
                }
            }
        }

        grouping_sets_params.emplace_back(std::move(keys_for_this_group));
    };

    for (const auto & grouping_set : group_by_analysis.grouping_sets)
        process_grouping_set(grouping_set);

    builder.withNewMappings(visible_fields, complex_expressions);

    if (select_query.group_by_with_rollup)
    {
        grouping_sets_params.clear();
        auto key_size = keys_for_all_group.size();
        for (size_t set_size = 0; set_size <= key_size; set_size++)
        {
            auto end = keys_for_all_group.begin();
            std::advance(end, set_size);
            Names keys_for_this_group{keys_for_all_group.begin(), end};
            grouping_sets_params.emplace_back(std::move(keys_for_this_group));
        }
    }

    if (select_query.group_by_with_cube)
    {
        grouping_sets_params.clear();
        for (auto keys_for_this_group : Utils::powerSet(keys_for_all_group))
        {
            grouping_sets_params.emplace_back(std::move(keys_for_this_group));
        }
        grouping_sets_params.emplace_back(GroupingSetsParams{});
    }

    auto agg_step = std::make_shared<AggregatingStep>(
        builder.getCurrentDataStream(),
        keys_for_all_group,
        NameSet{},
        aggregate_descriptions,
        select_query.group_by_with_grouping_sets || grouping_sets_params.size() > 1 ? grouping_sets_params : GroupingSetsParamsList{},
        !select_query.group_by_with_totals, // when WITH TOTALS exists, TotalsHavingStep is to finalize aggregates
        SortDescription{},
        grouping_operations_descs,
        needAggregateOverflowRow(select_query),
        false);

    addPlanHint(agg_step, select_query.hints, true);
    builder.addStep(std::move(agg_step));
    builder.withAdditionalMappings(mappings_for_aggregate);

    if (select_query.group_by_with_totals)
    {
        const auto & header = builder.getCurrentDataStream().header;
        ColumnNumbers keys_positions;

        for (const auto & key : keys_for_all_group)
            keys_positions.emplace_back(header.getPositionByName(key));

        Aggregator::Params params(
            header, keys_positions, aggregate_descriptions, needAggregateOverflowRow(select_query), context->getSettingsRef().max_threads);

        auto transform_params = std::make_shared<AggregatingTransformParams>(params, false);

        QueryPlanStepPtr merge_agg = std::make_shared<MergingAggregatedStep>(
            builder.getCurrentDataStream(),
            keys_for_all_group,
            select_query.group_by_with_grouping_sets || grouping_sets_params.size() > 1 ? grouping_sets_params : GroupingSetsParamsList{},
            grouping_operations_descs,
            transform_params,
            false,
            context->getSettingsRef().max_threads,
            context->getSettingsRef().aggregation_memory_efficient_merge_threads);

        builder.addStep(std::move(merge_agg));
    }

    PRINT_PLAN(builder.plan, plan_aggregate);
}

void QueryPlannerVisitor::planTotalsAndHaving(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    const auto & settings = context->getSettingsRef();

    auto totals_having_step = std::make_shared<TotalsHavingStep>(
        builder.getCurrentDataStream(),
        needAggregateOverflowRow(select_query),
        select_query.having() ? builder.translate(select_query.having()) : nullptr,
        settings.totals_mode,
        settings.totals_auto_threshold,
        true);
    builder.addStep(std::move(totals_having_step));
}

void QueryPlannerVisitor::planWindow(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (analysis.getWindowAnalysisOfSelectQuery(select_query).empty())
        return;

    auto & window_analysis = analysis.getWindowAnalysisOfSelectQuery(select_query);

    auto uniq_windows = deduplicateByAst(window_analysis, builder.getScope(), analysis, std::mem_fn(&WindowAnalysis::expression));

    // add projections for window function params, partition by keys, sorting keys
    {
        ASTs window_inputs;

        for (auto & window_item : uniq_windows)
        {
            append(window_inputs, window_item->expression->arguments->children);

            if (window_item->resolved_window->partition_by)
                append(window_inputs, window_item->resolved_window->partition_by->children);

            if (window_item->resolved_window->order_by)
                append(window_inputs, window_item->resolved_window->order_by->children, [](ASTPtr & order_item) {
                    return order_item->as<ASTOrderByElement &>().children[0];
                });
        }

        planExpression(builder, select_query, window_inputs);
        PRINT_PLAN(builder.plan, plan_prepare_window);
    }

    // build window description
    WindowDescriptions window_descriptions;

    for (auto & window_item : uniq_windows)
    {
        if (window_descriptions.find(window_item->window_name) == window_descriptions.end())
        {
            WindowDescription window_desc;
            const auto & resolved_window = window_item->resolved_window;

            window_desc.window_name = window_item->window_name;

            if (resolved_window->partition_by)
            {
                for (const auto & ast : resolved_window->partition_by->children)
                {
                    window_desc.partition_by.push_back(
                        SortColumnDescription(builder.translateToSymbol(ast), 1 /* direction */, 1 /* nulls_direction */));
                }
            }

            if (resolved_window->order_by)
            {
                for (const auto & ast : resolved_window->order_by->children)
                {
                    const auto & order_by_element = ast->as<ASTOrderByElement &>();
                    window_desc.order_by.push_back(SortColumnDescription(
                        builder.translateToSymbol(order_by_element.children.front()),
                        order_by_element.direction,
                        order_by_element.nulls_direction));
                }
            }

            window_desc.full_sort_description = window_desc.partition_by;
            window_desc.full_sort_description.insert(
                window_desc.full_sort_description.end(), window_desc.order_by.begin(), window_desc.order_by.end());

            window_desc.frame = resolved_window->frame;
            window_descriptions.insert({window_item->window_name, window_desc});
        }

        WindowFunctionDescription window_function;
        window_function.function_node = window_item->expression.get();
        window_function.column_name = context->getSymbolAllocator()->newSymbol(window_item->expression);
        window_function.aggregate_function = window_item->aggregator;
        window_function.function_parameters = window_item->parameters;

        const ASTs & arguments = window_item->expression->arguments->children;
        window_function.argument_types.resize(arguments.size());
        window_function.argument_names.resize(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            window_function.argument_types[i] = analysis.getExpressionType(arguments[i]);
            window_function.argument_names[i] = builder.translateToSymbol(arguments[i]);
        }

        window_descriptions[window_item->window_name].window_functions.push_back(window_function);
    }

    // add window steps
    for (const auto & [_, window_desc] : window_descriptions)
    {
        AstToSymbol mappings = createScopeAwaredASTMap<String>(analysis, builder.getScope());

        for (const auto & window_func : window_desc.window_functions)
        {
            ASTPtr window_expr = std::const_pointer_cast<IAST>(window_func.function_node->shared_from_this());
            mappings.emplace(window_expr, window_func.column_name);
        }

        auto window_step = std::make_shared<WindowStep>(builder.getCurrentDataStream(), window_desc, true, SortDescription{});
        builder.addStep(std::move(window_step));
        builder.withAdditionalMappings(mappings);
        PRINT_PLAN(builder.plan, plan_window);
    }
}

void QueryPlannerVisitor::planSelect(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    auto & select_expressions = analysis.getSelectExpressions(select_query);
    planExpression(builder, select_query, select_expressions);
}

void QueryPlannerVisitor::planDistinct(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (!select_query.distinct)
        return;

    auto & select_expressions = analysis.getSelectExpressions(select_query);
    UInt64 limit_for_distinct = 0;

    auto distinct_step = std::make_shared<DistinctStep>(
        builder.getCurrentDataStream(),
        extractDistinctSizeLimits(),
        limit_for_distinct,
        builder.translateToSymbols(select_expressions),
        false);

    builder.addStep(std::move(distinct_step));
    PRINT_PLAN(builder.plan, plan_distinct);
}

void QueryPlannerVisitor::planOrderBy(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (!select_query.orderBy())
        return;

    auto & order_by_analysis = analysis.getOrderByAnalysis(select_query);

    // project sort by keys
    ASTs sort_expressions;
    for (auto & order_by_item : order_by_analysis)
        sort_expressions.emplace_back(order_by_item->children.front());
    planExpression(builder, select_query, sort_expressions);
    PRINT_PLAN(builder.plan, plan_prepare_order_by);

    // build sort description
    SortDescription sort_description;
    for (auto & order_by_item : order_by_analysis)
    {
        String sort_symbol = builder.translateToSymbol(order_by_item->children.front());
        std::shared_ptr<Collator> collator;

        if (order_by_item->collation)
            collator = std::make_shared<Collator>(order_by_item->collation->as<ASTLiteral &>().value.get<String>());

        sort_description.emplace_back(sort_symbol, order_by_item->direction, order_by_item->nulls_direction, collator);
    }

    // collect limit hint
    UInt64 limit = 0;
    if (!select_query.distinct && !select_query.limitBy())
    {
        auto [limit_length, limit_offset] = getLimitLengthAndOffset(select_query);
        limit = limit_length + limit_offset;
    }

    auto sorting_step = std::make_shared<SortingStep>(
        builder.getCurrentDataStream(), sort_description, limit, SortingStep::Stage::FULL, SortDescription{});
    builder.addStep(std::move(sorting_step));
    PRINT_PLAN(builder.plan, plan_order_by);
}

void QueryPlannerVisitor::planWithFill(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (!select_query.orderBy())
        return;

    // get fill description
    SortDescription fill_description;
    for (auto & order_by_item : analysis.getOrderByAnalysis(select_query))
    {
        if (order_by_item->with_fill)
        {
            String sort_symbol = builder.translateToSymbol(order_by_item->children.front());
            std::shared_ptr<Collator> collator;

            if (order_by_item->collation)
                collator = std::make_shared<Collator>(order_by_item->collation->as<ASTLiteral &>().value.get<String>());

            FillColumnDescription fill_desc = InterpreterSelectQuery::getWithFillDescription(*order_by_item, context);
            fill_description.emplace_back(sort_symbol, order_by_item->direction, order_by_item->nulls_direction, collator, true, fill_desc);
        }
    }

    if (fill_description.empty())
        return;

    auto filling_step = std::make_shared<FillingStep>(builder.getCurrentDataStream(), std::move(fill_description));
    builder.addStep(std::move(filling_step));
    PRINT_PLAN(builder.plan, plan_with_fill);
}

void QueryPlannerVisitor::planLimitBy(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (!select_query.limitBy())
        return;

    // project limit by keys
    ASTs & limit_by_expressions = analysis.getLimitByItem(select_query);
    planExpression(builder, select_query, limit_by_expressions);
    PRINT_PLAN(builder.plan, plan_prepare_limit_by);

    UInt64 offset = select_query.getLimitByOffset() ? analysis.getLimitByOffsetValue(select_query) : 0;

    // plan limit by node
    auto step = std::make_shared<LimitByStep>(
        builder.getCurrentDataStream(),
        analysis.getLimitByValue(select_query),
        offset,
        builder.translateToUniqueSymbols(limit_by_expressions));
    builder.addStep(std::move(step));
    PRINT_PLAN(builder.plan, plan_limit_by);
}

RelationPlan QueryPlannerVisitor::projectFieldSymbols(const RelationPlan & plan, const FieldSubColumnIDs & sub_column_positions)
{
    const auto & old_root = plan.getRoot();
    const auto & old_mappings = plan.getFieldSymbolInfos();
    PlanNodePtr new_root;
    FieldSymbolInfos new_mappings;

    Assignments assignments;
    NameToType input_types = old_root->getOutputNamesToTypes();
    NameToType output_types;

    for (const auto & input_symbol_info : old_mappings)
    {
        const auto & field_symbol = input_symbol_info.getPrimarySymbol();
        assignments.emplace_back(field_symbol, toSymbolRef(field_symbol));
        output_types[field_symbol] = input_types.at(field_symbol);
        new_mappings.emplace_back(field_symbol);
    }

    // when enable_subcolumn_optimization_through_union, append sub-columns after primary columns.
    for (const auto & field_sub_col_id : sub_column_positions)
    {
        auto field_id = field_sub_col_id.first;
        const auto & sub_col_id = field_sub_col_id.second;
        auto sub_col_sym = old_mappings.at(field_id).tryGetSubColumnSymbol(sub_col_id);
        if (!sub_col_sym)
            throw Exception("Sub column not found in union element.", ErrorCodes::LOGICAL_ERROR);
        assignments.emplace_back(*sub_col_sym, toSymbolRef(*sub_col_sym));
        output_types[*sub_col_sym] = input_types.at(*sub_col_sym);
        new_mappings[field_id].sub_column_symbols[sub_col_id] = *sub_col_sym;
    }

    auto project_step = std::make_shared<ProjectionStep>(old_root->getCurrentDataStream(), assignments, output_types);
    new_root = old_root->addStep(context->nextNodeId(), std::move(project_step));

    return {new_root, std::move(new_mappings)};
}

static bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query)
{
    if (query.group_by_with_totals)
        return true;

    /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
     * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
     */
    if (auto query_table = extractTableExpression(query, 0))
    {
        if (const auto * ast_union = query_table->as<ASTSelectWithUnionQuery>())
        {
            /** NOTE
            * 1. For ASTSelectWithUnionQuery after normalization for union child node the height of the AST tree is at most 2.
            * 2. For ASTSelectIntersectExceptQuery after normalization in case there are intersect or except nodes,
            * the height of the AST tree can have any depth (each intersect/except adds a level), but the
            * number of children in those nodes is always 2.
            */
            std::function<bool(ASTPtr)> traverse_recursively = [&](ASTPtr child_ast) -> bool {
                if (const auto * select_child = child_ast->as<ASTSelectQuery>())
                {
                    if (hasWithTotalsInAnySubqueryInFromClause(select_child->as<ASTSelectQuery &>()))
                        return true;
                }
                else if (const auto * union_child = child_ast->as<ASTSelectWithUnionQuery>())
                {
                    for (const auto & subchild : union_child->list_of_selects->children)
                        if (traverse_recursively(subchild))
                            return true;
                }
                else if (const auto * intersect_child = child_ast->as<ASTSelectIntersectExceptQuery>())
                {
                    auto selects = intersect_child->getListOfSelects();
                    for (const auto & subchild : selects)
                        if (traverse_recursively(subchild))
                            return true;
                }
                return false;
            };

            for (const auto & elem : ast_union->list_of_selects->children)
                if (traverse_recursively(elem))
                    return true;
        }
    }

    return false;
}

void QueryPlannerVisitor::planLimitAndOffset(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (select_query.limitLength())
    {
        /** Rare case:
          *  if there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
          *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
          *  because if you cancel the query, we will not get `totals` data from the remote server.
          *
          * Another case:
          *  if there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
          *  otherwise TOTALS is counted according to incomplete data.
          */
        bool always_read_till_end = false;

        if (select_query.group_by_with_totals && !select_query.orderBy())
            always_read_till_end = true;

        if (!select_query.group_by_with_totals && hasWithTotalsInAnySubqueryInFromClause(select_query))
            always_read_till_end = true;

        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(select_query);
        auto step = std::make_shared<LimitStep>(builder.getCurrentDataStream(), limit_length, limit_offset, always_read_till_end);
        builder.addStep(std::move(step));
    }
    else if (select_query.getLimitOffset())
    {
        UInt64 offset = analysis.getLimitOffset(select_query);
        auto offsets_step = std::make_unique<OffsetStep>(builder.getCurrentDataStream(), offset);
        builder.addStep(std::move(offsets_step));
    }
}

void QueryPlannerVisitor::planSampling(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    if (select_query.sampleSize() && context->getSettingsRef().enable_final_sample)
    {
        ASTSampleRatio * sample = select_query.sampleSize()->as<ASTSampleRatio>();
        ASTSampleRatio::BigNum numerator = sample->ratio.numerator;
        ASTSampleRatio::BigNum denominator = sample->ratio.denominator;
        if (numerator <= 1 || denominator > 1)
        {
            return;
        }
        else
        {
            auto sampling = std::make_unique<FinalSampleStep>(builder.getCurrentDataStream(), numerator, context->getSettingsRef().max_block_size);
            builder.addStep(std::move(sampling));
        }
    }
}

RelationPlan QueryPlannerVisitor::planFinalSelect(PlanBuilder & builder, ASTSelectQuery & select_query)
{
    const auto & select_expressions = analysis.getSelectExpressions(select_query);
    FieldSymbolInfos field_symbol_infos;

    Assignments assignments;
    NameToType input_types = builder.getOutputNamesToTypes();
    NameToType output_types;
    NameSet existing_symbols;

    // select_column has 2 effects:
    //   1. prune irrelevant columns
    //   2. duplicate column for same expression
    auto select_column = [&](const auto & input_symbol) {
        String output_symbol = input_symbol;
        if (existing_symbols.find(output_symbol) != existing_symbols.end())
            output_symbol = context->getSymbolAllocator()->newSymbol(output_symbol);

        assignments.emplace_back(output_symbol, toSymbolRef(input_symbol));
        if (!input_types.contains(input_symbol))
            throw Exception("Unknown column " + input_symbol, ErrorCodes::PLAN_BUILD_ERROR);
        output_types[output_symbol] = input_types.at(input_symbol);
        existing_symbols.insert(output_symbol);

        return output_symbol;
    };

    for (const auto & select_expr : select_expressions)
    {
        FieldSymbolInfo field_symbol_info;

        if (auto column_reference = analysis.tryGetColumnReference(select_expr))
        {
            field_symbol_info = builder.getGlobalFieldSymbolInfo(*column_reference);
        }
        else
        {
            field_symbol_info = {builder.translateToSymbol(select_expr)};
        }

        field_symbol_info.primary_symbol = select_column(field_symbol_info.primary_symbol);
        auto & sub_column_symbols = field_symbol_info.sub_column_symbols;

        for (auto & sub_col : sub_column_symbols)
        {
            sub_column_symbols[sub_col.first] = select_column(sub_col.second);
        }

        field_symbol_infos.push_back(field_symbol_info);
    }

    auto project = std::make_shared<ProjectionStep>(builder.getCurrentDataStream(), assignments, output_types);
    addPlanHint(project, select_query.hints);
    builder.addStep(std::move(project));

    return {builder.getRoot(), field_symbol_infos};
}

namespace
{
    template <typename UserContext>
    class ExtractSubqueryTraversalVisitor : public ExpressionTraversalVisitor<UserContext>
    {
    public:
        using ExpressionTraversalIncludeSubqueryVisitor<UserContext>::process;

        ExtractSubqueryTraversalVisitor(
            AnalyzerExpressionVisitor<UserContext> & user_visitor_,
            UserContext & user_context_,
            Analysis & analysis_,
            ContextPtr context_,
            PlanBuilder & plan_builder_,
            bool include_lambda_)
            : ExpressionTraversalVisitor<UserContext>(user_visitor_, user_context_, analysis_, context_)
            , plan_builder(plan_builder_)
            , include_lambda(include_lambda_)
        {
        }

        void process(ASTPtr & node, const Void & traversal_context) override
        {
            // don't go through planned expresions(e.g. aggregate when planAggregate has been executed)
            if (plan_builder.canTranslateToSymbol(node))
                return;

            if (const auto * func = node->as<ASTFunction>(); func && func->name == "lambda" && !include_lambda)
                return;

            ExpressionTraversalVisitor<UserContext>::process(node, traversal_context);
        }

    private:
        PlanBuilder & plan_builder;
        bool include_lambda;
    };

    struct ExtractSubqueryVisitor : public AnalyzerExpressionVisitor<const Void>
    {
    protected:
        void visitExpression(ASTPtr &, IAST &, const Void &) override { }

        void visitScalarSubquery(ASTPtr & node, ASTSubquery &, const Void &) override { scalar_subqueries.push_back(node); }

        void visitInSubquery(ASTPtr & node, ASTFunction &, const Void &) override { in_subqueries.push_back(node); }

        void visitExistsSubquery(ASTPtr & node, ASTFunction &, const Void &) override { exists_subqueries.push_back(node); }

        void visitQuantifiedComparisonSubquery(ASTPtr & node, ASTQuantifiedComparison &, const Void &) override
        {
            quantified_comparison_subqueries.push_back(node);
        }

    public:
        using AnalyzerExpressionVisitor<const Void>::AnalyzerExpressionVisitor;

        std::vector<ASTPtr> scalar_subqueries;
        std::vector<ASTPtr> in_subqueries;
        std::vector<ASTPtr> exists_subqueries;
        std::vector<ASTPtr> quantified_comparison_subqueries;
    };

    class ExtractNonDeterministicFunctionVisitor : public AnalyzerExpressionVisitor<const Void>
    {
    protected:
        void visitExpression(ASTPtr &, IAST &, const Void &) override { }
        void visitOrdinaryFunction(ASTPtr & node, ASTFunction & func, const Void &) override
        {
            if (context->isNonDeterministicFunction(func.name))
                non_deterministic_functions.emplace_back(node);
        }

    public:
        using AnalyzerExpressionVisitor<const Void>::AnalyzerExpressionVisitor;
        std::vector<ASTPtr> non_deterministic_functions;
    };
}

template <typename T>
PlanNodes QueryPlannerVisitor::planSubqueryExpression(PlanBuilder & builder, ASTSelectQuery & select_query, const T & expressions)
{
    if constexpr (std::is_same_v<T, ASTs>)
    {
        for (auto & expr : expressions)
            planSubqueryExpression(builder, select_query, expr);

        return {};
    }
    else
    {
        ExtractSubqueryVisitor extract_visitor{context};
        ExtractSubqueryTraversalVisitor traversal_visitor{extract_visitor, {}, analysis, context, builder, true};
        traversal_visitor.process(const_cast<ASTPtr &>(expressions));

        PlanNodes scalar_apply;
        for (auto & scalar_subquery : extract_visitor.scalar_subqueries)
        {
            planScalarSubquery(builder, scalar_subquery);
            scalar_apply.emplace_back(builder.getRoot());
        }
        for (auto & in_subquery : extract_visitor.in_subqueries)
            planInSubquery(builder, in_subquery, select_query);
        for (auto & exists_subquery : extract_visitor.exists_subqueries)
            planExistsSubquery(builder, exists_subquery);
        for (auto & quantified_comparison_subquery : extract_visitor.quantified_comparison_subqueries)
            planQuantifiedComparisonSubquery(builder, quantified_comparison_subquery, select_query);
        return scalar_apply;
    }
    __builtin_unreachable();
}

void QueryPlannerVisitor::planScalarSubquery(PlanBuilder & builder, const ASTPtr & scalar_subquery)
{
    // filter out planned subqueries
    if (builder.canTranslateToSymbol(scalar_subquery))
        return;

    auto subquery_plan = QueryPlanner().planQuery(scalar_subquery, builder.translation, analysis, context, cte_plans);

    subquery_plan = combineSubqueryOutputsToTuple(subquery_plan, scalar_subquery);

    if (auto coerced_type = analysis.getTypeCoercion(scalar_subquery))
    {
        coerceTypeForSubquery(subquery_plan, coerced_type);
    }

    // Add EnforceSingleRow Step
    auto enforce_single_row_step = std::make_shared<EnforceSingleRowStep>(subquery_plan.getRoot()->getCurrentDataStream());
    auto enforce_single_row_node = subquery_plan.getRoot()->addStep(context->nextNodeId(), std::move(enforce_single_row_step));
    subquery_plan.withNewRoot(enforce_single_row_node);

    // Add Apply Step
    String subquery_output_symbol = subquery_plan.getFirstPrimarySymbol();
    //String apply_output_symbol = context.getSymbolAllocator()->newSymbol("_scalar_subquery");
    Assignment scalar_assignment{subquery_output_symbol, toSymbolRef(subquery_output_symbol)};

    auto apply_step = std::make_shared<ApplyStep>(
        DataStreams{builder.getCurrentDataStream(), subquery_plan.getRoot()->getCurrentDataStream()},
        builder.getOutputNames(),
        ApplyStep::ApplyType::CROSS,
        ApplyStep::SubqueryType::SCALAR,
        scalar_assignment,
        NameSet{},
        analysis.subquery_support_semi_anti[scalar_subquery]);
    builder.addStep(std::move(apply_step), {builder.getRoot(), subquery_plan.getRoot()});
    builder.withAdditionalMapping(scalar_subquery, subquery_output_symbol);
    PRINT_PLAN(builder.plan, plan_scalar_subquery);
}

void QueryPlannerVisitor::planInSubquery(PlanBuilder & builder, const ASTPtr & node, ASTSelectQuery & select_query)
{
    // filter out planned subqueries
    if (builder.canTranslateToSymbol(node))
        return;

    auto & function = node->as<ASTFunction &>();
    if (function.name == "nullIn" || function.name == "globalNullIn" || function.name == "notNullIn" || function.name == "globalNotNullIn")
        throw Exception(
            "nullIn,globalNullIn,notNullIn and globalNotNullIn are not implemented, when optimizer is opened", ErrorCodes::NOT_IMPLEMENTED);

    //process two children of function
    RelationPlan rhs_plan;
    String rhs_symbol, lhs_symbol;
    processSubqueryArgs(builder, function.arguments->children, rhs_symbol, lhs_symbol, rhs_plan, select_query);

    // Add Apply Step
    String apply_output_symbol = context->getSymbolAllocator()->newSymbol("_in_subquery");
    Assignment in_assignment{apply_output_symbol, makeASTFunction(function.name, ASTs{toSymbolRef(lhs_symbol), toSymbolRef(rhs_symbol)})};

    auto apply_step = std::make_shared<ApplyStep>(
        DataStreams{builder.getCurrentDataStream(), rhs_plan.getRoot()->getCurrentDataStream()},
        builder.getOutputNames(),
        ApplyStep::ApplyType::CROSS,
        ApplyStep::SubqueryType::IN,
        in_assignment,
        NameSet{},
        analysis.subquery_support_semi_anti[node]);

    builder.addStep(std::move(apply_step), {builder.getRoot(), rhs_plan.getRoot()});
    builder.withAdditionalMapping(node, apply_output_symbol);
    PRINT_PLAN(builder.plan, plan_in_subquery);
}

void QueryPlannerVisitor::planExistsSubquery(PlanBuilder & builder, const ASTPtr & node)
{
    // filter out planned subqueries
    if (builder.canTranslateToSymbol(node))
        return;

    auto exists_subquery = node->as<ASTFunction &>();
    bool is_not_exist = false;
    if (exists_subquery.name == "not")
    {
        is_not_exist = true;
        exists_subquery = exists_subquery.arguments->children[0]->as<ASTFunction &>();
    }
    auto subquery_plan
        = QueryPlanner().planQuery(exists_subquery.arguments->children.at(0), builder.translation, analysis, context, cte_plans);
    // Add Projection Step
    {
        const auto & output_name = subquery_plan.getFirstPrimarySymbol();
        Assignment assignment{output_name, toSymbolRef(output_name)};
        Assignments assignments{assignment};
        NameToType types;
        types[output_name] = subquery_plan.getRoot()->getOutputNamesToTypes().at(output_name);

        auto expression_step = std::make_shared<ProjectionStep>(subquery_plan.getRoot()->getCurrentDataStream(), assignments, types);
        auto expression_node = subquery_plan.getRoot()->addStep(context->nextNodeId(), std::move(expression_step));
        subquery_plan = {expression_node, FieldSymbolInfos{{output_name}}};
    }

    // Add Apply Step
    String apply_output_symbol = context->getSymbolAllocator()->newSymbol(is_not_exist ? "_not_exists_subquery" : "_exists_subquery");
    Assignment exist_assignment{
        apply_output_symbol, is_not_exist ? std::make_shared<ASTLiteral>(false) : std::make_shared<ASTLiteral>(true)};
    auto apply_step = std::make_shared<ApplyStep>(
        DataStreams{builder.getCurrentDataStream(), subquery_plan.getRoot()->getCurrentDataStream()},
        builder.getOutputNames(),
        ApplyStep::ApplyType::CROSS,
        ApplyStep::SubqueryType::EXISTS,
        exist_assignment,
        NameSet{},
        analysis.subquery_support_semi_anti[node]);

    builder.addStep(std::move(apply_step), {builder.getRoot(), subquery_plan.getRoot()});
    builder.withAdditionalMapping(node, apply_output_symbol);
    PRINT_PLAN(builder.plan, plan_exists_subquery);
}

void QueryPlannerVisitor::planQuantifiedComparisonSubquery(PlanBuilder & builder, const ASTPtr & node, ASTSelectQuery & select_query)
{
    if (builder.canTranslateToSymbol(node))
        return;

    auto & quantified_comparison = node->as<ASTQuantifiedComparison &>();

    //process two children of quantified_comparison
    RelationPlan rhs_plan;
    String rhs_symbol, lhs_symbol;
    processSubqueryArgs(builder, quantified_comparison.children, rhs_symbol, lhs_symbol, rhs_plan, select_query);

    String apply_output_symbol;
    std::shared_ptr<ApplyStep> apply_step;
    // A = ANY B <=> A IN B
    // A <> ALL B <=> (A notIn B)
    bool comparator_is_equals = quantified_comparison.comparator == "equals";
    bool quantifier_is_all = quantified_comparison.quantifier_type == QuantifierType::ALL;
    bool comparator_is_range_op = quantified_comparison.comparator != "equals" && quantified_comparison.comparator != "notEquals";
    if (comparator_is_range_op || comparator_is_equals == quantifier_is_all)
    {
        apply_output_symbol = context->getSymbolAllocator()->newSymbol("_quantified_comparison_subquery");
        Assignment quantified_comparison_assignment{
            apply_output_symbol,
            makeASTQuantifiedComparison(
                quantified_comparison.comparator,
                quantified_comparison.quantifier_type,
                ASTs{toSymbolRef(lhs_symbol), toSymbolRef(rhs_symbol)})};

        apply_step = std::make_shared<ApplyStep>(
            DataStreams{builder.getCurrentDataStream(), rhs_plan.getRoot()->getCurrentDataStream()},
            builder.getOutputNames(),
            ApplyStep::ApplyType::CROSS,
            ApplyStep::SubqueryType::QUANTIFIED_COMPARISON,
            quantified_comparison_assignment,
            NameSet{},
            analysis.subquery_support_semi_anti[node]);
    }
    else
    {
        String function_name = "in";
        if (!comparator_is_equals)
            function_name = "notIn";
        apply_output_symbol = context->getSymbolAllocator()->newSymbol("_in_subquery");
        Assignment in_assignment{
            apply_output_symbol, makeASTFunction(function_name, ASTs{toSymbolRef(lhs_symbol), toSymbolRef(rhs_symbol)})};
        apply_step = std::make_shared<ApplyStep>(
            DataStreams{builder.getCurrentDataStream(), rhs_plan.getRoot()->getCurrentDataStream()},
            builder.getOutputNames(),
            ApplyStep::ApplyType::CROSS,
            ApplyStep::SubqueryType::IN,
            in_assignment,
            NameSet{},
            analysis.subquery_support_semi_anti[node]);
    }

    builder.addStep(std::move(apply_step), {builder.getRoot(), rhs_plan.getRoot()});
    builder.withAdditionalMapping(node, apply_output_symbol);
    PRINT_PLAN(builder.plan, plan_quantified_comparison_subquery);
}

RelationPlan QueryPlannerVisitor::combineSubqueryOutputsToTuple(const RelationPlan & plan, const ASTPtr & subquery)
{
    const auto & outputs = plan.getFieldSymbolInfos();

    if (outputs.size() > 1)
    {
        auto subquery_type = analysis.getExpressionType(subquery);
        const auto & type_tuple = typeid_cast<const DataTypeTuple &>(*subquery_type);
        ASTs tuple_func_args(outputs.size());
        std::transform(
            outputs.begin(), outputs.end(), tuple_func_args.begin(), [](auto & out) { return toSymbolRef(out.getPrimarySymbol()); });

        ASTPtr tuple_func_expr = makeASTFunction("tuple", std::move(tuple_func_args));
        if (type_tuple.haveExplicitNames())
            tuple_func_expr = makeCastFunction(tuple_func_expr, subquery_type);
        auto tuple_func_symbol = context->getSymbolAllocator()->newSymbol(tuple_func_expr);
        Assignments assignments{{tuple_func_symbol, tuple_func_expr}};
        NameToType types{{tuple_func_symbol, subquery_type}};
        auto old_root = plan.getRoot();
        auto expression_step = std::make_shared<ProjectionStep>(old_root->getCurrentDataStream(), assignments, types);
        auto new_root = old_root->addStep(context->nextNodeId(), std::move(expression_step));

        return {new_root, FieldSymbolInfos{{tuple_func_symbol}}};
    }
    return plan;
}

RelationPlan QueryPlannerVisitor::planSetOperation(ASTs & selects, ASTSelectWithUnionQuery::Mode union_mode)
{
    RelationPlans sub_plans;

    // 1. plan set element
    for (auto & select : selects)
        sub_plans.push_back(process(select));

    if (sub_plans.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Set operation must have at least 1 element");

    if (sub_plans.size() == 1)
        return sub_plans.front();

    std::vector<NameToType> sub_plan_types;
    for (const auto & sub_plan : sub_plans)
        sub_plan_types.emplace_back(sub_plan.getRoot()->getOutputNamesToTypes());

    FieldSubColumnIDs sub_column_positions;
    DataTypes sub_column_type_coercions;

    // compute common subcolumns for each field
    if (enable_subcolumn_optimization_through_union)
    {
        auto field_size = sub_plans.front().getFieldSymbolInfos().size();
        for (size_t field_id = 0; field_id < field_size; ++field_id)
        {
            SubColumnIDSet common_sub_col_ids_prev;
            SubColumnIDSet common_sub_col_ids;

            for (size_t select_id = 0; select_id < selects.size(); ++select_id)
            {
                common_sub_col_ids_prev.swap(common_sub_col_ids);
                common_sub_col_ids.clear();

                for (const auto & sub_col : sub_plans[select_id].getFieldSymbolInfos().at(field_id).sub_column_symbols)
                    if (select_id == 0 || common_sub_col_ids_prev.count(sub_col.first))
                        common_sub_col_ids.emplace(sub_col.first);
            }

            for (const auto & sub_col_id : common_sub_col_ids)
                sub_column_positions.emplace_back(field_id, sub_col_id);
        }

        if (enable_implicit_type_conversion)
        {
            for (const auto & field_sub_col_id : sub_column_positions)
            {
                DataTypes sub_col_types;
                for (size_t select_id = 0; select_id < selects.size(); ++select_id)
                {
                    auto sub_col_symbol = sub_plans[select_id]
                                              .getFieldSymbolInfos()
                                              .at(field_sub_col_id.first)
                                              .sub_column_symbols.at(field_sub_col_id.second);
                    sub_col_types.push_back(sub_plan_types[select_id].at(sub_col_symbol));
                }
                sub_column_type_coercions.emplace_back(
                    getLeastSupertype(sub_col_types, context->getSettingsRef().allow_extended_type_conversion));
            }
        }
    }

    // 2. prepare sub plan & collect input info
    DataStreams input_streams;
    PlanNodes source_nodes;

    for (size_t select_id = 0; select_id < selects.size(); ++select_id)
    {
        auto & select = selects[select_id];
        auto & sub_plan = sub_plans[select_id];
        const auto & name_to_type = sub_plan_types[select_id];
        // prune invisible columns, copy duplicated columns, sort columns by a specific order(primary columns + sub columns)
        sub_plan = projectFieldSymbols(sub_plan, sub_column_positions);

#ifndef NDEBUG
        auto column_names1 = sub_plan.getRoot()->getOutputNames();
#endif
        // coerce to common type
        if (enable_implicit_type_conversion)
        {
            NameToType symbols_and_types;
            auto field_symbol_infos = sub_plan.getFieldSymbolInfos();

            if (analysis.hasRelationTypeCoercion(*select))
            {
                const auto & target_types = analysis.getRelationTypeCoercion(*select);
                assert(target_types.size() == field_symbol_infos.size());

                for (size_t i = 0; i < target_types.size(); ++i)
                {
                    auto target_type = target_types[i];
                    if (target_type)
                        symbols_and_types.emplace(field_symbol_infos[i].getPrimarySymbol(), target_type);
                }
            }

            if (!sub_column_type_coercions.empty())
            {
                for (size_t pos = 0; pos < sub_column_type_coercions.size(); ++pos)
                {
                    const auto & field_sub_col_id = sub_column_positions.at(pos);
                    const auto & sub_col_symbol
                        = field_symbol_infos.at(field_sub_col_id.first).sub_column_symbols.at(field_sub_col_id.second);
                    auto sub_col_type = name_to_type.at(sub_col_symbol);
                    auto target_type = sub_column_type_coercions[pos];
                    if (!target_type->equals(*sub_col_type))
                        symbols_and_types.emplace(sub_col_symbol, target_type);
                }
            }
            auto coercion_result = coerceTypesForSymbols(sub_plan.getRoot(), symbols_and_types, true);
            mapFieldSymbolInfos(field_symbol_infos, coercion_result.mappings, false);
            sub_plan = RelationPlan{coercion_result.plan, field_symbol_infos};
        }

#ifndef NDEBUG
        auto column_names2 = sub_plan.getRoot()->getOutputNames();

        // check column order unmodified after implicit type conversion
        for (size_t i = 0; i < column_names1.size(); ++i)
        {
            auto it = std::find(column_names2.begin(), column_names2.end(), column_names1.at(i));
            assert(it == column_names2.end() || it - column_names2.begin() == static_cast<long>(i));
        }
#endif
        assert(
            sub_plan.getRoot()->getCurrentDataStream().header.columns() == sub_plans[0].getRoot()->getCurrentDataStream().header.columns());
        assert(sub_plan.getFieldSymbolInfos().size() == sub_plans[0].getFieldSymbolInfos().size());
        source_nodes.push_back(sub_plan.getRoot());
        input_streams.push_back(sub_plan.getRoot()->getCurrentDataStream());
    }

    // 3. build output info
    DataStream output_stream;
    FieldSymbolInfos field_symbols;

    const auto & field_symbol_info = sub_plans[0].getFieldSymbolInfos();
    const auto & columns = sub_plans[0].getRoot()->getCurrentDataStream().header;

    for (size_t i = 0; i < field_symbol_info.size(); ++i)
    {
        const auto & col = columns.getByPosition(i);
        auto new_name = context->getSymbolAllocator()->newSymbol(col.name);
        output_stream.header.insert(ColumnWithTypeAndName{col.type, new_name});
        field_symbols.emplace_back(new_name);
    }

    assert(field_symbol_info.size() + sub_column_positions.size() == columns.columns());
    for (size_t i = 0; i < sub_column_positions.size(); ++i)
    {
        auto & field_sub_col_id = sub_column_positions.at(i);
        const auto & col = columns.getByPosition(field_symbol_info.size() + i);
        auto new_name = context->getSymbolAllocator()->newSymbol(col.name);
        output_stream.header.insert(ColumnWithTypeAndName{col.type, new_name});
        field_symbols.at(field_sub_col_id.first).sub_column_symbols.emplace(field_sub_col_id.second, new_name);
    }

    assert(output_stream.header.columns() == columns.columns());

    // 4. build step
    QueryPlanStepPtr set_operation_step;
    switch (union_mode)
    {
        case ASTSelectWithUnionQuery::Mode::UNION_ALL:
        case ASTSelectWithUnionQuery::Mode::UNION_DISTINCT:
            set_operation_step = std::make_shared<UnionStep>(input_streams, output_stream);
            break;
        case ASTSelectWithUnionQuery::Mode::INTERSECT_ALL:
            set_operation_step = std::make_shared<IntersectStep>(input_streams, output_stream, false);
            break;
        case ASTSelectWithUnionQuery::Mode::INTERSECT_DISTINCT:
            set_operation_step = std::make_shared<IntersectStep>(input_streams, output_stream, true);
            break;
        case ASTSelectWithUnionQuery::Mode::EXCEPT_ALL:
            set_operation_step = std::make_shared<ExceptStep>(input_streams, output_stream, false);
            break;
        case ASTSelectWithUnionQuery::Mode::EXCEPT_DISTINCT:
            set_operation_step = std::make_shared<ExceptStep>(input_streams, output_stream, true);
            break;
        default:
            throw Exception("Unsupported union mode: " + std::to_string(static_cast<UInt8>(union_mode)), ErrorCodes::PLAN_BUILD_ERROR);
    }

    auto set_operation_node = sub_plans[0].getRoot()->addStep(context->nextNodeId(), std::move(set_operation_step), source_nodes);

    if (union_mode == ASTSelectWithUnionQuery::Mode::UNION_DISTINCT)
    {
        // this logic depends on how `projectFieldSymbols` handle subcolumns
        Names distinct_columns;
        for (size_t i = 0; i < field_symbols.size(); ++i)
            distinct_columns.push_back(set_operation_node->getCurrentDataStream().header.getByPosition(i).name);

        auto distinct_step = std::make_shared<DistinctStep>(
            set_operation_node->getCurrentDataStream(), extractDistinctSizeLimits(), 0, distinct_columns, false);

        auto distinct_node = set_operation_node->addStep(context->nextNodeId(), std::move(distinct_step));

        return {distinct_node, std::move(field_symbols)};
    }

    return {set_operation_node, std::move(field_symbols)};
}

PlanWithSymbolMappings
QueryPlannerVisitor::coerceTypesForSymbols(const PlanNodePtr & node, const NameToType & symbol_and_types, bool replace_symbol)
{
    Assignments assignments;
    NameToType output_types;
    NameToNameMap symbol_mappings;
    bool necessary = false;

    for (const auto & input_symbol_and_type : node->getCurrentDataStream().header)
    {
        const auto & input_symbol = input_symbol_and_type.name;
        const auto & input_type = input_symbol_and_type.type;

        if (auto it = symbol_and_types.find(input_symbol); it != symbol_and_types.end() && it->second)
        {
            const auto & output_type = it->second;
            String output_symbol = context->getSymbolAllocator()->newSymbol(input_symbol);
            symbol_mappings.emplace(input_symbol, output_symbol);

            if (!replace_symbol)
            {
                assignments.emplace_back(input_symbol, toSymbolRef(input_symbol));
                output_types[input_symbol] = input_type;
            }

            assignments.emplace_back(output_symbol, makeCastFunction(toSymbolRef(input_symbol), output_type));
            output_types[output_symbol] = output_type;
            necessary = true;
        }
        else
        {
            assignments.emplace_back(input_symbol, toSymbolRef(input_symbol));
            output_types[input_symbol] = input_type;
        }
    }

    if (!necessary)
    {
        return {node, symbol_mappings};
    }

    auto casting_step = std::make_shared<ProjectionStep>(node->getCurrentDataStream(), assignments, output_types);
    auto casting_node = node->addStep(context->nextNodeId(), std::move(casting_step));

    return {casting_node, symbol_mappings};
}

NameToNameMap QueryPlannerVisitor::coerceTypesForSymbols(PlanBuilder & builder, const NameToType & symbol_and_types, bool replace_symbol)
{
    auto plan_with_mapping = coerceTypesForSymbols(builder.getRoot(), symbol_and_types, replace_symbol);
    builder.withNewRoot(plan_with_mapping.plan);
    return plan_with_mapping.mappings;
}

void QueryPlannerVisitor::coerceTypeForSubquery(RelationPlan & plan, const DataTypePtr & type)
{
    NameToType symbol_and_types{{plan.getFirstPrimarySymbol(), type}};
    auto plan_with_mapping = coerceTypesForSymbols(plan.getRoot(), symbol_and_types, true);
    plan.withNewRoot(plan_with_mapping.plan);
    mapFieldSymbolInfos(plan.field_symbol_infos, plan_with_mapping.mappings, false);
}

SizeLimits QueryPlannerVisitor::extractDistinctSizeLimits()
{
    const auto & settings = context->getSettingsRef();
    return {settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode};
}

PlanBuilder QueryPlannerVisitor::toPlanBuilder(const RelationPlan & plan, ScopePtr scope)
{
    auto translation_map = std::make_shared<TranslationMap>(outer_context, scope, plan.field_symbol_infos, analysis, context);
    return {analysis, context->getPlanNodeIdAllocator(), context->getSymbolAllocator(), plan.root, translation_map};
}
std::pair<UInt64, UInt64> QueryPlannerVisitor::getLimitLengthAndOffset(ASTSelectQuery & query)
{
    UInt64 length = 0;
    UInt64 offset = 0;

    if (query.limitLength())
    {
        length = analysis.getLimitLength(query);
        if (query.limitOffset())
            offset = analysis.getLimitOffset(query);
    }

    return {length, offset};
}

void QueryPlannerVisitor::processSubqueryArgs(
    PlanBuilder & builder,
    ASTs & children,
    String & rhs_symbol,
    String & lhs_symbol,
    RelationPlan & rhs_plan,
    ASTSelectQuery & select_query)
{
    //process lhs
    auto & lhs_ast = children.at(0);
    planExpression(builder, select_query, lhs_ast);
    lhs_symbol = builder.translateToSymbol(lhs_ast);

    if (auto coerced_type = analysis.getTypeCoercion(lhs_ast))
    {
        auto mapping = coerceTypesForSymbols(builder, {{lhs_symbol, coerced_type}}, false);
        lhs_symbol = mapping.at(lhs_symbol);
    }

    // process rhs
    auto & rhs_ast = children.at(1);
    rhs_plan = QueryPlanner().planQuery(rhs_ast, builder.translation, analysis, context, cte_plans);
    rhs_plan = combineSubqueryOutputsToTuple(rhs_plan, rhs_ast);

    if (auto coerced_type = analysis.getTypeCoercion(rhs_ast))
    {
        coerceTypeForSubquery(rhs_plan, coerced_type);
    }
    rhs_symbol = rhs_plan.getFirstPrimarySymbol();
}

void QueryPlannerVisitor::addPlanHint(const QueryPlanStepPtr & step, SqlHints & sql_hints, bool check_step_type)
{
    if (sql_hints.empty())
        return;

    SqlHints hint_list;
    auto & hint_info = analysis.getHintInfo();
    for (const auto & hint : sql_hints)
    {
        if (hint_info.leading_hint_count > 1 && Poco::toLower(hint.getName()) == "leading")
            continue;
        hint_list.emplace_back(hint);
    }

    if (!hint_list.empty())
        step->addHints(hint_list, context, check_step_type);
}

bool QueryPlannerVisitor::needAggregateOverflowRow(ASTSelectQuery & select_query) const
{
    const auto & settings = context->getSettingsRef();
    return select_query.group_by_with_totals && settings.max_rows_to_group_by && settings.group_by_overflow_mode == OverflowMode::ANY
        && settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;
}

template <typename T>
void QueryPlannerVisitor::planExpression(PlanBuilder & builder, ASTSelectQuery & select_query, const T & expressions)
{
    if constexpr (std::is_same_v<T, ASTPtr>)
    {
        planExpression(builder, select_query, ASTs{expressions});
        return;
    }
    else
    {
        // subqueries should be planned first, as non-deterministic expressions may contain subqueries
        planSubqueryExpression(builder, select_query, expressions);
        planNonDeterministicFunction(builder, select_query, expressions);
        builder.appendProjection(expressions);
        return;
    }
    __builtin_unreachable();
}

template <typename T>
void QueryPlannerVisitor::planNonDeterministicFunction(PlanBuilder & builder, ASTSelectQuery & select_query, const T & expressions)
{
    if constexpr (std::is_same_v<T, ASTPtr>)
    {
        planNonDeterministicFunction(builder, select_query, ASTs{expressions});
        return;
    }
    else
    {
        // TODO: currently it's hard to get the determinism of a lambda,
        // so if lambda has arrayJoin, it will be computed twice
        ExtractNonDeterministicFunctionVisitor extract_visitor{context};
        ExtractSubqueryTraversalVisitor traversal_visitor{extract_visitor, {}, analysis, context, builder, false};

        for (auto & expr : expressions)
            traversal_visitor.process(const_cast<ASTPtr &>(expr));

        builder.appendProjection(extract_visitor.non_deterministic_functions);
        return;
    }
    __builtin_unreachable();
}
}
