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

#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/PushIntoTableScanRules.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include "Parsers/ASTSelectQuery.h"

namespace DB
{

namespace
{
    bool isOptimizerProjectionSupportEnabled(RuleContext & rule_context)
    {
        return rule_context.context->getSettingsRef().optimizer_projection_support;
    }

    bool isOptimizerIndexProjectionSupportEnabled(RuleContext & rule_context)
    {
        return rule_context.context->getSettingsRef().optimizer_index_projection_support;
    }
}

ConstRefPatternPtr PushStorageFilter::getPattern() const
{
    static auto pattern = Patterns::filter()
        .withSingle(Patterns::tableScan().matchingStep<TableScanStep>([](const auto & step) {
            // check repeat calls
            const auto & query_info = step.getQueryInfo();
            const auto * select_query = query_info.getSelectQuery();
            return !(query_info.partition_filter || select_query->where());
        }))
        .result();
    return pattern;
}

TransformResult PushStorageFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto table_scan = node->getChildren()[0];

    const auto * filter_step = dynamic_cast<const FilterStep *>(node->getStep().get());
    auto copy_table_step = table_scan->getStep()->copy(rule_context.context);

    // try get statistics of base table
    PlanNodeStatisticsPtr stat;
    if (rule_context.context->getSettingsRef().enable_active_prewhere)
    {
        if (table_scan->getStatistics().has_value())
            stat = table_scan->getStatistics().value();
        else
            stat = TableScanEstimator::estimate(rule_context.context, static_cast<const TableScanStep &>(*table_scan->getStep()));
    }

    // TODO: check repeat calls by checking query_info has been set
    auto remaining_filter
        = pushStorageFilter(dynamic_cast<TableScanStep &>(*copy_table_step), filter_step->getFilter()->clone(), stat, rule_context.context);
    table_scan->setStep(copy_table_step);

    if (PredicateUtils::isTruePredicate(remaining_filter))
        return table_scan;

    if (ASTEquality::ASTEquals()(filter_step->getFilter(), remaining_filter))
        return {};

    auto new_filter_step
        = std::make_shared<FilterStep>(table_scan->getStep()->getOutputStream(), remaining_filter, filter_step->removesFilterColumn());
    return PlanNodeBase::createPlanNode(
        rule_context.context->nextNodeId(), std::move(new_filter_step), PlanNodes{table_scan}, node->getStatistics());
}

ASTPtr PushStorageFilter::pushStorageFilter(TableScanStep & table_step, ASTPtr query_filter, PlanNodeStatisticsPtr storage_statistics, ContextMutablePtr context)
{
    std::unordered_map<String, String> column_to_alias;
    for (const auto & item : table_step.getColumnAlias())
        column_to_alias.emplace(item.first, item.second);
    auto alias_to_column = Utils::reverseMap(column_to_alias);
    ASTs conjuncts = PredicateUtils::extractConjuncts(query_filter);

    // split functions into pushable conjuncts & non-pushable conjuncts
    ASTs pushable_conjuncts;
    ASTs non_pushable_conjuncts;
    {
        auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & conjunct) {
            bool all_in = true;
            auto symbols = SymbolsExtractor::extract(conjunct);
            for (const auto & item : symbols)
                all_in &= alias_to_column.contains(item);

            return all_in && ExpressionDeterminism::isDeterministic(conjunct, context);
        });

        pushable_conjuncts.insert(pushable_conjuncts.end(), conjuncts.begin(), iter);
        non_pushable_conjuncts.insert(non_pushable_conjuncts.end(), iter, conjuncts.end());
    }

    // construct push filter by mapping symbol to origin column
    ASTPtr push_filter;
    {
        auto mapper = SymbolMapper::simpleMapper(alias_to_column);
        std::vector<ConstASTPtr> mapped_pushable_conjuncts;
        for (auto & conjunct : pushable_conjuncts)
            mapped_pushable_conjuncts.push_back(mapper.map(conjunct));

        push_filter = PredicateUtils::combineConjuncts(mapped_pushable_conjuncts);
    }

    // push filter into storage
    if (!PredicateUtils::isTruePredicate(push_filter))
        push_filter = table_step.getStorage()->applyFilter(push_filter, table_step.getQueryInfo(), context, storage_statistics);

    // construnct the remaing filter
    auto mapper = SymbolMapper::simpleMapper(column_to_alias);
    non_pushable_conjuncts.push_back(mapper.map(push_filter));
    return PredicateUtils::combineConjuncts(non_pushable_conjuncts);
}

ConstRefPatternPtr PushLimitIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::limit()
        .matchingStep<LimitStep>([](auto const & limit_step) { return !limit_step.isAlwaysReadTillEnd(); })
        .withSingle(Patterns::tableScan())
        .result();
    return pattern;
}

TransformResult PushLimitIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto * limit_step = dynamic_cast<const LimitStep *>(node->getStep().get());
    auto table_scan = node->getChildren()[0];

    if (limit_step->hasPreparedParam())
        return {};

    auto copy_table_step = table_scan->getStep()->copy(rule_context.context);

    auto table_step = dynamic_cast<TableScanStep *>(copy_table_step.get());
    bool applied = table_step->setLimit(limit_step->getLimitValue() + limit_step->getOffsetValue(), rule_context.context);
    if (!applied)
        return {}; // repeat calls

    table_scan->setStep(copy_table_step);
    node->replaceChildren({table_scan});
    return node;
}


ConstRefPatternPtr PushAggregationIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStep>([](auto & step) { return !step.isGroupingSet(); })
        .withSingle(Patterns::tableScan())
        .result();
    return pattern;
}

TransformResult PushAggregationIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerProjectionSupportEnabled(rule_context))
        return {};

    auto copy_step = node->getChildren()[0]->getStep()->copy(rule_context.context);
    auto *copy_table_step = dynamic_cast<TableScanStep *>(copy_step.get());

    // TODO: combine aggregates if grouping keys are the same
    chassert(copy_table_step != nullptr);
    // coverity[var_deref_model]
    if (copy_table_step->getPushdownAggregation())
        return {};

    copy_table_step->setPushdownAggregation(node->getStep()->copy(rule_context.context));
    copy_table_step->formatOutputStream(rule_context.context);
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

ConstRefPatternPtr PushProjectionIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::project()
        .withSingle(Patterns::tableScan().matchingStep<TableScanStep>([](const auto & step) { return !step.getPushdownAggregation(); }))
        .result();
    return pattern;
}

TransformResult PushProjectionIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerProjectionSupportEnabled(rule_context))
        return {};

    auto copy_step = node->getChildren()[0]->getStep()->copy(rule_context.context);
    auto *copy_table_step = dynamic_cast<TableScanStep *>(copy_step.get());

    // TODO: inline projection
    chassert(copy_table_step != nullptr);
    // coverity[var_deref_model]
    if (copy_table_step->getPushdownProjection())
        return {};

    copy_table_step->setPushdownProjection(node->getStep()->copy(rule_context.context));
    copy_table_step->formatOutputStream(rule_context.context);
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

ConstRefPatternPtr PushFilterIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::filter().withSingle(
               Patterns::tableScan().matchingStep<TableScanStep>(
                   [](const auto & step) { return !step.getPushdownProjection() && !step.getPushdownAggregation(); })).result();
    return pattern;
}

TransformResult PushFilterIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerProjectionSupportEnabled(rule_context))
        return {};

    auto copy_step = node->getChildren()[0]->getStep()->copy(rule_context.context);
    auto *copy_table_step = dynamic_cast<TableScanStep *>(copy_step.get());
    chassert(copy_table_step != nullptr);

    // in case of TableScan has already a pushdown filter, combine them into one
    if (const auto * pushdown_filter_step = copy_table_step->getPushdownFilterCast())
    {
        const auto & old_pushdown_filter = pushdown_filter_step->getFilter();
        const auto & filter_step_filter = dynamic_cast<const FilterStep *>(node->getStep().get())->getFilter();

        auto new_pushdown_filter = PredicateUtils::combineConjuncts(ConstASTs{old_pushdown_filter, filter_step_filter});
        copy_table_step->setPushdownFilter(std::make_shared<FilterStep>(pushdown_filter_step->getInputStreams()[0], new_pushdown_filter));
    }
    else
    {
        copy_table_step->setPushdownFilter(node->getStep()->copy(rule_context.context));
    }

    // coverity[var_deref_model]
    copy_table_step->formatOutputStream(rule_context.context);


    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

ConstRefPatternPtr PushIndexProjectionIntoTableScan::getPattern() const
{
    static auto pattern = Patterns::project().withSingle(
               Patterns::tableScan().matchingStep<TableScanStep>(
                   [](const auto & step) { return !step.getPushdownAggregation() ; })).result();
    return pattern;
}

TransformResult PushIndexProjectionIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!isOptimizerIndexProjectionSupportEnabled(rule_context))
        return {};

    auto * projection_step = dynamic_cast<ProjectionStep *>(node->getStep().get());

    if (projection_step && !projection_step->isIndexProject())
        return {};

    auto copy_step = node->getChildren()[0]->getStep()->copy(rule_context.context);
    auto * copy_table_step = dynamic_cast<TableScanStep *>(copy_step.get());

    if (!dynamic_cast<StorageCnchMergeTree *>(copy_table_step->getStorage().get()))
        return {};

    if (copy_table_step->hasInlineExpressions())
        return {};

    const auto & all_name_to_type = projection_step->getNameToType();

    // split node into two projection a, b.
    // a contains all original assignments, but convert function to identifier about `arraysetcheck`.
    // b contains all output symbols, and `arraysetcheck` with function type.
    // we push b into table_scan, then return a->(table_scan).
    Assignments a_assignments;
    NameToType a_name_to_type;
    Assignments  b_assignments;
    // NameToType b_name_to_type;
    bool projection_a_no_need = true;
    for (const auto & assignment : projection_step->getAssignments())
    {
        if (const auto * func = assignment.second->as<ASTFunction>())
        {
            if (functionCanUseBitmapIndex(*func))
            {
                b_assignments.emplace_back(assignment);
                // b_name_to_type.emplace(assignment.first, all_name_to_type.at(assignment.first));

                // remaining reference arraysetchecks, but convert function to identifier(calculate only occur in function type).
                a_assignments.emplace_back(assignment.first, std::make_shared<ASTIdentifier>(assignment.first));
                a_name_to_type.emplace(assignment.first, all_name_to_type.at(assignment.first));
                continue;
            }
            projection_a_no_need = false;
        }
        a_assignments.emplace_back(assignment);
        a_name_to_type.emplace(assignment.first, all_name_to_type.at(assignment.first));
    }

    if (b_assignments.empty())
        return {};

    // prune simple expressions & map symbols
    auto prepare_index_projection = [&](const Assignments & assignments) {
        Assignments new_assignments;
        auto aliases_to_columns = copy_table_step->getAliasToColumnMap();
        SymbolMapper mapper = SymbolMapper::simpleMapper(aliases_to_columns);

        for (const auto & ass : assignments)
            if (!ass.second->as<ASTIdentifier>())
                new_assignments.emplace_back(ass.first, mapper.map(ass.second));

        return new_assignments;
    };

    if (projection_a_no_need)
    {
        copy_table_step->setInlineExpressions(prepare_index_projection(projection_step->getAssignments()), rule_context.context);
        return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
    }

    copy_table_step->setInlineExpressions(prepare_index_projection(b_assignments), rule_context.context);

    auto table_scan_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
    auto a_projection_step = std::make_shared<ProjectionStep>(table_scan_node->getCurrentDataStream(), a_assignments, a_name_to_type);
    return ProjectionNode::createPlanNode(rule_context.context->nextNodeId(), std::move(a_projection_step), {table_scan_node}, node->getStatistics());
}

}
