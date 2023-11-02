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

#include <Optimizer/Rule/Rewrite/PushIntoTableScanRules.h>

#include <Interpreters/pushFilterIntoStorage.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/TableScanStep.h>

namespace DB
{

namespace
{
    bool isOptimizerProjectionSupportEnabled(RuleContext & rule_context)
    {
        return rule_context.context->getSettingsRef().optimizer_projection_support;
    }
}

PatternPtr PushStorageFilter::getPattern() const
{
    return Patterns::filter()
        .withSingle(Patterns::tableScan().matchingStep<TableScanStep>([](const auto & step) {
            // check repeat calls
            const auto & query_info = step.getQueryInfo();
            const auto * select_query = query_info.getSelectQuery();
            return !(query_info.partition_filter || select_query->where());
        }))
        .result();
}

TransformResult PushStorageFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto table_scan = node->getChildren()[0];

    const auto * filter_step = dynamic_cast<const FilterStep *>(node->getStep().get());
    auto copy_table_step = table_scan->getStep()->copy(rule_context.context);

    // TODO: check repeat calls by checking query_info has been set
    auto remaining_filter
        = pushStorageFilter(dynamic_cast<TableScanStep &>(*copy_table_step), filter_step->getFilter()->clone(), rule_context.context);
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

ASTPtr PushStorageFilter::pushStorageFilter(TableScanStep & table_step, ASTPtr query_filter, ContextPtr context)
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
    {
        auto storage = Utils::getLocalStorage(table_step.getStorage(), context);
        auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());
        push_filter = pushFilterIntoStorage(push_filter, merge_tree, table_step.getQueryInfo(), context);
    }

    // construnct the remaing filter
    auto mapper = SymbolMapper::simpleMapper(column_to_alias);
    non_pushable_conjuncts.push_back(mapper.map(push_filter));
    return PredicateUtils::combineConjuncts(non_pushable_conjuncts);
}

PatternPtr PushLimitIntoTableScan::getPattern() const
{
    return Patterns::limit()
        .matchingStep<LimitStep>([](auto const & limit_step) { return !limit_step.isAlwaysReadTillEnd(); })
        .withSingle(Patterns::tableScan())
        .result();
}

TransformResult PushLimitIntoTableScan::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto * limit_step = dynamic_cast<const LimitStep *>(node->getStep().get());
    auto table_scan = node->getChildren()[0];

    auto copy_table_step = table_scan->getStep()->copy(rule_context.context);

    auto * table_step = dynamic_cast<TableScanStep *>(copy_table_step.get());
    bool applied = table_step->setLimit(limit_step->getLimit() + limit_step->getOffset(), rule_context.context);
    if (!applied)
        return {}; // repeat calls

    table_scan->setStep(copy_table_step);
    node->replaceChildren({table_scan});
    return node;
}


PatternPtr PushAggregationIntoTableScan::getPattern() const
{
    return Patterns::aggregating()
        .matchingStep<AggregatingStep>([](auto & step) { return !step.isGroupingSet(); })
        .withSingle(Patterns::tableScan())
        .result();
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
    copy_table_step->formatOutputStream();
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

PatternPtr PushProjectionIntoTableScan::getPattern() const
{
    return Patterns::project().withSingle(
               Patterns::tableScan().matchingStep<TableScanStep>(
                   [](const auto & step) { return !step.getPushdownAggregation(); })).result();
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
    copy_table_step->formatOutputStream();
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

PatternPtr PushFilterIntoTableScan::getPattern() const
{
    return Patterns::filter().withSingle(
               Patterns::tableScan().matchingStep<TableScanStep>(
                   [](const auto & step) { return !step.getPushdownProjection() && !step.getPushdownAggregation(); })).result();
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
    copy_table_step->formatOutputStream();
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(copy_step), {}, node->getStatistics());
}

}
