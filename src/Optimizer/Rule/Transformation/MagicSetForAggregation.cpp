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

#include <Optimizer/Rule/Transformation/MagicSetForAggregation.h>

#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <Common/Exception.h>

#include <memory>

namespace DB
{
const std::vector<RuleType> & MagicSetRule::blockRules() const
{
    static std::vector<RuleType> block{RuleType::MAGIC_SET_FOR_AGGREGATION, RuleType::MAGIC_SET_FOR_PROJECTION_AGGREGATION};
    return block;
}

double MagicSetRule::getFilterFactor(
    const PlanNodeStatisticsPtr & source_statistics,
    const PlanNodeStatisticsPtr & filter_statistics,
    const Names & source_names,
    const Names & filter_names)
{
    double filter_factor = 1.0;
    for (size_t i = 0; i < source_names.size(); i++)
    {
        auto source_name_statistic = source_statistics->getSymbolStatistics(source_names[i]);
        auto filter_name_statistic = filter_statistics->getSymbolStatistics(filter_names[i]);
        if (!source_name_statistic->isUnknown() && !filter_name_statistic->isUnknown())
        filter_factor = std::min(filter_factor, static_cast<double>(filter_name_statistic->getNdv()) / source_name_statistic->getNdv());
    }
    return filter_factor;
}

PlanNodePtr MagicSetRule::buildMagicSetAsSemiJoin(
    const PlanNodePtr & source,
    PlanNodePtr filter_source,
    const Names & source_names,
    const Names & filter_names,
    CascadesContext & context)
{
    // TODO: output symbols not reallocate may case duplicate symbol problem, but there is no easy way to do so.
    if (context.getContext()->getSettingsRef().enable_magic_set_cte)
        recordCTERefIntoGroup(filter_source, context);

    auto magic_set_join_step = std::make_shared<JoinStep>(
        DataStreams{source->getStep()->getOutputStream(), filter_source->getStep()->getOutputStream()},
        source->getStep()->getOutputStream(),
        ASTTableJoin::Kind::Left,
        ASTTableJoin::Strictness::Semi,
        context.getContext()->getSettingsRef().max_threads,
        context.getContext()->getSettingsRef().optimize_read_in_order,
        source_names,
        filter_names);
    magic_set_join_step->setMagic(true);

    return PlanNodeBase::createPlanNode(
        context.getContext()->nextNodeId(), std::move(magic_set_join_step), PlanNodes{source, filter_source});
}

void MagicSetRule::recordCTERefIntoGroup(PlanNodePtr plan_node, CascadesContext & context)
{
    if (plan_node->getType() != IQueryPlanStep::Type::Any)
        throw Exception("expected any node", ErrorCodes::LOGICAL_ERROR);

    GroupId target_group_id = dynamic_cast<AnyStep *>(plan_node->getStep().get())->getGroupId();

    // Check existed cte
    GroupPtr target_group = context.getMemo().getGroupById(target_group_id);
    for (const auto & group_expr : target_group->getLogicalExpressions())
    {
        if (group_expr->getStep()->getType() == IQueryPlanStep::Type::CTERef)
            return;
    }

    const auto & output_stream = plan_node->getStep()->getOutputStream();
    std::unordered_map<String, String> output_columns;
    for (const auto & column : output_stream.header.getColumnsWithTypeAndName())
        output_columns.emplace(column.name, column.name);

    auto & cte_info = context.getCTEInfo();
    auto cte_id = cte_info.nextCTEId();
    cte_info.add(cte_id, plan_node);
    context.getMemo().recordCTEDefGroupId(cte_id, target_group_id);
    auto cte_node = PlanNodeBase::createPlanNode(
        context.getContext()->nextNodeId(), std::make_shared<CTERefStep>(output_stream, cte_id, output_columns, false));
    GroupExprPtr group_expr;
    context.recordPlanNodeIntoGroup(cte_node, group_expr, RuleType::MAGIC_SET_FOR_AGGREGATION, target_group_id);
    group_expr->setRuleExplored(RuleType::INLINE_CTE);
}

ConstRefPatternPtr MagicSetForProjectionAggregation::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStep>([](const JoinStep & s) {
            return (s.getKind() == ASTTableJoin::Kind::Inner || s.getKind() == ASTTableJoin::Kind::Right)
                && s.getStrictness() == ASTTableJoin::Strictness::All;
        })
        .with(Patterns::project().withSingle(Patterns::aggregating().withSingle(Patterns::any())), Patterns::any())
        .result();
    return pattern;
}

TransformResult MagicSetForProjectionAggregation::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto & join_step = dynamic_cast<const JoinStep &>(*node->getStep());

    auto magic_set_node = node->getChildren()[1];
    auto magic_set_group_id = dynamic_cast<const AnyStep &>(*magic_set_node->getStep().get()).getGroupId();
    auto magic_set_group = rule_context.optimization_context->getOptimizerContext().getMemo().getGroupById(magic_set_group_id);

    auto & context = rule_context.context;
    if (magic_set_group->getMaxTableScans() > context->getSettingsRef().magic_set_max_search_tree)
    {
        return {};
    }

    auto project_node = node->getChildren()[0];
    const auto & project_step = dynamic_cast<const ProjectionStep &>(*project_node->getStep());

    auto agg_node = project_node->getChildren()[0];

    const auto & target_node = agg_node->getChildren()[0];
    const auto & source_statistics = target_node->getStatistics();
    const auto & filter_statistics = magic_set_node->getStatistics();

    auto target_node_group_id = dynamic_cast<const AnyStep &>(*target_node->getStep().get()).getGroupId();
    auto target_node_group = rule_context.optimization_context->getOptimizerContext().getMemo().getGroupById(target_node_group_id);
    if (magic_set_group->getMaxTableScanRows() > target_node_group->getMaxTableScanRows() * context->getSettingsRef().magic_set_rows_factor)
    {
        return {};
    }

    if (!source_statistics || !filter_statistics
        || source_statistics.value()->getRowCount() * context->getSettingsRef().magic_set_rows_factor
            <= filter_statistics.value()->getRowCount()
        || source_statistics.value()->getRowCount() < context->getSettingsRef().magic_set_source_min_rows)
    {
        return {};
    }

    const auto & agg_step = dynamic_cast<const AggregatingStep &>(*agg_node->getStep());

    NameSet grouping_key{agg_step.getKeys().begin(), agg_step.getKeys().end()};
    std::map<String, String> projection_mapping;
    for (const auto & assignment : project_step.getAssignments())
    {
        if (assignment.second->getType() == ASTType::ASTIdentifier)
        {
            const ASTIdentifier & identifier = assignment.second->as<ASTIdentifier &>();
            if (grouping_key.contains(identifier.name()))
            {
                projection_mapping.emplace(assignment.first, identifier.name());
            }
        }
    }

    Names target_keys;
    Names filter_keys;
    for (auto left_key = join_step.getLeftKeys().begin(), right_key = join_step.getRightKeys().begin();
         left_key != join_step.getLeftKeys().end();
         ++left_key, ++right_key)
    {
        if (projection_mapping.contains(*left_key))
        {
            target_keys.emplace_back(projection_mapping.at(*left_key));
            filter_keys.emplace_back(*right_key);
        }
    }

    double filter_factor = getFilterFactor(source_statistics.value(), filter_statistics.value(), target_keys, filter_keys);
    if (filter_factor > context->getSettingsRef().magic_set_filter_factor)
    {
        return {};
    }

    PlanNodePtr filter_join_node = buildMagicSetAsSemiJoin(
        target_node, node->getChildren()[1], target_keys, filter_keys, rule_context.optimization_context->getOptimizerContext());

    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        join_step.copy(context),
        PlanNodes{
            PlanNodeBase::createPlanNode(
                context->nextNodeId(),
                project_step.copy(context),
                PlanNodes{
                    PlanNodeBase::createPlanNode(context->nextNodeId(), agg_step.copy(context), PlanNodes{filter_join_node}),
                }),
            node->getChildren()[1]});
}

ConstRefPatternPtr MagicSetForAggregation::getPattern() const
{
    static auto pattern = Patterns::join()
        .matchingStep<JoinStep>([](const JoinStep & s) {
            return (s.getKind() == ASTTableJoin::Kind::Inner || s.getKind() == ASTTableJoin::Kind::Right)
                && s.getStrictness() == ASTTableJoin::Strictness::All;
        })
        .with(Patterns::aggregating().withSingle(Patterns::any()), Patterns::any())
        .result();
    return pattern;
}

TransformResult MagicSetForAggregation::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto & join_step = dynamic_cast<const JoinStep &>(*node->getStep());

    auto magic_set_node = node->getChildren()[1];
    auto magic_set_group_id = dynamic_cast<const AnyStep &>(*magic_set_node->getStep().get()).getGroupId();
    auto magic_set_group = rule_context.optimization_context->getOptimizerContext().getMemo().getGroupById(magic_set_group_id);

    auto & context = rule_context.context;
    if (magic_set_group->getMaxTableScans() > context->getSettingsRef().magic_set_max_search_tree)
    {
        return {};
    }

    auto agg_node = node->getChildren()[0];

    auto target_node = agg_node->getChildren()[0];
    auto target_node_group_id = dynamic_cast<const AnyStep &>(*target_node->getStep().get()).getGroupId();
    auto target_node_group = rule_context.optimization_context->getOptimizerContext().getMemo().getGroupById(target_node_group_id);
    if (magic_set_group->getMaxTableScanRows() > target_node_group->getMaxTableScanRows() * context->getSettingsRef().magic_set_rows_factor)
    {
        return {};
    }

    const auto & source_statistics = target_node->getStatistics();
    const auto & filter_statistics = magic_set_node->getStatistics();
    if (!source_statistics || !filter_statistics
        || source_statistics.value()->getRowCount() * context->getSettingsRef().magic_set_rows_factor
            <= filter_statistics.value()->getRowCount()
        || source_statistics.value()->getRowCount() < context->getSettingsRef().magic_set_source_min_rows)
    {
        return {};
    }

    const auto & agg_step = dynamic_cast<const AggregatingStep &>(*agg_node->getStep().get());

    NameSet grouping_key{agg_step.getKeys().begin(), agg_step.getKeys().end()};
    std::vector<String> target_keys;
    std::vector<String> filter_keys;
    for (auto left_key = join_step.getLeftKeys().begin(), right_key = join_step.getRightKeys().begin();
         left_key != join_step.getLeftKeys().end();
         ++left_key, ++right_key)
    {
        if (!grouping_key.contains(*left_key))
        {
            continue;
        }
        target_keys.emplace_back(*left_key);
        filter_keys.emplace_back(*right_key);
    }

    if (target_keys.empty())
    {
        return {};
    }

    double filter_factor = getFilterFactor(source_statistics.value(), filter_statistics.value(), target_keys, filter_keys);
    if (filter_factor > context->getSettingsRef().magic_set_filter_factor)
    {
        return {};
    }

    PlanNodePtr magic_set_join = buildMagicSetAsSemiJoin(
        target_node, node->getChildren()[1], target_keys, filter_keys, rule_context.optimization_context->getOptimizerContext());

    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        node->getStep(),
        PlanNodes{
            PlanNodeBase::createPlanNode(context->nextNodeId(), agg_node->getStep(), PlanNodes{magic_set_join}), node->getChildren()[1]});
}

}
