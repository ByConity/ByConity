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

#include <memory>
#include <Optimizer/Rule/Implementation/SetJoinDistribution.h>

#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Rule/Patterns.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
ConstRefPatternPtr SetJoinDistribution::getPattern() const
{
    static auto pattern = Patterns::join()
    .matchingStep<JoinStep>([](const JoinStep & s) { return s.getDistributionType() == DistributionType::UNKNOWN; })
    .with(Patterns::any(), Patterns::any())
    .result();
    return pattern;
}

TransformResult SetJoinDistribution::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    PlanNodes result;
    auto * join_node = dynamic_cast<JoinNode *>(node.get());
    if (!join_node)
        return {};

    const auto & step = *join_node->getStep();

    auto left_group_id = dynamic_cast<const AnyStep *>(node->getChildren()[0]->getStep().get())->getGroupId();
    auto left_stats = context.optimization_context->getMemo().getGroupById(left_group_id)->getStatistics();
    auto right_group_id = dynamic_cast<const AnyStep *>(node->getChildren()[1]->getStep().get())->getGroupId();
    auto right_stats = context.optimization_context->getMemo().getGroupById(right_group_id)->getStatistics();

    bool need_parallel_hash = false;

    auto construct_renode = [&](DistributionType type) -> PlanNodePtr
    {
        auto re_step = std::dynamic_pointer_cast<JoinStep>(node->getStep()->copy(context.context));
        re_step->setDistributionType(type);
        if (need_parallel_hash)
            re_step->setJoinAlgorithm(JoinAlgorithm::PARALLEL_HASH);
        return {PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(re_step), node->getChildren())};
    };

    if (right_stats)
    {
        double max_ndv = -1;
        for (const auto & right_key : step.getRightKeys())
        {
            if (right_stats.value()->getSymbolStatistics().contains(right_key))
            {
                max_ndv = std::max(max_ndv, double(right_stats.value()->getSymbolStatistics(right_key)->getNdv()));
            }
        }

        if (!step.getRightKeys().empty() && right_stats.value()->getRowCount() > context.context->getSettingsRef().parallel_join_threshold)
        {
            need_parallel_hash = true;
        }

        if (max_ndv > context.context->getSettingsRef().max_replicate_build_size
            || right_stats.value()->getRowCount() > context.context->getSettingsRef().max_replicate_shuffle_size)
        {
            return construct_renode(DistributionType::REPARTITION);
        }
    }

    if (step.mustRepartition())
    {
        return {construct_renode(DistributionType::REPARTITION)};
    }

    if (step.mustReplicate())
    {
        return {construct_renode(DistributionType::BROADCAST)};
    }

    // when statistics exists, enum both repartition-join and replicated-join.
    if (left_stats && right_stats)
    {
        if (context.context->getSettingsRef().enum_repartition)
        {
            result.emplace_back(construct_renode(DistributionType::REPARTITION));
        }
        if (context.context->getSettingsRef().enum_replicate)
        {
            result.emplace_back(construct_renode(DistributionType::BROADCAST));
        }
    }
    else
    // when statistics not exists, default enum replicated-join.
    {
        if (context.context->getSettingsRef().enum_replicate_no_stats)
        {
            result.emplace_back(construct_renode(DistributionType::BROADCAST));
        }
        else
        {
            result.emplace_back(construct_renode(DistributionType::REPARTITION));
        }
    }

    return TransformResult{result};
}

}
