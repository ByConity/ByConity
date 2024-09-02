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

#include <Optimizer/Property/PropertyEnforcer.h>

#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/UnionStep.h>
#include "QueryPlan/TableWriteStep.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_ENFORCE;
}

PlanNodePtr
PropertyEnforcer::enforceNodePartitioning(const PlanNodePtr & node, const Property & required, const Property & property, Context & context)
{
    QueryPlanStepPtr step_ptr = enforceNodePartitioning(node->getStep(), required, property, context);
    if (!step_ptr)
    {
        return node;
    }
    auto exchange = PlanNodeBase::createPlanNode(context.nextNodeId(), std::move(step_ptr), std::vector{node});
    return exchange;
}

PlanNodePtr PropertyEnforcer::enforceStreamPartitioning(
    const PlanNodePtr & node, const Property & required, const Property & property, Context & context)
{
    QueryPlanStepPtr step_ptr = enforceStreamPartitioning(node->getStep(), required, property, context);
    auto exchange = PlanNodeBase::createPlanNode(context.nextNodeId(), std::move(step_ptr), std::vector{node});
    return exchange;
}

GroupExprPtr PropertyEnforcer::enforceNodePartitioning(
    const GroupExprPtr & group_expr, const Property & required, const Property & property, const Context & context)
{
    QueryPlanStepPtr step_ptr = enforceNodePartitioning(group_expr->getStep(), required, property, context);
    if (!step_ptr)
    {
        return nullptr;
    }
    std::vector<GroupId> children = {group_expr->getGroupId()};
    auto result = std::make_shared<GroupExpression>(std::move(step_ptr), children);
    return result;
}

GroupExprPtr PropertyEnforcer::enforceStreamPartitioning(
    const GroupExprPtr & group_expr, const Property & required, const Property & property, const Context & context)
{
    QueryPlanStepPtr step_ptr = enforceStreamPartitioning(group_expr->getStep(), required, property, context);
    std::vector<GroupId> children = {group_expr->getGroupId()};
    auto result = std::make_shared<GroupExpression>(std::move(step_ptr), children);
    return result;
}

QueryPlanStepPtr PropertyEnforcer::enforceNodePartitioning(
    QueryPlanStepPtr step, const Property & required, const Property & actual, const Context & context)
{
    const auto & output_stream = step->getOutputStream();
    DataStreams streams{output_stream};
    Partitioning partitioning = required.getNodePartitioning();

    // if the stream is ordered, we need keep order when exchange data.
    bool keep_order = context.getSettingsRef().enable_shuffle_with_order;

    switch (partitioning.getHandle())
    {
        case Partitioning::Handle::SINGLE:
            return std::make_unique<ExchangeStep>(streams, ExchangeMode::GATHER, partitioning, keep_order);
        case Partitioning::Handle::FIXED_BROADCAST:
            return std::make_unique<ExchangeStep>(streams, ExchangeMode::BROADCAST, partitioning, keep_order);
        case Partitioning::Handle::FIXED_ARBITRARY:
            if (partitioning.getComponent() == Partitioning::Component::WORKER
                && actual.getNodePartitioning().getComponent() == Partitioning::Component::COORDINATOR)
            {
                return std::make_unique<ExchangeStep>(streams, ExchangeMode::GATHER, partitioning, keep_order);
            }
            return std::make_unique<ExchangeStep>(streams, ExchangeMode::LOCAL_NO_NEED_REPARTITION, partitioning, keep_order);
        case Partitioning::Handle::ARBITRARY:
            return nullptr;
        case Partitioning::Handle::FIXED_HASH:
        case Partitioning::Handle::BUCKET_TABLE:
            return std::make_unique<ExchangeStep>(streams, ExchangeMode::REPARTITION, partitioning, keep_order);
        default:
            throw Exception("Property Enforce error", ErrorCodes::ILLEGAL_ENFORCE);
    }
}

QueryPlanStepPtr
PropertyEnforcer::enforceStreamPartitioning(QueryPlanStepPtr step, const Property & required, const Property &, const Context &)
{
    DataStreams streams;
    const DataStream & input_stream = step->getOutputStream();
    streams.emplace_back(input_stream);

    Partitioning partitioning = required.getStreamPartitioning();
    switch (partitioning.getHandle())
    {
        case Partitioning::Handle::SINGLE:
            return std::make_unique<UnionStep>(streams, DataStream{}, OutputToInputs{}, 0, true);
        case Partitioning::Handle::FIXED_HASH:
            return std::make_unique<LocalExchangeStep>(streams[0], ExchangeMode::REPARTITION, partitioning);
        case Partitioning::Handle::FIXED_ARBITRARY:
            return std::make_unique<LocalExchangeStep>(streams[0], ExchangeMode::LOCAL_NO_NEED_REPARTITION, partitioning);
        case Partitioning::Handle::ARBITRARY:
            return nullptr;
        default:
            throw Exception("Property Enforce error", ErrorCodes::ILLEGAL_ENFORCE);
}
}

PlanNodePtr PropertyEnforcer::enforceOffloadingGatherNode(const PlanNodePtr & node, Context & context)
{
    // TableWrite or Projection-TableWrite don't need gather
    if (node->getType() == IQueryPlanStep::Type::TableFinish
        || (node->getChildren().size() == 1 && node->getChildren()[0]->getType() == IQueryPlanStep::Type::TableFinish))
        return node;

    // already Exchange
    if (node->getType() == IQueryPlanStep::Type::Exchange
        || (node->getChildren().size() == 1 && node->getChildren()[0]->getType() == IQueryPlanStep::Type::Exchange))
            return node;

    auto * projection = dynamic_cast<ProjectionStep *>(node->getStep().get());
    // add gather before final projection
    if (projection && projection->isFinalProject())
    {
        // offloading_with_query_plan need keep_order
        auto gather_step = std::make_unique<ExchangeStep>(
        DataStreams{node->getChildren()[0]->getStep()->getOutputStream()}, ExchangeMode::GATHER, Partitioning{}, true);
        auto gather_node = PlanNodeBase::createPlanNode(context.nextNodeId(), std::move(gather_step), node->getChildren());
        node->replaceChildren(PlanNodes{gather_node});
        return node;
    }
    
    // offloading_with_query_plan need keep_order
    auto gather_step
        = std::make_unique<ExchangeStep>(DataStreams{node->getStep()->getOutputStream()}, ExchangeMode::GATHER, Partitioning{}, true);
    return PlanNodeBase::createPlanNode(context.nextNodeId(), std::move(gather_step), PlanNodes{node});
}
}
