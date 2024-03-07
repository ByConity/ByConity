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

#include <cstddef>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/PlanSegmentSourceStep.h>
#include <QueryPlan/ReadNothingStep.h>
#include <QueryPlan/RemoteExchangeSourceStep.h>
#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/ValuesStep.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/RemoteFile/IStorageCnchFile.h>
#include <Storages/StorageMemory.h>

namespace DB
{
void PlanSegmentSplitter::split(QueryPlan & query_plan, PlanSegmentContext & plan_segment_context)
{
    PlanSegmentVisitor visitor{plan_segment_context, query_plan.getCTENodes()};

    size_t exchange_id = 0;
    PlanSegmentVisitorContext split_context{{}, {}, exchange_id};
    visitor.createPlanSegment(query_plan.getRoot(), split_context);

    std::unordered_map<size_t, PlanSegmentTree::Node *> plan_mapping;

    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        plan_mapping[node.plan_segment->getPlanSegmentId()] = &node;
        if (node.plan_segment->getPlanSegmentOutputs()[0]->getPlanSegmentType() == PlanSegmentType::OUTPUT)
            plan_segment_context.plan_segment_tree->setRoot(&node);
    }

    // 1. set segment parallel
    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        auto inputs = node.plan_segment->getPlanSegmentInputs();
        for (auto & input : inputs)
        {
            /***
             * If a output is a gather node, its parallel size is always 1 since we should gather all data.
             */
            if (input->getExchangeMode() == ExchangeMode::GATHER)
            {
                node.plan_segment->setParallelSize(1);
            }
        }
    }

    // 2. set output parallel the same to segment parallel
    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        auto inputs = node.plan_segment->getPlanSegmentInputs();

        for (auto & input : inputs)
        {
            /***
             * SOURCE input has no plan semgnet id and it shouldn't include any child.
             */
            if (input->getPlanSegmentType() != PlanSegmentType::SOURCE && input->getPlanSegmentType() != PlanSegmentType::UNKNOWN)
            {
                auto child_node = plan_mapping[input->getPlanSegmentId()];
                node.children.push_back(child_node);

                for (auto & output : child_node->plan_segment->getPlanSegmentOutputs())
                {
                    if (output->getExchangeId() == input->getExchangeId())
                    {
                        output->setShufflekeys(input->getShufflekeys());
                        output->setPlanSegmentId(node.plan_segment->getPlanSegmentId());
                        output->setExchangeMode(input->getExchangeMode());
                        output->setParallelSize(node.plan_segment->getParallelSize());
                    }
                }
                /**
                 * If a output is a gather node, its parallel size is always 1 since we should gather all data.
                 */
                if (child_node->plan_segment->getPlanSegmentOutput()->getExchangeMode() == ExchangeMode::GATHER)
                {
                    node.plan_segment->setParallelSize(1);
                }
            }
        }
        
    }
}

PlanSegmentResult PlanSegmentVisitor::visitNode(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context)
{
    for (size_t i = 0; i < node->children.size(); ++i)
    {
        auto result_node = visitChild(node->children[i], split_context);
        if (result_node)
            node->children[i] = result_node;
    }

    return nullptr;
}

PlanSegmentResult PlanSegmentVisitor::visitChild(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context)
{
    return VisitorUtil::accept(node, *this, split_context);
}

PlanSegmentResult PlanSegmentVisitor::visitExchangeNode(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context)
{
    ExchangeStep * step = dynamic_cast<ExchangeStep *>(node->step.get());

    PlanSegmentInputs inputs;
    bool is_add_totals = false;
    bool is_add_extremes = false;
    for (auto & child : node->children)
    {
        PlanSegmentVisitorContext child_context{{}, {}, split_context.exchange_id};
        auto plan_segment = createPlanSegment(child, child_context);

        is_add_totals |= child_context.is_add_totals;
        is_add_extremes |= child_context.is_add_extremes;
        auto input = std::make_shared<PlanSegmentInput>(step->getHeader(), PlanSegmentType::EXCHANGE);
        input->setShufflekeys(step->getSchema().getPartitioningColumns());
        input->setPlanSegmentId(plan_segment->getPlanSegmentId());
        input->setExchangeMode(step->getExchangeMode());
        // TODO: Not support one ExchangeStep with multi children yet(multi children can't share one exchange id), we may need to support it later.
        input->setExchangeId(plan_segment->getPlanSegmentOutputs().back()->getExchangeId());
        input->setKeepOrder(step->needKeepOrder());
        inputs.push_back(input);

        if (auto * output = dynamic_cast<PlanSegmentOutput *>(plan_segment->getPlanSegmentOutput().get()))
        {
            output->setKeepOrder(step->needKeepOrder());
        }

        // split_context.inputs.emplace_back(input);
        split_context.children.emplace_back(plan_segment);
    }
    QueryPlanStepPtr remote_step
        = std::make_unique<RemoteExchangeSourceStep>(inputs, step->getOutputStream(), is_add_totals, is_add_extremes);
    remote_step->setStepDescription(step->getStepDescription());
    QueryPlan::Node remote_node{.step = std::move(remote_step), .children = {}, .id = node->id};
    plan_segment_context.query_plan.addNode(std::move(remote_node));
    return plan_segment_context.query_plan.getLastNode();
}

PlanSegmentResult PlanSegmentVisitor::visitCTERefNode(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context)
{
    auto * step = dynamic_cast<CTERefStep *>(node->step.get());
    auto * cte_node = cte_nodes.at(step->getId());
    Block header = cte_node->step->getOutputStream().header;

    PlanSegment * plan_segment;
    ExchangeStep * exchange_step = nullptr;
    if (!cte_plan_segments.contains(step->getId()))
    {
        if (cte_node->step->getType() == IQueryPlanStep::Type::Exchange)
        {
            exchange_step = dynamic_cast<ExchangeStep *>(cte_node->step.get());
            plan_segment = createPlanSegment(cte_node->children[0], split_context);
        }
        else
            plan_segment = createPlanSegment(cte_node, split_context);
        cte_plan_segments.emplace(step->getId(), std::make_pair(plan_segment, exchange_step));
    }
    else
    {
        std::tie(plan_segment, exchange_step) = cte_plan_segments.at(step->getId());
        auto output = std::make_shared<PlanSegmentOutput>(*plan_segment->getPlanSegmentOutput());
        output->setExchangeId(split_context.exchange_id++);
        plan_segment->appendPlanSegmentOutput(output);
    }

    std::shared_ptr<PlanSegmentInput> input = std::make_shared<PlanSegmentInput>(header, PlanSegmentType::EXCHANGE);
    input->setPlanSegmentId(plan_segment->getPlanSegmentId());
    input->setExchangeId(plan_segment->getPlanSegmentOutputs().back()->getExchangeId());
    if (exchange_step)
    {
        input->setShufflekeys(exchange_step->getSchema().getPartitioningColumns());
        input->setExchangeMode(exchange_step->getExchangeMode());
    }
    else
        input->setExchangeMode(ExchangeMode::LOCAL_NO_NEED_REPARTITION);

    // split_context.inputs.emplace_back(input);
    split_context.children.emplace_back(plan_segment);

    QueryPlanStepPtr remote_step = std::make_unique<RemoteExchangeSourceStep>(
        PlanSegmentInputs{input}, step->getOutputStream(), false, false); // with totals is not expected used in queries with multiple table
    remote_step->setStepDescription(step->getStepDescription());
    QueryPlan::Node remote_node{.step = std::move(remote_step), .children = {}, .id = node->id};
    plan_segment_context.query_plan.addNode(std::move(remote_node));

    // add projection to rename symbol
    QueryPlan::Node projection_node{
        .step = step->toProjectionStep(),
        .children = {plan_segment_context.query_plan.getLastNode()},
        .id
        = plan_segment_context.context->getPlanNodeIdAllocator() ? plan_segment_context.context->getPlanNodeIdAllocator()->nextId() : 1};
    plan_segment_context.query_plan.addNode(std::move(projection_node));

    return plan_segment_context.query_plan.getLastNode();
}

PlanSegmentResult PlanSegmentVisitor::visitTotalsHavingNode(QueryPlan::Node * node, PlanSegmentVisitorContext & context)
{
    context.is_add_totals = true;
    return visitNode(node, context);
}

PlanSegmentResult PlanSegmentVisitor::visitExtremesNode(QueryPlan::Node * node, PlanSegmentVisitorContext & context)
{
    context.is_add_extremes = true;
    return visitNode(node, context);
}

PlanSegment * PlanSegmentVisitor::createPlanSegment(QueryPlan::Node * node, size_t segment_id, PlanSegmentVisitorContext & split_context)
{
    /**
     * Be careful, after we create a sub_plan, some nodes in the original plan have been deleted and deconstructed.
     * More precisely, nodes that moved to sub_plan are deleted.
     */
    auto all_inputs = findInputs(node);
    split_context.inputs = all_inputs;

    QueryPlan sub_plan = plan_segment_context.query_plan.getSubPlan(node);
    auto [cluster_name, parallel] = findClusterAndParallelSize(sub_plan.getRoot(), split_context);

    auto plan_segment = std::make_unique<PlanSegment>(segment_id, plan_segment_context.query_id, cluster_name);
    plan_segment->setQueryPlan(std::move(sub_plan));
    plan_segment->setExchangeParallelSize(plan_segment_context.context->getSettingsRef().exchange_parallel_size);

    PlanSegmentType output_type = segment_id == 0 ? PlanSegmentType::OUTPUT : PlanSegmentType::EXCHANGE;

    auto output = std::make_shared<PlanSegmentOutput>(plan_segment->getQueryPlan().getRoot()->step->getOutputStream().header, output_type);
    if (output_type == PlanSegmentType::OUTPUT)
    {
        plan_segment->setParallelSize(1);
        output->setParallelSize(1);
    }
    else
    {
        plan_segment->setParallelSize(parallel);
        if (output->getExchangeMode() == ExchangeMode::GATHER)
            output->setParallelSize(1);
        else
            output->setParallelSize(parallel);
    }
    output->setExchangeParallelSize(plan_segment_context.context->getSettingsRef().exchange_parallel_size);
    output->setExchangeId(split_context.exchange_id++);
    plan_segment->appendPlanSegmentOutput(output);

    auto inputs = findInputs(plan_segment->getQueryPlan().getRoot());
    if (inputs.empty())
        inputs.push_back(std::make_shared<PlanSegmentInput>(Block(), PlanSegmentType::UNKNOWN));
    for (auto & input : inputs)
        input->setExchangeParallelSize(plan_segment_context.context->getSettingsRef().exchange_parallel_size);

    if (inputs[0]->getExchangeMode() == ExchangeMode::GATHER)
        plan_segment->setParallelSize(1);

    plan_segment->appendPlanSegmentInputs(inputs);

    PlanSegmentTree::Node plan_segment_node{.plan_segment = std::move(plan_segment)};
    plan_segment_context.plan_segment_tree->addNode(std::move(plan_segment_node));
    return plan_segment_context.plan_segment_tree->getLastNode()->getPlanSegment();
}

PlanSegment * PlanSegmentVisitor::createPlanSegment(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context)
{
    size_t segment_id = plan_segment_context.getSegmentId();
    auto result_node = VisitorUtil::accept(node, *this, split_context);
    if (!result_node)
        result_node = node;

    return createPlanSegment(result_node, segment_id, split_context);
}

PlanSegmentInputs PlanSegmentVisitor::findInputs(QueryPlan::Node * node)
{
    if (!node)
        return {};

    if (auto * join_step = dynamic_cast<JoinStep *>(node->step.get()))
    {
        PlanSegmentInputs inputs;
        for (auto & child : node->children)
        {
            //            if (child->step->getType() == IQueryPlanStep::Type::RemoteExchangeSource)
            //            {
            auto child_input = findInputs(child);
            //            if (child_input.size() != 1)
            //                throw Exception("Join step should contain one input in each child", ErrorCodes::LOGICAL_ERROR);
            inputs.insert(inputs.end(), child_input.begin(), child_input.end());
            //            }
        }
        return inputs;
    }
    else if (auto * remote_step = dynamic_cast<RemoteExchangeSourceStep *>(node->step.get()))
    {
        return remote_step->getInput();
    }
    else if (auto * source_step = dynamic_cast<PlanSegmentSourceStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(source_step->getOutputStream().header, PlanSegmentType::SOURCE);
        input->setStorageID(source_step->getStorageID());
        return {input};
    }
    else if (auto * table_scan_step = dynamic_cast<TableScanStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(table_scan_step->getOutputStream().header, PlanSegmentType::SOURCE);
        input->setStorageID(table_scan_step->getStorageID());
        return {input};
    }
    else if (auto * read_nothing = dynamic_cast<ReadNothingStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(read_nothing->getOutputStream().header, PlanSegmentType::SOURCE);
        return {input};
    }
    else if (auto * values = dynamic_cast<ValuesStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(values->getOutputStream().header, PlanSegmentType::SOURCE);
        return {input};
    }
    else if (auto * read_row_count_step = dynamic_cast<ReadStorageRowCountStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(read_row_count_step->getOutputStream().header, PlanSegmentType::SOURCE);
        return {input};
    }
    else
    {
        PlanSegmentInputs inputs;
        for (auto & child : node->children)
        {
            auto sub_input = findInputs(child);
            inputs.insert(inputs.end(), sub_input.begin(), sub_input.end());
        }
        return inputs;
    }
}

std::pair<String, size_t> PlanSegmentVisitor::findClusterAndParallelSize(QueryPlan::Node * node, PlanSegmentVisitorContext & split_context)
{
    // if (split_context.coordinator)
    //     return {"", 1}; // dispatch to coordinator if server is empty
    bool input_has_table = false;
    for (auto & input : split_context.inputs)
    {
        if (input->getPlanSegmentType() == PlanSegmentType::SOURCE)
        {
            input_has_table = true;
            break;
        }
    }

    auto partitioning = SourceNodeFinder::find(node, *plan_segment_context.context);
    switch (partitioning)
    {
        case Partitioning::Handle::COORDINATOR:
            return {"", 1}; // dispatch to coordinator if server is empty
        case Partitioning::Handle::SINGLE:
            return {plan_segment_context.cluster_name, 1};
        case Partitioning::Handle::FIXED_PASSTHROUGH:
            for (auto & input : split_context.inputs)
            {
                if (input->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION)
                {
                    size_t input_segment_id = input->getPlanSegmentId();
                    for (auto & child_segment : split_context.children)
                        if (input_segment_id == child_segment->getPlanSegmentId())
                            return {plan_segment_context.cluster_name, child_segment->getParallelSize()};
                }
            }
            break;
        case Partitioning::Handle::FIXED_HASH: {
            /// if all input are not table type, parallel size should respect distributed_max_parallel_size setting
            size_t max_parallel_size = plan_segment_context.context->getSettingsRef().distributed_max_parallel_size;
            if (!input_has_table && !split_context.inputs.empty())
            {
                size_t ret = plan_segment_context.shard_number;
                if (max_parallel_size > 0 || plan_segment_context.health_parallel)
                {
                    if (max_parallel_size > 0 && max_parallel_size < ret)
                        ret = max_parallel_size;
                    if (plan_segment_context.health_parallel && *plan_segment_context.health_parallel < ret)
                        ret = *plan_segment_context.health_parallel;
                    // In bsp mode, we ignore the number of health node.
                    if (plan_segment_context.context->getSettingsRef().bsp_mode && max_parallel_size > ret)
                        ret = max_parallel_size;
                    return {plan_segment_context.cluster_name, ret};
                }
            }
            /// Respect distributed_max_parallel_size in bsp mode.
            if (max_parallel_size > 0 && max_parallel_size > plan_segment_context.shard_number)
                return {plan_segment_context.cluster_name, max_parallel_size};
            else
                return {plan_segment_context.cluster_name, plan_segment_context.shard_number};
        }
        default:
            break;
    }
    throw Exception("Unknown partition for PlanSegmentSplitter", ErrorCodes::LOGICAL_ERROR);
}

Partitioning::Handle SourceNodeFinder::find(QueryPlan::Node * node, const Context & context)
{
    SourceNodeFinder visitor;
    auto result = VisitorUtil::accept(node, visitor, context);
    return result.value_or(Partitioning::Handle::FIXED_HASH);
}

std::optional<Partitioning::Handle> SourceNodeFinder::visitNode(QueryPlan::Node * node, const Context & context)
{
    for (const auto & child : node->children)
    {
        if (auto result = VisitorUtil::accept(child, *this, context))
            return result;
    }
    return {};
}

std::optional<Partitioning::Handle> SourceNodeFinder::visitValuesNode(QueryPlan::Node *, const Context &)
{
    return Partitioning::Handle::SINGLE;
}

std::optional<Partitioning::Handle> SourceNodeFinder::visitReadNothingNode(QueryPlan::Node *, const Context &)
{
    return Partitioning::Handle::SINGLE;
}

std::optional<Partitioning::Handle> SourceNodeFinder::visitReadStorageRowCountNode(QueryPlan::Node *, const Context &)
{
    return Partitioning::Handle::COORDINATOR;
}

std::optional<Partitioning::Handle> SourceNodeFinder::visitTableScanNode(QueryPlan::Node * node, const Context & context)
{
    auto * source_step = dynamic_cast<TableScanStep *>(node->step.get());
    // check is bucket table instead of cnch table?
    if (source_step->getStorage()->supportsDistributedRead())
        return Partitioning::Handle::FIXED_HASH;

    // hack for unittest
    else if (context.getSettingsRef().enable_memory_catalog)
        if (auto memory_tree = dynamic_pointer_cast<StorageMemory>(source_step->getStorage()))
            return Partitioning::Handle::FIXED_HASH;
    // if source node is not cnch table, schedule to coordinator. eg, system tables.
    return Partitioning::Handle::COORDINATOR;
}

std::optional<Partitioning::Handle> SourceNodeFinder::visitRemoteExchangeSourceNode(QueryPlan::Node * node, const Context &)
{
    const auto * source_step = dynamic_cast<RemoteExchangeSourceStep *>(node->step.get());
    for (const auto & input : source_step->getInput())
    {
        switch (input->getExchangeMode())
        {
            case ExchangeMode::GATHER:
                return Partitioning::Handle::SINGLE;
            case ExchangeMode::BROADCAST:
                continue;
            case ExchangeMode::REPARTITION:
                return Partitioning::Handle::FIXED_HASH;
            case ExchangeMode::LOCAL_NO_NEED_REPARTITION:
                return Partitioning::Handle::FIXED_PASSTHROUGH;
            case ExchangeMode::LOCAL_MAY_NEED_REPARTITION:
            case ExchangeMode::UNKNOWN:
                throw Exception("Unknown exchange mode", ErrorCodes::LOGICAL_ERROR);
        }
    }
    return {};
}

}
