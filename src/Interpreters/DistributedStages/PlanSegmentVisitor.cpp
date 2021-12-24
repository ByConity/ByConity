#include <Interpreters/DistributedStages/PlanSegmentVisitor.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Processors/QueryPlan/ExchangeStep.h>
#include <Processors/QueryPlan/RemoteExchangeSourceStep.h>

namespace DB
{

PlanSegmentResult PlanSegmentVisitor::visitPlan(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context)
{
    for (size_t i = 0; i < node->children.size(); ++i)
    {
        auto result_node = visitChild(node->children[i], plan_segment_context);
        if (result_node)
            node->children[i] = result_node;
    }

    return nullptr;
}

PlanSegmentResult PlanSegmentVisitor::visitChild(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context)
{
    return VisitorUtil::accept(node, *this, plan_segment_context);
}

PlanSegmentResult PlanSegmentVisitor::visitExchangeStep(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context)
{
    ExchangeStep * step = dynamic_cast<ExchangeStep *>(node->step.get());

    PlanSegmentInputs inputs;
    for (auto & child : node->children)
    {
        auto plan_segment = createPlanSegment(child, plan_segment_context);
        
        auto input = std::make_shared<PlanSegmentInput>(step->getHeader(), PlanSegmentType::EXCHANGE);
        input->setShufflekeys(step->getSchema().getPartitioningColumns());
        input->setPlanSegmentId(plan_segment->getPlanSegmentId());
        input->setExchangeMode(step->getExchangeMode());

        inputs.push_back(input);
    }
    QueryPlanStepPtr remote_step = std::make_unique<RemoteExchangeSourceStep>(inputs, step->getOutputStream());
    remote_step->setStepDescription(step->getStepDescription());
    QueryPlan::Node remote_node{.step = std::move(remote_step), .children = {}};
    plan_segment_context.query_plan.addNode(std::move(remote_node));
    return plan_segment_context.query_plan.getLastNode();
}

PlanSegment * PlanSegmentVisitor::createPlanSegment(QueryPlan::Node * node, size_t segment_id, PlanSegmentContext & plan_segment_context)
{
    /**   
     * Be careful, after we create a sub_plan, some nodes in the original plan have been deleted and deconstructed. 
     * More precisely, nodes that moved to sub_plan are deleted.
     */ 
    QueryPlan sub_plan = plan_segment_context.query_plan.getSubPlan(node);

    auto plan_segment = std::make_unique<PlanSegment>(segment_id, plan_segment_context.query_id, plan_segment_context.cluster_name);
    plan_segment->setQueryPlan(std::move(sub_plan));
    plan_segment->setContext(plan_segment_context.context);
    plan_segment->setParallelSize(plan_segment_context.shard_number);
    plan_segment->setExchangeParallelSize(plan_segment_context.context->getSettingsRef().exchange_parallel_size);

    PlanSegmentType output_type = segment_id == 0? PlanSegmentType::OUTPUT : PlanSegmentType::EXCHANGE;
    auto output = std::make_shared<PlanSegmentOutput>(plan_segment->getQueryPlan().getRoot()->step->getOutputStream().header, output_type);
    output->setParallelSize(plan_segment_context.shard_number);
    plan_segment->setPlanSegmentOutput(output);

    auto inputs = findInputs(plan_segment->getQueryPlan().getRoot());
    if (inputs.empty())
        inputs.push_back(std::make_shared<PlanSegmentInput>(Block(), PlanSegmentType::SOURCE));

    plan_segment->appendPlanSegmentInputs(inputs);

    plan_segment->setPlanSegmentToQueryPlan(plan_segment->getQueryPlan().getRoot());

    PlanSegmentTree::Node plan_segment_node{.plan_segment = std::move(plan_segment)};
    plan_segment_context.plan_segment_tree->addNode(std::move(plan_segment_node));
    return plan_segment_context.plan_segment_tree->getLastNode()->getPlanSegment();
}

PlanSegment * PlanSegmentVisitor::createPlanSegment(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context)
{
    size_t segment_id = plan_segment_context.getSegmentId();
    auto result_node = VisitorUtil::accept(node, *this, plan_segment_context);
    if (!result_node)
        result_node = node;

    return createPlanSegment(result_node, segment_id, plan_segment_context);
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
            if (child->step->getType() == IQueryPlanStep::Type::RemoteExchangeSource)
            {
                auto child_input = findInputs(child);
                if (child_input.size() != 1)
                    throw Exception("Join step should contain one input in each child", ErrorCodes::LOGICAL_ERROR);
                inputs.insert(inputs.end(), child_input.begin(), child_input.end());
            }
        }
        return inputs;
    }
    else if (auto * remote_step = dynamic_cast<RemoteExchangeSourceStep *>(node->step.get()))
    {
        return remote_step->getInput();
    }
    else if (auto * storage_step = dynamic_cast<ReadFromStorageStep *>(node->step.get()))
    {
        auto input = std::make_shared<PlanSegmentInput>(storage_step->getOutputStream().header, PlanSegmentType::SOURCE);
        input->setStorageID(storage_step->getStorageID());
        return {input};
    }
    else if (auto * merge_tree_step = dynamic_cast<ReadFromMergeTree *>(node->step.get()))
    {
        return {std::make_shared<PlanSegmentInput>(merge_tree_step->getOutputStream().header, PlanSegmentType::SOURCE)};
    }
    else
    {
        for (auto & child : node->children)
        {
            auto input = findInputs(child);
            if (!input.empty())
                return input;
        }
    }

    return {};
}

void PlanSegmentSpliter::rewrite(QueryPlan & query_plan, PlanSegmentContext & plan_segment_context)
{
    PlanSegmentVisitor visitor{};
    visitor.createPlanSegment(query_plan.getRoot(), plan_segment_context);

    std::unordered_map<size_t, PlanSegmentTree::Node *> plan_mapping;

    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        plan_mapping[node.plan_segment->getPlanSegmentId()] = &node;
        if (node.plan_segment->getPlanSegmentOutput()->getPlanSegmentType() == PlanSegmentType::OUTPUT)
            plan_segment_context.plan_segment_tree->setRoot(&node);
    }

    for (auto & node : plan_segment_context.plan_segment_tree->getNodes())
    {
        auto inputs = node.plan_segment->getPlanSegmentInputs();

        for (auto & input : inputs)
        {
            /***
             * SOURCE input has no plan semgnet id and it shouldn't include any child.
             */
            if (input->getPlanSegmentType() != PlanSegmentType::SOURCE)
            {
                auto child_node = plan_mapping[input->getPlanSegmentId()];
                node.children.push_back(child_node);
                child_node->plan_segment->getPlanSegmentOutput()->setShufflekeys(input->getShufflekeys());
                child_node->plan_segment->getPlanSegmentOutput()->setPlanSegmentId(node.plan_segment->getPlanSegmentId());
                child_node->plan_segment->getPlanSegmentOutput()->setExchangeMode(input->getExchangeMode());
            }
        }
    }
}

}
