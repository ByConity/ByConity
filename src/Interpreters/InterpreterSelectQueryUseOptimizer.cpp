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

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/SegmentScheduler.h>
#include <Optimizer/PlanNodeSearcher.h>
#include <Optimizer/PlanOptimizer.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/QueryPlanner.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDistributed.h>
#include "QueryPlan/QueryPlan.h"

namespace DB
{
QueryPlanPtr InterpreterSelectQueryUseOptimizer::buildQueryPlan()
{
        // When interpret sub query, reuse context info, e.g. PlanNodeIdAllocator, SymbolAllocator.
    if (interpret_sub_query) 
    {
        QueryPlanPtr sub_query_plan = std::make_unique<QueryPlan>(sub_plan_ptr, cte_info, context->getPlanNodeIdAllocator());
        PlanOptimizer::optimize(*sub_query_plan, context);
        return sub_query_plan;
    }
    
    context->createPlanNodeIdAllocator();
    context->createSymbolAllocator();
    context->createOptimizerMetrics();

    Stopwatch stage_watch;

    stage_watch.start();
    query_ptr = QueryRewriter::rewrite(query_ptr, context);
    LOG_DEBUG(log, "optimizer stage run time: rewrite, {} ms", stage_watch.elapsedMillisecondsAsDouble());
 
    stage_watch.restart();
    AnalysisPtr analysis = QueryAnalyzer::analyze(query_ptr, context);
    LOG_DEBUG(log, "optimizer stage run time: analyze, {} ms", stage_watch.elapsedMillisecondsAsDouble());
 
    stage_watch.restart();
    QueryPlanPtr query_plan = QueryPlanner::plan(query_ptr, *analysis, context);
    LOG_DEBUG(log, "optimizer stage run time: planning, {} ms", stage_watch.elapsedMillisecondsAsDouble());
 
    stage_watch.restart();
    PlanOptimizer::optimize(*query_plan, context);
    LOG_DEBUG(log, "optimizer stage run time: optimize, {} ms", stage_watch.elapsedMillisecondsAsDouble());

    return query_plan;
}

BlockIO InterpreterSelectQueryUseOptimizer::execute()
{
    Stopwatch stage_watch, total_watch;
    total_watch.start();
    QueryPlanPtr query_plan = buildQueryPlan();

    stage_watch.start();
    QueryPlan plan = PlanNodeToNodeVisitor::convert(*query_plan);

    LOG_DEBUG(log, "optimizer stage run time: plan normalize, {} ms", stage_watch.elapsedMillisecondsAsDouble());
 
    stage_watch.restart();

    PlanSegmentTreePtr plan_segment_tree = std::make_unique<PlanSegmentTree>();
    ClusterInfoContext cluster_info_context{.query_plan = *query_plan, .context = context, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(*query_plan, cluster_info_context);

    plan.allocateLocalTable(context);
    PlanSegmentSplitter::split(plan, plan_segment_context);
    LOG_DEBUG(log, "optimizer stage run time: plan segment split, {} ms", stage_watch.elapsedMillisecondsAsDouble());

    GraphvizPrinter::printPlanSegment(plan_segment_tree, context);
    LOG_DEBUG(log, "optimizer total run time: {} ms", total_watch.elapsedMillisecondsAsDouble());
    return executePlanSegmentTree(plan_segment_tree, context);
}

QueryPlan PlanNodeToNodeVisitor::convert(QueryPlan & query_plan)
{
    QueryPlan plan;
    PlanNodeToNodeVisitor visitor(plan);
    Void c;
    auto * root = VisitorUtil::accept(query_plan.getPlanNode(), visitor, c);
    plan.setRoot(root);

    for (const auto & cte : query_plan.getCTEInfo().getCTEs())
        plan.getCTENodes().emplace(cte.first, VisitorUtil::accept(cte.second, visitor, c));
    return plan;
}

QueryPlan::Node * PlanNodeToNodeVisitor::visitPlanNode(PlanNodeBase & node, Void & c)
{
    if (node.getChildren().empty())
    {
        auto res = QueryPlan::Node{
            .step = std::const_pointer_cast<IQueryPlanStep>(node.getStep()), .children = {}, .id = node.getId()};
        node.setStep(res.step);
        plan.addNode(std::move(res));
        return plan.getLastNode();
    }

    std::vector<QueryPlan::Node *> children;
    for (const auto & item : node.getChildren())
    {
        auto * child = VisitorUtil::accept(*item, *this, c);
        children.emplace_back(child);
    }
    QueryPlan::Node query_plan_node{
        .step = std::const_pointer_cast<IQueryPlanStep>(node.getStep()), .children = children, .id = node.getId()};
    node.setStep(query_plan_node.step);
    plan.addNode(std::move(query_plan_node));
    return plan.getLastNode();
}

PlanSegmentContext ClusterInfoFinder::find(QueryPlan & plan, ClusterInfoContext & cluster_info_context)
{
    ClusterInfoFinder visitor{plan.getCTEInfo()};

    // default schedule to worker cluster
    std::optional<PlanSegmentContext> result = VisitorUtil::accept(plan.getPlanNode(), visitor, cluster_info_context);
    if (result.has_value())
    {
        return result.value();
    }

    // if query is a constant query, like, select 1, schedule to server (coordinator)
    PlanSegmentContext plan_segment_context{
        .context = cluster_info_context.context,
        .query_plan = cluster_info_context.query_plan,
        .query_id = cluster_info_context.context->getCurrentQueryId(),
        .shard_number = 1,
        .cluster_name = "",
        .plan_segment_tree = cluster_info_context.plan_segment_tree.get()};
    return plan_segment_context;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitPlanNode(PlanNodeBase & node, ClusterInfoContext & cluster_info_context)
{
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, cluster_info_context);
        if (result.has_value())
            return result;
    }
    return std::nullopt;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitTableScanNode(TableScanNode & node, ClusterInfoContext & cluster_info_context)
{
    auto source_step = node.getStep();
    const auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(source_step->getStorage().get());
    const auto * cnch_hive = dynamic_cast<StorageCnchHive *>(source_step->getStorage().get());

    if (cnch_table || cnch_hive)
    {
        const auto & worker_group = cluster_info_context.context->getCurrentWorkerGroup();
        PlanSegmentContext plan_segment_context{
            .context = cluster_info_context.context,
            .query_plan = cluster_info_context.query_plan,
            .query_id = cluster_info_context.context->getCurrentQueryId(),
            .shard_number =  worker_group->getShardsInfo().size(),
            .cluster_name = worker_group->getID(),
            .plan_segment_tree = cluster_info_context.plan_segment_tree.get()};
        return plan_segment_context;
    }
    return std::nullopt;
}

std::optional<PlanSegmentContext> ClusterInfoFinder::visitCTERefNode(CTERefNode & node, ClusterInfoContext & cluster_info_context)
{
    const auto * cte = dynamic_cast<const CTERefStep *>(node.getStep().get());
    return cte_helper.accept(cte->getId(), *this, cluster_info_context);
}

}
