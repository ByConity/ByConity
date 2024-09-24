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

#include <Optimizer/Cascades/CascadesOptimizer.h>

#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/Cascades/Task.h>
#include <Optimizer/OptimizerMetrics.h>
#include <Optimizer/Property/PropertyEnforcer.h>
#include <Optimizer/Rule/Implementation/SetJoinDistribution.h>
#include <Optimizer/Rule/Rewrite/PullProjectionOnJoinThroughJoin.h>
#include <Optimizer/Rule/Rewrite/PushAggThroughJoinRules.h>
#include <Optimizer/Rule/Transformation/CardinalityBasedJoinReorder.h>
#include <Optimizer/Rule/Transformation/InlineCTE.h>
#include <Optimizer/Rule/Transformation/InnerJoinAssociate.h>
#include <Optimizer/Rule/Transformation/InnerJoinCommutation.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/Rule/Transformation/JoinToMultiJoin.h>
#include <Optimizer/Rule/Transformation/LeftJoinToRightJoin.h>
#include <Optimizer/Rule/Transformation/MagicSetForAggregation.h>
#include <Optimizer/Rule/Transformation/PullOuterJoin.h>
#include <Optimizer/Rule/Transformation/SelectivityBasedJoinReorder.h>
#include <Optimizer/Rule/Transformation/SemiJoinPushDown.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/GraphvizPrinter.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/MultiJoinStep.h>
#include <QueryPlan/PlanPattern.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <Storages/RemoteFile/IStorageCnchFile.h>
#include <Storages/StorageCnchMergeTree.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPTIMIZER_TIMEOUT;
}

void CascadesOptimizer::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    int id = context->getRuleId();
    CascadesContext cascades_context{
        context, plan.getCTEInfo(), WorkerSizeFinder::find(plan, *context), PlanPattern::maxJoinSize(plan, context), enable_cbo};

    auto start = std::chrono::high_resolution_clock::now();
    auto root = cascades_context.initMemo(plan.getPlanNode());

    auto root_id = root->getGroupId();
    auto single = Property{Partitioning{Partitioning::Handle::SINGLE}};
    single.getNodePartitioningRef().setComponent(Partitioning::Component::COORDINATOR);

    WinnerPtr winner;
    try
    {
        winner = optimize(root_id, cascades_context, single);
    }
    catch (...)
    {
        LOG_WARNING(cascades_context.getLog(), "Optimize failed: {}", cascades_context.getInfo());
        GraphvizPrinter::printMemo(cascades_context.getMemo(), root_id, context, std::to_string(id) + "_CascadesOptimizer-Memo-Graph");
        throw;
    }
    LOG_DEBUG(cascades_context.getLog(), cascades_context.getInfo());
    GraphvizPrinter::printMemo(cascades_context.getMemo(), root_id, context, std::to_string(id) + "_CascadesOptimizer-Memo-Graph");

    auto result = buildPlanNode(root_id, cascades_context, single);

    // enforce a gather with keep_order if offloading_with_query_plan enabled
    if (context->getSettingsRef().offloading_with_query_plan)
        result = PropertyEnforcer::enforceOffloadingGatherNode(result, *context);

    plan.getCTEInfo().clear();
    for (const auto & item : winner->getCTEActualProperties())
    {
        auto cte_id = item.first;
        auto cte_def_group = cascades_context.getMemo().getCTEDefGroupByCTEId(cte_id);
        auto cte = buildPlanNode(cte_def_group->getId(), cascades_context, item.second.first);
        plan.getCTEInfo().add(cte_id, cte);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms_int = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG_DEBUG(cascades_context.getLog(), "Cascades use {} ms", ms_int.count());

    plan.update(result);
}

WinnerPtr CascadesOptimizer::optimize(GroupId root_group_id, CascadesContext & context, const Property & required_prop)
{
    auto root_context = std::make_shared<OptimizationContext>(context, required_prop, std::numeric_limits<double>::max());
    auto root_group = context.getMemo().getGroupById(root_group_id);
    context.getTaskStack().push(std::make_shared<OptimizeGroup>(root_group, root_context));

    Stopwatch watch{CLOCK_THREAD_CPUTIME_ID};
    while (!context.getTaskStack().empty())
    {
        auto task = context.getTaskStack().top();
        context.getTaskStack().pop();
        task->execute();

        // Check to see if we have at least one plan, and if we have exceeded our
        // timeout limit
        double duration = watch.elapsedMillisecondsAsDouble();
        if (duration >= context.getTaskExecutionTimeout())
        {
            throw Exception(
                "Cascades exhausted the time limit of " + std::to_string(context.getTaskExecutionTimeout()) + " ms",
                ErrorCodes::OPTIMIZER_TIMEOUT);
        }
        if (context.getTaskStack().size() > 100000)
        {
            throw Exception(
                "Cascades exhausted the task limit of " + std::to_string(100000) + ", there are " + std::to_string(context.getTaskStack().size()) + " tasks",
                ErrorCodes::OPTIMIZER_TIMEOUT);
        }
    }

    return root_group->getBestExpression(required_prop);
}


PlanNodePtr
CascadesOptimizer::buildPlanNode(GroupId root, CascadesContext & context, const Property & required_prop) // NOLINT(misc-no-recursion)
{
    auto group = context.getMemo().getGroupById(root);
    auto winner = group->getBestExpression(required_prop);

    auto input_properties = winner->getRequireChildren();

    PlanNodes children;
    for (size_t index = 0; index < input_properties.size(); ++index)
    {
        auto child = buildPlanNode(winner->getGroupExpr()->getChildrenGroups()[index], context, input_properties[index]);
        children.emplace_back(child);
    }

    return winner->buildPlanNode(context, children);
}

GroupExprPtr CascadesContext::initMemo(const PlanNodePtr & plan_node)
{
    PlanNodes nodes;
    std::queue<PlanNodePtr> queue;
    queue.push(plan_node);

    while (!queue.empty())
    {
        auto node = queue.front();
        for (const auto & child : node->getChildren())
        {
            queue.push(child);
        }
        if (const auto * read_step = dynamic_cast<const CTERefStep *>(node->getStep().get()))
        {
            if (!memo.containsCTEId(read_step->getId()))
            {
                auto cte_expr = initMemo(cte_info.getCTEDef(read_step->getId()));
                memo.recordCTEDefGroupId(read_step->getId(), cte_expr->getGroupId());
            }
        }
        queue.pop();
    }

    GroupExprPtr root_expr;
    recordPlanNodeIntoGroup(plan_node, root_expr, RuleType::INITIAL);
    return root_expr;
}

bool CascadesContext::recordPlanNodeIntoGroup(
    const PlanNodePtr & plan_node, GroupExprPtr & group_expr, RuleType produce_rule, GroupId target_group)
{
    auto new_group_expr = makeGroupExpression(plan_node, produce_rule);
    group_expr = memo.insertGroupExpr(new_group_expr, *this, target_group);
    // if memo exists the same expr, it will return the old expr
    // so it is not equal, and return false
    return group_expr == new_group_expr;
}

GroupExprPtr CascadesContext::makeGroupExpression(const PlanNodePtr & node, RuleType produce_rule)
{
    std::vector<GroupId> child_groups;
    for (auto & child : node->getChildren())
    {
        if (child->getStep()->getType() == IQueryPlanStep::Type::Any)
        {
            // Special case for LEAF
            const auto * const leaf = dynamic_cast<const AnyStep *>(child->getStep().get());
            auto child_group = leaf->getGroupId();
            child_groups.push_back(child_group);
        }
        else
        {
            // Create a GroupExpression for the child
            auto group_expr = makeGroupExpression(child, produce_rule);

            // Insert into the memo (this allows for duplicate detection)
            auto memo_expr = memo.insertGroupExpr(group_expr, *this);
            if (memo_expr == nullptr)
            {
                // Delete if need to (see InsertExpression spec)
                child_groups.push_back(group_expr->getGroupId());
            }
            else
            {
                child_groups.push_back(memo_expr->getGroupId());
            }
        }
    }
    return std::make_shared<GroupExpression>(node->getStep(), std::move(child_groups), produce_rule);
}

CascadesContext::CascadesContext(
    ContextMutablePtr context_, CTEInfo & cte_info_, size_t worker_size_, size_t max_join_size_, bool enable_cbo_)
    : context(context_)
    , cte_info(cte_info_)
    , worker_size(worker_size_)
    , support_filter(context->getSettingsRef().enable_join_graph_support_filter)
    , task_execution_timeout(context->getSettingsRef().cascades_optimizer_timeout)
    , enable_pruning((context->getSettingsRef().enable_cascades_pruning))
    , enable_auto_cte(context->getSettingsRef().cte_mode == CTEMode::AUTO)
    , enable_trace((context->getSettingsRef().log_optimizer_run_time))
    , enable_cbo(enable_cbo_ && context->getSettingsRef().enable_cbo)
    , max_join_size(max_join_size_)
    , cost_model(CostModel(*context_))
    , log(&Poco::Logger::get("CascadesOptimizer"))
{
    LOG_DEBUG(log, "max join size: {}", max_join_size_);
    LOG_DEBUG(log, "worker size: {}", worker_size_);
    implementation_rules.emplace_back(std::make_shared<SetJoinDistribution>());

    if (enable_cbo)
    {
        if (context->getSettingsRef().enable_join_reorder)
        {
            if (context->getSettingsRef().enable_non_equijoin_reorder && max_join_size_ <= context->getSettingsRef().max_graph_reorder_size)
            {
                transformation_rules.emplace_back(std::make_shared<InnerJoinAssociate>());
            }
            transformation_rules.emplace_back(std::make_shared<SemiJoinPushDown>());
            transformation_rules.emplace_back(std::make_shared<JoinEnumOnGraph>(support_filter));
            transformation_rules.emplace_back(std::make_shared<InnerJoinCommutation>());
            if (context->getSettingsRef().heuristic_join_reorder_enumeration_times > 0)
                transformation_rules.emplace_back(std::make_shared<CardinalityBasedJoinReorder>(context->getSettingsRef().max_graph_reorder_size));
            transformation_rules.emplace_back(std::make_shared<SelectivityBasedJoinReorder>(context->getSettingsRef().max_graph_reorder_size));
            transformation_rules.emplace_back(std::make_shared<JoinToMultiJoin>());
        }

        // left join inner join reorder q78, 80
        transformation_rules.emplace_back(std::make_shared<PullLeftJoinThroughInnerJoin>());
        transformation_rules.emplace_back(std::make_shared<PullLeftJoinProjectionThroughInnerJoin>());
        transformation_rules.emplace_back(std::make_shared<PullLeftJoinFilterThroughInnerJoin>());

        transformation_rules.emplace_back(std::make_shared<LeftJoinToRightJoin>());
        transformation_rules.emplace_back(std::make_shared<MagicSetForAggregation>());
        transformation_rules.emplace_back(std::make_shared<MagicSetForProjectionAggregation>());
        transformation_rules.emplace_back(std::make_shared<SemiJoinPushDownProjection>());
        transformation_rules.emplace_back(std::make_shared<SemiJoinPushDownAggregate>());

        if (!cte_info.empty() && enable_auto_cte)
        {
            transformation_rules.emplace_back(std::make_shared<InlineCTE>());
            transformation_rules.emplace_back(std::make_shared<InlineCTEWithFilter>());
        }
    }

        // transformation_rules.emplace_back(std::make_shared<PushAggThroughInnerJoin>());
    }

size_t WorkerSizeFinder::find(QueryPlan & query_plan, const Context & context)
{
    if (context.getSettingsRef().enable_memory_catalog)
        return context.getSettingsRef().memory_catalog_worker_size;

    WorkerSizeFinder visitor{query_plan.getCTEInfo()};
    // default schedule to worker cluster
    std::optional<size_t> result = VisitorUtil::accept(query_plan.getPlanNode(), visitor, context);
    if (result.has_value())
        return result.value();
    return 1;
}

std::optional<size_t> WorkerSizeFinder::visitPlanNode(PlanNodeBase & node, const Context & context)
{
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, context);
        if (result.has_value())
            return result;
    }
    return std::nullopt;
}

std::optional<size_t> WorkerSizeFinder::visitTableScanNode(TableScanNode & node, const Context & context)
{
    const auto storage = node.getStep()->getStorage();
    const auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get());
    const auto * cnch_hive = dynamic_cast<StorageCnchHive *>(storage.get());
    const auto * cnch_file = dynamic_cast<IStorageCnchFile *>(storage.get());

    if (cnch_table || cnch_hive || cnch_file)
    {
        const auto & worker_group = context.getCurrentWorkerGroup();
        return worker_group->getShardsInfo().size();
    }
    return std::nullopt;
}

std::optional<size_t> WorkerSizeFinder::visitCTERefNode(CTERefNode & node, const Context & context)
{
    const auto * step = dynamic_cast<const CTERefStep *>(node.getStep().get());
    return VisitorUtil::accept(cte_info.getCTEDef(step->getId()), *this, context);
}

void CascadesContext::trace(const String & task_name, GroupId /*group_id*/, RuleType rule_type, UInt64 elapsed_ns)
{
    if (!enable_trace)
    {
        return;
    }
    auto & counter = rule_trace[rule_type][task_name];
    counter.elapsed_ns += elapsed_ns;
    counter.counts += 1;
}

String CascadesContext::getInfo() const
{
    std::stringstream ss;

    ss << "Group: " << memo.getGroups().size() << ' ';

    size_t total_logical_expr = 0;
    size_t total_physical_expr = 0;
    for (const auto & group : memo.getGroups())
    {
        total_logical_expr += group->getLogicalExpressions().size();
        total_physical_expr += group->getPhysicalExpressions().size();
    }
    ss << "Logical Expr: " << total_logical_expr << ' ';
    ss << "Physical Expr: " << total_physical_expr << ' ';
    ss << "Total Expr: " << memo.getExprs().size() << ' ';

    if (enable_trace)
    {
        ss << '\n';
        for (const auto & item : implementation_rules)
            if (rule_trace.contains(item->getType()))
                for (const auto & task_to_counter : rule_trace.at(item->getType()))
                    ss << "[" << task_to_counter.first << "] " << item->getName() << ": "
                       << static_cast<double>(task_to_counter.second.elapsed_ns) / 1000000 << " ms / " << task_to_counter.second.counts
                       << " counts" << '\n';

        for (const auto & item : transformation_rules)
            if (rule_trace.contains(item->getType()))
                for (const auto & task_to_counter : rule_trace.at(item->getType()))
                    ss << "[" << task_to_counter.first << "] " << item->getName() << ": "
                       << static_cast<double>(task_to_counter.second.elapsed_ns) / 1000000 << " ms / " << task_to_counter.second.counts
                       << " counts" << '\n';

        std::unordered_map<RuleType, UInt64> rule_to_logical_expression_counts;
        for (const auto & group : memo.getGroups())
        {
            for (const auto & item : group->getLogicalExpressions())
                rule_to_logical_expression_counts[item->getProduceRule()]++;
            for (const auto & item : group->getPhysicalExpressions())
                rule_to_logical_expression_counts[item->getProduceRule()]++;
        }

        for (const auto & item : implementation_rules)
            if (rule_to_logical_expression_counts.contains(item->getType()))
                ss << item->getName() << " produced: " << rule_to_logical_expression_counts[item->getType()] << " logical exprs.\n";

        for (const auto & item : transformation_rules)
            if (rule_to_logical_expression_counts.contains(item->getType()))
                ss << item->getName() << " produced: " << rule_to_logical_expression_counts[item->getType()] << " logical exprs.\n";
    }
    return ss.str();
}
    }
