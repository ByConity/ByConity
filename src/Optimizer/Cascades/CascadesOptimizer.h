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

#pragma once

#include <Common/Logger.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/Cascades/Memo.h>
#include <Optimizer/CostModel/CostModel.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTEVisitHelper.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanNodeIdAllocator.h>

#include <stack>
#include <unordered_set>

namespace DB
{
class CascadesContext;

class OptimizerTask;
using OptimizerTaskPtr = std::shared_ptr<OptimizerTask>;
using TaskStack = std::stack<OptimizerTaskPtr>;
using CTEDefPropertyRequirements = std::unordered_map<CTEId, std::unordered_set<Property, PropertyHash>>;

class OptimizationContext;
using OptContextPtr = std::shared_ptr<OptimizationContext>;

class CascadesOptimizer : public Rewriter
{
public:
    explicit CascadesOptimizer(bool enable_cbo_ = true): enable_cbo(enable_cbo_) {}

    String name() const override { return "CascadesOptimizer"; }
    static WinnerPtr optimize(GroupId root, CascadesContext & context, const Property & required_prop);
    static PlanNodePtr buildPlanNode(GroupId root, CascadesContext & context, const Property & required_prop);
private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_cascades_optimizer; }
    bool enable_cbo;
};

class CascadesContext
{
public:
    explicit CascadesContext(ContextMutablePtr context_, CTEInfo & cte_info, size_t worker_size_, size_t max_join_size_, bool enable_cbo_);

    GroupExprPtr initMemo(const PlanNodePtr & plan_node);

    bool recordPlanNodeIntoGroup(
        const PlanNodePtr & plan_node,
        GroupExprPtr & group_expr,
        RuleType produce_rule = RuleType::UNDEFINED,
        GroupId target_group = UNDEFINED_GROUP);
    GroupExprPtr makeGroupExpression(const PlanNodePtr & plan_node, RuleType produce_rule = RuleType::UNDEFINED);

    ContextMutablePtr & getContext() { return context; }
    TaskStack & getTaskStack() { return task_stack; }
    Memo & getMemo() { return memo; }
    size_t getWorkerSize() const { return worker_size; }
    LoggerPtr getLog() const { return log; }
    bool isSupportFilter() const { return support_filter; }
    CTEInfo & getCTEInfo() { return cte_info; }
    CTEDefPropertyRequirements & getCTEDefPropertyRequirements() { return cte_property_requirements; }

    const std::vector<RulePtr> & getTransformationRules() const { return transformation_rules; }
    const std::vector<RulePtr> & getImplementationRules() const { return implementation_rules; }

    void trace(const String & task_name, GroupId group_id, RuleType rule_type, UInt64 elapsed_ns);

    String getInfo() const;

    UInt64 getTaskExecutionTimeout() const { return task_execution_timeout; }

    bool isEnablePruning() const { return enable_pruning; }
    bool isEnableAutoCTE() const { return enable_auto_cte; }
    bool isEnableTrace() const { return enable_trace; }
    bool isEnableCbo() const { return enable_cbo; }

    size_t getMaxJoinSize() const { return max_join_size; }

    const CostModel & getCostModel() const { return cost_model; }

private:
    ContextMutablePtr context;
    CTEInfo & cte_info;
    CTEDefPropertyRequirements cte_property_requirements;
    TaskStack task_stack;
    Memo memo;
    std::vector<RulePtr> transformation_rules;
    std::vector<RulePtr> implementation_rules;
    size_t worker_size = 1;
    bool support_filter;
    UInt64 task_execution_timeout;
    bool enable_pruning;
    bool enable_auto_cte;
    bool enable_trace;
    bool enable_cbo;
    size_t max_join_size;
    CostModel cost_model;

    struct Metric
    {
        UInt64 elapsed_ns;
        UInt64 counts;
    };
    std::unordered_map<RuleType, std::unordered_map<String, Metric>> rule_trace;
    
    LoggerPtr log;
};

class OptimizationContext
{
public:
    OptimizationContext(CascadesContext & context_, const Property & required_prop_, double cost_upper_bound_)
        : context(context_), required_prop(required_prop_), cost_upper_bound(cost_upper_bound_)
    {
        if (!context.isEnablePruning())
        {
            cost_upper_bound = std::numeric_limits<double>::max();
        }
    }

    const Property & getRequiredProp() const { return required_prop; }
    double getCostUpperBound() const { return cost_upper_bound; }
    void setCostUpperBound(double cost_upper_bound_)
    {
        if (context.isEnablePruning())
        {
            cost_upper_bound = cost_upper_bound_;
        }
    }
    void pushTask(const OptimizerTaskPtr & task) const { context.getTaskStack().push(task); }

    const std::vector<RulePtr> & getTransformationRules() const { return context.getTransformationRules(); }
    const std::vector<RulePtr> & getImplementationRules() const { return context.getImplementationRules(); }

    CascadesContext & getOptimizerContext() const { return context; }
    Memo & getMemo() { return context.getMemo(); }
    PlanNodeId nextNodeId() { return context.getContext()->nextNodeId(); }

private:
    CascadesContext & context;

    /**
     * Required properties
     */
    Property required_prop;

    /**
     * Cost Upper Bound (for pruning)
     */
    double cost_upper_bound;
};

class WorkerSizeFinder : public PlanNodeVisitor<std::optional<size_t>, const Context>
{
public:
    static size_t find(QueryPlan & query_plan, const Context & context);
    std::optional<size_t> visitPlanNode(PlanNodeBase & node, const Context & context) override;
    std::optional<size_t> visitTableScanNode(TableScanNode & node, const Context & context) override;
    std::optional<size_t> visitCTERefNode(CTERefNode & node, const Context & context) override;

private:
    explicit WorkerSizeFinder(CTEInfo & cte_info_) : cte_info(cte_info_) { }
    CTEInfo & cte_info;
};
}
