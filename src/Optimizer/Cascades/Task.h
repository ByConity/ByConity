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
#include <Optimizer/Cascades/Group.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/Rule/Rule.h>

#include <memory>
#include <utility>

namespace DB
{
class OptimizerTask;
using OptimizerTaskPtr = std::shared_ptr<OptimizerTask>;

class OptimizationContext;
using OptContextPtr = std::shared_ptr<OptimizationContext>;

/*
 *   OptimizeGroup <-- OptimizeInput
 *         |                ^
 *         v                |
 * OptimizeExpression <-> ApplyRule
 *         |
 *         v
 *    ExploreGroup
 *        |  ^
 *        v  |
 *  ExploreExpression <-> ApplyRule
 */
class OptimizerTask : public std::enable_shared_from_this<OptimizerTask>
{
public:
    explicit OptimizerTask(OptContextPtr context_);

    virtual ~OptimizerTask() = default;

    /**
     * Function to execute the task
     */
    virtual void execute() = 0;

    void pushTask(const OptimizerTaskPtr & task);

    const std::vector<RulePtr> & getTransformationRules() const;

    const std::vector<RulePtr> & getImplementationRules() const;

    /**
     * Construct valid rules with their promises for a group expression,
     * promises are used to determine the order of applying the rules. We
     * currently use the promise to enforce that physical rules to be applied
     * before logical rules
     *
     * @param group_expr The group expressions to apply rules
     * @param rules The candidate rule set
     * @param valid_rules The valid rules to apply in the current rule set will be
     *  append to valid_rules, with their promises
     */
    void constructValidRules(const GroupExprPtr & group_expr, const std::vector<RulePtr> & rules, std::vector<RulePtr> & valid_rules);

protected:
    OptContextPtr context;
    LoggerPtr log;
};

class OptimizeGroup : public OptimizerTask
{
public:
    OptimizeGroup(GroupPtr group_, const OptContextPtr & context_) : OptimizerTask(context_), group(std::move(group_)) { }
    void execute() override;

private:
    GroupPtr group;
};

class OptimizeExpression : public OptimizerTask
{
public:
    OptimizeExpression(GroupExprPtr group_expr_, const OptContextPtr & context_)
        : OptimizerTask(context_), group_expr(std::move(group_expr_))
    {
    }
    void execute() override;

private:
    GroupExprPtr group_expr;
};

class ExploreGroup : public OptimizerTask
{
public:
    ExploreGroup(GroupPtr group_, const OptContextPtr & context_) : OptimizerTask(context_), group(std::move(group_)) { }
    void execute() override;

private:
    GroupPtr group;
};

class ExploreExpression : public OptimizerTask
{
public:
    ExploreExpression(GroupExprPtr group_expr_, const OptContextPtr & context_)
        : OptimizerTask(context_), group_expr(std::move(group_expr_))
    {
    }
    void execute() override;

private:
    GroupExprPtr group_expr;
};

class OptimizeInput : public OptimizerTask
{
public:
    OptimizeInput(GroupExprPtr group_expr_, const OptContextPtr & context_)
        : OptimizerTask(context_), group_expr(std::move(group_expr_)) { }
    void execute() override;

/**
     * Enforce remote exchange and local exchange base on required and actual property, 
     * then caluclate cost and update group winner.
     * @param context cost upper bound may be update
     * @param group_expr group expression to optimize
     * @param output_prop output property
     * @param total_cost sum of children cost and group_expr step cost
     * @param input_props input actual properties
     * @param cte_common_ancestor cost of the cte need to be calculated
     * @param actual_intput_cte_props input cte actual properties
     */
    static void enforcePropertyAndUpdateWinner(
        OptContextPtr & context,
        GroupExprPtr group_expr,
        Property output_prop,
        double total_cost,
        const PropertySet & input_props,
        const std::set<CTEId> & cte_common_ancestor,
        const std::vector<std::pair<CTEId, std::pair<Property, double>>> & actual_intput_cte_props);

private:
    /**
     * Determine input properties.
     */
    void initInputProperties();

    /**
     * Init input properties with cte.
     */
    void initPropertiesForCTE(PropertySets & required_properties);

    /**
     * Explore input properties. This function may be called repeatedly
     * until all possible input properties are explored.
     */
    void exploreInputProperties();

    /**
     * Explore and derive input properties considering CTE(id) shared and its property.
     * @param id cte id
     * @param cte_description property for cte
     */
    void addInputPropertiesForCTE(CTEId id, CTEDescription cte_description);

    /**
     * Check whether join input properties satisfy the same handle.
     * If not, correct required input properties will be add into input_properties.
     */
    bool checkJoinInputProperties(const PropertySet & requried_input_props, const PropertySet & actual_input_props);

    /**
     * GroupExpression to optimize
     */
    GroupExprPtr group_expr;

    /**
     * Input properties for children
     */
    PropertySets input_properties;

    /**
     * The CTEs of children groups
     */
    std::vector<std::unordered_set<CTEId>> input_cte_ids;

    /**
     * The CTEs should be explored
     */
    std::set<CTEId> cte_common_ancestor;

    /**
     * Indicate whether cte property is enumerated
     */
    std::set<CTEId> cte_property_enumerated;

    /**
     * Indicate whether cte inline property is enumerated
     */
    bool cte_inlined_enumerated = false;

    /**
     * Explored property for CTE
     */
    std::unordered_map<CTEId, std::unordered_set<CTEDescription, CTEDescriptionHash>> explored_cte_properties;

    /**
     * Current total cost
     */
    double cur_total_cost = 0;

    /**
     * Current stage of enumeration through child groups
     */
    int cur_child_idx = -1;

    /**
     * Indicator of last child group that we waited for optimization
     */
    int prev_child_idx = -1;

    /**
     * Current stage of enumeration through output_input_properties_
     */
    int cur_prop_pair_idx = 0;

    /**
     * Trace elapsed time
     */
    UInt64 elapsed_ns = 0;
};

class OptimizeCTE : public OptimizerTask
{
public:
    OptimizeCTE(GroupExprPtr group_expr_, const OptContextPtr & context_) : OptimizerTask(context_), group_expr(std::move(group_expr_))
    {
    }

    void execute() override;

private:
    /**
     * GroupExpression to optimize
     */
    GroupExprPtr group_expr;

    /**
     * Trace elapsed time
     */
    UInt64 elapsed_ns = 0;
};

class ApplyRule : public OptimizerTask
{
public:
    ApplyRule(GroupExprPtr group_expr_, RulePtr rule_, const OptContextPtr & context_, bool explore_only_ = false)
        : OptimizerTask(context_), group_expr(std::move(group_expr_)), rule(std::move(rule_)), explore_only(explore_only_)
    {
    }
    void execute() override;

private:
    /**
     * GroupExpression to apply rule against
     *
     */
    GroupExprPtr group_expr;

    /**
     * Rule to apply
     */
    RulePtr rule;

    /**
     * Whether explore-only or explore and optimize
     */
    bool explore_only;
};

}
