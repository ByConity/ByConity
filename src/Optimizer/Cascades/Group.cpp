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

#include <Optimizer/Cascades/Group.h>

#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/DataDependency/DataDependencyDeriver.h>
#include <Optimizer/Property/ConstantsDeriver.h>
#include <Optimizer/Rule/Rule.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/Rule/Transformation/JoinReorderUtils.h>
#include <Optimizer/Rule/Transformation/JoinToMultiJoin.h>
#include <QueryPlan/AnyStep.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/MultiJoinStep.h>

namespace DB
{
void Group::addExpression(const GroupExprPtr & expression, CascadesContext & context)
{
    expression->setGroupId(id);

    if (expression->isPhysical())
    {
        physical_expressions.emplace_back(expression);
    }

    if (expression->isLogical())
    {
        logical_expressions.emplace_back(expression);
        if ((expression->getStep()->getType() != IQueryPlanStep::Type::Join
               || !dynamic_cast<const JoinStep &>(*expression->getStep()).supportReorder(context.isSupportFilter())
               || dynamic_cast<const JoinStep &>(*expression->getStep()).isOrdered())
              && expression->getStep()->getType() != IQueryPlanStep::Type::MultiJoin)
        {
            join_sets.insert(JoinSet(id));
            // if this is the top node of one join root.
            for (auto child : expression->getChildrenGroups())
            {
                context.getMemo().getGroupById(child)->is_join_root = true;
                context.getMemo().getGroupById(child)->makeRootJoinInfo(context);
            }
        }

        if (expression->getStep()->getType() == IQueryPlanStep::Type::Join
            || expression->getStep()->getType() == IQueryPlanStep::Type::MultiJoin)
        {
            simple_children = false;
        }


        if (expression->getStep()->getType() == IQueryPlanStep::Type::MultiJoin)
        {
            makeRootJoinInfo(*expression, context);
        }

        if (expression->getStep()->getType() == IQueryPlanStep::Type::TableScan)
        {
            is_table_scan = true;
            max_table_scans = std::max(max_table_scans, 1ul);
            if (stats_derived && statistics.has_value())
            {
                max_table_scan_rows = std::max(max_table_scan_rows, (*statistics)->getRowCount());
            }
        }
        if (expression->getStep()->getType() == IQueryPlanStep::Type::Projection && expression->getChildrenGroups().size() == 1)
        {
            is_table_scan = context.getMemo().getGroupById(expression->getChildrenGroups()[0])->isTableScan();
        }

        if (expression->getStep()->getType() == IQueryPlanStep::Type::CTERef)
        {
            const auto & cte_ref_step = dynamic_cast<const CTERefStep &>(*expression->getStep());
            auto cte_def_group = context.getMemo().getCTEDefGroupByCTEId(cte_ref_step.getId());
            const auto & cte_def_contains_cte_ids = cte_def_group->getCTESet();
            cte_set.emplace(cte_ref_step.getId());
            cte_set.insert(cte_def_contains_cte_ids.begin(), cte_def_contains_cte_ids.end());

            max_table_scans = std::max(max_table_scans, cte_def_group->getMaxTableScans());
            max_table_scan_rows = std::max(max_table_scan_rows, cte_def_group->getMaxTableScanRows());
        }

        UInt64 children_table_scans = 0;
        UInt64 children_table_scan_rows = 0;
        for (auto group_id : expression->getChildrenGroups())
        {
            auto child_group = context.getMemo().getGroupById(group_id);
            for (auto cte_id : child_group->getCTESet())
                cte_set.emplace(cte_id);
            children_table_scans += child_group->getMaxTableScans();
            children_table_scan_rows += child_group->getMaxTableScanRows();
        }
        max_table_scans = std::max(max_table_scans, children_table_scans);
        max_table_scan_rows = std::max(max_table_scan_rows, children_table_scan_rows);
    }

    if (!stats_derived && context.isEnableCbo())
    {
        std::vector<PlanNodeStatisticsPtr> children_stats;
        InclusionDependency inclusion_dependency;
        std::vector<bool> is_table_scans;
        std::vector<double> children_filter_selectivity;
        for (const auto & child : expression->getChildrenGroups())
        {
            inclusion_dependency = inclusion_dependency
                | context.getMemo().getGroupById(child)->getDataDependency().value_or(DataDependency{}).getInclusionDependencyRef();
            children_stats.emplace_back(context.getMemo().getGroupById(child)->getStatistics().value_or(nullptr));
            simple_children &= context.getMemo().getGroupById(child)->isSimpleChildren();
            is_table_scans.emplace_back(context.getMemo().getGroupById(child)->isTableScan());
            double child_filter_selectivity = JoinReorderUtils::computeFilterSelectivity(child, context.getMemo());
            children_filter_selectivity.emplace_back(child_filter_selectivity);
        }
        statistics = CardinalityEstimator::estimate(
            expression->getStep(),
            context.getCTEInfo(),
            children_stats,
            context.getContext(),
            simple_children,
            is_table_scans,
            children_filter_selectivity,
            inclusion_dependency);

        if (expression->getStep()->getType() == IQueryPlanStep::Type::TableScan)
        {
            if (statistics.has_value())
            {
                max_table_scan_rows = std::max(max_table_scan_rows, (*statistics)->getRowCount());
            }
        }
        else if (expression->getStep()->getType() == IQueryPlanStep::Type::CTERef)
        {
            if (!statistics.has_value())
            {
                statistics = context.getMemo()
                                 .getCTEDefGroupByCTEId(dynamic_cast<const CTERefStep *>(expression->getStep().get())->getId())
                                 ->statistics;
            }
        }

        stats_derived = true;
    }

    if (!equivalences)
    {
        if (context.getContext()->getSettingsRef().enable_equivalences)
        {
            std::vector<SymbolEquivalencesPtr> children;
            for (const auto & child : expression->getChildrenGroups())
            {
                children.emplace_back(context.getMemo().getGroupById(child)->getEquivalences());
            }
            equivalences = SymbolEquivalencesDeriver::deriveEquivalences(expression->getStep(), std::move(children));
        }
        else
        {
            equivalences = std::make_shared<SymbolEquivalences>();
        }
    }
    if (!constants.has_value())
    {
        std::vector<Constants> children;
        for (const auto & child : expression->getChildrenGroups())
        {
            children.emplace_back(context.getMemo().getGroupById(child)->getConstants().value_or(Constants{}));
        }
        constants = ConstantsDeriver::deriveConstants(expression->getStep(), children, context.getCTEInfo(), context.getContext());
    }
    if (!data_dependency.has_value())
    {
        std::vector<DataDependency> children;
        for (const auto & child : expression->getChildrenGroups())
        {
            children.emplace_back(context.getMemo().getGroupById(child)->getDataDependency().value_or(DataDependency{}));
        }
        data_dependency
            = DataDependencyDeriver::deriveDataDependency(expression->getStep(), children, context.getCTEInfo(), context.getContext());
    }

    if (context.getMaxJoinSize() > 10 && expression->isLogical())
    {
        if (expression->getStep()->getType() == IQueryPlanStep::Type::Join)
        {
            auto * step = dynamic_cast<JoinStep *>(expression->getStep().get());
            if (JoinToMultiJoin::isSupport(*step) && !expression->hasRuleExplored(RuleType::JOIN_TO_MULTI_JOIN))
            {
                for (const auto & multi_join : JoinToMultiJoin::createMultiJoin(
                         context.getContext(), context, step, id, expression->getChildrenGroups()[0], expression->getChildrenGroups()[1]))
                {
                    GroupExprPtr new_group_expr = nullptr;
                    context.recordPlanNodeIntoGroup(multi_join, new_group_expr, RuleType::JOIN_TO_MULTI_JOIN, id);
                }
                expression->setRuleExplored(RuleType::JOIN_TO_MULTI_JOIN);
            }
        }
    }
}

void Group::makeRootJoinInfo(CascadesContext & context)
{
    for (auto & expression : logical_expressions)
    {
        makeRootJoinInfo(*expression, context);
    }
}

void Group::makeRootJoinInfo(GroupExpression & expression, CascadesContext & context)
{
    if (is_join_root && expression.getStep()->getType() == IQueryPlanStep::Type::MultiJoin
        && expression.getChildrenGroups().size() > context.getContext()->getSettingsRef().max_graph_reorder_size)
    {
        if (join_root_id == 0)
        {
            join_root_id = context.getMemo().nextJoinRootId();
            if (join_root_id >= Memo::MAX_JOIN_ROOT_ID)
            {
                return;
            }
        }
        auto * s = dynamic_cast<MultiJoinStep *>(expression.getStep().get());
        for (auto child_id : s->getGraph().getNodes())
        {
            context.getMemo().setJoinRootId(child_id, join_root_id);
        }
    }
}

bool Group::setExpressionCost(const WinnerPtr & expr, const Property & property)
{
    auto it = lowest_cost_expressions.find(property);
    if (it == lowest_cost_expressions.end())
    {
        // not exist so insert
        lowest_cost_expressions[property] = expr;
        return true;
    }

    if (it->second->getCost() > expr->getCost() + 1e-5)
    {
        // this is lower cost
        it->second = expr;
        return true;
    }

    return false;
}

double Group::getCostLowerBound(const Property & property) const
{
    auto it = cost_lower_bounds.find(property);
    return (it != cost_lower_bounds.end()) ? it->second : -1;
}

void Group::setCostLowerBound(const Property & property, double lower_bound)
{
    cost_lower_bounds[property] = lower_bound;
}

bool Group::hasOptimized(const Property & property) const
{
    return cost_lower_bounds.find(property) != cost_lower_bounds.end();
}

void Group::deleteExpression(const GroupExprPtr & expression)
{
    expression->setDeleted(true);
    for (auto itr = lowest_cost_expressions.begin(); itr != lowest_cost_expressions.end(); ++itr)
    {
        if (itr->second->getGroupExpr() == expression)
        {
            lowest_cost_expressions.erase(itr);
            break;
        }
    }
    // TODO: join_sets
}

void Group::deleteAllExpression()
{
    for (const auto & expr : getLogicalExpressions())
    {
        expr->setDeleted(true);
    }
    for (const auto & expr : getPhysicalExpressions())
    {
        expr->setDeleted(true);
    }
    lowest_cost_expressions.clear();
    cost_lower_bounds.clear();
    join_sets.clear();
}

PlanNodePtr Group::createLeafNode(ContextMutablePtr context) const
{
    auto leaf_step = std::make_shared<AnyStep>(getStep()->getOutputStream(), id);
    return AnyNode::createPlanNode(context->nextNodeId(), std::move(leaf_step));
}


}
