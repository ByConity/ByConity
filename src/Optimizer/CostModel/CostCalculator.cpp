#include <Optimizer/CostModel/CostCalculator.h>

#include <Optimizer/CostModel/AggregatingCost.h>
#include <Optimizer/CostModel/CTECost.h>
#include <Optimizer/CostModel/ExchangeCost.h>
#include <Optimizer/CostModel/FilterCost.h>
#include <Optimizer/CostModel/JoinCost.h>
#include <Optimizer/CostModel/ProjectionCost.h>
#include <Optimizer/CostModel/TableScanCost.h>
#include <Optimizer/CostModel/ValuesCost.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>

namespace DB
{
PlanCostMap CostCalculator::calculate(QueryPlan & plan, const Context & context)
{
    PlanCostMap plan_cost_map;
    if (!plan.getPlanNode()->getStatistics())
        return plan_cost_map;
    size_t worker_size = WorkerSizeFinder::find(plan, context);
    auto cte_ref_counts = plan.getCTEInfo().collectCTEReferenceCounts(plan.getPlanNode());
    PlanCostVisitor visitor{CostModel{context}, worker_size, plan.getCTEInfo(), cte_ref_counts};
    VisitorUtil::accept(plan.getPlanNode(), visitor, plan_cost_map);
    return plan_cost_map;
}

PlanNodeCost CostCalculator::calculate(
    ConstQueryPlanStepPtr & step,
    const PlanNodeStatisticsPtr & stats,
    const std::vector<PlanNodeStatisticsPtr> & children_stats,
    const Context & context,
    size_t worker_size)
{
    static CostVisitor visitor;
    CostContext cost_context{
        .cost_model = CostModel{context}, .stats = stats, .children_stats = children_stats, .worker_size = worker_size};
    return VisitorUtil::accept(step, visitor, cost_context);
}

PlanNodeCost CostVisitor::visitStep(const IQueryPlanStep &, CostContext &)
{
    return PlanNodeCost::ZERO;
}

PlanNodeCost CostVisitor::visitProjectionStep(const ProjectionStep & step, CostContext & context)
{
    return ProjectionCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitFilterStep(const FilterStep & step, CostContext & context)
{
    return FilterCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitJoinStep(const JoinStep & step, CostContext & cost_context)
{
    return JoinCost::calculate(step, cost_context);
}

PlanNodeCost CostVisitor::visitAggregatingStep(const AggregatingStep & step, CostContext & context)
{
    return AggregatingCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitWindowStep(const WindowStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitMergingAggregatedStep(const MergingAggregatedStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitUnionStep(const UnionStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitIntersectStep(const IntersectStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitExceptStep(const ExceptStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitExchangeStep(const ExchangeStep & step, CostContext & cost_context)
{
    return ExchangeCost::calculate(step, cost_context);
}


PlanNodeCost CostVisitor::visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitTableScanStep(const TableScanStep & step, CostContext & context)
{
    return TableScanCost::calculate(step, context);
}

PlanNodeCost CostVisitor::visitReadNothingStep(const ReadNothingStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitValuesStep(const ValuesStep & step, CostContext & context)
{
    return ValuesCost::calculate(step, context);
}
PlanNodeCost CostVisitor::visitLimitStep(const LimitStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitLimitByStep(const LimitByStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitMergeSortingStep(const MergeSortingStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitPartialSortingStep(const PartialSortingStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitMergingSortedStep(const MergingSortedStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitDistinctStep(const DistinctStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitExtremesStep(const ExtremesStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitApplyStep(const ApplyStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitEnforceSingleRowStep(const EnforceSingleRowStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitAssignUniqueIdStep(const AssignUniqueIdStep & step, CostContext & context)
{
    return visitStep(step, context);
}

PlanNodeCost CostVisitor::visitCTERefStep(const CTERefStep & step, CostContext & context)
{
    return CTECost::calculate(step, context);
}

CostWithCTEReferenceCounts PlanCostVisitor::visitPlanNode(PlanNodeBase & node, PlanCostMap & plan_cost_map)
{
    double cost = 0;
    std::unordered_map<CTEId, UInt64> cte_reference_counts;
    std::vector<PlanNodeStatisticsPtr> children_stats;
    for (auto & child : node.getChildren())
    {
        auto res = VisitorUtil::accept(*child, *this, plan_cost_map);
        cost += res.cost;
        for (auto & item : res.cte_reference_counts)
            cte_reference_counts[item.first] += item.second;
        children_stats.emplace_back(child->getStatistics().value_or(nullptr));
    }

    for (auto itr = cte_reference_counts.begin(); itr != cte_reference_counts.end();)
    {
        // lowest common ancestor for cte
        CTEId cte_id = itr->first;
        if (itr->second == cte_ref_counts.at(cte_id))
        {
            auto res = VisitorUtil::accept(*cte_info.getCTEDef(cte_id), *this, plan_cost_map);
            cost += res.cost;
            for (auto & item : res.cte_reference_counts)
                cte_reference_counts[item.first] += item.second;
            itr = cte_reference_counts.erase(itr);
        }
        else
            ++itr;
    }

    static CostVisitor visitor;
    CostContext cost_context{.cost_model = cost_model, .stats = node.getStatistics().value_or(nullptr),
                             .children_stats = children_stats, .worker_size = worker_size};
    cost += VisitorUtil::accept(node.getStep(), visitor, cost_context).getCost();
    plan_cost_map.emplace(node.getId(), cost);
    return CostWithCTEReferenceCounts{cost, cte_reference_counts};
}

CostWithCTEReferenceCounts PlanCostVisitor::visitCTERefNode(CTERefNode & node, PlanCostMap & plan_cost_map)
{
    const auto * cte_step = dynamic_cast<const CTERefStep *>(node.getStep().get());
    auto res = visitPlanNode(node, plan_cost_map);
    res.cte_reference_counts[cte_step->getId()] += 1;
    return res;
}

}
