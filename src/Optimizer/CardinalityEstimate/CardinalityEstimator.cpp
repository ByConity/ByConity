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

#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>

#include <Optimizer/CardinalityEstimate/AggregateEstimator.h>
#include <Optimizer/CardinalityEstimate/AssignUniqueIdEstimator.h>
#include <Optimizer/CardinalityEstimate/EnforceSingleRowEstimator.h>
#include <Optimizer/CardinalityEstimate/ExchangeEstimator.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/JoinEstimator.h>
#include <Optimizer/CardinalityEstimate/LimitEstimator.h>
#include <Optimizer/CardinalityEstimate/ProjectionEstimator.h>
#include <Optimizer/CardinalityEstimate/SampleEstimator.h>
#include <Optimizer/CardinalityEstimate/SortingEstimator.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Optimizer/CardinalityEstimate/UnionEstimator.h>
#include <Optimizer/CardinalityEstimate/WindowEstimator.h>
#include <Optimizer/PredicateUtils.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <QueryPlan/QueryPlan.h>
#include <Functions/InternalFunctionRuntimeFilter.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED; // NOLINT
}

std::optional<PlanNodeStatisticsPtr> CardinalityEstimator::estimate(
    QueryPlanStepPtr & step,
    CTEInfo & cte_info,
    std::vector<PlanNodeStatisticsPtr> children_stats,
    ContextMutablePtr context,
    bool simple_children,
    std::vector<bool> children_are_table_scan,
    const InclusionDependency & inclusion_dependency)
{
    static CardinalityVisitor visitor;
    CardinalityContext cardinality_context{
        .context = context,
        .cte_info = cte_info,
        .children_stats = std::move(children_stats),
        .simple_children = simple_children,
        .children_are_table_scan = std::move(children_are_table_scan),
        .inclusion_dependency = inclusion_dependency};
    auto stats = VisitorUtil::accept(step, visitor, cardinality_context);
    return stats ? std::make_optional(stats) : std::nullopt;
}

std::optional<PlanNodeStatisticsPtr>
CardinalityEstimator::estimate(PlanNodeBase & node, CTEInfo & cte_info, ContextMutablePtr context, bool recursive, bool re_estimate)
{
    auto statistics = node.getStatistics();
    if (statistics.isDerived() && !recursive)
        return statistics.getStatistics();

    PlanCardinalityVisitor visitor{cte_info};
    CardinalityContext cardinality_context{.context = context, .cte_info = cte_info, .children_stats = {}, .re_estimate = re_estimate};
    auto stats = VisitorUtil::accept(node, visitor, cardinality_context);
    return stats ? std::make_optional(stats) : std::nullopt;
}

void CardinalityEstimator::estimate(QueryPlan & node, ContextMutablePtr context, bool re_estimate)
{
    estimate(*node.getPlanNode(), node.getCTEInfo(), context, true, re_estimate);
}

PlanNodeStatisticsPtr CardinalityVisitor::visitStep(const IQueryPlanStep &, CardinalityContext & context)
{
    //    throw Exception("Not impl card estimate", ErrorCodes::NOT_IMPLEMENTED);
    return context.children_stats[0];
}

PlanNodeStatisticsPtr CardinalityVisitor::visitOffsetStep(const OffsetStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = LimitEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTotalsHavingStep(const TotalsHavingStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTableFinishStep(const TableFinishStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;

}
PlanNodeStatisticsPtr CardinalityVisitor::visitBufferStep(const BufferStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;

}
PlanNodeStatisticsPtr CardinalityVisitor::visitMarkDistinctStep(const MarkDistinctStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;

}
PlanNodeStatisticsPtr CardinalityVisitor::visitIntersectOrExceptStep(const IntersectOrExceptStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}
PlanNodeStatisticsPtr CardinalityVisitor::visitTableWriteStep(const TableWriteStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitOutfileWriteStep(const OutfileWriteStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitOutfileFinishStep(const OutfileFinishStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFinalSampleStep(const FinalSampleStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return SampleEstimator::estimate(child_stats, step);
}

PlanNodeStatisticsPtr CardinalityVisitor::visitProjectionStep(const ProjectionStep & step, CardinalityContext & context)
{
    if (context.children_stats.empty())
    {
        return std::make_shared<PlanNodeStatistics>(0);
    }

    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = ProjectionEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFilterStep(const FilterStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = FilterEstimator::estimate(child_stats, step, context.context, context.simple_children);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitJoinStep(const JoinStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr left_child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr right_child_stats = context.children_stats[1];
    PlanNodeStatisticsPtr stats = JoinEstimator::estimate(
        left_child_stats,
        right_child_stats,
        step,
        *context.context,
        context.children_are_table_scan[0],
        context.children_are_table_scan[1],
        context.inclusion_dependency);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitArrayJoinStep(const ArrayJoinStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitAggregatingStep(const AggregatingStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AggregateEstimator::estimate(child_stats, step, context.context);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitWindowStep(const WindowStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = WindowEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMergingAggregatedStep(const MergingAggregatedStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AggregateEstimator::estimate(child_stats, step, context.context);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitUnionStep(const UnionStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr stats = UnionEstimator::estimate(context.children_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitIntersectStep(const IntersectStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitExceptStep(const ExceptStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitExchangeStep(const ExchangeStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr stats = ExchangeEstimator::estimate(context.children_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep &, CardinalityContext &)
{
    throw Exception("RemoteExchangeSourceNode should not run here", ErrorCodes::NOT_IMPLEMENTED);
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTableScanStep(const TableScanStep & step, CardinalityContext & card_context)
{
    PlanNodeStatisticsPtr stats = TableScanEstimator::estimate(card_context.context, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitReadNothingStep(const ReadNothingStep &, CardinalityContext &)
{
    return std::make_shared<PlanNodeStatistics>();
}

PlanNodeStatisticsPtr CardinalityVisitor::visitReadStorageRowCountStep(const ReadStorageRowCountStep &, CardinalityContext &)
{
    return std::make_shared<PlanNodeStatistics>();
}

PlanNodeStatisticsPtr CardinalityVisitor::visitValuesStep(const ValuesStep & step, CardinalityContext &)
{
    return std::make_shared<PlanNodeStatistics>(step.getRows());
}

PlanNodeStatisticsPtr CardinalityVisitor::visitLimitStep(const LimitStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = LimitEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitLimitByStep(const LimitByStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = LimitEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFinishSortingStep(const FinishSortingStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitSortingStep(const SortingStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMergeSortingStep(const MergeSortingStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitPartialSortingStep(const PartialSortingStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMergingSortedStep(const MergingSortedStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = SortingEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitPartitionTopNStep(const PartitionTopNStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

//PlanNodeStatisticsPtr CardinalityVisitor::visitMaterializingStep(const MaterializingStep &, CardinalityContext &)
//{
//    throw Exception("MaterializingNode current not support", ErrorCodes::NOT_IMPLEMENTED);
//}
//
//PlanNodeStatisticsPtr CardinalityVisitor::visitDecompressionStep(const DecompressionStep &, CardinalityContext &)
//{
//    throw Exception("DecompressionNode current not support", ErrorCodes::NOT_IMPLEMENTED);
//}

PlanNodeStatisticsPtr CardinalityVisitor::visitDistinctStep(const DistinctStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AggregateEstimator::estimate(child_stats, step, context.context);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitExtremesStep(const ExtremesStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

//PlanNodeStatisticsPtr CardinalityVisitor::visitFinalSamplingStep(const FinalSamplingStep &, CardinalityContext &)
//{
//    throw Exception("FinalSamplingNode current not support", ErrorCodes::NOT_IMPLEMENTED);
//}

PlanNodeStatisticsPtr CardinalityVisitor::visitApplyStep(const ApplyStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitCTERefStep(const CTERefStep & step, CardinalityContext & context)
{
    auto cte_def = context.cte_info.getCTEDef(step.getId());
    auto result = CardinalityEstimator::estimate(*cte_def, context.cte_info, context.context);

    if (!result)
        return nullptr;

    auto & stats = result.value();
    std::unordered_map<String, SymbolStatisticsPtr> calculated_symbol_statistics;
    for (const auto & item : step.getOutputColumns())
        calculated_symbol_statistics[item.first] = stats->getSymbolStatistics(item.second);
    return std::make_shared<PlanNodeStatistics>(stats->getRowCount(), std::move(calculated_symbol_statistics));
}

PlanNodeStatisticsPtr CardinalityVisitor::visitEnforceSingleRowStep(const EnforceSingleRowStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = EnforceSingleRowEstimator::estimate(child_stats, step);
    return stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitAssignUniqueIdStep(const AssignUniqueIdStep & step, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    PlanNodeStatisticsPtr stats = AssignUniqueIdEstimator::estimate(child_stats, step);
    return stats;
}

// PlanNodeStatisticsPtr CardinalityVisitor::visitGlobalDecodeStep(const GlobalDecodeStep &, CardinalityContext & context)
// {
//     PlanNodeStatisticsPtr child_stats = context.children_stats[0];
//     return child_stats;
// }

PlanNodeStatisticsPtr CardinalityVisitor::visitExplainAnalyzeStep(const ExplainAnalyzeStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitTopNFilteringStep(const TopNFilteringStep &, CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitMultiJoinStep(const MultiJoinStep & , CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr CardinalityVisitor::visitFillingStep(const FillingStep & , CardinalityContext & context)
{
    PlanNodeStatisticsPtr child_stats = context.children_stats[0];
    return child_stats;
}

PlanNodeStatisticsPtr PlanCardinalityVisitor::visitPlanNode(PlanNodeBase & node, CardinalityContext & context)
{
    static CardinalityVisitor visitor;

    std::vector<PlanNodeStatisticsPtr> children_stats;
    bool simple_children = true;
    bool is_table_scan = node.getStep()->getType() == IQueryPlanStep::Type::TableScan;
    std::vector<bool> children_are_table_scan;

    for (auto & child : node.getChildren())
    {
        CardinalityContext children_context{.context = context.context, .cte_info = context.cte_info, .children_stats = {}};
        children_stats.emplace_back(VisitorUtil::accept(*child, *this, children_context));

        simple_children &= children_context.simple_children;
        children_are_table_scan.emplace_back(children_context.is_table_scan);
        if (node.getStep()->getType() == IQueryPlanStep::Type::Projection)
        {
            is_table_scan = children_context.is_table_scan;
        }
        
        // ignore runtime filter 
        if (node.getStep()->getType() == IQueryPlanStep::Type::Filter)
        {
            const FilterStep * step = dynamic_cast<const FilterStep *>(node.getStep().get());
            bool all_runtime_filters = true;
            for (auto & conjunct : PredicateUtils::extractConjuncts(step->getFilter()))
            {
                // $runtimeFilter(1265, `ws_sold_date_sk`, 0.8028399781540142)
                if (conjunct->getColumnName().find(InternalFunctionRuntimeFilter::name) == std::string::npos)
                {
                    all_runtime_filters = false;
                }
            }
            if (all_runtime_filters) 
            {
                is_table_scan = children_context.is_table_scan;
            }
        }
    }

    simple_children &= node.getStep()->getType() != IQueryPlanStep::Type::Join;

    context.is_table_scan = is_table_scan;
    context.simple_children = simple_children;

    if (node.getStatistics().isDerived() && !context.re_estimate)
        return node.getStatistics().value_or(nullptr);

    CardinalityContext cardinality_context{
        .context = context.context,
        .cte_info = context.cte_info,
        .children_stats = std::move(children_stats),
        .simple_children = simple_children,
        .children_are_table_scan = children_are_table_scan,
        .re_estimate = context.re_estimate};
    auto step = node.getStep();
    auto stats = VisitorUtil::accept(step, visitor, cardinality_context);
    node.setStatistics(stats ? std::make_optional(stats) : std::nullopt);
    return stats;
}

PlanNodeStatisticsPtr PlanCardinalityVisitor::visitCTERefNode(CTERefNode & node, CardinalityContext & context)
{
    const auto * step = dynamic_cast<const CTERefStep *>(node.getStep().get());
    cte_helper.accept(step->getId(), *this, context);

    if (node.getStatistics().isDerived())
        return node.getStatistics().value_or(nullptr);
    auto result = cte_helper.getCTEInfo().getCTEDef(step->getId())->getStatistics();
    if (!result)
        return nullptr;

    const auto & stats = result.value();
    std::unordered_map<String, SymbolStatisticsPtr> calculated_symbol_statistics;
    for (const auto & item : step->getOutputColumns())
        calculated_symbol_statistics[item.first] = stats->getSymbolStatistics(item.second);
    return std::make_shared<PlanNodeStatistics>(stats->getRowCount(), calculated_symbol_statistics);
}
}
