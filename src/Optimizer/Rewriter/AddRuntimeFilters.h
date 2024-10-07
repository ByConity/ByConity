#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include "Interpreters/DistributedStages/PlanSegment.h"

namespace DB
{
/**
 * AddRuntimeFilters generates, analyze, merge and remove inefficient or unused runtime filters.
 *
 * Runtime Filter, also as dynamic filter, improve the performance of queries with selective joins
 * by filtering data early that would be filtered by join condition.
 *
 * When runtime Filtering is enabled, values are collected from the build side of join, and sent to
 * probe side of join in runtime.
 *
 * Runtime Filter could be used for dynamic partition pruning, reduce table scan data with index,
 * reduce exchange shuffle, and so on.
 *
 * Runtime Filter has two parts in plan:
 *  1. build side, model as join attribute.
 *  2. consumer side, model as a filter predicates.
 */
class AddRuntimeFilters : public Rewriter
{
public:
    String name() const override { return "AddRuntimeFilters"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_runtime_filter; }
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;

    class AddRuntimeFilterRewriter;
    class RuntimeFilterInfoExtractor;
    class RemoveUnusedRuntimeFilterProbRewriter;
    class RemoveUnusedRuntimeFilterBuildRewriter;
    class AddExchange;
};

class AddRuntimeFilters::AddRuntimeFilterRewriter : public PlanNodeVisitor<PlanPropEquivalences, Void>
{
public:
    AddRuntimeFilterRewriter(ContextMutablePtr context_, CTEInfo & cte_info_)
        : context(std::move(context_)), cte_info(cte_info_), cte_helper(cte_info_)
    {
    }
    bool rewrite(QueryPlan & plan);
    PlanPropEquivalences visitPlanNode(PlanNodeBase & node, Void & c) override;
    PlanPropEquivalences visitJoinNode(JoinNode & node, Void & c) override;
    PlanPropEquivalences visitCTERefNode(CTERefNode & node, Void & c) override;

    PlanPropEquivalences replaceChildren(
        PlanNodeBase & node, PlanNodes children, std::vector<SymbolEquivalencesPtr> children_equivalences, PropertySet input_properties);

    RuntimeFilterId getId() const { return id; }

private:
    RuntimeFilterId nextId() { return id++; }

    RuntimeFilterId id = 0;
    ContextMutablePtr context;
    CTEInfo & cte_info;
    SimpleCTEVisitHelper<PlanPropEquivalences> cte_helper;
    LoggerPtr logger = getLogger("AddRuntimeFilters");
};

struct RuntimeFilterContext
{
    std::unordered_map<RuntimeFilterId, SymbolStatisticsPtr> runtime_filter_build_statistics;
    std::unordered_map<RuntimeFilterId, RuntimeFilterId> merged_runtime_filters;
    std::unordered_set<RuntimeFilterId> distributed_runtime_filters;
};

using InheritedRuntimeFilters = std::unordered_map<std::string, RuntimeFilterId>;

class AddRuntimeFilters::RuntimeFilterInfoExtractor : public PlanNodeVisitor<InheritedRuntimeFilters, std::unordered_set<RuntimeFilterId>>
{
public:
    static RuntimeFilterContext extract(QueryPlan & plan, ContextMutablePtr & context);

protected:
    RuntimeFilterInfoExtractor(ContextMutablePtr & context_, CTEInfo & cte_info_)
        : context(context_), cte_info(cte_info_), cte_helper(cte_info_)
    {
    }

    InheritedRuntimeFilters visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitProjectionNode(ProjectionNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitExchangeNode(ExchangeNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;
    InheritedRuntimeFilters visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> & local_runtime_filter) override;

private:
    ContextMutablePtr context;
    CTEInfo & cte_info;
    SimpleCTEVisitHelper<InheritedRuntimeFilters> cte_helper;

    RuntimeFilterContext runtime_filter_context;
};

class AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter : public PlanNodeVisitor<PlanNodePtr, std::unordered_set<RuntimeFilterId>>
{
public:
    RemoveUnusedRuntimeFilterProbRewriter(
        ContextMutablePtr context, CTEInfo & cte_info, RuntimeFilterContext & runtime_filter_context);

    PlanNodePtr rewrite(const PlanNodePtr & plan);

    const std::unordered_set<RuntimeFilterId> & getEffectiveRuntimeFilters() const { return effective_runtime_filters; }

protected:
    PlanNodePtr visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;

    RuntimeFilterContext & runtime_filter_context;
    std::unordered_set<RuntimeFilterId> effective_runtime_filters;
};

class AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter : public PlanNodeVisitor<PlanNodePtr, Void>
{
public:
    explicit RemoveUnusedRuntimeFilterBuildRewriter(
        ContextMutablePtr & context_,
        CTEInfo & cte_info,
        const std::unordered_set<RuntimeFilterId> & effective_runtime_filters,
        const RuntimeFilterContext & runtime_filter_context);
    PlanNodePtr rewrite(PlanNodePtr & node);

protected:
    PlanNodePtr visitPlanNode(PlanNodeBase & node, Void &) override;
    PlanNodePtr visitJoinNode(JoinNode & node, Void &) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, Void & context) override;

    ContextMutablePtr & context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    const std::unordered_set<RuntimeFilterId> & effective_runtime_filters;
    const RuntimeFilterContext & runtime_filter_context;
};

class AddRuntimeFilters::AddExchange : public SimplePlanRewriter<std::unordered_set<RuntimeFilterId>>
{
public:
    static PlanNodePtr rewrite(const PlanNodePtr & node, ContextMutablePtr context, CTEInfo & cte_info);

protected:
    explicit AddExchange(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitExchangeNode(ExchangeNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> &) override;
    PlanNodePtr visitBufferNode(BufferNode & node, std::unordered_set<RuntimeFilterId> &) override;
};

}
