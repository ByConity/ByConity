#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/CardinalityEstimate/FilterEstimator.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>

namespace DB
{
/**
 * AddRuntimeFilters generates, analyze, merge and remove inefficient or unused runtime filters.
 */
class AddRuntimeFilters : public Rewriter
{
public:
    String name() const override { return "AddRuntimeFilters"; }

private:
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_runtime_filter; }
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;

    class AddRuntimeFilterRewriter;
    class RuntimeFilterInfoExtractor;
    class RemoveUnusedRuntimeFilterProbRewriter;
    class RemoveUnusedRuntimeFilterBuildRewriter;
    class AddExchange;
};

class AddRuntimeFilters::AddRuntimeFilterRewriter : public SimplePlanRewriter<Void>
{
public:
    AddRuntimeFilterRewriter(ContextMutablePtr context, CTEInfo & cte_info);
    void rewrite(QueryPlan & plan);
    PlanNodePtr visitJoinNode(JoinNode & node, Void & c) override;
};

struct RuntimeFilterContext
{
    std::unordered_map<RuntimeFilterId, std::pair<UInt64, SymbolStatisticsPtr>> runtime_filter_build_statistics;
    std::unordered_map<RuntimeFilterId, size_t> runtime_filter_probe_cost;
    std::unordered_map<RuntimeFilterId, size_t> runtime_filter_build_cost;
    std::unordered_map<RuntimeFilterId, RuntimeFilterId> merged_runtime_filters;
    std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterDistribution>> runtime_filter_distribution;
};

struct RuntimeFilterWithScanRows
{
    std::unordered_map<std::string, RuntimeFilterId> inherited_runtime_filters;
    size_t scan_rows;
};

class AddRuntimeFilters::RuntimeFilterInfoExtractor : public PlanNodeVisitor<RuntimeFilterWithScanRows, std::unordered_set<RuntimeFilterId>>
{
public:
    static RuntimeFilterContext extract(QueryPlan & plan, ContextMutablePtr & context);

protected:
    explicit RuntimeFilterInfoExtractor(ContextMutablePtr & context_, CTEInfo & cte_info_)
        : context(context_), cte_info(cte_info_), cte_helper(cte_info_)
    {
    }

    RuntimeFilterWithScanRows visitPlanNode(PlanNodeBase & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;
    RuntimeFilterWithScanRows visitProjectionNode(ProjectionNode & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;
    RuntimeFilterWithScanRows visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;
    RuntimeFilterWithScanRows visitExchangeNode(ExchangeNode & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;
    RuntimeFilterWithScanRows visitTableScanNode(TableScanNode & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;
    RuntimeFilterWithScanRows visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;
    RuntimeFilterWithScanRows visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> & no_exchange) override;

private:
    ContextMutablePtr context;
    CTEInfo & cte_info;
    SimpleCTEVisitHelper<RuntimeFilterWithScanRows> cte_helper;
    std::unordered_map<RuntimeFilterId, std::pair<UInt64, SymbolStatisticsPtr>> runtime_filter_build_statistics;
    std::unordered_map<RuntimeFilterId, size_t> runtime_filter_build_cost;
    std::unordered_map<RuntimeFilterId, size_t> runtime_filter_probe_cost;
    std::unordered_map<RuntimeFilterId, RuntimeFilterId> merged_runtime_filters;
    std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterDistribution>> runtime_filter_distribution;
};

class AddRuntimeFilters::RemoveUnusedRuntimeFilterProbRewriter : public PlanNodeVisitor<PlanNodePtr, std::unordered_set<RuntimeFilterId>>
{
public:
    RemoveUnusedRuntimeFilterProbRewriter(
        ContextMutablePtr context, CTEInfo & cte_info, const RuntimeFilterContext & runtime_filter_context);

    PlanNodePtr rewrite(const PlanNodePtr & plan);

    const std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterId>> & getEffectiveRuntimeFilters() const
    {
        return effective_runtime_filters;
    }

protected:
    PlanNodePtr visitPlanNode(PlanNodeBase & plan, std::unordered_set<RuntimeFilterId> & allowed_runtime_filters) override;
    PlanNodePtr visitFilterNode(FilterNode & node, std::unordered_set<RuntimeFilterId> & context) override;
    PlanNodePtr visitJoinNode(JoinNode & node, std::unordered_set<RuntimeFilterId> & context) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, std::unordered_set<RuntimeFilterId> & context) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    const RuntimeFilterContext & runtime_filter_context;

    std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterId>> effective_runtime_filters{};
};

class AddRuntimeFilters::RemoveUnusedRuntimeFilterBuildRewriter : public PlanNodeVisitor<PlanNodePtr, Void>
{
public:
    explicit RemoveUnusedRuntimeFilterBuildRewriter(
        ContextMutablePtr & context_,
        CTEInfo & cte_info,
        const std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterId>> & effective_runtime_filters,
        const RuntimeFilterContext & runtime_filter_context);
    PlanNodePtr rewrite(PlanNodePtr & node);

protected:
    PlanNodePtr visitPlanNode(PlanNodeBase & plan, Void &) override;
    PlanNodePtr visitJoinNode(JoinNode & node, Void &) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, Void & context) override;

    ContextMutablePtr & context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
    const std::unordered_map<RuntimeFilterId, std::unordered_set<RuntimeFilterId>> & effective_runtime_filters;
    const RuntimeFilterContext & runtime_filter_context;
};

class AddRuntimeFilters::AddExchange : public SimplePlanRewriter<bool>
{
public:
    static PlanNodePtr rewrite(const PlanNodePtr & node, ContextMutablePtr context, CTEInfo & cte_info);

protected:
    explicit AddExchange(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanRewriter(context_, cte_info_) { }
    PlanNodePtr visitPlanNode(PlanNodeBase & node, bool &) override;
    PlanNodePtr visitExchangeNode(ExchangeNode & node, bool &) override;
    PlanNodePtr visitFilterNode(FilterNode & node, bool & context) override;
};

}
