#pragma once

#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/prepared_statement.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include <Poco/Logger.h>

namespace DB
{

struct PlanAndPropConstants
{
    PlanNodePtr plan;
    Property property;
    Constants constants;
};

class SortingOrderedSource : public Rewriter
{
public:
    String name() const override { return "SortingOrderedSource"; }
private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override
    {
        return context->getSettingsRef().enable_sorting_property && context->getSettingsRef().optimize_read_in_order;
    }   
    class Rewriter;
};

class SortingOrderedSource::Rewriter : public PlanNodeVisitor<PlanAndPropConstants, SortDescription>
{
public:
    Rewriter(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_) { }

    PlanAndPropConstants visitPlanNode(PlanNodeBase &, SortDescription & required) override;
    PlanAndPropConstants visitSortingNode(SortingNode &, SortDescription & required) override;
    PlanAndPropConstants visitAggregatingNode(AggregatingNode &, SortDescription & required) override;
    PlanAndPropConstants visitWindowNode(WindowNode &, SortDescription & required) override;
    PlanAndPropConstants visitTopNFilteringNode(TopNFilteringNode & node, SortDescription & required) override;

    PlanAndPropConstants visitCTERefNode(CTERefNode & node, SortDescription & required) override;
    PlanAndPropConstants visitProjectionNode(ProjectionNode & node, SortDescription & required) override;
    PlanAndPropConstants visitFilterNode(FilterNode & node, SortDescription & required) override;
    PlanAndPropConstants visitTableScanNode(TableScanNode & node, SortDescription & required) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanAndPropConstants> cte_helper;
};

struct SortInfo
{
    SortDescription sort_desc;
    SizeOrVariable limit = 0ul;
};

class PruneSortingInfoRewriter : public SimplePlanRewriter<SortInfo>
{
public:
    PruneSortingInfoRewriter(ContextMutablePtr context_, CTEInfo & cte_info_)
        : SimplePlanRewriter(context_, cte_info_), logger(getLogger("PruneSortingInfoRewriter"))
    {
    }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, SortInfo & required) override;
    PlanNodePtr visitSortingNode(SortingNode &, SortInfo &) override;
    PlanNodePtr visitAggregatingNode(AggregatingNode &, SortInfo &) override;
    PlanNodePtr visitWindowNode(WindowNode &, SortInfo &) override;
    PlanNodePtr visitTopNFilteringNode(TopNFilteringNode & node, SortInfo &) override;
    PlanNodePtr visitProjectionNode(ProjectionNode & node, SortInfo & required) override;
    PlanNodePtr visitTableScanNode(TableScanNode &, SortInfo & required) override;

private:
    LoggerPtr logger;
};

}
