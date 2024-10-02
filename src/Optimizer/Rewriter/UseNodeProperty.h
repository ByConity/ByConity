#pragma once

#include <Interpreters/Context.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/SymbolEquivalencesDeriver.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include "Optimizer/Rewriter/UseNodeProperty.h"

namespace DB
{
class UseNodeProperty : public Rewriter
{
public:
    String name() const override { return "UseNodeProperty"; }

private:
    bool rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    bool isEnabled(ContextMutablePtr context) const override { return context->getSettingsRef().enable_use_node_property; }
    class Rewriter;
    class ExchangeRewriter;
};

class UseNodeProperty::ExchangeRewriter : public PlanNodeVisitor<PlanNodePtr, Property>
{
public:
    ExchangeRewriter(ContextMutablePtr context_, CTEInfo & cte_info_) : context(context_), cte_helper(cte_info_) { }
    PlanNodePtr visitPlanNode(PlanNodeBase &, Property &) override;
    PlanNodePtr visitCTERefNode(CTERefNode & node, Property &) override;
    PlanNodePtr visitTableScanNode(TableScanNode & node, Property &) override;

private:
    ContextMutablePtr context;
    SimpleCTEVisitHelper<PlanNodePtr> cte_helper;
};

}
