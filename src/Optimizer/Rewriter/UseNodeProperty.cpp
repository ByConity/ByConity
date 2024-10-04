#include <Optimizer/Rewriter/UseNodeProperty.h>

#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/PropertyDeriver.h>
#include <Optimizer/Property/PropertyDeterminer.h>
#include <Optimizer/Property/PropertyMatcher.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/SymbolMapper.h>
#include "Optimizer/Property/PropertyEnforcer.h"
#include "QueryPlan/QueryPlan.h"

namespace DB
{
bool UseNodeProperty::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    auto single = Property{Partitioning{Partitioning::Handle::SINGLE}};
    single.getNodePartitioningRef().setComponent(Partitioning::Component::COORDINATOR);
    UseNodeProperty::ExchangeRewriter exchange_rewriter{context, plan.getCTEInfo()};
    auto new_plan = VisitorUtil::accept(plan.getPlanNode(), exchange_rewriter, single);

    plan.update(new_plan);
    return true;
}

PlanNodePtr UseNodeProperty::ExchangeRewriter::visitPlanNode(PlanNodeBase & node, Property & require)
{
    auto require_children = PropertyDeterminer::determineRequiredProperty(node.getStep(), require, *context)[0];
    PlanNodes children;

    size_t child_index = 0;
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, require_children[child_index++]);
        children.emplace_back(result);
    }

    node.replaceChildren(children);
    return node.shared_from_this();
}

PlanNodePtr UseNodeProperty::ExchangeRewriter::visitCTERefNode(CTERefNode & node, Property & require)
{
    const auto * step = node.getStep().get();
    require = require.translate(step->getOutputColumns());
    auto result = cte_helper.acceptAndUpdate(step->getId(), *this, require);
    return node.shared_from_this();
}

PlanNodePtr UseNodeProperty::ExchangeRewriter::visitTableScanNode(TableScanNode & node, Property & require)
{
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), context, require);
    if (!require.getNodePartitioning().getColumns().empty()
        && prop.getNodePartitioning().getHandle() == Partitioning::Handle::BUCKET_TABLE
        && !prop.getNodePartitioning().getColumns().empty())
    {
        node.getStep()->setBucketScan(true);
    }

    return node.shared_from_this();
}

}
