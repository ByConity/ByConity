#include <Interpreters/DistributedStages/ExchangeStepVisitor.h>
#include <Interpreters/DistributedStages/ExchangeMode.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ExchangeStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>

namespace DB
{

ExchangeStepResult ExchangeStepVisitor::visitPlan(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    std::vector<QueryPlan::Node *> result_children;

    for (QueryPlan::Node * child : node->children)
    {
        auto result_node = visitChild(child, exchange_context);
        if (result_node)
            result_children.push_back(result_node);
    }

    if (!result_children.empty())
        node->children.swap(result_children);

    return nullptr;
}

ExchangeStepResult ExchangeStepVisitor::visitChild(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    return VisitorUtil::accept(node, *this, exchange_context);
}

ExchangeStepResult ExchangeStepVisitor::visitAggregatingStep(QueryPlan::Node * node, ExchangeStepContext & exchange_context)
{
    AggregatingStep * step = dynamic_cast<AggregatingStep *>(node->step.get());
    
    auto params = step->getParams();

    Block result_header = params.getHeader(false);

    Names keys;
    for (auto & index : params.keys)
        keys.push_back(result_header.safeGetByPosition(index).name);

    auto exchange_step = std::make_unique<ExchangeStep>(DataStreams{step->getOutputStream()}, ExchangeMode::REPARTITION, Partitioning(keys));
    QueryPlan::Node exchange_node{.step = std::move(exchange_step), .children = {node}};
    exchange_context.query_plan.addNode(std::move(exchange_node));

    return exchange_context.query_plan.getLastNode();
}

void AddExchangeRewriter::rewrite(QueryPlan & query_plan, ExchangeStepContext & exchange_context)
{
    ExchangeStepVisitor visitor{};
    ExchangeStepResult result_node = VisitorUtil::accept(query_plan.getRoot(), visitor, exchange_context);
    if (result_node)
        query_plan.setRoot(result_node);
}


}
