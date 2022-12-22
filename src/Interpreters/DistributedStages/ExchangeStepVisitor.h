#pragma once
#include <Optimizer/Property/Property.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/PlanVisitor.h>


namespace DB
{

using ExchangeStepResult = QueryPlan::Node *;



struct ExchangeStepContext
{
    ContextPtr context;
    QueryPlan & query_plan;
    bool has_gathered = false;
};

class ExchangeStepVisitor : public NodeVisitor<ExchangeStepResult, ExchangeStepContext>
{
public:

    ExchangeStepResult visitNode(QueryPlan::Node *, ExchangeStepContext & exchange_context) override;

    ExchangeStepResult visitMergingAggregatedNode(QueryPlan::Node * node, ExchangeStepContext &) override;

    ExchangeStepResult visitJoinNode(QueryPlan::Node * node, ExchangeStepContext & exchange_context) override;

    ExchangeStepResult visitLimitNode(QueryPlan::Node * node, ExchangeStepContext & exchange_context) override;

    ExchangeStepResult visitMergingSortedNode(QueryPlan::Node * node, ExchangeStepContext & exchange_context) override;
    
    void addGather(QueryPlan & query_plan, ExchangeStepContext & exchange_context);

private:

    ExchangeStepResult visitChild(QueryPlan::Node * node, ExchangeStepContext & exchange_context);

    void addExchange(QueryPlan::Node * node, ExchangeMode mode, const Partitioning & partition, ExchangeStepContext & exchange_context);

};

class AddExchangeRewriter
{
public:

    static void rewrite(QueryPlan & query_plan, ExchangeStepContext & exchange_context);

};

}
