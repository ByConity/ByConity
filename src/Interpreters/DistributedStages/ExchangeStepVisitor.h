#pragma once
#include <Interpreters/DistributedStages/Property.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/PlanVisitor.h>


namespace DB
{

using ExchangeStepResult = QueryPlan::Node *;



struct ExchangeStepContext
{
    ContextPtr context;
    QueryPlan & query_plan;
    bool has_gathered = false;
};

class ExchangeStepVisitor : public StepVisitor<ExchangeStepResult, ExchangeStepContext>
{
public:

    ExchangeStepResult visitPlan(QueryPlan::Node *, ExchangeStepContext & exchange_context) override;

    ExchangeStepResult visitMergingAggregatedStep(QueryPlan::Node * node, ExchangeStepContext &) override;

    ExchangeStepResult visitJoinStep(QueryPlan::Node * node, ExchangeStepContext & exchange_context) override;

    ExchangeStepResult visitLimitStep(QueryPlan::Node * node, ExchangeStepContext & exchange_context) override;

    ExchangeStepResult visitMergingSortedStep(QueryPlan::Node * node, ExchangeStepContext & exchange_context) override;
    
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
