#pragma once
#include <Interpreters/DistributedStages/Property.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/PlanVisitor.h>


namespace DB
{

using PlanSegmentResult = QueryPlan::Node *;

class PlanSegmentInput;
using PlanSegmentInputPtr = std::shared_ptr<PlanSegmentInput>;
using PlanSegmentInputs = std::vector<PlanSegmentInputPtr>;

struct PlanSegmentContext
{
    ContextPtr context;
    QueryPlan & query_plan;
    String query_id;
    size_t id = 0;
    PlanSegmentTree * plan_segment_tree;

    size_t getSegmentId() { return id++; }
};

class PlanSegmentVisitor : public StepVisitor<PlanSegmentResult, PlanSegmentContext>
{
public:

    PlanSegmentResult visitPlan(QueryPlan::Node *, PlanSegmentContext & plan_segment_context) override;

    PlanSegmentResult visitExchangeStep(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context) override;
    
    PlanSegment * createPlanSegment(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context);

    PlanSegment * createPlanSegment(QueryPlan::Node * node, size_t segment_id, PlanSegmentContext & plan_segment_context);

private:

    PlanSegmentResult visitChild(QueryPlan::Node * node, PlanSegmentContext & plan_segment_context);

    PlanSegmentInputs findInputs(QueryPlan::Node * node);

};

class PlanSegmentSpliter
{
public:

    static void rewrite(QueryPlan & query_plan, PlanSegmentContext & plan_segment_context);

};

}
