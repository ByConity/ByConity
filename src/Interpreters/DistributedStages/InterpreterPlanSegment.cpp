#include<Interpreters/DistributedStages/InterpreterPlanSegment.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

InterpreterPlanSegment::InterpreterPlanSegment
(
    PlanSegment * plan_segment_,
    ContextPtr context_
)
: plan_segment(plan_segment_)
, context(context_)
{

}

BlockIO InterpreterPlanSegment::execute()
{
    BlockIO res;
    if (plan_segment)
    {
        res.pipeline = std::move(*(plan_segment->getQueryPlan().buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)))
            );
    }
    return res;
}



}
