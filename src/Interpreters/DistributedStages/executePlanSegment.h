#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Context;

BlockIO executePlanSegment(PlanSegmentPtr plan_segment, ContextMutablePtr context);
    

void executePlanSegment(PlanSegmentPtr plan_segment, ContextMutablePtr context, bool is_async);
    
}
