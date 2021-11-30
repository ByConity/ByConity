#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Context;

BlockIO executePlanSegment(const PlanSegmentPtr & plan_segment, ContextMutablePtr context);

}
