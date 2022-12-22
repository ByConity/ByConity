#pragma once

#include <memory>
#include <DataStreams/BlockIO.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/Context_fwd.h>
#include <Protos/plan_segment_manager.pb.h>

namespace DB
{
class Context;

BlockIO lazyExecutePlanSegmentLocally(PlanSegmentPtr plan_segment, ContextMutablePtr context);

void executePlanSegmentInternal(PlanSegmentPtr plan_segment, ContextMutablePtr context, bool async);

void executePlanSegmentRemotely(const PlanSegment & plan_segment, ContextPtr context, bool async);

void executePlanSegmentLocally(const PlanSegment & plan_segment, ContextPtr initial_query_context);

/**
 * Extract execute PlanSegmentTree as a common logic.
 * Used by InterpreterSelectQueryUseOptimizer and InterpreterDistributedStages
 */
BlockIO executePlanSegmentTree(PlanSegmentTreePtr & plan_segment_tree, ContextMutablePtr context);

}
