#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DAGGraph.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Common/HostWithPorts.h>

namespace DB
{

AddressInfo getLocalAddress(ContextPtr & query_context);
AddressInfo getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context);

void sendPlanSegmentToLocal(PlanSegment * plan_segment_ptr, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr);
void sendPlanSegmentToRemote(
    AddressInfo & addressinfo,
    ContextPtr query_context,
    PlanSegment * plan_segment_ptr,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    const WorkerId & worker_id);

} // namespace DB
