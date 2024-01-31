#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DAGGraph.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Common/HostWithPorts.h>

namespace DB
{

AddressInfo getLocalAddress(const Context & query_context);
AddressInfo getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context);

void sendPlanSegmentToAddress(
    const AddressInfo & addressinfo,
    PlanSegment * plan_segment_ptr,
    PlanSegmentExecutionInfo & execution_info,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr = nullptr,
    const WorkerId & worker_id = WorkerId{});

} // namespace DB
