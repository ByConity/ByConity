#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{

void reportExecutionResult(const PlanSegmentExecutor::ExecutionResult & result) noexcept;

PlanSegmentExecutor::ExecutionResult convertFailurePlanSegmentStatusToResult(
    ContextPtr query_context,
    const AddressInfo & execution_address,
    int exception_code,
    const String & exception_message,
    Progress final_progress = {},
    SenderMetrics sender_metrics = {},
    PlanSegmentOutputs plan_segment_outputs = {});

PlanSegmentExecutor::ExecutionResult convertSuccessPlanSegmentStatusToResult(
    ContextPtr query_context,
    const AddressInfo & execution_address,
    Progress & final_progress,
    SenderMetrics & sender_metrics,
    PlanSegmentOutputs & plan_segment_outputs);
}
