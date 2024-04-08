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

void reportPlanSegmentStatus(const AddressInfo & coordinator_address, const RuntimeSegmentsStatus & status) noexcept;
}
