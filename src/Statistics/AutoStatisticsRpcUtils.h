#pragma once
#include <Protos/cnch_server_rpc.pb.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/CollectTarget.h>
#include <Common/HostWithPorts.h>


namespace DB::Statistics::AutoStats
{

std::optional<HostWithPorts> getRemoteTargetServerIfHas(ContextPtr context, const IStorage * table);
void redirectUdiCounter(ContextPtr context, const Protos::RedirectUdiCounterReq * request, Protos::RedirectUdiCounterResp * response);
void queryUdiCounter(ContextPtr context, const Protos::QueryUdiCounterReq * request, Protos::QueryUdiCounterResp * response);
void redirectAsyncStatsTasks(
    ContextPtr context, const Protos::RedirectAsyncStatsTasksReq * request, Protos::RedirectAsyncStatsTasksResp * response);

void submitAsyncTasks(ContextPtr context, const std::vector<CollectTarget> & collect_targets);
}
