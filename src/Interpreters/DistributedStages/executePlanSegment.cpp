/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <string>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <DataStreams/BlockIO.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Protos/plan_segment_manager.pb.h>
#include <brpc/callback.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

BlockIO lazyExecutePlanSegmentLocally(PlanSegmentPtr plan_segment, ContextMutablePtr context)
{
    if (!plan_segment)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);
    PlanSegmentExecutor executor(std::move(plan_segment), std::move(context));
    return executor.lazyExecute();
}

void executePlanSegmentInternal(PlanSegmentPtr plan_segment, ContextMutablePtr context, bool async)
{
    if (!plan_segment)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);

    if (context->getSettingsRef().debug_plan_generation)
        return;

    auto executor = std::make_shared<PlanSegmentExecutor>(std::move(plan_segment), std::move(context));

    if (async)
    {
        ThreadFromGlobalPool async_thread([executor = std::move(executor)]() { executor->execute(); });
        async_thread.detach();
        return;
    }

    executor->execute();
}

static void OnSendPlanSegmentCallback(Protos::ExecutePlanSegmentResponse * response, brpc::Controller * cntl, std::shared_ptr<RpcClient> rpc_channel, WorkerStatusManagerPtr worker_status_manager, WorkerId worker_id)
{
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<Protos::ExecutePlanSegmentResponse> response_guard(response);

    if (response->has_exception() || cntl->Failed())
        worker_status_manager->setWorkerNodeDead(worker_id, cntl->ErrorCode());
    else if (response->has_worker_resource_data())
        worker_status_manager->updateWorkerNode(response->worker_resource_data(), WorkerStatusManager::UpdateSource::ComeFromWorker);

    rpc_channel->checkAliveWithController(*cntl);
    if (cntl->Failed())
        LOG_ERROR(
            &Poco::Logger::get("executePlanSegment"),
            "send plansegment to {} failed, error: {},  msg: {}",
            butil::endpoint2str(cntl->remote_side()).c_str(),
            cntl->ErrorText(),
            response->message());
    else
        LOG_TRACE(
            &Poco::Logger::get("executePlanSegment"), "send plansegment to {} success", butil::endpoint2str(cntl->remote_side()).c_str());
}

void executePlanSegmentRemotely(const PlanSegment & plan_segment, ContextPtr context, bool async, const WorkerId & worker_id)
{
    auto execute_address = extractExchangeStatusHostPort(plan_segment.getCurrentAddress());
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
    Protos::PlanSegmentManagerService_Stub manager_stub(&rpc_channel->getChannel());
    Protos::ExecutePlanSegmentRequest request;
    request.set_brpc_protocol_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    request.set_brpc_protocol_minor_revision(DBMS_BRPC_PROTOCOL_MINOR_VERSION);
    request.set_query_id(plan_segment.getQueryId());
    request.set_plan_segment_id(plan_segment.getPlanSegmentId());
    request.set_initial_query_start_time(context->getClientInfo().initial_query_start_time_microseconds.value);
    auto settings = context->getSettingsRef().dumpToMap();
    request.mutable_settings()->insert(settings.begin(), settings.end());

    const auto & coordinator_address = plan_segment.getCoordinatorAddress();
    request.set_user(coordinator_address.getUser());
    request.set_password(coordinator_address.getPassword());
    request.set_current_host(coordinator_address.getHostName());
    request.set_current_port(coordinator_address.getPort());
    request.set_current_exchange_port(coordinator_address.getExchangePort());
    request.set_current_exchange_status_port(coordinator_address.getExchangeStatusPort());

    request.set_coordinator_host(coordinator_address.getHostName());
    request.set_coordinator_port(coordinator_address.getPort());
    request.set_coordinator_exchange_port(coordinator_address.getExchangePort());
    request.set_coordinator_exchange_status_port(coordinator_address.getExchangeStatusPort());

    request.set_database(context->getCurrentDatabase());

    const auto & client_info = context->getClientInfo();
    const String & quota_key = client_info.quota_key;
    if (!client_info.quota_key.empty())
        request.set_quota(quota_key);

    //OpenTelemetry trace
    const auto & trace_context = client_info.client_trace_context;
    if (trace_context.trace_id != UUID())
    {
        UInt128 trace_id = trace_context.trace_id.toUnderType();
        request.set_open_telemetry_trace_id_low(trace_id.items[0]);
        request.set_open_telemetry_trace_id_high(trace_id.items[1]);
        request.set_open_telemetry_span_id(trace_context.span_id);
        request.set_open_telemetry_tracestate(trace_context.tracestate);
        request.set_open_telemetry_trace_flags(static_cast<uint32_t>(trace_context.trace_flags));
    }

    // Set cnch Transaction id as seesion id
    request.set_txn_id(context->getCurrentTransactionID().toUInt64());

    WriteBufferFromBrpcBuf write_buf;
    plan_segment.serialize(write_buf);
    butil::IOBuf & iobuf = const_cast<butil::IOBuf &>(write_buf.getFinishedBuf());

    if (async)
    {
        /// async call
        brpc::Controller * cntl = new brpc::Controller();
        Protos::ExecutePlanSegmentResponse * response = new Protos::ExecutePlanSegmentResponse();
        cntl->request_attachment().append(iobuf.movable());
        google::protobuf::Closure * done = brpc::NewCallback(&OnSendPlanSegmentCallback, response, cntl, rpc_channel,
            context->getWorkerStatusManager(), worker_id);
        manager_stub.executeQuery(cntl, &request, response, done);
    }
    else
    {
        brpc::Controller cntl;
        Protos::ExecutePlanSegmentResponse response;
        cntl.request_attachment().append(iobuf.movable());
        manager_stub.executeQuery(&cntl, &request, &response, nullptr);
        rpc_channel->assertController(cntl);
    }
}

void executePlanSegmentLocally(const PlanSegment & plan_segment, ContextPtr initial_query_context)
{
    PlanSegmentPtr plan_segment_clone
        = std::make_unique<PlanSegment>(plan_segment.getPlanSegmentId(), plan_segment.getQueryId(), plan_segment.getClusterName());
    plan_segment_clone->setContext(initial_query_context);

    ContextMutablePtr context = plan_segment_clone->getContext();

    if (!context->hasQueryContext())
        context->makeQueryContext();

    ClientInfo & client_info = context->getClientInfo();
    client_info.initial_query_id = plan_segment.getQueryId();
    client_info.current_query_id = client_info.initial_query_id + "_" + std::to_string(plan_segment.getPlanSegmentId());
    client_info.brpc_protocol_major_version = DBMS_BRPC_PROTOCOL_MAJOR_VERSION;
    client_info.brpc_protocol_minor_version = DBMS_BRPC_PROTOCOL_MINOR_VERSION;
    client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    client_info.interface = ClientInfo::Interface::BRPC;
    context->setProcessListElement(nullptr);

    MemoryWriteBuffer write_buf;
    plan_segment.getQueryPlan().serialize(write_buf);
    auto read_buf = write_buf.tryGetReadBuffer();
    plan_segment_clone->getQueryPlan().addInterpreterContext(context);
    plan_segment_clone->getQueryPlan().deserialize(*read_buf);

    plan_segment_clone->setCoordinatorAddress(plan_segment.getCoordinatorAddress());
    plan_segment_clone->setCurrentAddress(plan_segment.getCurrentAddress());
    plan_segment_clone->appendPlanSegmentInputs(plan_segment.getPlanSegmentInputs());
    plan_segment_clone->appendPlanSegmentOutputs(plan_segment.getPlanSegmentOutputs());
    plan_segment_clone->setParallelSize(plan_segment.getParallelSize());
    plan_segment_clone->setExchangeParallelSize(plan_segment.getExchangeParallelSize());

    executePlanSegmentInternal(std::move(plan_segment_clone), context, true);
}
}
