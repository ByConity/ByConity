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

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <DataStreams/BlockIO.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Protos/registry.pb.h>
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

BlockIO lazyExecutePlanSegmentLocally(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context)
{
    if (!plan_segment_instance)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);
    PlanSegmentExecutor executor(std::move(plan_segment_instance), std::move(context));
    return executor.lazyExecute();
}

void executePlanSegmentInternal(PlanSegmentInstancePtr plan_segment_instance, ContextMutablePtr context, bool async)
{
    if (!plan_segment_instance)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);

    if (context->getSettingsRef().debug_plan_generation)
        return;

    auto executor = std::make_shared<PlanSegmentExecutor>(std::move(plan_segment_instance), std::move(context));

    if (async)
    {
        ThreadFromGlobalPool async_thread([executor = std::move(executor)]() { executor->execute(); });
        async_thread.detach();
        return;
    }

    executor->execute();
}

static void OnSendPlanSegmentCallback(
    Protos::ExecutePlanSegmentResponse * response,
    brpc::Controller * cntl,
    std::shared_ptr<RpcClient> rpc_channel,
    WorkerStatusManagerPtr worker_status_manager,
    AsyncContextPtr async_context,
    WorkerId worker_id)
{
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<Protos::ExecutePlanSegmentResponse> response_guard(response);

    if (response->has_exception() || cntl->Failed())
        worker_status_manager->setWorkerNodeDead(worker_id, cntl->ErrorCode());
    else if (response->has_worker_resource_data())
        worker_status_manager->updateWorkerNode(response->worker_resource_data(), WorkerStatusManager::UpdateSource::ComeFromWorker);

    rpc_channel->checkAliveWithController(*cntl);
    AsyncContext::AsyncResult result;
    if (cntl->Failed())
    {
        LOG_ERROR(
            &Poco::Logger::get("executePlanSegment"),
            "send plansegment to {} failed, error: {},  msg: {}",
            butil::endpoint2str(cntl->remote_side()).c_str(),
            cntl->ErrorText(),
            response->message());
        result.error_text = cntl->ErrorText();
        result.error_code = cntl->ErrorCode();
        result.is_success = false;
        result.failed_worker = butil::endpoint2str(cntl->remote_side()).c_str();
        async_context->asyncComplete(cntl->call_id(), result);
    }
    else
    {
        LOG_TRACE(
            &Poco::Logger::get("executePlanSegment"), "send plansegment to {} success", butil::endpoint2str(cntl->remote_side()).c_str());
        async_context->asyncComplete(cntl->call_id(), result);
    }
}

void executePlanSegmentRemotely(
    const PlanSegment & plan_segment, const PlanSegmentExecutionInfo & execution_info, ContextPtr context, AsyncContextPtr & async_context, const WorkerId & worker_id)
{
    auto execute_address = extractExchangeHostPort(execution_info.execution_address);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
    Protos::PlanSegmentManagerService_Stub manager_stub(&rpc_channel->getChannel());
    Protos::ExecutePlanSegmentRequest request;
    request.set_brpc_protocol_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    request.set_brpc_protocol_minor_revision(DBMS_BRPC_PROTOCOL_MINOR_VERSION);
    request.set_query_id(plan_segment.getQueryId());
    request.set_plan_segment_id(plan_segment.getPlanSegmentId());
    request.set_parallel_id(execution_info.parallel_id);
    if (execution_info.source_task_index)
        request.set_source_task_index(execution_info.source_task_index.value());
    if (execution_info.source_task_count)
        request.set_source_task_count(execution_info.source_task_count.value());
    request.set_initial_query_start_time(context->getClientInfo().initial_query_start_time_microseconds.value);
    auto settings = context->getSettingsRef().dumpToMap();
    request.mutable_settings()->insert(settings.begin(), settings.end());

    const auto & coordinator_address = plan_segment.getCoordinatorAddress();
    request.set_user(coordinator_address.getUser());
    request.set_password(coordinator_address.getPassword());
    request.set_initial_user(context->getClientInfo().initial_user);
    request.set_current_host(coordinator_address.getHostName());
    request.set_current_port(coordinator_address.getPort());
    request.set_current_exchange_port(coordinator_address.getExchangePort());
    request.set_current_exchange_status_port(coordinator_address.getExchangePort());

    request.set_coordinator_host(coordinator_address.getHostName());
    request.set_coordinator_port(coordinator_address.getPort());
    request.set_coordinator_exchange_port(coordinator_address.getExchangePort());
    request.set_coordinator_exchange_status_port(coordinator_address.getExchangePort());

    request.set_database(context->getCurrentDatabase());
    request.set_check_session(!(context->getSettingsRef().enable_new_scheduler && context->getSettingsRef().bsp_mode));

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
    request.set_primary_txn_id(context->getCurrentTransaction()->getPrimaryTransactionID().toUInt64());

    WriteBufferFromBrpcBuf write_buf;
    plan_segment.serialize(write_buf);
    butil::IOBuf & iobuf = const_cast<butil::IOBuf &>(write_buf.getFinishedBuf());

    /// async call
    brpc::Controller * cntl = new brpc::Controller();
    Protos::ExecutePlanSegmentResponse * response = new Protos::ExecutePlanSegmentResponse();
    auto call_id = cntl->call_id();
    cntl->set_timeout_ms(context->getSettingsRef().send_plan_segment_timeout_ms.totalMilliseconds());
    cntl->request_attachment().append(iobuf.movable());
    google::protobuf::Closure * done = brpc::NewCallback(
        &OnSendPlanSegmentCallback, response, cntl, rpc_channel, context->getWorkerStatusManager(), async_context, worker_id);
    async_context->addCallId(call_id);
    manager_stub.executeQuery(cntl, &request, response, done);
}

void cleanupExchangeDataForQuery(const AddressInfo & address, UInt64 & query_unique_id)
{
    auto execute_address = extractExchangeHostPort(address);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
    Protos::RegistryService_Stub manager_stub(&rpc_channel->getChannel());
    Protos::CleanupExchangeDataRequest request;
    request.set_query_unique_id(query_unique_id);

    brpc::Controller cntl;
    Protos::CleanupExchangeDataResponse response;
    manager_stub.cleanupExchangeData(&cntl, &request, &response, nullptr);
    rpc_channel->assertController(cntl);
}

void prepareQueryCommonBuf(
    butil::IOBuf & common_buf, const PlanSegment & any_plan_segment, ContextPtr & context)
{
    Protos::QueryCommon query_common;
    const auto & client_info = context->getClientInfo();
    auto min_compatible_brpc_minor_version = std::min(static_cast<UInt32>(DBMS_BRPC_PROTOCOL_MINOR_VERSION), static_cast<UInt32>(context->getSettingsRef().min_compatible_brpc_minor_version.value));
    query_common.set_brpc_protocol_minor_revision(min_compatible_brpc_minor_version);
    query_common.set_query_id(any_plan_segment.getQueryId());
    query_common.set_initial_query_start_time(client_info.initial_query_start_time_microseconds.value);
    query_common.set_initial_user(client_info.initial_user);
    query_common.set_initial_client_host(client_info.initial_address.host().toString());
    query_common.set_initial_client_port(client_info.initial_address.port());
    any_plan_segment.getCoordinatorAddress().toProto(*query_common.mutable_coordinator_address());
    query_common.set_database(context->getCurrentDatabase());
    query_common.set_check_session(!(context->getSettingsRef().enable_new_scheduler && context->getSettingsRef().bsp_mode));
    query_common.set_txn_id(context->getCurrentTransactionID().toUInt64());
    query_common.set_primary_txn_id(context->getCurrentTransaction()->getPrimaryTransactionID().toUInt64());
    const String & quota_key = client_info.quota_key;
    if (!client_info.quota_key.empty())
        query_common.set_quota(quota_key);

    butil::IOBuf query_common_buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&common_buf);
    query_common.SerializeToZeroCopyStream(&wrapper);
}

void executePlanSegmentRemotelyWithPreparedBuf(
    const PlanSegment & plan_segment,
    PlanSegmentExecutionInfo execution_info,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    const butil::IOBuf & plan_segment_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerId & worker_id)
{
    auto execute_address = extractExchangeHostPort(execution_info.execution_address);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
    Protos::PlanSegmentManagerService_Stub manager_stub(&rpc_channel->getChannel());
    Protos::SubmitPlanSegmentRequest request;
    request.set_brpc_protocol_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    request.set_plan_segment_id(plan_segment.getPlanSegmentId());
    request.set_parallel_id(execution_info.parallel_id);
    if (execution_info.source_task_index)
        request.set_source_task_index(execution_info.source_task_index.value());
    if (execution_info.source_task_count)
        request.set_source_task_count(execution_info.source_task_count.value());

    execution_info.execution_address.toProto(*request.mutable_execution_address());

    butil::IOBuf attachment;

    request.set_query_common_buf_size(query_common_buf.size());
    attachment.append(query_common_buf);

    request.set_query_settings_buf_size(query_settings_buf.size());
    if (!query_settings_buf.empty())
    {
        attachment.append(query_settings_buf);
    }

    request.set_plan_segment_buf_size(plan_segment_buf.size());
    attachment.append(plan_segment_buf);

    /// async call
    brpc::Controller * cntl = new brpc::Controller();
    Protos::ExecutePlanSegmentResponse * response = new Protos::ExecutePlanSegmentResponse();
    auto call_id = cntl->call_id();
    cntl->request_attachment().append(attachment.movable());
    cntl->set_timeout_ms(context.getSettingsRef().send_plan_segment_timeout_ms.totalMilliseconds());
    google::protobuf::Closure * done = brpc::NewCallback(
        &OnSendPlanSegmentCallback, response, cntl, rpc_channel, context.getWorkerStatusManager(), async_context, worker_id);
    async_context->addCallId(call_id);
    manager_stub.submitPlanSegment(cntl, &request, response, done);
}

}
