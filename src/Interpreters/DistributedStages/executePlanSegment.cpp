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
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentReport.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/WorkerStatusManager.h>
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

void executePlanSegmentInternal(
    PlanSegmentInstancePtr plan_segment_instance,
    ContextMutablePtr context,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry,
    bool async)
{
    if (!plan_segment_instance)
        throw Exception("Cannot execute empty plan segment", ErrorCodes::LOGICAL_ERROR);

    const auto & settings = context->getSettingsRef();
    if (settings.debug_plan_generation)
        return;

    bool inform_success_status = settings.enable_wait_for_post_processing || settings.bsp_mode || settings.report_segment_profiles;
    auto executor = std::make_shared<PlanSegmentExecutor>(
        std::move(plan_segment_instance), std::move(context), std::move(process_plan_segment_entry));
    if (async)
    {
        ThreadFromGlobalPool async_thread([executor = std::move(executor), inform_success_status = inform_success_status]() mutable {
            auto result = executor->execute();
            executor.reset(); /// release executor
            if (result)
                reportExecutionResult(*result, inform_success_status);
        });
        async_thread.detach();
        return;
    }
    else
    {
        auto result = executor->execute();
        executor.reset(); /// release executor
        if (result)
            reportExecutionResult(*result, inform_success_status);
    }
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
            getLogger("executePlanSegment"),
            "Send plansegment to {} failed, error: {},  msg: {}",
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
        LOG_TRACE(getLogger("executePlanSegment"), "Send plansegment to {} success", butil::endpoint2str(cntl->remote_side()).c_str());
        async_context->asyncComplete(cntl->call_id(), result);
    }
}

void cleanupExchangeDataForQuery(const AddressInfo & address, UInt64 & query_unique_id)
{
    auto execute_address = extractExchangeHostPort(address);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
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
    query_common.set_check_session(!context->getSettingsRef().bsp_mode && !context->getSettingsRef().enable_prune_source_plan_segment);
    query_common.set_txn_id(context->getCurrentTransactionID().toUInt64());
    query_common.set_primary_txn_id(context->getCurrentTransaction()->getPrimaryTransactionID().toUInt64());
    auto query_expiration_ts = context->getQueryExpirationTimeStamp();
    query_common.set_query_expiration_timestamp(query_expiration_ts.tv_sec * 1000 + query_expiration_ts.tv_nsec / 1000000);
    const String & quota_key = client_info.quota_key;
    if (!client_info.quota_key.empty())
        query_common.set_quota(quota_key);
    if (!client_info.parent_initial_query_id.empty())
    {
        query_common.set_parent_query_id(client_info.parent_initial_query_id);
        query_common.set_is_internal_query(context->isInternalQuery());
    }

    butil::IOBuf query_common_buf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&common_buf);
    query_common.SerializeToZeroCopyStream(&wrapper);
}

void executePlanSegmentRemotelyWithPreparedBuf(
    size_t segment_id,
    PlanSegmentExecutionInfo execution_info,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    const butil::IOBuf & plan_segment_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerId & worker_id)
{
    auto execute_address = extractExchangeHostPort(execution_info.execution_address);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
    Protos::PlanSegmentManagerService_Stub manager_stub(&rpc_channel->getChannel());
    Protos::SubmitPlanSegmentRequest request;
    request.set_brpc_protocol_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    request.set_plan_segment_id(segment_id);
    request.set_parallel_id(execution_info.parallel_id);
    request.set_attempt_id(execution_info.attempt_id);
    if (execution_info.source_task_filter.isValid())
        *request.mutable_source_task_filter() = execution_info.source_task_filter.toProto();

    execution_info.execution_address.toProto(*request.mutable_execution_address());
    for (const auto & iter : execution_info.sources)
    {
        for (const auto & source : iter.second)
        {
            source.toProto(*request.add_sources());
        }
    }

    if (execution_info.worker_epoch > 0)
        request.set_worker_epoch(execution_info.worker_epoch);

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
        &OnSendPlanSegmentCallback, response, cntl, std::move(rpc_channel), context.getWorkerStatusManager(), async_context, worker_id);
    async_context->addCallId(call_id);
    manager_stub.submitPlanSegment(cntl, &request, response, done);
}

void executePlanSegmentsRemotely(
    const AddressInfo & address_info,
    const PlanSegmentHeaders & plan_segment_headers,
    const butil::IOBuf & query_common_buf,
    const butil::IOBuf & query_settings_buf,
    AsyncContextPtr & async_context,
    const Context & context,
    const WorkerId & worker_id)
{
    auto execute_address = extractExchangeHostPort(address_info);
    auto rpc_channel = RpcChannelPool::getInstance().getClient(execute_address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
    Protos::PlanSegmentManagerService_Stub manager_stub(&rpc_channel->getChannel());

    // common
    Protos::SubmitPlanSegmentsRequest request;
    request.set_brpc_protocol_major_revision(DBMS_BRPC_PROTOCOL_MAJOR_VERSION);
    address_info.toProto(*request.mutable_execution_address());

    butil::IOBuf attachment;
    request.set_query_common_buf_size(query_common_buf.size());
    attachment.append(query_common_buf);
    request.set_query_settings_buf_size(query_settings_buf.size());
    if (!query_settings_buf.empty())
        attachment.append(query_settings_buf);

    // private
    for (const auto & header : plan_segment_headers)
    {
        auto * proto = request.add_plan_segment_headers();
        header.toProto(*proto);
        attachment.append(*header.plan_segment_buf_ptr);
    }

    /// async call
    auto * response = new Protos::ExecutePlanSegmentResponse;
    auto * cntl = new brpc::Controller;
    cntl->set_timeout_ms(context.getSettingsRef().send_plan_segment_timeout_ms.totalMilliseconds());
    auto call_id = cntl->call_id();
    cntl->request_attachment().append(attachment.movable());
    google::protobuf::Closure * done = brpc::NewCallback(
        &OnSendPlanSegmentCallback, response, cntl, std::move(rpc_channel), context.getWorkerStatusManager(), async_context, worker_id);
    async_context->addCallId(call_id);
    manager_stub.submitPlanSegments(cntl, &request, response, done);
}
}
