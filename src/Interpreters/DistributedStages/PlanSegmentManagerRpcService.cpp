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
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/PlanSegmentManagerRpcService.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/NamedSession.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <mutex>
#include <common/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_PROTOCOL_VERSION_UNSUPPORT;
}

WorkerNodeResourceData ResourceMonitorTimer::getResourceData() const {
    std::lock_guard lock(resource_data_mutex);
    return cached_resource_data;
}

void ResourceMonitorTimer::updateResourceData() {
    auto data = resource_monitor.createResourceData();
    data.id = data.host_ports.id;
    if (auto vw = getenv("VIRTUAL_WAREHOUSE_ID"))
        data.vw_name = vw;
    if (auto group_id = getenv("WORKER_GROUP_ID"))
        data.worker_group_id = group_id;
    std::lock_guard lock(resource_data_mutex);
    cached_resource_data = data;

}

void ResourceMonitorTimer::run() {
    updateResourceData();
    task->scheduleAfter(interval);
}

void PlanSegmentManagerRpcService::executeQuery(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ExecutePlanSegmentRequest * request,
    ::DB::Protos::ExecutePlanSegmentResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        ContextMutablePtr query_context;
        UInt64 txn_id = request->txn_id();
        UInt64 primary_txn_id = request->primary_txn_id();
        /// Create session context for worker
        if (context->getServerType() == ServerType::cnch_worker)
        {
            auto named_session = context->acquireNamedCnchSession(txn_id, {}, request->check_session());
            query_context = Context::createCopy(named_session->context);
            query_context->setSessionContext(query_context);
            query_context->setTemporaryTransaction(txn_id, primary_txn_id);
        }
        else
        {
            query_context = Context::createCopy(context);
            query_context->setTemporaryTransaction(txn_id, primary_txn_id, false);
        }

        /// TODO: Authentication supports inter-server cluster secret, see https://github.com/ClickHouse/ClickHouse/commit/0159c74f217ec764060c480819e3ccc9d5a99a63
        Poco::Net::SocketAddress initial_socket_address(request->coordinator_host(), request->coordinator_port());
        query_context->setUser(request->user(), request->password(), initial_socket_address);

        /// Set client info.
        ClientInfo & client_info = query_context->getClientInfo();
        client_info.brpc_protocol_major_version = request->brpc_protocol_major_revision();
        client_info.brpc_protocol_minor_version = request->brpc_protocol_minor_revision();
        if (client_info.brpc_protocol_major_version != DBMS_BRPC_PROTOCOL_MAJOR_VERSION)
            throw Exception(
                "brpc protocol major version different - current is " + std::to_string(client_info.brpc_protocol_major_version) +
                "remote is " + std::to_string(DBMS_BRPC_PROTOCOL_MAJOR_VERSION) +
                ", plan segment is not compatible", ErrorCodes::BRPC_PROTOCOL_VERSION_UNSUPPORT
            );
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        client_info.interface = ClientInfo::Interface::BRPC;
        Decimal64 initial_query_start_time_microseconds {request->initial_query_start_time()};
        client_info.initial_query_start_time = initial_query_start_time_microseconds / 1000000;
        client_info.initial_query_start_time_microseconds = initial_query_start_time_microseconds;
        /// Fall back to user when initial_user not set for compatibility.
        client_info.initial_user = request->has_initial_user() ? request->initial_user() : request->user();
        client_info.initial_query_id = request->query_id();

        client_info.initial_address = initial_socket_address;

        client_info.current_query_id = request->query_id() + "_" + std::to_string(request->plan_segment_id());
        client_info.current_address = Poco::Net::SocketAddress(request->current_host(), request->current_port());

        /// Prepare settings.
        SettingsChanges settings_changes;
        settings_changes.reserve(request->settings_size());
        for (const auto & [key, value] : request->settings())
        {
            settings_changes.push_back({key, value});
        }

        /// Sets an extra row policy based on `client_info.initial_user`
        query_context->setInitialRowPolicy();

        /// Quietly clamp to the constraints since it's a secondary query.
        query_context->clampToSettingsConstraints(settings_changes);
        query_context->applySettingsChanges(settings_changes);

        /// Disable function name normalization when it's a secondary query, because queries are either
        /// already normalized on initiator node, or not normalized and should remain unnormalized for
        /// compatibility.
        query_context->setSetting("normalize_function_names", Field(0));

        /// Set quota
        if (!request->has_quota())
            query_context->setQuotaKey(request->quota());

        if (!query_context->hasQueryContext())
            query_context->makeQueryContext();

        query_context->setQueryExpirationTimeStamp();

        report_metrics_timer->getResourceData().fillProto(*response->mutable_worker_resource_data());
        LOG_DEBUG(log, "adaptive scheduler worker status: {}", response->worker_resource_data().ShortDebugString());

        ThreadFromGlobalPool async_thread([query_context = std::move(query_context),
                                           plan_segment_buf = std::make_shared<butil::IOBuf>(cntl->request_attachment().movable())]() {
            try
            {
                /// Plan segment Deserialization can't run in bthread since checkStackSize method is not compatible with all user-space lightweight threads that manually allocated stacks.
                ReadBufferFromBrpcBuf plan_segment_read_buf(*plan_segment_buf);
                auto plan_segment = PlanSegment::deserializePlanSegment(plan_segment_read_buf, query_context);
                executePlanSegmentInternal(std::move(plan_segment), std::move(query_context), false);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        });
        async_thread.detach();
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(false);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "executeQuery failed: {}", error_msg);
    }
}

void PlanSegmentManagerRpcService::cancelQuery(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::CancelQueryRequest * request,
    ::DB::Protos::CancelQueryResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);

    try
    {
        auto cancel_code
            = context->getPlanSegmentProcessList().tryCancelPlanSegmentGroup(request->query_id(), request->coordinator_address());
        response->set_ret_code(std::to_string(static_cast<int>(cancel_code)));
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(false);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "cancelQuery failed: {}", error_msg);
    }
}

void PlanSegmentManagerRpcService::sendPlanSegmentStatus(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::SendPlanSegmentStatusRequest * request,
    ::DB::Protos::SendPlanSegmentStatusResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);

    try
    {
        RuntimeSegmentsStatus status{
            request->query_id(),
            request->segment_id(),
            request->parallel_index(),
            request->is_succeed(),
            request->is_canceled(),
            RuntimeSegmentsMetrics(request->metrics()),
            request->message(),
            request->code()};
        SegmentSchedulerPtr scheduler = context->getSegmentScheduler();
        scheduler->updateSegmentStatus(status);
        scheduler->updateQueryStatus(status);
        if (request->has_sender_metrics())
        {
            for (const auto & [ex_id, exg_status] : fromSenderMetrics(request->sender_metrics()))
            {
                context->getExchangeDataTracker()->registerExchangeStatus(
                    request->query_id(), ex_id, request->parallel_index(), exg_status);
            }
        }
        // TODO(WangTao): fine grained control, conbining with retrying.
        if (status.is_succeed)
            scheduler->updateReceivedSegmentStatusCounter(request->query_id(), request->segment_id(), request->parallel_index());


        if (scheduler->explainQueryHasReceivedAllSegmentStatus(request->query_id()))
        {
            ProfileLogHub<ProcessorProfileLogElement>::getInstance().stopConsume(status.query_id);
            LOG_DEBUG(log, "Query:{} have received all segment status.", status.query_id);
        }

        if (scheduler->bspQueryReceivedAllStatusOfSegment(request->query_id(), request->segment_id()))
            scheduler->onSegmentFinished(status);

        if (!status.is_canceled && status.code == 0)
        {
            try
            {
                scheduler->checkQueryCpuTime(status.query_id);
            }
            catch (const Exception & e)
            {
                status.message = e.message();
                status.code = e.code();
                status.is_succeed = false;
            }
        }

        // this means exception happened during execution.
        if (!status.is_succeed && !status.is_canceled)
        {
            auto coodinator = MPPQueryManager::instance().getCoordinator(request->query_id());
            if (coodinator)
                coodinator->updateSegmentInstanceStatus(status);
            scheduler->onSegmentFinished(status);
        }
        // todo  scheduler.cancelSchedule
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(false);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "sendPlanSegmentStatus failed: {}", error_msg);
    }
}

void parseReportProcessorProfileMetricRequest(ProcessorProfileLogElement & profile_element, const ::DB::Protos::ReportProcessorProfileMetricRequest * request)
{
    profile_element.query_id = request->query_id();
    profile_element.event_time = request->event_time();
    profile_element.event_time_microseconds = request->event_time_microseconds();
    profile_element.elapsed_us = request->elapsed_us();
    profile_element.input_wait_elapsed_us = request->input_wait_elapsed_us();
    profile_element.output_wait_elapsed_us = request->output_wait_elapsed_us();
    profile_element.id = request->id();
    profile_element.input_rows = request->input_rows();
    profile_element.input_bytes = request->input_bytes();
    profile_element.output_rows = request->output_rows();
    profile_element.output_bytes = request->output_bytes();
    profile_element.processor_name = request->processor_name();
    profile_element.plan_group = request->plan_group();
    profile_element.plan_step = request->plan_step();
    profile_element.step_id = request->step_id();
    profile_element.worker_address = request->worker_address();
    profile_element.parent_ids = std::vector<UInt64>(request->parent_ids().begin(), request->parent_ids().end());
}

void PlanSegmentManagerRpcService::reportProcessorProfileMetrics(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ReportProcessorProfileMetricRequest * request,
    ::DB::Protos::ReportProcessorProfileMetricResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    ProcessorProfileLogElement element;
    try
    {
        parseReportProcessorProfileMetricRequest(element, request);
        auto query_id = element.query_id;
        auto timeout = context->getSettingsRef().report_processors_profiles_timeout_millseconds;

        if (ProfileLogHub<ProcessorProfileLogElement>::getInstance().hasConsumer())
            ProfileLogHub<ProcessorProfileLogElement>::getInstance().tryPushElement(query_id, element, timeout);
    }
    catch (...)
    {
        controller->SetFailed("Report fail.");
    }
}

void PlanSegmentManagerRpcService::batchReportProcessorProfileMetrics(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::BatchReportProcessorProfileMetricRequest * request,
    ::DB::Protos::ReportProcessorProfileMetricResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    std::vector<ProcessorProfileLogElement> elements;
    try
    {
        const auto & query_id = request->query_id();
        for (const auto & inner_request : request->request())
        {
            ProcessorProfileLogElement element;
            parseReportProcessorProfileMetricRequest(element, &inner_request);
            elements.emplace_back(std::move(element));
        }
        auto timeout = context->getSettingsRef().report_processors_profiles_timeout_millseconds;
        if (ProfileLogHub<ProcessorProfileLogElement>::getInstance().hasConsumer())
            ProfileLogHub<ProcessorProfileLogElement>::getInstance().tryPushElement(query_id, elements, timeout);
    }
    catch (...)
    {
        controller->SetFailed("Report fail.");
    }
}
}
