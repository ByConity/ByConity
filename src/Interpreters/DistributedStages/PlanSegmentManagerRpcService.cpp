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

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <IO/Progress.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentManagerRpcService.h>
#include <Interpreters/DistributedStages/PlanSegmentReport.h>
#include <Interpreters/DistributedStages/executePlanSegment.h>
#include <Interpreters/NamedSession.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Protos/plan_segment_manager.pb.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <Common/Exception.h>
#include <common/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_PROTOCOL_VERSION_UNSUPPORT;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_INTERNAL;
    extern const int TIMEOUT_EXCEEDED;
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
    LOG_DEBUG(
        log,
        "Received status of query {}, segment {}, parallel index {}, succeed: {}, cancelled: {}, code is {}",
        request->query_id(),
        request->segment_id(),
        request->parallel_index(),
        request->is_succeed(),
        request->is_canceled(),
        request->code());

    try
    {
        bool is_cancelled = (request->code() == ErrorCodes::QUERY_WAS_CANCELLED_INTERNAL) || (request->code() == ErrorCodes::QUERY_WAS_CANCELLED);
        RuntimeSegmentStatus status{
            request->query_id(),
            request->segment_id(),
            request->parallel_index(),
            request->retry_id(),
            request->is_succeed(),
            is_cancelled,
            RuntimeSegmentsMetrics(request->metrics()),
            request->message(),
            request->code()};
        SegmentSchedulerPtr scheduler = context->getSegmentScheduler();
        /// if retry is successful, this status is not used anymore
        auto bsp_scheduler = scheduler->getBSPScheduler(request->query_id());
        if (bsp_scheduler && !status.is_succeed && !status.is_cancelled)
        {
            bsp_scheduler->updateSegmentStatusCounter(request->segment_id(), request->parallel_index(), status);
            if (bsp_scheduler->retryTaskIfPossible(request->segment_id(), request->parallel_index(), status))
                return;
        }
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
        scheduler->updateReceivedSegmentStatusCounter(request->query_id(), request->segment_id(), request->parallel_index(), status);

        if (!status.is_cancelled && status.code == 0)
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
        auto coordinator = MPPQueryManager::instance().getCoordinator(request->query_id());
        if (coordinator && request->metrics().has_progress())
        {
            Progress progress;
            progress.fromProto(status.metrics.final_progress);
            coordinator->onFinalProgress(request->segment_id(), request->parallel_index(), progress);
        }
        if (!status.is_succeed)
        {
            if (coordinator)
                coordinator->updateSegmentInstanceStatus(status);
            else
            {
                LOG_INFO(
                    log,
                    "can't find coordinator for query_id:{} segment_id:{} parallel_index:{}",
                    request->query_id(),
                    request->segment_id(),
                    request->parallel_index());
            }
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

void PlanSegmentManagerRpcService::reportPlanSegmentError(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ReportPlanSegmentErrorRequest * request,
    ::DB::Protos::ReportPlanSegmentErrorResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);

    try
    {
        auto coordinator = MPPQueryManager::instance().getCoordinator(request->query_id());
        if (coordinator)
            coordinator->tryUpdateRootErrorCause(QueryError{.code = request->code(), .message = request->message()}, false);
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(false);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "reportPlanSegmentError failed: {}", error_msg);
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
        auto error_msg = getCurrentExceptionMessage(true);
        controller->SetFailed(error_msg);
        LOG_ERROR(log, "reportProcessorProfileMetrics failed: {}", error_msg);
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
        auto error_msg = getCurrentExceptionMessage(true);
        controller->SetFailed(error_msg);
        LOG_ERROR(log, "batchReportProcessorProfileMetrics failed: {}", error_msg);
    }
}

void PlanSegmentManagerRpcService::sendProgress(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::SendProgressRequest * request,
    ::DB::Protos::SendProgressResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        Progress progress;
        progress.fromProto(request->progress());
        auto coordinator = MPPQueryManager::instance().getCoordinator(request->query_id());
        if (coordinator)
        {
            coordinator->onProgress(request->segment_id(), request->parallel_id(), progress);
        }
        else
            LOG_INFO(log, "sendProgress cant find coordinator for query_id:{}", request->query_id());
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        controller->SetFailed(error_msg);
        LOG_ERROR(log, "sendProgress failed: {}", error_msg);
    }
}

void PlanSegmentManagerRpcService::prepareCommonParams(
    UInt32 major_revision,
    UInt32 query_common_buf_size,
    UInt32 query_settings_buf_size,
    brpc::Controller * cntl,
    std::shared_ptr<Protos::QueryCommon> & query_common,
    std::shared_ptr<butil::IOBuf> & settings_io_buf)
{
    if (major_revision != DBMS_BRPC_PROTOCOL_MAJOR_VERSION)
        throw Exception(
            "brpc protocol major version different - current is " + std::to_string(major_revision) + "remote is "
                + std::to_string(DBMS_BRPC_PROTOCOL_MAJOR_VERSION) + ", plan segment is not compatible",
            ErrorCodes::BRPC_PROTOCOL_VERSION_UNSUPPORT);
    /// Prepare query_common.
    butil::IOBuf query_common_buf;
    auto query_common_buf_size_act = cntl->request_attachment().cutn(&query_common_buf, query_common_buf_size);
    if (query_common_buf_size_act != query_common_buf_size)
    {
        throw Exception(
            "Impossible query_common_buf_size_act: " + std::to_string(query_common_buf_size_act)
                + "expected: " + std::to_string(query_common_buf_size),
            ErrorCodes::LOGICAL_ERROR);
    }

    butil::IOBufAsZeroCopyInputStream wrapper(query_common_buf);
    bool res = query_common->ParseFromZeroCopyStream(&wrapper);
    if (!res)
        throw Exception("Fail to parse Protos::QueryCommon!", ErrorCodes::LOGICAL_ERROR);

    /// Prepare settings.
    butil::IOBuf settings_common_buf;
    if (query_settings_buf_size > 0)
    {
        auto query_settings_buf_size_act = cntl->request_attachment().cutn(&settings_common_buf, query_settings_buf_size);
        if (query_settings_buf_size_act != query_settings_buf_size)
        {
            throw Exception(
                "Impossible query_settings_buf_size_act: " + std::to_string(query_settings_buf_size_act)
                    + "expected: " + std::to_string(query_settings_buf_size),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    settings_io_buf = std::make_shared<butil::IOBuf>(settings_common_buf.movable());
}

ContextMutablePtr PlanSegmentManagerRpcService::createQueryContext(
    ContextMutablePtr global_context,
    std::shared_ptr<Protos::QueryCommon> & query_common,
    std::shared_ptr<butil::IOBuf> & settings_io_buf,
    UInt16 remote_side_port,
    PlanSegmentInstanceId instance_id,
    const AddressInfo & execution_address)
{
    /// Create context.
    ContextMutablePtr query_context;
    UInt64 txn_id = query_common->txn_id();
    UInt64 primary_txn_id = query_common->primary_txn_id();
    /// Create session context for worker
    if (global_context->getServerType() == ServerType::cnch_worker)
    {
        size_t max_execution_time_ms = 0;
        if (query_common->has_query_expiration_timestamp())
        {
            auto duration_ms = duration_ms_from_now(query_common->query_expiration_timestamp());
            if (!duration_ms)
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "Max execution time exceeded before submit plan segment, try increase max_execution_time, current timestamp:{} "
                    "expires at:{}",
                    time_in_milliseconds(std::chrono::system_clock::now()),
                    query_common->query_expiration_timestamp());
            max_execution_time_ms = duration_ms.value();
        }
        auto named_session
            = global_context->acquireNamedCnchSession(txn_id, (max_execution_time_ms / 1000) + 1, query_common->check_session());
        query_context = Context::createCopy(named_session->context);
        query_context->setSessionContext(query_context);
        query_context->setTemporaryTransaction(txn_id, primary_txn_id);
    }
    /// execute plan semgent instance in server
    else
    {
        query_context = Context::createCopy(global_context);
        query_context->setTemporaryTransaction(txn_id, primary_txn_id, false);
    }
    query_context->setCoordinatorAddress(query_common->coordinator_address());
    query_context->setPlanSegmentInstanceId(instance_id);

    /// Authentication
    Poco::Net::SocketAddress initial_socket_address(query_common->initial_client_host(), query_common->initial_client_port());
    Poco::Net::SocketAddress current_socket_address(query_common->coordinator_address().host_name(), remote_side_port);
    Decimal64 initial_query_start_time_microseconds{query_common->initial_query_start_time()};

    /// Set client info.
    ClientInfo & client_info = query_context->getClientInfo();
    client_info.brpc_protocol_minor_version = query_common->brpc_protocol_minor_revision();
    client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    client_info.interface = ClientInfo::Interface::BRPC;
    client_info.initial_query_start_time = initial_query_start_time_microseconds / 1000000;
    client_info.initial_query_start_time_microseconds = initial_query_start_time_microseconds;
    client_info.initial_user = query_common->initial_user();
    client_info.initial_query_id = query_common->query_id();
    client_info.initial_address = std::move(initial_socket_address);
    client_info.current_query_id = client_info.initial_query_id + "_" + std::to_string(instance_id.segment_id);
    client_info.current_address = std::move(current_socket_address);
    client_info.rpc_port = query_common->coordinator_address().exchange_port();

    /// Authentication
    const auto & current_user = execution_address.getUser();
    query_context->setUser(current_user, execution_address.getPassword(), query_context->getClientInfo().current_address);

    if (!settings_io_buf->empty())
    {
        ReadBufferFromBrpcBuf settings_read_buf(*settings_io_buf);
        /// Sets an extra row policy based on `client_info.initial_user`
        // query_context->setInitialRowPolicy();
        /// apply settings changed
        const size_t MIN_MINOR_VERSION_ENABLE_STRINGS_WITH_FLAGS = 4;
        if (query_common->brpc_protocol_minor_revision() >= MIN_MINOR_VERSION_ENABLE_STRINGS_WITH_FLAGS)
            const_cast<Settings &>(query_context->getSettingsRef()).read(settings_read_buf, SettingsWriteFormat::STRINGS_WITH_FLAGS);
        else
            const_cast<Settings &>(query_context->getSettingsRef()).read(settings_read_buf, SettingsWriteFormat::BINARY);
    }

    /// Disable function name normalization when it's a secondary query, because queries are either
    /// already normalized on initiator node, or not normalized and should remain unnormalized for
    /// compatibility.
    query_context->setSetting("normalize_function_names", Field(0));

    if (query_context->getServerType() == ServerType::cnch_worker)
        query_context->grantAllAccess();

    /// Set quota
    if (query_common->has_quota())
        query_context->setQuotaKey(query_common->quota());

    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();

    query_context->setQueryExpirationTimeStamp();

    return query_context;
}

void PlanSegmentManagerRpcService::executePlanSegment(
    std::shared_ptr<Protos::QueryCommon> query_common,
    std::shared_ptr<butil::IOBuf> settings_io_buf,
    UInt16 remote_side_port,
    UInt32 segment_id,
    PlanSegmentExecutionInfo & execution_info,
    std::shared_ptr<butil::IOBuf> plan_segment_buf,
    PlanSegmentProcessList::EntryPtr process_plan_segment_entry,
    ContextMutablePtr query_context)
{
    LOG_INFO(log, "execute plan segment: {}_{}, parallel index {}", query_common->query_id(), segment_id, execution_info.parallel_id);

    ThreadFromGlobalPool async_thread([global_context = context,
                                       query_common = std::move(query_common),
                                       settings_io_buf = std::move(settings_io_buf),
                                       remote_side_port = remote_side_port,
                                       segment_id = segment_id,
                                       execution_info = std::move(execution_info),
                                       plan_segment_buf = std::move(plan_segment_buf),
                                       process_plan_segment_entry = std::move(process_plan_segment_entry),
                                       query_context = std::move(query_context)]() mutable {
        bool before_execute = true;
        try
        {
            if (!query_context)
                query_context = createQueryContext(
                    global_context,
                    query_common,
                    settings_io_buf,
                    remote_side_port,
                    {segment_id, execution_info.parallel_id},
                    execution_info.execution_address);

            if (!process_plan_segment_entry)
                process_plan_segment_entry = query_context->getPlanSegmentProcessList().insertGroup(query_context, segment_id);

            /// Plan segment Deserialization can't run in bthread since checkStackSize method is not compatible with all user-space lightweight threads that manually allocated stacks.
            butil::IOBufAsZeroCopyInputStream plansegment_buf_wrapper(*plan_segment_buf);
            Protos::PlanSegment plan_segment_proto;
            plan_segment_proto.ParseFromZeroCopyStream(&plansegment_buf_wrapper);
            // copy some commnon field from query_common;
            plan_segment_proto.set_query_id(query_common->query_id());
            plan_segment_proto.set_segment_id(segment_id);
            plan_segment_proto.mutable_coordinator_address()->CopyFrom(query_common->coordinator_address());
            auto plan_segment = std::make_unique<PlanSegment>();
            plan_segment->fillFromProto(plan_segment_proto, query_context);
            plan_segment->update(query_context);
            auto segment_instance = std::make_unique<PlanSegmentInstance>();

            before_execute = false;
            segment_instance->info = std::move(execution_info);
            segment_instance->plan_segment = std::move(plan_segment);
            // keep query_context for log query_id when exception
            executePlanSegmentInternal(std::move(segment_instance), query_context, std::move(process_plan_segment_entry), false);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, query_context ? query_context->getCurrentQueryId() : "");
            if (before_execute)
            {
                int exception_code = getCurrentExceptionCode();
                auto exception_message = getCurrentExceptionMessage(false);

                auto result = convertFailurePlanSegmentStatusToResult(query_context, execution_info, exception_code, exception_message);
                reportExecutionResult(result);
            }
        }
    });
    async_thread.detach();
}

void PlanSegmentManagerRpcService::submitPlanSegment(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::SubmitPlanSegmentRequest * request,
    ::DB::Protos::ExecutePlanSegmentResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        auto query_common = std::make_shared<Protos::QueryCommon>();
        std::shared_ptr<butil::IOBuf> settings_io_buf;
        prepareCommonParams(
            request->brpc_protocol_major_revision(),
            request->query_common_buf_size(),
            request->query_settings_buf_size(),
            cntl,
            query_common,
            settings_io_buf);

        PlanSegmentExecutionInfo execution_info;
        execution_info.parallel_id = request->parallel_id();
        execution_info.execution_address = AddressInfo(request->execution_address());
        // TODO source_task_index && source_task_count will be removed in future @lianxuechao
        if (request->has_source_task_index() && request->has_source_task_count())
        {
            execution_info.source_task_filter.index = request->source_task_index();
            execution_info.source_task_filter.count = request->source_task_count();
        }
        else if (request->has_source_task_filter())
        {
            execution_info.source_task_filter.fromProto(request->source_task_filter());
        }
        if (request->has_retry_id())
            execution_info.retry_id = request->retry_id();

        butil::IOBuf plan_segment_buf;
        auto plan_segment_buf_size = cntl->request_attachment().cutn(&plan_segment_buf, request->plan_segment_buf_size());
        if (plan_segment_buf_size != request->plan_segment_buf_size())
        {
            throw Exception(
                "Impossible plan_segment_buf_size: " + std::to_string(plan_segment_buf_size)
                    + "expected: " + std::to_string(request->plan_segment_buf_size()),
                ErrorCodes::LOGICAL_ERROR);
        }

        executePlanSegment(
            std::move(query_common),
            std::move(settings_io_buf),
            cntl->remote_side().port,
            request->plan_segment_id(),
            execution_info,
            std::make_shared<butil::IOBuf>(plan_segment_buf.movable()));

        report_metrics_timer->getResourceData().fillProto(*response->mutable_worker_resource_data());
        LOG_TRACE(log, "adaptive scheduler worker status: {}", response->worker_resource_data().ShortDebugString());
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "executeQuery failed: {}", error_msg);
    }
}

void PlanSegmentManagerRpcService::submitPlanSegments(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::SubmitPlanSegmentsRequest * request,
    ::DB::Protos::ExecutePlanSegmentResponse * response,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    try
    {
        auto query_common = std::make_shared<Protos::QueryCommon>();
        std::shared_ptr<butil::IOBuf> settings_io_buf;
        prepareCommonParams(
            request->brpc_protocol_major_revision(),
            request->query_common_buf_size(),
            request->query_settings_buf_size(),
            cntl,
            query_common,
            settings_io_buf);

        // prepare segmentGroup
        std::vector<size_t> segment_ids;
        std::optional<PlanSegmentInstanceId> first_instance_id;
        for (const auto & header : request->plan_segment_headers())
        {
            segment_ids.emplace_back(header.plan_segment_id());
            if (!first_instance_id)
                first_instance_id = {header.plan_segment_id(), header.parallel_id()};
        }

        const auto execution_address = AddressInfo(request->execution_address());
        auto first_query_context
            = createQueryContext(context, query_common, settings_io_buf, cntl->remote_side().port, *first_instance_id, execution_address);
        auto process_plan_segment_entries = first_query_context->getPlanSegmentProcessList().insertGroup(first_query_context, segment_ids);

        const auto & headers = request->plan_segment_headers();
        size_t header_size = headers.size();
        for (size_t i = 0; i < header_size; ++i)
        {
            const auto & header = headers[i];
            PlanSegmentExecutionInfo execution_info;
            execution_info.parallel_id = header.parallel_id();
            execution_info.execution_address = execution_address;
            if (header.has_retry_id())
                execution_info.retry_id = header.retry_id();
            if (header.has_source_task_filter())
                execution_info.source_task_filter.fromProto(header.source_task_filter());

            butil::IOBuf plan_segment_buf;
            auto plan_segment_buf_size = cntl->request_attachment().cutn(&plan_segment_buf, header.plan_segment_buf_size());
            if (plan_segment_buf_size != header.plan_segment_buf_size())
            {
                throw Exception(
                    "Impossible plan_segment_buf_size: " + std::to_string(plan_segment_buf_size)
                        + "expected: " + std::to_string(header.plan_segment_buf_size()),
                    ErrorCodes::LOGICAL_ERROR);
            }

            executePlanSegment(
                query_common,
                settings_io_buf,
                cntl->remote_side().port,
                header.plan_segment_id(),
                execution_info,
                std::make_shared<butil::IOBuf>(plan_segment_buf.movable()),
                std::move(process_plan_segment_entries[i]),
                (i == 0) ? first_query_context : nullptr);
        }

        report_metrics_timer->getResourceData().fillProto(*response->mutable_worker_resource_data());
        LOG_TRACE(log, "adaptive scheduler worker status: {}", response->worker_resource_data().ShortDebugString());
    }
    catch (...)
    {
        auto error_msg = getCurrentExceptionMessage(true);
        cntl->SetFailed(error_msg);
        LOG_ERROR(log, "executeQuery failed: {}", error_msg);
    }
}

void PlanSegmentManagerRpcService::sendPlanSegmentProfile(
    ::google::protobuf::RpcController * /*controller*/,
    const ::DB::Protos::PlanSegmentProfileRequest * request,
    ::DB::Protos::PlanSegmentProfileResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    PlanSegmentProfilePtr profile = PlanSegmentProfile::fromProto(*request);
    const SegmentSchedulerPtr & scheduler = context->getSegmentScheduler();
    scheduler->updateSegmentProfile(profile);
}
}
