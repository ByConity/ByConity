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
#pragma once

#include <memory>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/MPPQueryCoordinator.h>
#include <Interpreters/DistributedStages/MPPQueryManager.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/profile/ProfileLogHub.h>
#include <Protos/plan_segment_manager.pb.h>
#include <ResourceManagement/CommonData.h>
#include <brpc/server.h>
#include <Common/Brpc/BrpcServiceDefines.h>
#include <Common/ResourceMonitor.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
class SegmentScheduler;
class Context;

class ResourceMonitorTimer : public RepeatedTimerTask {
public:
    ResourceMonitorTimer(ContextMutablePtr & global_context_, UInt64 interval_, const std::string& name_, Poco::Logger* log_) :
        RepeatedTimerTask(global_context_->getSchedulePool(), interval_, name_), resource_monitor(global_context_) {
        log = log_;
    }
    virtual ~ResourceMonitorTimer() override {}
    virtual void run() override;
    WorkerNodeResourceData getResourceData() const;
    void updateResourceData();

private:
    ResourceMonitor resource_monitor;
    WorkerNodeResourceData cached_resource_data;
    mutable std::mutex resource_data_mutex;
    Poco::Logger * log;
};

class PlanSegmentManagerRpcService : public Protos::PlanSegmentManagerService
{
public:
    explicit PlanSegmentManagerRpcService(ContextMutablePtr context_)
        : context(context_)
        , log(&Poco::Logger::get("PlanSegmentManagerRpcService"))
    {
        report_metrics_timer = std::make_unique<ResourceMonitorTimer>(context, 1000, "ResourceMonitorTimer", log);
        report_metrics_timer->start();
    }

    ~PlanSegmentManagerRpcService() override
    {
        try
        {
            LOG_DEBUG(log, "Waiting report metrics timer finishing");
            report_metrics_timer->stop();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

    }

    /// submit query described by plan segment
    void submitPlanSegment(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::SubmitPlanSegmentRequest * request,
        ::DB::Protos::ExecutePlanSegmentResponse * response,
        ::google::protobuf::Closure * done) override;

    /// execute queries described by plan segments
    void submitPlanSegments(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::SubmitPlanSegmentsRequest * request,
        ::DB::Protos::ExecutePlanSegmentResponse * response,
        ::google::protobuf::Closure * done) override;

    /// receive exception report send terminate query (coordinate host ---> segment executor host)
    void cancelQuery(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::CancelQueryRequest * request,
        ::DB::Protos::CancelQueryResponse * response,
        ::google::protobuf::Closure * done) override;

    /// send plan segment status (segment executor host --> coordinator host)
    void sendPlanSegmentStatus(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::SendPlanSegmentStatusRequest * request,
        ::DB::Protos::SendPlanSegmentStatusResponse * /*response*/,
        ::google::protobuf::Closure * done) override;

    /// only report plan segment error, used by bsp mode upstream reader
    void reportPlanSegmentError(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::ReportPlanSegmentErrorRequest * request,
        ::DB::Protos::ReportPlanSegmentErrorResponse * /*response*/,
        ::google::protobuf::Closure * done) override;

    void reportProcessorProfileMetrics(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::ReportProcessorProfileMetricRequest * request,
        ::DB::Protos::ReportProcessorProfileMetricResponse * /*response*/,
        ::google::protobuf::Closure * done) override;

    void batchReportProcessorProfileMetrics(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::BatchReportProcessorProfileMetricRequest * request,
        ::DB::Protos::ReportProcessorProfileMetricResponse * /*response*/,
        ::google::protobuf::Closure * done) override;

    void sendProgress(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::SendProgressRequest * request,
        ::DB::Protos::SendProgressResponse * response,
        ::google::protobuf::Closure * done) override;

    void sendPlanSegmentProfile(
        ::google::protobuf::RpcController * /*controller*/,
        const ::DB::Protos::PlanSegmentProfileRequest * request,
        ::DB::Protos::PlanSegmentProfileResponse * /*response*/,
        ::google::protobuf::Closure * done) override;

private:
    void prepareCommonParams(
        UInt32 major_revision,
        UInt32 query_common_buf_size,
        UInt32 query_settings_buf_size,
        brpc::Controller * cntl,
        std::shared_ptr<Protos::QueryCommon> & query_common,
        std::shared_ptr<butil::IOBuf> & settings_io_buf);

    static ContextMutablePtr createQueryContext(
        ContextMutablePtr global_context,
        std::shared_ptr<Protos::QueryCommon> & query_common,
        std::shared_ptr<butil::IOBuf> & settings_io_buf,
        UInt16 remote_side_port,
        PlanSegmentInstanceId instance_id,
        const AddressInfo & execution_address);

    void executePlanSegment(
        std::shared_ptr<Protos::QueryCommon> query_common,
        std::shared_ptr<butil::IOBuf> settings_io_buf,
        UInt16 remote_side_port,
        UInt32 segment_id,
        PlanSegmentExecutionInfo & execution_info,
        std::shared_ptr<butil::IOBuf> plan_segment_buf,
        PlanSegmentProcessList::EntryPtr process_plan_segment_entry = nullptr,
        ContextMutablePtr query_context = nullptr);

    ContextMutablePtr context;
    std::unique_ptr<ResourceMonitorTimer> report_metrics_timer;
    Poco::Logger * log;
};

REGISTER_SERVICE_IMPL(PlanSegmentManagerRpcService);

}
