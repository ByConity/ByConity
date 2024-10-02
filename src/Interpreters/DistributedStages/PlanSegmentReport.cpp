#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/PlanSegmentExecutor.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/DistributedStages/PlanSegmentReport.h>
#include <Protos/plan_segment_manager.pb.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

void reportExecutionResult(const PlanSegmentExecutor::ExecutionResult & result) noexcept
{
    static auto * logger = &Poco::Logger::get("PlanSegmentExecutor");
    try
    {
        if (result.segment_profile)
            reportSuccessPlanSegmentProfile(result);
        auto address = extractExchangeHostPort(result.coordinator_address);
        const auto & status = result.runtime_segment_status;

        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller cntl;
        Protos::SendPlanSegmentStatusRequest request;
        Protos::SendPlanSegmentStatusResponse response;
        request.set_query_id(status.query_id);
        request.set_segment_id(status.segment_id);
        request.set_parallel_index(status.parallel_index);
        request.set_retry_id(status.retry_id);
        request.set_is_succeed(status.is_succeed);
        request.set_is_canceled(status.is_cancelled);
        status.metrics.setProtos(*request.mutable_metrics());
        request.set_code(status.code);
        request.set_message(status.message);
        *request.mutable_sender_metrics() = std::move(result.sender_metrics);

        manager.sendPlanSegmentStatus(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_INFO(
            logger,
            "PlanSegment-{} send status to coordinator successfully, query id-{} parallel_index-{} cpu_micros-{} is_succeed:{} "
            "is_cancelled:{} code:{}",
            request.segment_id(),
            request.query_id(),
            status.parallel_index,
            status.metrics.cpu_micros,
            status.is_succeed,
            status.is_cancelled,
            status.code);
    }
    catch (...)
    {
        tryLogCurrentException(logger, __PRETTY_FUNCTION__);
    }
}

Protos::SenderMetrics
senderMetricsToProto(const PlanSegmentOutputs & plan_segment_outputs, SenderMetrics & sender_metrics, const AddressInfo & execution_address)
{
    Protos::SenderMetrics sender_metrics_proto;
    if (!sender_metrics.bytes_sent.empty())
    {
        execution_address.toProto(*sender_metrics_proto.mutable_address());
        for (const auto & cur_plan_segment_output : plan_segment_outputs)
        {
            size_t exchange_parallel_size = cur_plan_segment_output->getExchangeParallelSize();
            size_t parallel_size = cur_plan_segment_output->getParallelSize();
            size_t exchange_id = cur_plan_segment_output->getExchangeId();
            const auto & output_for_exchange = sender_metrics.bytes_sent[exchange_id];
            std::vector<size_t> bytes_sum(parallel_size);
            std::generate(bytes_sum.begin(), bytes_sum.end(), []() { return 0; });
            for (const auto & [p_id, b] : output_for_exchange)
            {
                bytes_sum[p_id / exchange_parallel_size] += b;
            }
            auto & b = *sender_metrics_proto.mutable_send_bytes()->Add();
            b.set_exchange_id(exchange_id);
            for (size_t i = 0; i < bytes_sum.size(); i++)
            {
                auto & b_i = *b.mutable_bytes_by_index()->Add();
                b_i.set_parallel_index(i);
                b_i.set_bytes_sent(bytes_sum[i]);
            }
        }
    }

    return sender_metrics_proto;
}

PlanSegmentExecutor::ExecutionResult convertFailurePlanSegmentStatusToResult(
    ContextPtr query_context,
    const PlanSegmentExecutionInfo & execution_info,
    int exception_code,
    const String & exception_message,
    Progress final_progress,
    SenderMetrics sender_metrics,
    PlanSegmentOutputs plan_segment_outputs)
{
    PlanSegmentExecutor::ExecutionResult result;

    result.coordinator_address = query_context->getCoordinatorAddress();
    result.runtime_segment_status.query_id = query_context->getClientInfo().initial_query_id;
    result.runtime_segment_status.segment_id = query_context->getPlanSegmentInstanceId().segment_id;
    result.runtime_segment_status.parallel_index = execution_info.parallel_id;
    result.runtime_segment_status.retry_id = execution_info.retry_id;
    result.runtime_segment_status.is_succeed = false;
    result.runtime_segment_status.is_cancelled = exception_code == ErrorCodes::QUERY_WAS_CANCELLED;
    result.runtime_segment_status.code = exception_code;
    result.runtime_segment_status.message
        = "Worker host:" + extractExchangeHostPort(execution_info.execution_address) + ", exception:" + exception_message;
    result.runtime_segment_status.metrics.final_progress = final_progress.toProto();
    result.sender_metrics = senderMetricsToProto(plan_segment_outputs, sender_metrics, execution_info.execution_address);

    return result;
}

PlanSegmentExecutor::ExecutionResult convertSuccessPlanSegmentStatusToResult(
    ContextPtr query_context,
    const PlanSegmentExecutionInfo & execution_info,
    Progress & final_progress,
    SenderMetrics & sender_metrics,
    PlanSegmentOutputs & plan_segment_outputs,
    PlanSegmentProfilePtr & segment_profile)
{
    PlanSegmentExecutor::ExecutionResult result;

    result.coordinator_address = query_context->getCoordinatorAddress();
    result.runtime_segment_status.query_id = query_context->getClientInfo().initial_query_id;
    result.runtime_segment_status.segment_id = query_context->getPlanSegmentInstanceId().segment_id;
    result.runtime_segment_status.parallel_index = execution_info.parallel_id;
    result.runtime_segment_status.retry_id = execution_info.retry_id;
    result.runtime_segment_status.is_succeed = true;
    result.runtime_segment_status.is_cancelled = false;
    result.runtime_segment_status.code = 0;
    result.runtime_segment_status.message = "execute success";
    result.runtime_segment_status.metrics.final_progress = final_progress.toProto();
    result.sender_metrics = senderMetricsToProto(plan_segment_outputs, sender_metrics, execution_info.execution_address);
    if (query_context->getSettingsRef().report_segment_profiles && segment_profile)
        result.segment_profile = segment_profile;

    return result;
}

void reportSuccessPlanSegmentProfile(const PlanSegmentExecutor::ExecutionResult & result)
{
    static auto * logger = &Poco::Logger::get("PlanSegmentExecutor");
    try
    {
        std::shared_ptr<RpcClient> rpc_client = RpcChannelPool::getInstance().getClient(
            extractExchangeHostPort(result.coordinator_address), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller cntl;
        Protos::PlanSegmentProfileRequest request;
        Protos::PlanSegmentProfileResponse response;
        result.segment_profile->is_succeed = true;
        result.segment_profile->toProto(request);

        manager.sendPlanSegmentProfile(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_TRACE(
            logger, "PlanSegment-{} send profile to coordinator successfully, query id-{}.", request.segment_id(), request.query_id());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}
}
