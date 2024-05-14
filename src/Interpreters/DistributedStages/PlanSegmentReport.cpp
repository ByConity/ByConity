#include <Interpreters/DistributedStages/PlanSegmentReport.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

void reportPlanSegmentStatus(
    const AddressInfo & coordinator_address,
    const AddressInfo & execution_address,
    const RuntimeSegmentsStatus & status,
    Progress & final_progress,
    SenderMetrics & sender_metrics,
    PlanSegmentOutputs & plan_segment_outputs) noexcept
{
    static auto * logger = &Poco::Logger::get("PlanSegmentExecutor");
    try
    {
        auto address = extractExchangeHostPort(coordinator_address);

        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(address, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY, true);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller cntl;
        Protos::SendPlanSegmentStatusRequest request;
        Protos::SendPlanSegmentStatusResponse response;
        request.set_query_id(status.query_id);
        request.set_segment_id(status.segment_id);
        request.set_parallel_index(status.parallel_index);
        request.set_is_succeed(status.is_succeed);
        request.set_is_canceled(status.is_cancelled);
        status.metrics.setProtos(*request.mutable_metrics());
        request.set_code(status.code);
        request.set_message(status.message);

        if (!sender_metrics.bytes_sent.empty())
        {
            execution_address.toProto(*request.mutable_sender_metrics()->mutable_address());
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
                auto & b = *request.mutable_sender_metrics()->mutable_send_bytes()->Add();
                b.set_exchange_id(exchange_id);
                for (size_t i = 0; i < bytes_sum.size(); i++)
                {
                    auto & b_i = *b.mutable_bytes_by_index()->Add();
                    b_i.set_parallel_index(i);
                    b_i.set_bytes_sent(bytes_sum[i]);
                }
            }
        }

        manager.sendPlanSegmentStatus(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_INFO(
            logger,
            "PlanSegment-{} send status to coordinator successfully, query id-{} parallel_index-{} cpu_micros-{} is_succeed:{} "
            "is_cancelled:{} code:{} final_progress:{}",
            request.segment_id(),
            request.query_id(),
            status.parallel_index,
            status.metrics.cpu_micros,
            status.is_succeed,
            status.is_cancelled,
            status.code,
            final_progress.getValues().toString());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void reportFailurePlanSegmentStatus(
    ContextPtr query_context,
    const AddressInfo & execution_address,
    int exception_code,
    const String & exception_message,
    Progress final_progress,
    SenderMetrics sender_metrics,
    PlanSegmentOutputs plan_segment_outputs)
{
    auto host = extractExchangeHostPort(execution_address);

    RuntimeSegmentsStatus runtime_segment_status;
    runtime_segment_status.query_id = query_context->getClientInfo().initial_query_id;
    runtime_segment_status.segment_id = query_context->getPlanSegmentInstanceId().segment_id;
    runtime_segment_status.parallel_index = query_context->getPlanSegmentInstanceId().parallel_id;
    runtime_segment_status.is_succeed = false;
    runtime_segment_status.is_cancelled = exception_code == ErrorCodes::QUERY_WAS_CANCELLED;
    runtime_segment_status.code = exception_code;
    runtime_segment_status.message = "Worker host:" + host + ", exception:" + exception_message;
    runtime_segment_status.metrics.final_progress = final_progress.toProto();

    reportPlanSegmentStatus(query_context->getCoordinatorAddress(), execution_address, runtime_segment_status, final_progress, sender_metrics, plan_segment_outputs);
}

void reportSuccessPlanSegmentStatus(
    ContextPtr query_context,
    const AddressInfo & execution_address,
    Progress & final_progress,
    SenderMetrics & sender_metrics,
    PlanSegmentOutputs & plan_segment_outputs)
{
    RuntimeSegmentsStatus runtime_segment_status;
    runtime_segment_status.query_id = query_context->getClientInfo().initial_query_id;
    runtime_segment_status.segment_id = query_context->getPlanSegmentInstanceId().segment_id;
    runtime_segment_status.parallel_index = query_context->getPlanSegmentInstanceId().parallel_id;
    runtime_segment_status.is_succeed = true;
    runtime_segment_status.is_cancelled = false;
    runtime_segment_status.code = 0;
    runtime_segment_status.message = "execute success";
    runtime_segment_status.metrics.final_progress = final_progress.toProto();

    reportPlanSegmentStatus(query_context->getCoordinatorAddress(), execution_address, runtime_segment_status, final_progress, sender_metrics, plan_segment_outputs);
}

}
