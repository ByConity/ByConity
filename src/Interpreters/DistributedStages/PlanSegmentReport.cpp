#include <Interpreters/DistributedStages/PlanSegmentReport.h>


namespace DB
{

void reportPlanSegmentStatus(const AddressInfo & coordinator_address, const RuntimeSegmentsStatus & status) noexcept
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

        manager.sendPlanSegmentStatus(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
        LOG_TRACE(
            logger,
            "PlanSegment-{} send status to coordinator successfully, query id-{} cpu_micros-{} is_succeed:{} is_cancelled:{} code:{}",
            request.segment_id(),
            request.query_id(),
            status.metrics.cpu_micros,
            status.is_succeed,
            status.is_cancelled,
            status.code);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
