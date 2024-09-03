#include "sendPlanSegment.h"

namespace DB
{

AddressInfo getLocalAddress(const Context & query_context)
{
    const auto & host = getHostIPFromEnv();
    auto port = query_context.getTCPPort();
    const ClientInfo & info = query_context.getClientInfo();
    return AddressInfo(host, port, info.current_user, info.current_password, query_context.getRPCPort());
}

AddressInfo getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context)
{
    const ClientInfo & info = query_context->getClientInfo();
    return AddressInfo(
        host_with_ports.host,
        host_with_ports.tcp_port,
        info.current_user,
        info.current_password,
        host_with_ports.rpc_port);
}

void sendPlanSegmentToAddress(
    const AddressInfo & addressinfo,
    PlanSegment * plan_segment_ptr,
    PlanSegmentExecutionInfo & execution_info,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr,
    const WorkerId & worker_id)
{
    static auto * log = &Poco::Logger::get("SegmentScheduler::sendPlanSegment");
    LOG_TRACE(
        log,
        "query id {} segment id {}, parallel index {}, address {}, plansegment {}",
        plan_segment_ptr->getQueryId(),
        plan_segment_ptr->getPlanSegmentId(),
        execution_info.parallel_id,
        addressinfo.toString(),
        plan_segment_ptr->toString());
    if (execution_info.source_task_filter.isValid())
        LOG_TRACE(log, "send additional filter {}", execution_info.source_task_filter.toString());
    execution_info.execution_address = addressinfo;

    executePlanSegmentRemotelyWithPreparedBuf(
        *plan_segment_ptr,
        std::move(execution_info),
        dag_graph_ptr->query_common_buf,
        dag_graph_ptr->query_settings_buf,
        *plan_segment_buf_ptr,
        dag_graph_ptr->async_context,
        *query_context.get(),
        worker_id);

    std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
    dag_graph_ptr->plan_send_addresses.emplace(addressinfo);
}

} // namespace DB
