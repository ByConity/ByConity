#include "sendPlanSegment.h"

namespace DB
{

AddressInfo getLocalAddress(ContextPtr & query_context)
{
    const auto & host = getHostIPFromEnv();
    auto port = query_context->getTCPPort();
    const ClientInfo & info = query_context->getClientInfo();
    return AddressInfo(
        host, port, info.current_user, info.current_password, query_context->getExchangePort(), query_context->getExchangeStatusPort());
}

AddressInfo getRemoteAddress(HostWithPorts host_with_ports, ContextPtr & query_context)
{
    const ClientInfo & info = query_context->getClientInfo();
    return AddressInfo(
        host_with_ports.getHost(),
        host_with_ports.tcp_port,
        info.current_user,
        info.current_password,
        host_with_ports.exchange_port,
        host_with_ports.exchange_status_port);
}

void sendPlanSegmentToLocal(PlanSegment * plan_segment_ptr, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    const auto local_address = getLocalAddress(query_context);
    plan_segment_ptr->setCurrentAddress(local_address);

    /// FIXME: deserializePlanSegment is heavy task, using executePlanSegmentRemotely can deserialize plansegment asynchronous
    // executePlanSegmentLocally(*plan_segment_ptr, query_context);
    LOG_TRACE(
        &Poco::Logger::get("SegmentScheduler::sendPlanSegment"),
        "begin sendPlanSegment locally: " + std::to_string(plan_segment_ptr->getPlanSegmentId()) + " : " + plan_segment_ptr->toString());
    executePlanSegmentRemotely(*plan_segment_ptr, query_context, true, dag_graph_ptr->async_context);
    if (dag_graph_ptr)
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(std::move(local_address));
    }
}

void sendPlanSegmentToRemote(
    AddressInfo & addressinfo,
    ContextPtr query_context,
    PlanSegment * plan_segment_ptr,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    const WorkerId & worker_id)
{
    plan_segment_ptr->setCurrentAddress(addressinfo);
    LOG_TRACE(
        &Poco::Logger::get("SegmentScheduler::sendPlanSegment"),
        "begin sendPlanSegment remotely: " + std::to_string(plan_segment_ptr->getPlanSegmentId()) + " : " + plan_segment_ptr->toString());
    executePlanSegmentRemotely(*plan_segment_ptr, query_context, true, dag_graph_ptr->async_context, worker_id);
    if (dag_graph_ptr)
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(addressinfo);
    }
}

} // namespace DB
