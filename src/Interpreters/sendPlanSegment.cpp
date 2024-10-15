#include "sendPlanSegment.h"
#include <common/logger_useful.h>

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
    if(query_context->getSettingsRef().enable_internal_communication_user)
    {
        // Trick for avoiding RBAC performace loss
        static auto [user, password] = query_context->getCnchInterserverCredentials();
        return AddressInfo(
            host_with_ports.host,
            host_with_ports.tcp_port,
            user,
            password,
            host_with_ports.rpc_port);
    }

    const ClientInfo & info = query_context->getClientInfo();
    return AddressInfo(
        host_with_ports.host,
        host_with_ports.tcp_port,
        info.current_user,
        info.current_password,
        host_with_ports.rpc_port);

}

void sendPlanSegmentToAddress(
    const AddressInfo & address_info,
    PlanSegment * plan_segment_ptr,
    PlanSegmentExecutionInfo & execution_info,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    std::shared_ptr<butil::IOBuf> plan_segment_buf_ptr,
    const WorkerId & worker_id)
{
    static auto log = getLogger("SegmentScheduler::sendPlanSegment");
    LOG_TRACE(
        log,
        "query id {} segment id {}, parallel index {}, address {}, addtional filters {}, plansegment {}",
        plan_segment_ptr->getQueryId(),
        plan_segment_ptr->getPlanSegmentId(),
        execution_info.parallel_id,
        address_info.toString(),
        execution_info.source_task_filter.isValid() ? execution_info.source_task_filter.toString() : "Invalid",
        plan_segment_ptr->toString());
    execution_info.execution_address = address_info;

    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(address_info);
    }

    executePlanSegmentRemotelyWithPreparedBuf(
        plan_segment_ptr->getPlanSegmentId(),
        std::move(execution_info),
        dag_graph_ptr->query_common_buf,
        dag_graph_ptr->query_settings_buf,
        *plan_segment_buf_ptr,
        dag_graph_ptr->async_context,
        *query_context.get(),
        worker_id);
}

void sendPlanSegmentsToAddress(
    const AddressInfo & address_info,
    const PlanSegmentHeaders & plan_segment_headers,
    ContextPtr query_context,
    std::shared_ptr<DAGGraph> dag_graph_ptr,
    const WorkerId & worker_id)
{
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(address_info);
    }

    LOG_TRACE(
        getLogger("SegmentScheduler::sendPlanSegments"),
        "Worker id {} -> [query resource size {}, plan_segment_buf_size {}]",
        worker_id.toString(),
        dag_graph_ptr->query_resource_map[worker_id].size(),
        plan_segment_headers[0].plan_segment_buf_size);

    executePlanSegmentsRemotely(
        address_info,
        plan_segment_headers,
        dag_graph_ptr->query_resource_map[worker_id],
        dag_graph_ptr->query_common_buf,
        dag_graph_ptr->query_settings_buf,
        dag_graph_ptr->async_context,
        *query_context.get(),
        worker_id);
}

} // namespace DB
