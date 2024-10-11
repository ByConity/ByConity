#include "ResourceScheduler.h"

#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Protos/resource_manager_rpc.pb.h>
#include <ResourceManagement/ResourceManagerController.h>
#include "Interpreters/DistributedStages/AddressInfo.h"

namespace DB::ResourceManagement
{

ResourceScheduler::ResourceScheduler(ResourceManagerController & rm_controller_)
    : rm_controller(rm_controller_), log(getLogger("ResourceScheduler"))
{
    // todo (wangtao.vip): add config for thread number.
    schedule_pool = std::make_unique<ThreadPool>(16UL);
    if (schedule_pool->trySchedule([this]() { scheduleResource(); }))
        LOG_INFO(log, "Resource scheduler started.");
    else
        LOG_ERROR(log, "Start scheduling failed, please restart resource manager to fix it.");
}

ResourceScheduler::~ResourceScheduler()
{
}

void ResourceScheduler::scheduleResource()
{
    // todo (wangtao.vip): decouple scheduling and sending
    // todo (wangtao.vip): add timeout and exception handling
    // todo (wangtao.vip): fill true logic
    Protos::SendResourceRequestReq req;
    while (queue.pop(req))
    {
        AddressInfo server;
        server.fillFromProto(req.server_addr());
        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(extractExchangeHostPort(server), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        Protos::PlanSegmentManagerService_Stub manager(&rpc_client->getChannel());
        brpc::Controller * cntl = new brpc::Controller;
        Protos::GrantResourceRequestReq * request = new Protos::GrantResourceRequestReq;
        Protos::GrantResourceRequestResp * response = new Protos::GrantResourceRequestResp;
        request->set_req_type(req.req_type());
        request->set_query_id(req.query_id());
        request->set_query_start_ts(req.query_start_ts());
        request->set_segment_id(req.segment_id());
        request->set_parallel_index(req.parallel_index());
        request->set_epoch(req.epoch());
        request->set_ok(true);

        LOG_DEBUG(
            log,
            "Granted resource request({} {}_{}_{}), result: {}",
            request->req_type(),
            request->query_id(),
            request->segment_id(),
            request->parallel_index(),
            request->ok());


        // todo (wangtao.vip): add callback
        manager.grantResourceRequest(cntl, request, response, nullptr);
    }
}

ContextPtr ResourceScheduler::getContext() const
{
    return rm_controller.getContext();
}

} // namespace DB::ResourceManagement
