#pragma once

#include <Common/Logger.h>
#include <CloudServices/CnchServerServiceImpl.h>
#include <CloudServices/CnchWorkerServiceImpl.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegmentManagerRpcService.h>
#include <Storages/DistributedDataService.h>
#include <Storages/RemoteDiskCacheService.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterService.h>
#include <Statistics/OptimizerStatisticsService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

void addService(
    brpc::Server & rpc_server,
    std::vector<std::unique_ptr<::google::protobuf::Service>> & rpc_services,
    std::unique_ptr<google::protobuf::Service> service)
{
    rpc_server.AddService(service.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
    rpc_services.emplace_back(std::move(service));
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}
class BrpcServerHolder
{
public:
    BrpcServerHolder(String & host_port, ContextMutablePtr global_context, bool listen_try)
    {
        rpc_server = std::make_unique<brpc::Server>();
        brpc::ServerOptions options;

        if (global_context->getServerType() == ServerType::cnch_server)
        {
            addService(*rpc_server, rpc_services, CnchServerServiceImpl_RegisterService(global_context).service);
        }
        else if (global_context->getServerType() == ServerType::cnch_worker)
        {
            addService(*rpc_server, rpc_services, CnchWorkerServiceImpl_RegisterService(global_context).service);
            LOG_DEBUG(getLogger("BrpcServerHolder"), "Start register RemoteDiskCacheService: {}", host_port);
            addService(*rpc_server, rpc_services, RemoteDiskCacheService_RegisterService(global_context).service);
        }

        if (global_context->getComplexQueryActive())
        {
            addService(*rpc_server, rpc_services, BrpcExchangeReceiverRegistryService_RegisterService(global_context).service);
            addService(*rpc_server, rpc_services, RuntimeFilterService_RegisterService(global_context).service);
            addService(*rpc_server, rpc_services, PlanSegmentManagerRpcService_RegisterService(global_context).service);
            addService(*rpc_server, rpc_services, OptimizerStatisticsService_RegisterService(global_context).service);
        }

        if (rpc_server->Start(host_port.c_str(), &options) != 0)
        {
            start_success = false;
            if (listen_try)
                LOG_ERROR(getLogger("BrpcServerHolder"), "Failed tp start rpc server on {}", host_port);
            else
                throw Exception("Failed tp start rpc server on " + host_port, ErrorCodes::BRPC_EXCEPTION);
        }
    }

    void stop()
    {
        rpc_server->Stop(0);
    }

    void join()
    {
        rpc_server->Join();
    }

    bool available()
    {
        return start_success;
    }

private:
    bool start_success{true};
    std::vector<std::unique_ptr<::google::protobuf::Service>> rpc_services;
    std::unique_ptr<brpc::Server> rpc_server;
};

}
