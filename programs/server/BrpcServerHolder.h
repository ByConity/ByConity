#pragma once

#include <CloudServices/CnchServerServiceImpl.h>
#include <CloudServices/CnchWorkerServiceImpl.h>
#include <Interpreters/Context.h>
#include <Interpreters/DistributedStages/PlanSegmentManagerRpcService.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterService.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Statistics/OptimizerStatisticsService.h>
#include <Storages/DistributedDataService.h>
#include <Storages/RemoteDiskCacheService.h>
#include <brpc/server.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <common/logger_useful.h>

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
#define ADD_SERVICE(SERVICE) addService<SERVICE>(global_context);

        rpc_server = std::make_unique<brpc::Server>();
        if (global_context->getServerType() == ServerType::cnch_server)
        {
            ADD_SERVICE(CnchServerServiceImpl);
        }
        else if (global_context->getServerType() == ServerType::cnch_worker)
        {
            ADD_SERVICE(CnchWorkerServiceImpl);
            LOG_DEBUG(getLogger("BrpcServerHolder"), "Start register RemoteDiskCacheService: {}", host_port);
            ADD_SERVICE(RemoteDiskCacheService);
        }
        if (global_context->getComplexQueryActive())
        {
            ADD_SERVICE(BrpcExchangeReceiverRegistryService);
            ADD_SERVICE(RuntimeFilterService);
            ADD_SERVICE(PlanSegmentManagerRpcService);
            ADD_SERVICE(Statistics::OptimizerStatisticsService);
        }

        brpc::ServerOptions options;
        if (rpc_server->Start(host_port.c_str(), &options) != 0)
        {
            start_success = false;
            if (listen_try)
                LOG_ERROR(getLogger("BrpcServerHolder"), "Failed tp start rpc server on {}", host_port);
            else
                throw Exception("Failed tp start rpc server on " + host_port, ErrorCodes::BRPC_EXCEPTION);
        }
    }

    void stop() { rpc_server->Stop(0); }

    void join() { rpc_server->Join(); }

    bool available() { return start_success; }

private:
    bool start_success{true};
    std::vector<std::unique_ptr<::google::protobuf::Service>> rpc_services;
    std::unique_ptr<brpc::Server> rpc_server;

    template <typename T>
    void addService(ContextMutablePtr global_context)
    {
        //auto service = REGISTER_SERVICE(T, global_context);
        auto service = std::make_unique<T>(global_context);
        rpc_server->AddService(service.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
        rpc_services.emplace_back(std::move(service));
    }
};

}

