#pragma once

#include <brpc/server.h>
#include <bthread/bthread.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <CloudServices/CnchServerServiceImpl.h>
#include <CloudServices/CnchWorkerServiceImpl.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterService.h>
#include <Interpreters/DistributedStages/PlanSegmentManagerRpcService.h>

#define SERVER_REGISTER_SERVICE(service_name) \
{ \
    service_name##_RegisterService service_register(global_context); \
    rpc_server->AddService(service_register.service.get(), brpc::SERVER_DOESNT_OWN_SERVICE); \
    rpc_services.emplace_back(std::move(service_register.service)); \
}

#define REGISTER_SERVER_SERVICES() \
{ \
    SERVER_REGISTER_SERVICE(CnchServerServiceImpl); \
}

#define REGISTER_WORKER_SERVICES() \
{ \
    SERVER_REGISTER_SERVICE(CnchWorkerServiceImpl); \
}

#define REGISTER_COMPLEX_QUERY_SERVICES() \
{ \
    SERVER_REGISTER_SERVICE(BrpcExchangeReceiverRegistryService); \
    SERVER_REGISTER_SERVICE(RuntimeFilterService); \
    SERVER_REGISTER_SERVICE(PlanSegmentManagerRpcService); \
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
            REGISTER_SERVER_SERVICES()
        }
        else if (global_context->getServerType() == ServerType::cnch_worker)
        {
            REGISTER_WORKER_SERVICES()
        }

        if (global_context->getComplexQueryActive())
        {
            REGISTER_COMPLEX_QUERY_SERVICES()
        }

        if (rpc_server->Start(host_port.c_str(), &options) != 0)
        {
            start_success = false;
            if (listen_try)
                LOG_ERROR(&Poco::Logger::get("BrpcServerHolder"), "Failed tp start rpc server on {}", host_port);
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
