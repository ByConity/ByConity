#pragma once

#include <Protos/registry.pb.h>
#include <brpc/stream.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>

#include <memory>


namespace DB
{
struct AsyncRegisterResult
{
    std::shared_ptr<RpcClient> channel;
    std::unique_ptr<brpc::Controller> cntl;
    std::unique_ptr<Protos::RegistryRequest> request;
    std::unique_ptr<Protos::RegistryResponse> response;
};

}
