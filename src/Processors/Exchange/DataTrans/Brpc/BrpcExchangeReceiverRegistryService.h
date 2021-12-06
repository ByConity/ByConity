#pragma once

#include <Interpreters/Context.h>
#include <Protos/registry.pb.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <common/logger_useful.h>

namespace DB
{
class BrpcExchangeReceiverRegistryService : public Protos::RegistryService
{
public:
    explicit BrpcExchangeReceiverRegistryService(int max_buf_size_) : max_buf_size(max_buf_size_) { }

    void registry(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::RegistryRequest * request,
        ::DB::Protos::RegistryResponse * response,
        ::google::protobuf::Closure * done) override;

private:
    int max_buf_size;
    Poco::Logger * log = &Poco::Logger::get("BrpcExchangeReceiverRegistryService");
};
}
