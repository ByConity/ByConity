#pragma once

#include <Protos/runtime_filter.pb.h>
#include <brpc/stream.h>
#include <brpc/server.h>
#include <Interpreters/Context.h>

namespace DB
{
class RuntimeFilterService : public Protos::RuntimeFilterService
{
public:
    explicit RuntimeFilterService(ContextMutablePtr context_) : context(context_), log(&Poco::Logger::get("RuntimeFilterService")){}

    /// transfer dynamic filer (segment executor host --> coordinator host)
    void transferRuntimeFilter(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::TransferRuntimeFilterRequest * request,
        ::DB::Protos::TransferRuntimeFilterResponse * response,
        ::google::protobuf::Closure * done) override;

    /// dispatch dynamic filter (coordinator host --> segment executor host)
    void dispatchRuntimeFilter(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::DispatchRuntimeFilterRequest * request,
        ::DB::Protos::DispatchRuntimeFilterResponse * response,
        ::google::protobuf::Closure * done) override;

private:
    ContextMutablePtr context;
    Poco::Logger * log;
};
}
