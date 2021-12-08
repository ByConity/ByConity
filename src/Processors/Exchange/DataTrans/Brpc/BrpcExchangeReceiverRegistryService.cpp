#include "BrpcExchangeReceiverRegistryService.h"
#include "BrpcExchangeRegistryCenter.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BRPC_EXCEPTION;
}

void BrpcExchangeReceiverRegistryService::registry(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::RegistryRequest * request,
    ::DB::Protos::RegistryResponse * /*response*/,
    ::google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    brpc::StreamOptions stream_options;
    brpc::StreamId sender_stream_id;
    stream_options.max_buf_size = max_buf_size;
    if (brpc::StreamAccept(&sender_stream_id, *cntl, &stream_options) != 0)
    {
        cntl->SetFailed("Fail to accept stream");
        throw Exception("Create stream for data_key-" + request->data_key() + " failed", ErrorCodes::BRPC_EXCEPTION);
    }
    BrpcExchangeRegistryCenter::getInstance().submit(request->data_key(), sender_stream_id);
    LOG_TRACE(log, "BrpcExchangeReceiverRegistryService create stream_id-{}, data_key-{}", sender_stream_id, request->data_key());
}
}
