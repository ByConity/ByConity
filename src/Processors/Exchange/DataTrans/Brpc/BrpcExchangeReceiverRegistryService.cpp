#include "BrpcExchangeReceiverRegistryService.h"

#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include "Processors/Exchange/ExchangeUtils.h"

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
    auto data_key = ExchangeUtils::parseDataKey(request->data_key());
    auto sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    sender_proxy->waitAccept(5000);
    auto real_sender = std::dynamic_pointer_cast<IBroadcastSender>(
        std::make_shared<BrpcRemoteBroadcastSender>(std::move(data_key), sender_stream_id, sender_proxy->getContext(), sender_proxy->getHeader()));
    sender_proxy->becomeRealSender(std::move(real_sender));
    LOG_TRACE(log, "BrpcExchangeReceiverRegistryService create sender stream_id-{}, data_key-{}", sender_stream_id, request->data_key());
}

}
