#include "BrpcExchangeReceiverRegistryService.h"

#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <brpc/stream.h>
#include <Common/Exception.h>
#include <common/scope_guard.h>

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
    brpc::StreamId sender_stream_id = brpc::INVALID_STREAM_ID;
    BroadcastSenderProxyPtr sender_proxy;
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    /// SCOPE_EXIT wrap logic which run after done->Run(),
    /// since host socket of the accpeted stream is set in done->Run()
    SCOPE_EXIT({
        if (sender_proxy && sender_stream_id != brpc::INVALID_STREAM_ID)
        {
            try
            {
                auto real_sender = std::dynamic_pointer_cast<IBroadcastSender>(std::make_shared<BrpcRemoteBroadcastSender>(
                    sender_proxy->getDataKey(), sender_stream_id, sender_proxy->getContext(), sender_proxy->getHeader()));
                sender_proxy->becomeRealSender(std::move(real_sender));
            }
            catch (...)
            {
                brpc::StreamClose(sender_stream_id);
                LOG_ERROR(log, "Create stream failed for {} by exception: {}", sender_proxy->getDataKey()->getKey(), getCurrentExceptionMessage(false));
            }
        }
    });

    /// this done_guard guarantee to call done->Run() in any situation
    brpc::ClosureGuard done_guard(done);
    brpc::StreamOptions stream_options;
    stream_options.max_buf_size = max_buf_size;
    auto data_key = ExchangeUtils::parseDataKey(request->data_key());
    if (!data_key)
    {
        LOG_ERROR(log, "Fail to parse data_key {}", request->data_key());
        cntl->SetFailed(EINVAL, "Fail to parse data_key");
        return;
    }
    try
    {
        sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
        sender_proxy->waitAccept(std::max(cntl->timeout_ms() - 500, 1000l));
    }
    catch (...)
    {
        String error_msg = "Create stream for " + request->data_key() + " failed by exception: " + getCurrentExceptionMessage(false);
        LOG_ERROR(log, error_msg);
        cntl->SetFailed(error_msg);
        return;
    }
    
    if (brpc::StreamAccept(&sender_stream_id, *cntl, &stream_options) != 0)
    {
        sender_stream_id = brpc::INVALID_STREAM_ID;
        String error_msg = "Fail to accept stream for data_key-" + request->data_key();
        LOG_ERROR(log, error_msg);
        cntl->SetFailed(error_msg);
        return;
    }

}

}
