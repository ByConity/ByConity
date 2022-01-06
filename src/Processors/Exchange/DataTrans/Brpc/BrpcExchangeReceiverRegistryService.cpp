#include "BrpcExchangeReceiverRegistryService.h"

#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <common/scope_guard.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <brpc/stream.h>

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
    DataTransKeyPtr data_key;
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);
    /// SCOPE_EXIT wrap logic which run after done->Run(),
    /// since host socket of the accpeted stream is set in done->Run()
    SCOPE_EXIT({
        if (data_key && sender_stream_id != brpc::INVALID_STREAM_ID)
        {
            try
            {
                auto sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
                sender_proxy->waitAccept(cntl->timeout_ms());
                auto real_sender = std::dynamic_pointer_cast<IBroadcastSender>(std::make_shared<BrpcRemoteBroadcastSender>(
                    std::move(data_key), sender_stream_id, sender_proxy->getContext(), sender_proxy->getHeader()));
                sender_proxy->becomeRealSender(std::move(real_sender));
            }
            catch (const Exception & e)
            {
                brpc::StreamClose(sender_stream_id);
                LOG_ERROR(log, "Create stream failed for {} by exception: {}", data_key->getKey(), e.displayText());
            }
            catch (const std::exception & e)
            {
                brpc::StreamClose(sender_stream_id);
                LOG_ERROR(log, "Create stream failed for {} by exception: {}", data_key->getKey(), e.what());
            }
            catch (...)
            {
                brpc::StreamClose(sender_stream_id);
                LOG_ERROR(log, "Create stream failed for {} by unknown error", data_key->getKey());
            }
        }
    });

    /// this done_guard guarantee to call done->Run() in any situation
    brpc::ClosureGuard done_guard(done);
    brpc::StreamOptions stream_options;
    stream_options.max_buf_size = max_buf_size;
    data_key = ExchangeUtils::parseDataKey(request->data_key());
    if (!data_key)
    {
        LOG_ERROR(log, "Fail to parse data_key {}", request->data_key());
        cntl->SetFailed(EINVAL, "Fail to parse data_key");
        return;
    }
    if (brpc::StreamAccept(&sender_stream_id, *cntl, &stream_options) != 0)
    {
        sender_stream_id = brpc::INVALID_STREAM_ID;
        String error_msg = "Fail to accept stream for data_key-" + request->data_key() + " failed";
        LOG_ERROR(log, error_msg);
        cntl->SetFailed(error_msg);
        return;
    }
}

}
