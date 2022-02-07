#include "BrpcRemoteBroadcastReceiver.h"
#include "StreamHandler.h"

#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTransStruct.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/DataTrans/RpcClientFactory.h>
#include <Protos/registry.pb.h>
#include <brpc/stream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_EXCEPTION;
    extern const int DISTRIBUTE_STAGE_QUERY_EXCEPTION;
}

BrpcRemoteBroadcastReceiver::BrpcRemoteBroadcastReceiver(
    DataTransKeyPtr trans_key_, String registry_address_, ContextPtr context_, Block header_)
    : trans_key(std::move(trans_key_))
    , registry_address(std::move(registry_address_))
    , context(std::move(context_))
    , header(std::move(header_))
{
    data_key = trans_key->getKey();
}

BrpcRemoteBroadcastReceiver::~BrpcRemoteBroadcastReceiver()
{
    try
    {
        if (stream_id != brpc::INVALID_STREAM_ID)
        {
            brpc::StreamClose(stream_id);
            LOG_TRACE(log, "Stream {} for {} @ {} Close", stream_id, data_key, registry_address);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void BrpcRemoteBroadcastReceiver::registerToSenders(UInt32 timeout_ms)
{
    std::shared_ptr<RpcClient> rpc_client = RpcClientFactory::getInstance().getClient(registry_address, false);
    Protos::RegistryService_Stub stub(Protos::RegistryService_Stub(&rpc_client->getChannel()));
    brpc::Controller cntl;
    brpc::StreamOptions stream_options;
    const auto stream_max_buf_size_bytes = -1;
    stream_options.max_buf_size = stream_max_buf_size_bytes;
    stream_options.handler = std::make_shared<StreamHandler>(context, weak_from_this());
    if (timeout_ms == 0)
        cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
    else
        cntl.set_timeout_ms(timeout_ms);
    cntl.set_max_retry(3);
    if (brpc::StreamCreate(&stream_id, cntl, &stream_options) != 0)
        throw Exception("Fail to create stream for " + getName(), ErrorCodes::BRPC_EXCEPTION);

    if (stream_id == brpc::INVALID_STREAM_ID)
        throw Exception("Stream id is invalid for " + getName(), ErrorCodes::BRPC_EXCEPTION);

    Protos::RegistryRequest request;
    Protos::RegistryResponse response;
    request.set_data_key(trans_key->getKey());
    stub.registry(&cntl, &request, &response, nullptr);
    // if exchange_enable_force_remote_mode = 1, sender and receiver in same process and sender stream may close before rpc end
    if (cntl.ErrorCode() == brpc::EREQUEST && cntl.ErrorText().ends_with("was closed before responded"))
    {
        LOG_INFO(
            log,
            "Receiver register sender successfully but sender already finished, host-{} , data_key-{}, stream_id-{}",
            registry_address,
            data_key,
            stream_id);
        return;
    }
    rpc_client->assertController(cntl);
    LOG_DEBUG(log, "Receiver register sender successfully, host-{} , data_key-{}, stream_id-{}", registry_address, data_key, stream_id);
}

void BrpcRemoteBroadcastReceiver::pushReceiveQueue(Chunk & chunk)
{
    if (brpc::StreamFinishedCode(stream_id) > 0)
        return;

    if (!queue->receive_queue->tryEmplace(context->getSettingsRef().exchange_timeout_ms, std::move(chunk)))
        throw Exception(
            "Push exchange data to receiver for " + getName() + " timeout for "
                + std::to_string(context->getSettingsRef().exchange_timeout_ms) + " ms.",
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
}

void BrpcRemoteBroadcastReceiver::pushException(const String & exception)
{
    if (brpc::StreamFinishedCode(stream_id) > 0)
        return;

    if (!queue->receive_queue->tryEmplace(context->getSettingsRef().exchange_timeout_ms, exception))
        throw Exception(
            "Push exchange exception to receiver for " + getName() + " timeout",
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
}

RecvDataPacket BrpcRemoteBroadcastReceiver::recv(UInt32 timeout_ms) noexcept
{
    DataTransPacket brpc_data_packet;
    int stream_finished_code = brpc::StreamFinishedCode(stream_id);
    /// Positive status code means that we should close immediately and negative code means we should conusme all in flight data before close
    if (stream_finished_code > 0)
        return BroadcastStatus(static_cast<BroadcastStatusCode>(stream_finished_code), false, "BrpcRemoteBroadcastReceiver::recv");

    if (!queue->receive_queue->tryPop(brpc_data_packet, timeout_ms))
    {
        const auto error_msg
            = "Try pop receive queue for " + getName() +  " timeout for " + std::to_string(timeout_ms) + " ms.";
        LOG_ERROR(log, error_msg);
        BroadcastStatus current_status = finish(BroadcastStatusCode::RECV_TIMEOUT, error_msg);
        return std::move(current_status);
    }
    if (!brpc_data_packet.exception.empty())
    {
        const auto & error_msg
            = "Try pop receive queue for " + getName() + " failed. Exception:" + brpc_data_packet.exception;
        LOG_ERROR(log, error_msg);
        BroadcastStatus current_status = finish(BroadcastStatusCode::RECV_UNKNOWN_ERROR, error_msg);
        return std::move(current_status);
    }
    // receive a empty chunk means StreamHandler::on_finished is called and the receive is done.
    if (brpc_data_packet.chunk.empty())
    {
        auto code = brpc::StreamFinishedCode(stream_id);
        LOG_DEBUG(log, "Receive for stream id: {} finished, name: {}, status_code: {}.", stream_id, getName(), code);
        return BroadcastStatus(static_cast<BroadcastStatusCode>(code), false, "BrpcRemoteBroadcastReceiver::recv done");
    }

    return RecvDataPacket(std::move(brpc_data_packet.chunk));
}

BroadcastStatus BrpcRemoteBroadcastReceiver::finish(BroadcastStatusCode status_code_, String message)
{
    int actual_status_code = status_code_;
    int ret_code = brpc::StreamFinish(stream_id, actual_status_code, status_code_);
    if (ret_code != 0)
        return BroadcastStatus(static_cast<BroadcastStatusCode>(actual_status_code), false, "BrpcRemoteBroadcastReceiver::finish, already has been finished");
    else
        return BroadcastStatus(status_code_, true, message);
}

String BrpcRemoteBroadcastReceiver::getName() const
{
    return "BrpcReciver[" +  trans_key->getKey() +"]@" + registry_address;
}
}
