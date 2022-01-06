#include "BrpcRemoteBroadcastReceiver.h"
#include "StreamHandler.h"

#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTransStruct.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/DataTrans/RpcClientFactory.h>
#include <Protos/registry.pb.h>

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
    broadcast_status.store(new BroadcastStatus{BroadcastStatusCode::RUNNING});
    data_key = trans_key->getKey();
}

BrpcRemoteBroadcastReceiver::~BrpcRemoteBroadcastReceiver()
{
    try
    {
        delete broadcast_status.load(std::memory_order_relaxed);
        if (stream_id != brpc::INVALID_STREAM_ID)
        {
            brpc::StreamClose(stream_id);
            LOG_TRACE(log, "Stream {} Close", stream_id);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void BrpcRemoteBroadcastReceiver::registerToSenders(UInt32 timeout_ms)
{
    Stopwatch s;
    size_t retry_times = 0;
    while (s.elapsedMilliseconds() < timeout_ms)
    {
        try
        {
            std::shared_ptr<RpcClient> rpc_client = RpcClientFactory::getInstance().getClient(registry_address, false);
            Protos::RegistryService_Stub stub(Protos::RegistryService_Stub(&rpc_client->getChannel()));
            brpc::Controller cntl;
            brpc::StreamOptions stream_options;
            const auto stream_max_buf_size_bytes = -1;
            stream_options.max_buf_size = stream_max_buf_size_bytes;
            stream_options.handler = std::make_shared<StreamHandler>(context, weak_from_this());
            cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
            // FIXME we should close stream at any situation
            if (brpc::StreamCreate(&stream_id, cntl, &stream_options) != 0)
                throw Exception("Fail to create stream for data_key-" + data_key, ErrorCodes::BRPC_EXCEPTION);

            if (stream_id == brpc::INVALID_STREAM_ID)
                throw Exception("Stream id is invalid for data_key-" + data_key, ErrorCodes::BRPC_EXCEPTION);

            Protos::RegistryRequest request;
            Protos::RegistryResponse response;
            request.set_data_key(trans_key->getKey());
            stub.registry(&cntl, &request, &response, nullptr);
            rpc_client->assertController(cntl);
            LOG_DEBUG(
                log, "Receiver register sender successfully, host-{} , data_key-{}, stream_id-{}", registry_address, data_key, stream_id);
            return;
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::BRPC_EXCEPTION)
            {
                retry_times++;
                LOG_WARNING(log, "Catch brpc exception when registering to sender:{} retrying:{}", e.message(), retry_times);
                if (s.elapsedMilliseconds() >= timeout_ms || retry_times > 3)
                    throw e;
                else
                    bthread_usleep(10000L);
            }
            else
            {
                LOG_WARNING(log, "Catch other exception when registering to sender:{}", e.message());
                throw e;
            }
        }
    }
}

void BrpcRemoteBroadcastReceiver::pushReceiveQueue(Chunk & chunk)
{
    if (!queue->receive_queue->tryEmplace(context->getSettingsRef().exchange_timeout_ms, std::move(chunk)))
        throw Exception(
            "Push exchange data to receiver for stream id-" + std::to_string(stream_id) + " timeout for "
                + std::to_string(context->getSettingsRef().exchange_timeout_ms) + " ms.",
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
}

void BrpcRemoteBroadcastReceiver::pushException(const String & exception)
{
    if (!queue->receive_queue->tryEmplace(context->getSettingsRef().exchange_timeout_ms, exception))
        throw Exception(
            "Push exchange exception to receiver for stream id-" + std::to_string(stream_id) + " timeout",
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
}

RecvDataPacket BrpcRemoteBroadcastReceiver::recv(UInt32 timeout_ms) noexcept
{
    DataTransPacket brpc_data_packet;
    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_relaxed);
    /// Positive status code means that we should close immediately and negative code means we should conusme all in flight data before close
    if (current_status_ptr->code > 0 || (current_status_ptr->code < 0 && queue->receive_queue->size() == 0))
        return *current_status_ptr;

    if (!queue->receive_queue->tryPop(brpc_data_packet, timeout_ms))
    {
        const auto error_msg
            = "Try pop receive queue for stream id-" + std::to_string(stream_id) + " timeout for " + std::to_string(timeout_ms) + " ms.";
        LOG_ERROR(log, error_msg);
        BroadcastStatus current_status = finish(BroadcastStatusCode::RECV_TIMEOUT, error_msg);
        return std::move(current_status);
    }
    if (!brpc_data_packet.exception.empty())
    {
        const auto & error_msg
            = "Try pop receive queue for stream id-" + std::to_string(stream_id) + " failed. Exception:" + brpc_data_packet.exception;
        LOG_ERROR(log, error_msg);
        BroadcastStatus current_status = finish(BroadcastStatusCode::RECV_UNKNOWN_ERROR, error_msg);
        return std::move(current_status);
    }
    // receive a empty chunk means StreamHandler::on_finished is called and the receive is done.
    if (brpc_data_packet.chunk.empty())
    {
        auto * status = broadcast_status.load(std::memory_order_relaxed);
        LOG_DEBUG(log, "Receive for stream id-{} finished, data_key-{}, status_code:{}.", stream_id, data_key, status->code);
        return *status;
    }

    return RecvDataPacket(std::move(brpc_data_packet.chunk));
}

BroadcastStatus BrpcRemoteBroadcastReceiver::finish(BroadcastStatusCode status_code_, String message)
{
    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_relaxed);
    if (current_status_ptr->code != BroadcastStatusCode::RUNNING)
    {
        LOG_WARNING(
            log,
            "Broadcast receiver {} finished and status can't be changed to {} any more. Current status: {}",
            data_key,
            status_code_,
            current_status_ptr->code);
        return *current_status_ptr;
    }
    else
    {
        BroadcastStatus * new_status_ptr = new BroadcastStatus(status_code_, false, message);
        if (broadcast_status.compare_exchange_strong(
                current_status_ptr, new_status_ptr, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            LOG_INFO(
                log,
                "{} BroadcastStatus from {} to {} with message: {}",
                data_key,
                current_status_ptr->code,
                new_status_ptr->code,
                new_status_ptr->message);
            delete current_status_ptr;
            auto res = *new_status_ptr;
            res.is_modifer = true;
            //FIXME: we should check return code, since receiver finish status may be changed by sender.
            brpc::StreamFinish(stream_id, status_code_);
            return res;
        }
        else
        {
            LOG_WARNING(
                log, "Fail to change broadcast status to {}, current status is: {} ", new_status_ptr->code, current_status_ptr->code);
            delete new_status_ptr;
            return *current_status_ptr;
        }
    }
}

String BrpcRemoteBroadcastReceiver::getName() const
{
    return "BrpcReciver[" +  trans_key->getKey() +"]@" + registry_address;
}
}
