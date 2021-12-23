#include "BrpcRemoteBroadcastReceiver.h"
#include "BrpcExchangeRegistryCenter.h"
#include "StreamHandler.h"

#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTransStruct.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/DataTrans/RpcClientFactory.h>
#include <Protos/registry.pb.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_EXCEPTION;
}

BrpcRemoteBroadcastReceiver::BrpcRemoteBroadcastReceiver(DataTransKeyPtr trans_key_, String registry_address_, ContextPtr context_, Block header_)
    : trans_key(std::move(trans_key_)), registry_address(std::move(registry_address_)), context(std::move(context_)), header(std::move(header_))
{
    data_key = trans_key->getKey();
}

BrpcRemoteBroadcastReceiver::~BrpcRemoteBroadcastReceiver()
{
    if (stream_id != brpc::INVALID_STREAM_ID)
    {
        brpc::StreamClose(stream_id);
        LOG_TRACE(log, "Stream {} Close", stream_id);
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
            // FIXME: not register to coodinator adress
            std::shared_ptr<RpcClient> rpc_client = RpcClientFactory::getInstance().getClient(registry_address, false);
            Protos::RegistryService_Stub stub(Protos::RegistryService_Stub(&rpc_client->getChannel()));
            brpc::Controller cntl;
            brpc::StreamOptions stream_options;
            const auto stream_max_buf_size_bytes = -1;
            stream_options.max_buf_size = stream_max_buf_size_bytes;
            stream_options.handler = std::make_shared<StreamHandler>(context, weak_from_this());
            cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
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
                log,
                "Receiver register sender successfully, host-{} , data_key-{}, stream_id-{}",
                registry_address,
                data_key,
                stream_id);
            return;
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::BRPC_EXCEPTION)
            {
                retry_times++;
                LOG_WARNING(log, "Catch brpc exception when registering to sender:{} retrying:{}", e.message(), retry_times);
                if (s.elapsedMilliseconds() >= timeout_ms)
                    throw e;
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

RecvDataPacket BrpcRemoteBroadcastReceiver::recv(UInt32 timeout_ms)
{
    RecvDataPacket res_packet;
    DataTransPacket brpc_data_packet;
    if (!queue->receive_queue->tryPop(brpc_data_packet, timeout_ms))
        throw Exception(
            "Try pop receive queue for stream id-" + std::to_string(stream_id) + " timeout for " + std::to_string(timeout_ms) + " ms.",
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
    if(!brpc_data_packet.exception.empty()){
        return BroadcastStatus(BroadcastStatusCode::RECV_UNKNOWN_ERROR, true);
    }

    return RecvDataPacket(std::move(brpc_data_packet.chunk));
}

BroadcastStatus BrpcRemoteBroadcastReceiver::finish(BroadcastStatusCode status_code, String message){
    //TODO
    return BroadcastStatus(status_code, true, message);
}
}
