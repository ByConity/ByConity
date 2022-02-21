#include "BrpcRemoteBroadcastReceiver.h"
#include "StreamHandler.h"

#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/DataTrans/RpcClientFactory.h>
#include <Processors/Exchange/ExchangeUtils.h>
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
    DataTransKeyPtr trans_key_, String registry_address_, ContextPtr context_, Block header_, bool keep_order_)
    : trans_key(std::move(trans_key_))
    , registry_address(std::move(registry_address_))
    , context(std::move(context_))
    , header(std::move(header_))
    , queue(context->getSettingsRef().exchange_remote_receiver_queue_size)
    , data_key(trans_key->getKey())
    , keep_order(keep_order_)
{
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
    stream_options.handler = std::make_shared<StreamHandler>(context, shared_from_this(), header, keep_order);
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

void BrpcRemoteBroadcastReceiver::pushReceiveQueue(Chunk chunk)
{
    if (queue.closed())
        return;
    if (!queue.tryEmplace(context->getSettingsRef().exchange_timeout_ms, std::move(chunk)))
        throw Exception(
            "Push exchange data to receiver for " + getName() + " timeout for "
            + std::to_string(context->getSettingsRef().exchange_timeout_ms) + " ms.",
            ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
}

RecvDataPacket BrpcRemoteBroadcastReceiver::recv(UInt32 timeout_ms) noexcept
{
    Chunk received_chunk;
    if (!queue.tryPop(received_chunk, timeout_ms))
    {
        const auto error_msg = "Try pop receive queue for " + getName() + " timeout for " + std::to_string(timeout_ms) + " ms.";
        BroadcastStatus current_status = finish(BroadcastStatusCode::RECV_TIMEOUT, error_msg);
        return std::move(current_status);
    }

    // receive a empty chunk without any chunk info means the receive is done.
    if (!received_chunk && !received_chunk.getChunkInfo())
    {
        LOG_DEBUG(log, "{} finished ", getName());
        return RecvDataPacket(finish(BroadcastStatusCode::ALL_SENDERS_DONE, "receiver done"));
    }

    if(keep_order)
        // Chunk in queue is created in StreamHanlder's on_received_messages callback, which is run in bthread.
        // Allocator (ref srcs/Common/Allocator.cpp) will add the momory of chunk to global memory tacker. 
        // When this chunk is poped, we should add this memory to current query momory tacker, and subtract from global memory tacker.
        ExchangeUtils::transferGlobalMemoryToThread(received_chunk.allocatedBytes());
    return RecvDataPacket(std::move(received_chunk));
}

BroadcastStatus BrpcRemoteBroadcastReceiver::finish(BroadcastStatusCode status_code_, String message)
{
    int actual_status_code = BroadcastStatusCode::RUNNING;
    if (status_code_ < 0)
    {
        int rc = brpc::StreamFinish(stream_id, actual_status_code, status_code_, false);
        if (rc == EINVAL)
        {
            if (queue.closed())
                return BroadcastStatus(
                    BroadcastStatusCode::SEND_UNKNOWN_ERROR, false, "BrpcRemoteBroadcastReceiver: stream closed before finish");
            else
                return BroadcastStatus(status_code_, false, "BrpcRemoteBroadcastReceiver: stream closed gracefully before finish");
        }
        if (rc != 0)
            return BroadcastStatus(
                static_cast<BroadcastStatusCode>(actual_status_code),
                false,
                "BrpcRemoteBroadcastReceiver::finish, already has been finished");

        return BroadcastStatus(static_cast<BroadcastStatusCode>(status_code_), false, message);
    }

    int rc = brpc::StreamFinish(stream_id, actual_status_code, status_code_, true);
    if (rc == EINVAL)
    {
        if (queue.close())
            return BroadcastStatus(static_cast<BroadcastStatusCode>(status_code_), true, message);
        else
            return BroadcastStatus(BroadcastStatusCode::SEND_UNKNOWN_ERROR, false, "BrpcRemoteBroadcastReceiver::finish: stream closed");
    }

    if (actual_status_code > 0)
        queue.close();

    if (rc != 0)
        return BroadcastStatus(static_cast<BroadcastStatusCode>(actual_status_code), false, "BrpcRemoteBroadcastReceiver::finish, already has been finished");

    LOG_INFO(log, "{} finished with code: {} and message: {}", getName(), status_code_, message);
    return BroadcastStatus(status_code_, true, message);
}

String BrpcRemoteBroadcastReceiver::getName() const
{
    return "BrpcReciver[" +  trans_key->getKey() +"]@" + registry_address;
}
}
