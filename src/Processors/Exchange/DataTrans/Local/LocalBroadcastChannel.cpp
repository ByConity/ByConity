#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <Processors/Exchange/ExchangeUtils.h>

namespace DB
{
LocalBroadcastChannel::LocalBroadcastChannel(DataTransKeyPtr data_key_, LocalChannelOptions options_)
    : data_key(std::move(data_key_))
    , options(std::move(options_))
    , receive_queue(options_.queue_size)
    , logger(&Poco::Logger::get("LocalBroadcastChannel"))
{
    broadcast_status.store(new BroadcastStatus{BroadcastStatusCode::RUNNING});
}

RecvDataPacket LocalBroadcastChannel::recv(UInt32 timeout_ms)
{
    Chunk recv_chunk;

    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_relaxed);
    /// Positive status code means that we should close immediately and negative code means we should conusme all in flight data before close
    if (current_status_ptr->code > 0)
        return *current_status_ptr;

    if (receive_queue.tryPop(recv_chunk, timeout_ms))
    {
        if (recv_chunk)
        {
            ExchangeUtils::transferGlobalMemoryToThread(recv_chunk.allocatedBytes());
            return RecvDataPacket(std::move(recv_chunk));
        }
        else
            return RecvDataPacket(*broadcast_status.load(std::memory_order_relaxed));
    }

    BroadcastStatus current_status = finish(
        BroadcastStatusCode::RECV_TIMEOUT,
        "Receive from channel " + data_key->getKey() + " timeout after ms: " + std::to_string(timeout_ms));
    return current_status;
}


BroadcastStatus LocalBroadcastChannel::send(Chunk chunk)
{
    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_relaxed);
    if (current_status_ptr->code != BroadcastStatusCode::RUNNING)
        return *broadcast_status.load(std::memory_order_relaxed);
    auto bytes = chunk.allocatedBytes();
    if (receive_queue.tryEmplace(options.max_timeout_ms / 2, std::move(chunk)))
    {       
        ExchangeUtils::transferThreadMemoryToGlobal(bytes);     
        return *broadcast_status.load(std::memory_order_relaxed);
    }

    BroadcastStatus current_status = finish(
        BroadcastStatusCode::SEND_TIMEOUT,
        "Send to channel " + data_key->getKey() + " timeout after ms: " + std::to_string(options.max_timeout_ms));
    return current_status;
}


BroadcastStatus LocalBroadcastChannel::finish(BroadcastStatusCode status_code, String message)
{
    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_relaxed);
    if (current_status_ptr->code != BroadcastStatusCode::RUNNING)
    {
        LOG_TRACE(
            logger,
            "Broadcast {} finished and status can't be changed to {} any more. Current status: {}",
            data_key->getKey(),
            status_code,
            current_status_ptr->code);
        return *current_status_ptr;
    }

    BroadcastStatus * new_status_ptr = new BroadcastStatus(status_code);
    new_status_ptr->message = message;

    if (broadcast_status.compare_exchange_strong(current_status_ptr, new_status_ptr, std::memory_order_relaxed, std::memory_order_relaxed))
    {
        LOG_INFO(
            logger,
            "{} BroadcastStatus from {} to {} with message: {}",
            data_key->getKey(),
            current_status_ptr->code,
            new_status_ptr->code,
            new_status_ptr->message);
        delete current_status_ptr;
        if (new_status_ptr->code > 0)
            // close queue immediately
            receive_queue.close();
        else
            receive_queue.tryEmplace(options.max_timeout_ms, Chunk());
        auto res = *new_status_ptr;
        res.is_modifer = true;
        return res;
    }
    else
    {
        LOG_TRACE(
            logger, "Fail to change broadcast status to {}, current status is: {} ", new_status_ptr->code, current_status_ptr->code);
        delete new_status_ptr;
        return *current_status_ptr;
    }
}

void LocalBroadcastChannel::registerToSenders(UInt32 timeout_ms)
{
    auto sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);

    sender_proxy->waitAccept(timeout_ms);
    sender_proxy->becomeRealSender(shared_from_this());
}

void LocalBroadcastChannel::merge(IBroadcastSender &&)
{
    throw Exception("merge is not implemented for LocalBroadcastChannel", ErrorCodes::NOT_IMPLEMENTED);
}

String LocalBroadcastChannel::getName() const
{
    return "Local: " + data_key->getKey();
};

LocalBroadcastChannel::~LocalBroadcastChannel()
{
    try
    {
        delete broadcast_status.load(std::memory_order_relaxed);
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}
}
