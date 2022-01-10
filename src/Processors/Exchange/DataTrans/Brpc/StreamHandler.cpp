#include "StreamHandler.h"
#include "ReadBufferFromBrpcBuf.h"

#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <brpc/stream.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

int StreamHandler::on_received_messages(brpc::StreamId stream_id, butil::IOBuf * const messages[], size_t size) noexcept
{
    BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
    if (!receiver_ptr)
    {
        LOG_WARNING(log, "StreamHandler::on_received_messages receiver is expired.");
        return 0;
    }
    try
    {
        auto header = receiver_ptr->getHeader();
        for (size_t index = 0; index < size; index++)
        {
            butil::IOBuf & msg = *messages[index];
            auto read_buffer = std::make_unique<ReadBufferFromBrpcBuf>(msg);
            std::unique_ptr<ReadBuffer> buf;
            if (context->getSettingsRef().exchange_enable_block_compress)
                buf = std::make_unique<CompressedReadBuffer>(*read_buffer);
            else
                buf = std::move(read_buffer);
            NativeChunkInputStream chunk_in(*buf, header);
            Chunk chunk = chunk_in.readImpl();
            LOG_DEBUG(
                log,
                "StreamHandler::on_received_messages: StreamId-{} received exchange data successfully, io-buffer size:{}, chunk rows:{}",
                stream_id,
                msg.size(),
                chunk.getNumRows());
            receiver_ptr->pushReceiveQueue(chunk);
        }
    }
    catch (...)
    {
        try
        {
            String exception_str = getCurrentExceptionMessage(true);
            receiver_ptr->pushException(exception_str);
        }
        catch (...)
        {
            LOG_WARNING(log, "StreamHandler::on_received_messages:pushReceiveQueue exception happen-" + getCurrentExceptionMessage(true));
        }
    }
    return 0;
}

void StreamHandler::on_idle_timeout(brpc::StreamId id)
{
    LOG_WARNING(log, "StreamHandler::StreamId-{} idle timeout.", id);
}

void StreamHandler::on_closed(brpc::StreamId stream_id)
{
    BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
    if (!receiver_ptr)
    {
        LOG_WARNING(log, "StreamHandler::on_closed receiver is expired.");
    }
    else
    {
        /// Try close receiver gracefully
        auto res = receiver_ptr->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Finish in StreamHandler");
        if (res.is_modifer || res.code == BroadcastStatusCode::ALL_SENDERS_DONE)
        {
            Chunk empty = Chunk();
            // push an empty as finish
            receiver_ptr->pushReceiveQueue(empty);
        }
        LOG_DEBUG(log, "Close StreamId: {} , datakey: {} ", stream_id, receiver_ptr->getName());
    }
}

void StreamHandler::on_finished(brpc::StreamId id, int32_t finish_status_code)
{
    BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
    if (!receiver_ptr)
    {
        LOG_WARNING(log, "StreamHandler::on_finished receiver is expired.");
    }
    else
    {
        ///Only care about finish status which need close receiver immediately
        if (finish_status_code <= 0)
            return;

        receiver_ptr->clearQueue();
        receiver_ptr->finish(static_cast<BroadcastStatusCode>(finish_status_code), "StreamHandler::on_finished called");
        Chunk empty = Chunk();
        // push an empty as finish
        receiver_ptr->pushReceiveQueue(empty);
        LOG_DEBUG(log, "on_finished: StreamId-{}, data-key {}, finish code:{}", id, receiver_ptr->getName(), finish_status_code);
    }
}

}
