/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "StreamHandler.h"
#include "Processors/Exchange/DataTrans/MultiPathBoundedQueue.h"
#include "ReadBufferFromBrpcBuf.h"

#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <brpc/stream.h>
#include <common/types.h>
#include <common/logger_useful.h>

#include <memory>

namespace DB
{

int StreamHandler::on_received_messages([[maybe_unused]] brpc::StreamId stream_id, butil::IOBuf * const messages[], size_t size) noexcept
{
    BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
    if (!receiver_ptr)
    {
        LOG_WARNING(log, "on_received_messages receiver is expired.");
        return 0;
    }
    try
    {
        if (!keep_order)
        {
            for (size_t index = 0; index < size; index++)
            {
                butil::IOBuf * msg = messages[index];
#ifndef NDEBUG
                LOG_TRACE(
                    log,
                    "on_received_messages: StreamId-{} received exchange data successfully, io-buffer size:{}",
                    stream_id,
                    msg->size());
#endif
                Chunk chunk;
                if (context->getSettingsRef().log_query_exchange)
                {
                    auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfoWithReceiver>();
                    chunk_info->io_buf.append(msg->movable());
                    chunk_info->receiver = receiver_ptr;
                    chunk.setChunkInfo(std::move(chunk_info));
                }
                else
                {
                    auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfo>();
                    chunk_info->io_buf.append(msg->movable());
                    chunk.setChunkInfo(std::move(chunk_info));
                }
                receiver_ptr->pushReceiveQueue(DataPacket{std::move(chunk)});
            }
            return 0;
        }
        for (size_t index = 0; index < size; index++)
        {
            Stopwatch s;
            butil::IOBuf & msg = *messages[index];
            auto read_buffer = std::make_unique<ReadBufferFromBrpcBuf>(msg);
            std::unique_ptr<ReadBuffer> buf;
            if (context->getSettingsRef().exchange_enable_block_compress)
                buf = std::make_unique<CompressedReadBuffer>(*read_buffer);
            else
                buf = std::move(read_buffer);
            NativeChunkInputStream chunk_in(*buf, header);
            Chunk chunk = chunk_in.readImpl();
            if (context->getSettingsRef().log_query_exchange)
            {
                auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfoWithReceiver>();
                chunk_info->receiver = receiver_ptr;
            }
            receiver_ptr->addToMetricsMaybe(0, s.elapsedMilliseconds(), 0, msg);
#ifndef NDEBUG
            LOG_TRACE(
                log,
                "on_received_messages: StreamId-{} received exchange data successfully, io-buffer size:{}, chunk rows:{}",
                stream_id,
                msg.size(),
                chunk.getNumRows());
#endif
            receiver_ptr->pushReceiveQueue(MultiPathDataPacket(DataPacket{std::move(chunk)}));
        }
    }
    catch (...)
    {
        try
        {
            String exception_str = getCurrentExceptionMessage(true);
            auto current_status = receiver_ptr->finish(BroadcastStatusCode::RECV_TIMEOUT, exception_str);
            if (current_status.is_modified_by_operator)
                LOG_ERROR(log, "on_received_messages:pushReceiveQueue exception happen-" + exception_str);
        }
        catch (...)
        {
            LOG_WARNING(log, "on_received_messages finish receiver exception happen-" + getCurrentExceptionMessage(true));
        }
    }
    return 0;
}

void StreamHandler::on_idle_timeout(brpc::StreamId id)
{
    try
    {
        LOG_WARNING(log, "StreamId-{} idle timeout.", id);
    }
    catch (...)
    {
        LOG_WARNING(log, "on_received_messages:on_idle_timeout exception happen-" + getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_closed(brpc::StreamId stream_id)
{
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_closed receiver is expired.");
        }
        else
        {
            LOG_DEBUG(log, "Close StreamId: {} , datakey: {} ", stream_id, receiver_ptr->getName());
            auto status = receiver_ptr->finish(BroadcastStatusCode::ALL_SENDERS_DONE, "Try close receiver grafully");
            if (status.is_modified_by_operator && status.code == BroadcastStatusCode::ALL_SENDERS_DONE)
            {
                LOG_DEBUG(log, "{} will close gracefully ", receiver_ptr->getName());
                // Push an empty as finish to close receiver gracefully
                receiver_ptr->pushReceiveQueue(MultiPathDataPacket(receiver_ptr->getName()));
            }
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "on_closed exception happen-" + getCurrentExceptionMessage(true));
    }
}

void StreamHandler::on_finished(brpc::StreamId id, int32_t finish_status_code)
{
    try
    {
        BrpcRemoteBroadcastReceiverShardPtr receiver_ptr = receiver.lock();
        if (!receiver_ptr)
        {
            LOG_WARNING(log, "on_finished receiver is expired.");
        }
        else
        {
            ///Only care about finish status which need close receiver immediately
            LOG_INFO(log, "on_finished: StreamId-{}, data-key {}, finish code:{}", id, receiver_ptr->getName(), finish_status_code);
            if (finish_status_code > 0)
                receiver_ptr->finish(static_cast<BroadcastStatusCode>(finish_status_code), "StreamHandler::on_finished called");
            else
                receiver_ptr->setSendDoneFlag();
        }
    }
    catch (...)
    {
        LOG_WARNING(log, "on_finished exception happen-" + getCurrentExceptionMessage(true));
    }
}

}
