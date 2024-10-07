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

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/Local/LocalChannelOptions.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Poco/Logger.h>
#include <Common/CurrentThread.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
LocalBroadcastChannel::LocalBroadcastChannel(
    ExchangeDataKeyPtr data_key_, LocalChannelOptions options_, const String & name_)
    : LocalBroadcastChannel(std::move(data_key_)
    , options_
    , name_
    , std::make_shared<MultiPathBoundedQueue>(options_.queue_size, nullptr))
{
}

LocalBroadcastChannel::LocalBroadcastChannel(
    ExchangeDataKeyPtr data_key_, LocalChannelOptions options_, const String & name_, MultiPathQueuePtr queue_, ContextPtr context_)
    : IBroadcastReceiver(options_.enable_metrics)
    , IBroadcastSender(options_.enable_metrics)
    , name(name_)
    , data_key(std::move(data_key_))
    , options(std::move(options_))
    , receive_queue(std::move(queue_))
    , context(std::move(context_))
    , logger(getLogger("LocalBroadcastChannel"))
{
}

RecvDataPacket LocalBroadcastChannel::recv(timespec timeout_ts)
{
    Stopwatch s;
    MultiPathDataPacket data_packet;

    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_acquire);
    /// Positive status code means that we should close immediately and negative code means we should conusme all in flight data before close
    if (current_status_ptr->code > 0)
        return *current_status_ptr;

    if (receive_queue->tryPopUntil(data_packet, timeout_ts))
    {
        if (std::holds_alternative<DataPacket>(data_packet))
        {
            Chunk & recv_chunk = std::get<DataPacket>(data_packet).chunk;
            addToMetricsMaybe(s.elapsedMilliseconds(), 0, 1, recv_chunk);
            return RecvDataPacket(std::move(recv_chunk));
        }
        else if (std::holds_alternative<SendDoneMark>(data_packet))
        {
            return RecvDataPacket(*broadcast_status.load(std::memory_order_acquire));
        }
        else
        {
            // 
        }
    }

    BroadcastStatus current_status = finish(
        BroadcastStatusCode::RECV_TIMEOUT,
        "Receive from channel " + name + " timeout after ms: " + DateLUT::serverTimezoneInstance().timeToString(timeout_ts.tv_sec));
    if (enable_receiver_metrics)
        receiver_metrics.recv_time_ms << s.elapsedMilliseconds();
    return current_status;
}

BroadcastStatus LocalBroadcastChannel::sendImpl(Chunk chunk)
{
    Stopwatch s;
    BroadcastStatus * current_status_ptr = broadcast_status.load(std::memory_order_acquire);
    if (current_status_ptr->code != BroadcastStatusCode::RUNNING)
        return *current_status_ptr;

    if (enable_receiver_metrics)
    {
        auto chunk_info = std::make_shared<DeserializeBufTransform::IOBufChunkInfoWithReceiver>();
        chunk_info->receiver = shared_from_this();
    }
    if (receive_queue->tryEmplaceUntil(options.max_timeout_ts, MultiPathDataPacket(DataPacket{std::move(chunk)})))
        return *broadcast_status.load(std::memory_order_acquire);

    // finished in other thread, receive_queue is closed.
    if(receive_queue->closed())
    {
        current_status_ptr = broadcast_status.load(std::memory_order_acquire);
        if(current_status_ptr->code != BroadcastStatusCode::RUNNING)
            return *current_status_ptr; 
        else
            /// queue is closed but status not set yet
            return BroadcastStatus(BroadcastStatusCode::SEND_UNKNOWN_ERROR, false, "Send operation was interrupted");
    }

    BroadcastStatus current_status = finish(
        BroadcastStatusCode::SEND_TIMEOUT,
        "Query send to local exchange channel " + name + " timeout after ms: " + std::to_string(options.max_timeout_ts.tv_sec));
    return current_status;
}

BroadcastStatus LocalBroadcastChannel::finish(BroadcastStatusCode status_code, String message)
{
    BroadcastStatus * current_status_ptr = &init_status;

    BroadcastStatus * new_status_ptr = new BroadcastStatus(status_code, false, message);

    if (broadcast_status.compare_exchange_strong(current_status_ptr, new_status_ptr, std::memory_order_release, std::memory_order_acquire))
    {
        LOG_DEBUG(
            logger,
            "{} BroadcastStatus from {} to {} with message: {}",
            name,
            current_status_ptr->code,
            new_status_ptr->code,
            new_status_ptr->message);
        if (new_status_ptr->code > 0)
            // close queue immediately
            receive_queue->close();
        else
            receive_queue->tryEmplaceUntil(options.max_timeout_ts, getName());
        auto res = *new_status_ptr;
        res.is_modified_by_operator = true;
        sender_metrics.finish_code = new_status_ptr->code;
        sender_metrics.is_modifier = 1;
        sender_metrics.message = new_status_ptr->message;
        // new_status_ptr will be deleted in the destructor as it is stored in broadcast_status
        // coverity[leaked_storage]
        return res;
    }
    else
    {
        LOG_TRACE(
            logger,
            "Fail to change broadcast(name:{}) status to {}, current status is:{} message:{}",
            name,
            new_status_ptr->code,
            current_status_ptr->code,
            message);
        sender_metrics.finish_code = current_status_ptr->code;
        sender_metrics.is_modifier = 0;
        delete new_status_ptr;
        return *current_status_ptr;
    }
}


void LocalBroadcastChannel::registerToSenders(UInt32 timeout_ms)
{
    Stopwatch s;
    auto sender_proxy = BroadcastSenderProxyRegistry::instance().getOrCreate(data_key);
    sender_proxy->waitAccept(timeout_ms);
    sender_proxy->becomeRealSender(shared_from_this());
    if (enable_receiver_metrics)
        receiver_metrics.register_time_ms << s.elapsedMilliseconds();
}

void LocalBroadcastChannel::merge(IBroadcastSender &&)
{
    throw Exception("merge is not implemented for LocalBroadcastChannel", ErrorCodes::NOT_IMPLEMENTED);
}

String LocalBroadcastChannel::getName() const
{
    return name;
};

LocalBroadcastChannel::~LocalBroadcastChannel()
{
    try
    {
        auto * status = broadcast_status.load(std::memory_order_acquire);
        if (status != &init_status)
            delete status;
        auto query_exchange_log = context ? context->getQueryExchangeLog() : nullptr;
        if ((enable_sender_metrics || enable_receiver_metrics) && query_exchange_log)
        {
            QueryExchangeLogElement element;
            element.initial_query_id = context->getInitialQueryId();
            element.exchange_id = data_key->exchange_id;
            element.partition_id = data_key->partition_id;
            element.type = "local";
            element.event_time =
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            // sender
            element.send_time_ms = sender_metrics.send_time_ms.get_value();
            element.send_rows = sender_metrics.send_rows.get_value();
            element.send_uncompressed_bytes = sender_metrics.send_uncompressed_bytes.get_value();
            element.num_send_times = sender_metrics.num_send_times.get_value();

            element.finish_code = sender_metrics.finish_code;
            element.is_modifier = sender_metrics.is_modifier;
            element.message = sender_metrics.message;

            // receiver
            element.recv_counts = receiver_metrics.recv_counts.get_value();
            element.recv_rows = receiver_metrics.recv_rows.get_value();
            element.recv_time_ms = receiver_metrics.recv_time_ms.get_value();
            element.register_time_ms = receiver_metrics.register_time_ms.get_value();
            element.recv_bytes = receiver_metrics.recv_bytes.get_value();
            element.recv_uncompressed_bytes = receiver_metrics.recv_uncompressed_bytes.get_value();

            query_exchange_log->add(element);
        }
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}
}
