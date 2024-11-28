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

#include "BrpcRemoteBroadcastSender.h"
#include "WriteBufferFromBrpcBuf.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastSender.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <brpc/protocol.h>
#include <brpc/stream.h>
#include <bthread/bthread.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/time.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_EXCEPTION;
}

/**
 * 1-1 sender, to make 1-n sener, merge n 1-1 sender
 */
BrpcRemoteBroadcastSender::BrpcRemoteBroadcastSender(
    ExchangeDataKeyPtr trans_key_, brpc::StreamId stream_id, ContextPtr context_, Block header_)
    : IBroadcastSender(context_->getSettingsRef().log_query_exchange), context(std::move(context_)), header(std::move(header_))
{
    trans_keys.emplace_back(std::move(trans_key_));
    sender_stream_ids.push_back(stream_id);
}

BrpcRemoteBroadcastSender::~BrpcRemoteBroadcastSender()
{
    try
    {
        for (brpc::StreamId sender_stream_id : sender_stream_ids)
        {
            if(sender_stream_id != brpc::INVALID_STREAM_ID)
                brpc::StreamClose(sender_stream_id);
        }
        if (trans_keys.empty())
            return;
        if (enable_sender_metrics)
        {
            QueryExchangeLogElement element;
            const auto & key = trans_keys.front();
            element.initial_query_id = context->getInitialQueryId();
            element.exchange_id = key->exchange_id;
            element.partition_id = key->partition_id;
            element.event_time =
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            element.send_time_ms = sender_metrics.send_time_ms.get_value();
            element.send_rows = sender_metrics.send_rows.get_value();
            element.send_bytes = sender_metrics.send_bytes.get_value();
            element.send_uncompressed_bytes = sender_metrics.send_uncompressed_bytes.get_value();
            element.num_send_times = sender_metrics.num_send_times.get_value();
            element.ser_time_ms = sender_metrics.ser_time_ms.get_value();
            element.send_retry = sender_metrics.send_retry.get_value();
            element.send_retry_ms = sender_metrics.send_retry_ms.get_value();
            element.overcrowded_retry = sender_metrics.overcrowded_retry.get_value();
            element.finish_code = sender_metrics.finish_code;
            element.is_modifier = sender_metrics.is_modifier;
            element.message = sender_metrics.message;
            element.type = "brpc_sender";
            if (auto query_exchange_log = context->getQueryExchangeLog())
                query_exchange_log->add(element);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

BroadcastStatus BrpcRemoteBroadcastSender::sendImpl(Chunk chunk)
{
    Stopwatch s;
    WriteBufferFromBrpcBuf out;
    serializeChunkToIoBuffer(std::move(chunk), out);
    const auto & buf = out.getFinishedBuf();
    if (enable_sender_metrics)
    {
        sender_metrics.send_bytes << buf.length();
        sender_metrics.ser_time_ms << s.elapsedMilliseconds();
    }
    auto res = BroadcastStatus(BroadcastStatusCode::RUNNING);
    for (size_t i = 0; i < sender_stream_ids.size(); ++i)
    {
        BroadcastStatus ret_status = sendIOBuffer(buf, sender_stream_ids[i], *trans_keys[i]);
        if (ret_status.is_modified_by_operator && ret_status.code != BroadcastStatusCode::RUNNING)
        {
            finish(
                BroadcastStatusCode::SEND_CANCELLED,
                "Cancelled by other, code: " + std::to_string(ret_status.code) + " msg: " + ret_status.message);
            return ret_status;
        }

        if (ret_status.code != BroadcastStatusCode::RUNNING)
            res = ret_status;
    }
    return res;
}

void BrpcRemoteBroadcastSender::serializeChunkToIoBuffer(Chunk chunk, WriteBufferFromBrpcBuf & out) const
{
    const auto & settings = context->getSettingsRef();
    if (settings.exchange_enable_block_compress)
    {
        std::string method = Poco::toUpper(settings.network_compression_method.toString());
        std::optional<int> level;
        if (method == "ZSTD")
            level = settings.network_zstd_compression_level;
        const CompressionCodecPtr & codec = CompressionCodecFactory::instance().get(method, level);
        CompressedWriteBuffer compressed_out(out, codec, DBMS_DEFAULT_BUFFER_SIZE * 2);
        NativeChunkOutputStream chunk_out(compressed_out, header);
        chunk_out.write(chunk);
        compressed_out.next();
    }
    else
    {
        NativeChunkOutputStream chunk_out(out, header);
        chunk_out.write(chunk);
    }
}

BroadcastStatus BrpcRemoteBroadcastSender::sendIOBuffer(const butil::IOBuf & io_buffer, brpc::StreamId stream_id, const ExchangeDataKey & data_key)
{
    if (io_buffer.size() > brpc::FLAGS_max_body_size)
        throw Exception(
            CurrentThread::getQueryId().toString() +
            " write stream-" + std::to_string(stream_id) + " buffer fail, io buffer size is bigger than "
                + std::to_string(brpc::FLAGS_max_body_size) + " current is " + std::to_string(io_buffer.size()),
            ErrorCodes::BRPC_EXCEPTION);

    size_t retry_count = 0;
    size_t overcrowded_retry = 0;
    Stopwatch s;
    bool success = false;
    size_t max_wait_ms = context->getSettingsRef().exchange_stream_back_pressure_max_wait_ms;
    timespec query_expiration_ts = context->getQueryExpirationTimeStamp();
    size_t query_expiration_ms_ts = query_expiration_ts.tv_sec * 1000 + query_expiration_ts.tv_nsec / 1000000;
    while (auto now = time_in_milliseconds(std::chrono::system_clock::now()) < query_expiration_ms_ts)
    {
        int rect_code = brpc::StreamWrite(stream_id, io_buffer);
        if (rect_code == 0)
        {
            success = true;
            break;
        }
        else if (rect_code == EAGAIN)
        {
            timespec wait_ts = query_expiration_ts;
            if (max_wait_ms)
            {
                size_t wait_ms_ts = std::min(now + max_wait_ms, query_expiration_ms_ts);
                wait_ts = {.tv_sec = time_t(wait_ms_ts / 1000), .tv_nsec = long((wait_ms_ts % 1000) * 1000000)};
            }
            int wait_res_code = brpc::StreamWait(stream_id, &wait_ts);
            if (wait_res_code == EINVAL)
            {
                // TODO: retain stream object before finish code is read.
                // Ingore error when writing to the closed stream, because this stream is closed by remote peer before read any finish code.
                LOG_INFO(log, "Stream-{} with key {} is closed", stream_id, data_key);
                return BroadcastStatus(BroadcastStatusCode::RECV_UNKNOWN_ERROR, false, "Stream is closed by peer");
            }

            LOG_TRACE(
                log,
                "Stream write buffer full wait, retry count-{}, stream_id-{} ,with data_key-{} wait res code:{} size:{} ",
                retry_count,
                stream_id,
                data_key,
                wait_res_code,
                io_buffer.size());
        }
        else if (rect_code == EINVAL)
        {
            // Ingore error when writing to the closed stream, because this stream is closed by remote peer before read any finish code.
            LOG_INFO(log, "Stream-{} with key {} is closed", stream_id, data_key);
            return BroadcastStatus(BroadcastStatusCode::RECV_UNKNOWN_ERROR, false, "Stream is closed by peer");
        }
        else if (rect_code == 1011) //EOVERCROWDED   | 1011 | The server is overcrowded
        {
            if (now < query_expiration_ms_ts)
            {
                if (enable_sender_metrics)
                    sender_metrics.overcrowded_retry << 1;
                overcrowded_retry += 1;
                size_t sleep_time = std::min(1000 * overcrowded_retry, query_expiration_ms_ts - now);
                if (max_wait_ms)
                    sleep_time = std::min(sleep_time, max_wait_ms);

                bthread_usleep(1000 * sleep_time);
            }
            LOG_WARNING(
                log,
                "Stream-{} write buffer error rect_code:{}, server is overcrowded, data_key:{}, retry_count:{}, overcrowded_retry:{}",
                stream_id,
                rect_code,
                data_key,
                retry_count,
                overcrowded_retry);
        }
        // stream finished
        else if (rect_code == -1)
        {
            int stream_finished_code = 0;
            auto rc = brpc::StreamFinishedCode(stream_id, stream_finished_code);
            // Stream is closed by remote peer and we can get finish code now
            if (rc == EINVAL)
                return BroadcastStatus(BroadcastStatusCode::RECV_UNKNOWN_ERROR, false, "Stream is closed by peer");
            LOG_INFO(log, "Stream-{} write receive finish request, finish code:{}, data_key-{}", stream_id, rect_code, data_key);
            return BroadcastStatus(static_cast<BroadcastStatusCode>(rect_code), false, "Stream Write receive finish request");
        }
        else
        {
            throw Exception(
                "Stream-" + std::to_string(stream_id) + " write buffer occurred error, the rect_code that we can not handle:"
                    + std::to_string(rect_code) + ", data_key-" + data_key.toString(),
                ErrorCodes::BRPC_EXCEPTION);
        }
        retry_count++;
    }
    if (enable_sender_metrics)
    {
        sender_metrics.send_retry_ms << s.elapsedMilliseconds();
        sender_metrics.send_retry << retry_count;
    }
    if (!success)
    {
        const auto msg = fmt::format("Write stream-{} timeout, with data_key-{}, size:{}, retry_count:{}, overcrowded_retry:{}, query_expiration_ms_ts:{}, maximum:{}", 
            stream_id, data_key, io_buffer.size(), retry_count, overcrowded_retry, query_expiration_ms_ts, context->getQueryMaxExecutionTime() / 1000);
        LOG_ERROR(log, msg);
        auto current_status = BroadcastStatus(BroadcastStatusCode::SEND_TIMEOUT, true, msg);
        int actual_status_code = BroadcastStatusCode::RUNNING;
        int ret_code = brpc::StreamFinish(stream_id, actual_status_code, BroadcastStatusCode::SEND_TIMEOUT, true);
        if (ret_code != 0)
            return BroadcastStatus(static_cast<BroadcastStatusCode>(actual_status_code), false, "Stream Write receive finish request");
         // coverity[uninit_use_in_call]
        return current_status;
    }
#ifndef NDEBUG
    LOG_TRACE(
        log,
        "Send exchange data size-{} KB with data_key-{}, stream-{} retry times:{} cost:{} ms",
        io_buffer.size() / 1024.0,
        data_key,
        stream_id,
        retry_count,
        s.elapsedMilliseconds());
#endif
    return BroadcastStatus(RUNNING);
}

BroadcastStatus BrpcRemoteBroadcastSender::finish(BroadcastStatusCode status_code, String message)
{
    int code = 0;
    bool is_modifer = false;
    for (auto stream_id : sender_stream_ids)
    {
        int actual_status_code = status_code;
        int ret_code = brpc::StreamFinish(stream_id, actual_status_code, status_code, true);
        if (ret_code == 0)
        {
            is_modifer = true;
            // Close stream if all data are sent to make peer stream finished faster
            if (actual_status_code == BroadcastStatusCode::ALL_SENDERS_DONE)
                brpc::StreamClose(stream_id);
        }
        else
            // already has been changed
            code = actual_status_code;
    }
    if (is_modifer)
    {
        sender_metrics.finish_code = status_code;
        sender_metrics.is_modifier = 1;
        sender_metrics.message = message;
        LOG_TRACE(log, "{} finished finish_code:{} message:'{}'", getName(), status_code, message);
        return BroadcastStatus(status_code, true, message);
    }
    else
    {
        const auto *const msg = "BrpcRemoteBroadcastSender::finish, already has been finished";
        sender_metrics.finish_code = status_code;
        sender_metrics.is_modifier = 0;
        return BroadcastStatus(
            static_cast<BroadcastStatusCode>(code), false, msg);
    }
}

void BrpcRemoteBroadcastSender::merge(IBroadcastSender && sender)
{
    BrpcRemoteBroadcastSender * other = dynamic_cast<BrpcRemoteBroadcastSender *>(&sender);
    if (!other)
        throw Exception("Sender to merge is not BrpcRemoteBroadcastSender", ErrorCodes::LOGICAL_ERROR);
    trans_keys.insert(
        trans_keys.end(), std::make_move_iterator(other->trans_keys.begin()), std::make_move_iterator(other->trans_keys.end()));
    other->trans_keys.clear();
    sender_stream_ids.insert(sender_stream_ids.end(), other->sender_stream_ids.begin(), other->sender_stream_ids.end());
    other->sender_stream_ids.clear();
}


String BrpcRemoteBroadcastSender::getName() const
{
    return fmt::format(
        "BrpcSender with keys:",
        boost::algorithm::join(
            trans_keys | boost::adaptors::transformed([](const ExchangeDataKeyPtr & key) { return key->toString(); }), "\n"));
}

}
