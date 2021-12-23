#include "BrpcRemoteBroadcastSender.h"
#include "WriteBufferFromBrpcBuf.h"

#include <cerrno>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteHelpers.h>
#include <Processors/Exchange/DataTrans/DataTransException.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <brpc/protocol.h>
#include <brpc/stream.h>
#include <bthread/bthread.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace DB
{
namespace
{
    const auto STREAM_WAIT_TIMEOUT_MS = 1000;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_EXCEPTION;
}

/**
 * 1-n, broadcast
 */
BrpcRemoteBroadcastSender::BrpcRemoteBroadcastSender(
    std::vector<DataTransKeyPtr> trans_keys_, ContextPtr context_, Block header_)
    : trans_keys(std::move(trans_keys_))
    , context(std::move(context_))
    , header(std::move(header_))
    , registry_center(BrpcExchangeRegistryCenter::getInstance())
{
}

/**
 * 1-1, repartition
 */
BrpcRemoteBroadcastSender::BrpcRemoteBroadcastSender(DataTransKeyPtr trans_key_, ContextPtr context_, Block header_)
    : trans_keys(std::vector<DataTransKeyPtr>{std::move(trans_key_)})
    , context(std::move(context_))
    , header(std::move(header_))
    , registry_center(BrpcExchangeRegistryCenter::getInstance())
{
}

BrpcRemoteBroadcastSender::~BrpcRemoteBroadcastSender()
{
    for (const auto & key : trans_keys)
    {
        registry_center.removeReceiver(key->getKey());
    }
}

void BrpcRemoteBroadcastSender::waitAllReceiversReady(UInt32 timeout_ms)
{
    size_t max_num = timeout_ms / 10;
    for (const auto & trans_key : trans_keys)
    {
        const auto & id = trans_key->getKey();
        // for each receiver_id check exists in registry_center
        size_t retry_count = 0;
        while (!registry_center.exist(id))
        {
            if (retry_count >= max_num)
            {
                throw DataTransException(
                    "Wait for receiver id-" + id + " registering timeout.", ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
            }
            retry_count++;
            bthread_usleep(10 * 1000);
        }
        auto stream_id = registry_center.getSenderStreamId(id);
        sender_stream_ids.push_back(stream_id);
        LOG_DEBUG(log, "Receiver-{} is ready, stream-id is {}", id, stream_id);
    }
    is_ready = true;
}

BroadcastStatus BrpcRemoteBroadcastSender::send(Chunk chunk)
{
    if (!is_ready)
    {
        throw DataTransException("Call wait_all_receivers_ready before sending chunk.", ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
    }
    WriteBufferFromBrpcBuf out;
    const auto & settings = context->getSettingsRef();
    if (settings.exchange_enable_block_compress)
    {
        std::string method = Poco::toUpper(settings.network_compression_method.toString());
        std::optional<int> level;
        if (method == "ZSTD")
            level = settings.network_zstd_compression_level;
        const CompressionCodecPtr & codec = CompressionCodecFactory::instance().get(method, level);
        CompressedWriteBuffer compressed_out(out, codec, DBMS_DEFAULT_BUFFER_SIZE * 2);
        NativeChunkOutputStream chunk_out(
            compressed_out, ClickHouseRevision::getVersionRevision(), header, !settings.low_cardinality_allow_in_native_format);
        chunk_out.write(chunk);
        compressed_out.next();
    }
    else
    {
        NativeChunkOutputStream chunk_out(
            out, ClickHouseRevision::getVersionRevision(), header, !settings.low_cardinality_allow_in_native_format);
        chunk_out.write(chunk);
    }
    const auto & buf = out.getFinishedBuf();
    for (size_t i = 0; i < sender_stream_ids.size(); ++i)
    {
        sendIOBuffer(buf, sender_stream_ids[i], trans_keys[i]->getKey());
    }
    //TODO
    return BroadcastStatus(BroadcastStatusCode::RUNNING);
}

bool BrpcRemoteBroadcastSender::sendIOBuffer(butil::IOBuf io_buffer, brpc::StreamId stream_id, const String & data_key)
{
    if (io_buffer.size() > brpc::FLAGS_max_body_size)
        throw Exception(
            "Write stream buffer fail, io buffer size is bigger than " + std::to_string(brpc::FLAGS_max_body_size) + " current is "
                + std::to_string(io_buffer.size()),
            ErrorCodes::BRPC_EXCEPTION);

    size_t retry_count = 0;
    Stopwatch s;
    bool success = false;
    while (s.elapsedMilliseconds() < context->getSettingsRef().exchange_timeout_ms)
    {
        int rect_code = brpc::StreamWrite(stream_id, io_buffer);
        if (rect_code == 0)
        {
            success = true;
            break;
        }
        else if (rect_code == EAGAIN)
        {
            bthread_usleep(50 * 1000);
            timespec timeout = butil::milliseconds_from_now(STREAM_WAIT_TIMEOUT_MS);
            int wait_res_code = brpc::StreamWait(stream_id, &timeout);
            if (wait_res_code == EINVAL)
                throw Exception("stream id-" + std::to_string(stream_id) + " is closed", ErrorCodes::BRPC_EXCEPTION);
            LOG_DEBUG(
                log,
                "Stream write buffer full wait for {} ms,  retry count-{}, stream_id-{} ,with data_key-{}  wait res code:{} size:{} ",
                STREAM_WAIT_TIMEOUT_MS,
                retry_count,
                stream_id,
                data_key,
                wait_res_code,
                io_buffer.size());
        }
        else if (rect_code == EINVAL)
            throw Exception(
                "Stream write receive rect_code EINVAL, stream id-" + std::to_string(stream_id) + " is closed", ErrorCodes::BRPC_EXCEPTION);
        else if (rect_code == 1011) //EOVERCROWDED   | 1011 | The server is overcrowded
        {
            bthread_usleep(50 * 1000);
            LOG_WARNING(log, "Stream write buffer error rect_code:{}, server is overcrowded", rect_code);
        }
        else
        {
            LOG_WARNING(log, "Stream write buffer other error rect_code:{}", rect_code);
        }

        retry_count++;
    }

    if (!success)
        throw Exception(
            "write stream buffer fail,with data_key-" + data_key + ", size:" + std::to_string(io_buffer.size()),
            ErrorCodes::BRPC_EXCEPTION);
    LOG_DEBUG(
        log,
        "Send exchange data size-{} M with data_key-{}, stream_id-{} retry times:{} cost:{} ms",
        io_buffer.size() / 1024.0 / 1024.0,
        data_key,
        stream_id,
        retry_count,
        s.elapsedMilliseconds());
    return true;
}

BroadcastStatus BrpcRemoteBroadcastSender::finish(BroadcastStatusCode /*status_code*/, String /*message*/)
{
    // todo::aron 需要 brpc 层支持 finish 接口. 优雅关闭
    return BroadcastStatus(BroadcastStatusCode::SEND_CANCELLED);
}
}
