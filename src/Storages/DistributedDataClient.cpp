#include <cstddef>
#include <memory>
#include <vector>
#include <Core/Types.h>
#include <IO/VarInt.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Protos/distributed_file.pb.h>
#include <Storages/DistributedDataClient.h>
#include <boost/algorithm/string/predicate.hpp>
#include <brpc/stream.h>
#include <fmt/core.h>
#include "Common/Exception.h"
#include "Common/Stopwatch.h"
#include "common/scope_guard.h"
#include "common/types.h"
#include <common/logger_useful.h>
#include "Disks/DiskType.h"
#include "MergeTreeCommon/MergeTreeMetaBase.h"
#include "Storages/DistributedDataCommon.h"
#include <CloudServices/CnchWorkerClient.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Interpreters/WorkerGroupHandle.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void DataStreamReader::pushReceiveQueue(butil::IOBuf packet, UInt64 timeout_ms)
{
    if (!queue->tryEmplace(timeout_ms, std::move(packet)))
    {
        if (queue->closed())
        {
            return;
        }
        throw Exception("Push file stream data to receiver: " +  readFileInfoMessage() , ErrorCodes::LOGICAL_ERROR);
    }
}

void DataStreamReader::readReceiveQueue(BufferBase::Buffer & buffer /*output*/, UInt64 timeout_ms)
{
    butil::IOBuf packet;
    if (!queue->tryPop(packet, timeout_ms))
        throw Exception(
            fmt::format(
                "Client read remote data stream timeout: {}ms, {}",
                timeout_ms,
                readFileInfoMessage()),
            ErrorCodes::LOGICAL_ERROR);
    if (packet.empty())
        return;

    ExchangeUtils::transferGlobalMemoryToThread(packet.length());

    Stopwatch watch;
    auto size = packet.copy_to(buffer.begin(), packet.length()); //todo(jiashuo): avoid copy data
    buffer.resize(size);
    LOG_TRACE(log, "Copy {} data take {}ms", readFileInfoMessage(), watch.elapsedMilliseconds());
}

int StreamClientHandler::on_received_messages(brpc::StreamId id, butil::IOBuf * const messages[], size_t size)
{
    for (size_t index = 0; index < size; index++)
    {
        butil::IOBuf * msg = messages[index];
        auto bytes = msg->size();
        auto raw_packet = std::make_unique<butil::IOBuf>(msg->movable());
        try
        {
            file_reader->pushReceiveQueue(*raw_packet, option.read_timeout_ms);
        }
        catch (...)
        {
            throw;
        }
        LOG_TRACE(
            log,
            "on_received_messages: StreamId: {} received and push queue file data successfully, io-buffer[{}] has {} bytes: {}",
            id,
            index,
            bytes,
            file_reader->readFileInfoMessage());
    }
    return 0;
}

// todo(jiashuo): same connection should be connect one time, support file connection pool
bool DistributedDataClient::createReadStream()
{
    Stopwatch watch;
    SCOPE_EXIT({
        LOG_TRACE(
        log,
        "Connect service({}) successfully: {}, take {}ms",
        stream_id,
        file_reader->readFileInfoMessage(), watch.elapsedMilliseconds());
    });

    std::shared_ptr<RpcClient> rpc_client
        = RpcChannelPool::getInstance().getClient(remote_addr, BrpcChannelPoolOptions::STREAM_DEFAULT_CONFIG_KEY);
    Protos::FileStreamService_Stub stub(Protos::FileStreamService_Stub(&rpc_client->getChannel()));
    brpc::Controller cntl;
    brpc::StreamOptions stream_options;
    stream_options.handler = std::make_shared<StreamClientHandler>(file_reader);
    if (option.connection_timeout_ms == 0)
        cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
    else
        cntl.set_timeout_ms(option.connection_timeout_ms);
    cntl.set_max_retry(option.max_retry_times);
    if (brpc::StreamCreate(&stream_id, cntl, &stream_options) != 0)
        throw Exception("Fail to create stream for " + local_key_name, ErrorCodes::LOGICAL_ERROR);

    if (stream_id == brpc::INVALID_STREAM_ID)
        throw Exception("Stream id is invalid for " + local_key_name, ErrorCodes::LOGICAL_ERROR);

    Protos::ConnectRequest request;
    Protos::ConnectResponse response;
    request.set_key(local_key_name);

    stub.acceptConnection(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == brpc::EREQUEST && boost::algorithm::ends_with(cntl.ErrorText(), "was closed before responded"))
    {
        LOG_INFO(
            log,
            "Connect service successfully but service already finished: {}", file_reader->readFileInfoMessage());
        return false;
    }
    rpc_client->assertController(cntl);
    remote_file_path = response.file_full_path();
    remote_file_size = response.file_size();

    file_reader->remote_file_path = remote_file_path;
    file_reader->remote_file_size = remote_file_size;
    return !remote_file_path.empty();
}

bool DistributedDataClient::read(UInt64 offset, UInt64 length, BufferBase::Buffer & buffer /*output*/)
{
    Stopwatch watch;
    bool success;
    SCOPE_EXIT({
        LOG_TRACE(
            log,
            "Stream_id {} Readed(success = {}) offset {}, length {}, result size: {}, {}, take {}ms",
            stream_id,
            success,
            offset,
            length,
            buffer.size(),
            file_reader->readFileInfoMessage(),
            watch.elapsedMilliseconds());
    });

    if (stream_id == brpc::INVALID_STREAM_ID)
        throw Exception("client read data stream is haven't init: " + local_key_name, ErrorCodes::LOGICAL_ERROR);

    if (offset > remote_file_size)
        throw Exception(
            fmt::format("Can't read invalid offset {} since file: {}", offset, file_reader->readFileInfoMessage()),
            ErrorCodes::LOGICAL_ERROR);

    if (offset == remote_file_size)
    {
        LOG_WARNING(log, fmt::format("Close read eof offset {} since file size: {}", offset, file_reader->readFileInfoMessage()));
        return false;
    }

    if (read_rate_throttler.has_value())
        read_rate_throttler.value().add(1);

    std::shared_ptr<WriteBufferFromBrpcBuf> out = std::make_shared<WriteBufferFromBrpcBuf>();
    writeVarUInt(offset, *out);
    writeVarUInt(length, *out);
    out->next();
    success = brpcWriteWithRetry(stream_id, out->getFinishedBuf(), option.max_retry_times, file_reader->readFileInfoMessage());
    file_reader->readReceiveQueue(buffer, option.read_timeout_ms);

    return !buffer.empty() || buffer.size() == length;
}

bool DistributedDataClient::write(const String & disk_name, const std::vector<WriteFile> & files) const
{
    Stopwatch watch;
    SCOPE_EXIT({
        LOG_TRACE(
            log,
            "Write {} file size {} take {}ms",
            disk_name,
            files.size(),
            watch.elapsedMilliseconds());
    });

    brpc::Controller cntl;
    try
    {
        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(remote_addr, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        Protos::FileStreamService_Stub stub(Protos::FileStreamService_Stub(&rpc_client->getChannel()));
        if (option.connection_timeout_ms == 0)
            cntl.set_timeout_ms(rpc_client->getChannel().options().timeout_ms);
        else
            cntl.set_timeout_ms(option.connection_timeout_ms);
        cntl.set_max_retry(option.max_retry_times);

        Protos::writeFileRquest request;
        Protos::writeFileResponse response;

        request.set_disk_name(disk_name);
        for (const auto & write_file : files)
        {
            Protos::FileInfo * file = request.add_files();
            file->set_key(write_file.key);
            file->set_path(write_file.path);
            file->set_offset(write_file.offset);
            file->set_length(write_file.length);
        }

        stub.writeRemoteFile(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "Failed write into remote file: {}", e.message());
        cntl.SetFailed(e.message());
    }
    return true;
}

bool DistributedDataClient::close() const
{
    return brpc::StreamClose(stream_id);
}

}
