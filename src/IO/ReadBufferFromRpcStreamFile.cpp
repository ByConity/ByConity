#include <cstddef>
#include <memory>
#include <Columns/ColumnConst.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromRpcStreamFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Storages/DistributedDataClient.h>
#include <brpc/channel.h>
#include <brpc/stream.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include "common/scope_guard.h"
#include "common/types.h"
#include <Common/Brpc/BrpcChannelPoolOptions.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <common/logger_useful.h>


namespace ProfileEvents
{
extern const Event ReadBufferFromRpcStreamFileRead;
extern const Event ReadBufferFromRpcStreamFileReadFailed;
extern const Event ReadBufferFromRpcStreamFileReadMs;
extern const Event ReadBufferFromRpcStreamFileReadBytes;
extern const Event ReadBufferFromRpcStreamFileConnect;
extern const Event ReadBufferFromRpcStreamFileConnectFailed;
extern const Event ReadBufferFromRpcStreamFileConnectMs;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_READ_FROM_SOCKET;
}

ReadBufferFromRpcStreamFile::ReadBufferFromRpcStreamFile(
    const std::shared_ptr<DistributedDataClient> & client_, size_t buffer_size_, char * existing_memory_, size_t alignment_)
    : ReadBufferFromFileBase(buffer_size_, existing_memory_, alignment_), client(client_)
{
    Stopwatch watch;
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileConnect, 1);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileConnectMs, watch.elapsedMilliseconds());
    });

    try
    {
        client->createReadStream();
    }
    catch (const Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileConnectFailed, 1);
        throw e;
    }
}

bool ReadBufferFromRpcStreamFile::nextImpl()
{
    Stopwatch watch;
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileRead, 1);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileReadBytes, working_buffer.size());
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileReadMs, watch.elapsedMilliseconds());
    });

    try
    {
        if (internal_buffer.empty())
            throw Exception(
                fmt::format(
                    "Can't read invalid data length {} since file offset {}, {}",
                    internal_buffer.size(),
                    current_file_offset,
                    client->file_reader->readFileInfoMessage()),
                ErrorCodes::BAD_ARGUMENTS);

        bool ret = client->read(current_file_offset, internal_buffer.size(), working_buffer);
        current_file_offset += working_buffer.size();
        return ret;
    }
    catch (const Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromRpcStreamFileReadFailed, 1);
        throw e;
    }
}

off_t ReadBufferFromRpcStreamFile::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
        throw Exception("ReadBufferFromRpcStreamFile::seek expects SEEK_SET as whence", ErrorCodes::BAD_ARGUMENTS);

    if (off > static_cast<Int64>(client->remote_file_size))
        throw Exception(
            fmt::format("ReadBufferFromRpcStreamFile::seek offset {} larger than file size: {}", off, client->file_reader->readFileInfoMessage()),
            ErrorCodes::BAD_ARGUMENTS);

    // Seek in buffer(or end of current buffer)
    off_t buffer_start_offset = current_file_offset - static_cast<off_t>(working_buffer.size());
    if (hasPendingData() && off <= static_cast<Int64>(current_file_offset) && off >= buffer_start_offset)
    {
        pos = working_buffer.begin() + off - buffer_start_offset;
    }
    else
    {
        pos = working_buffer.end();
        current_file_offset = off;
    }
    return off;
}

}
