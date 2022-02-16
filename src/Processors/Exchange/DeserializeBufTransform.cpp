
#include <memory>
#include <Columns/IColumn.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{
DeserializeBufTransform::DeserializeBufTransform(const Block & header_, bool enable_block_compress_)
    : ISimpleTransform(Block(), header_, true)
    , header(getOutputPort().getHeader())
    , enable_block_compress(enable_block_compress_)
    , logger(&Poco::Logger::get("DeserializeBufTransform"))
{
}

void DeserializeBufTransform::transform(Chunk & chunk)
{
    const ChunkInfoPtr & info = chunk.getChunkInfo();
    if (!info)
        return;

    auto iobuf_info = std::dynamic_pointer_cast<const DeserializeBufTransform::IOBufChunkInfo>(info);
    if (!iobuf_info)
        return;

    auto read_buffer = std::make_unique<ReadBufferFromBrpcBuf>(iobuf_info->io_buf);
    std::unique_ptr<ReadBuffer> buf;
    if (enable_block_compress)
        buf = std::make_unique<CompressedReadBuffer>(*read_buffer);
    else
        buf = std::move(read_buffer);
    NativeChunkInputStream chunk_in(*buf, header);
    chunk = chunk_in.readImpl();
    ExchangeUtils::transferGlobalMemoryToThread(chunk.allocatedBytes());
}

}
