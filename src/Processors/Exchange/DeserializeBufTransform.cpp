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
#include "Common/Stopwatch.h"
#include <common/logger_useful.h>

namespace DB
{
DeserializeBufTransform::DeserializeBufTransform(const Block & header_, bool enable_block_compress_)
    : ISimpleTransform(Block(), header_, true)
    , header(getOutputPort().getHeader())
    , enable_block_compress(enable_block_compress_)
    , logger(getLogger("DeserializeBufTransform"))
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
    s.restart();
    NativeChunkInputStream chunk_in(*buf, header);
    chunk = chunk_in.readImpl();
    if (const auto * io_buf_with_receiver = dynamic_cast<const DeserializeBufTransform::IOBufChunkInfoWithReceiver *>(iobuf_info.get()))
    {
        if (auto receiver = io_buf_with_receiver->receiver.lock())
            receiver->addToMetricsMaybe(0, s.elapsedMilliseconds(), 0, chunk);
    }
}
}
