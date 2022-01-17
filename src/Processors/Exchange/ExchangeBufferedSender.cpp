#include <cstddef>
#include <common/types.h>

#include <Columns/IColumn.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <Processors/Chunk.h>

namespace DB
{
ExchangeBufferedSender::ExchangeBufferedSender(
    const Block & header_, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes_, UInt64 threshold_in_row_num_)
    : header(header_)
    , column_num(header_.getColumns().size())
    , sender(sender_)
    , threshold_in_bytes(threshold_in_bytes_)
    , threshold_in_row_num(threshold_in_row_num_)
    , logger(&Poco::Logger::get("ExchangeBufferedSender"))
{
    resetBuffer();
}

void ExchangeBufferedSender::flush(bool force)
{
    size_t rows = partition_buffer[0]->size();

    if (rows == 0)
        return;

    if (!force)
    {
        if (bufferBytes() < threshold_in_bytes && rows < threshold_in_row_num)
            return;
    }

    LOG_TRACE(logger, "flush buffer, force: {}, row: {}", force, rows);

    Chunk chunk(std::move(partition_buffer), rows, std::move(current_chunk_info));
    current_chunk_info = ChunkInfoPtr();

    ExchangeUtils::sendAndCheckReturnStatus(*sender, std::move(chunk));
    resetBuffer();
}

bool ExchangeBufferedSender::compareBufferChunkInfo(const ChunkInfoPtr & chunk_info) const
{
    return ((current_chunk_info && chunk_info && *current_chunk_info == *chunk_info) || (!current_chunk_info && !chunk_info));
}


void ExchangeBufferedSender::updateBufferChunkInfo(ChunkInfoPtr chunk_info)
{
    flush(true);
    current_chunk_info = std::move(chunk_info);
}

void ExchangeBufferedSender::sendThrough(Chunk chunk)
{
    ExchangeUtils::sendAndCheckReturnStatus(*sender, std::move(chunk));
}

void ExchangeBufferedSender::resetBuffer()
{
    partition_buffer = header.cloneEmptyColumns();
}

void ExchangeBufferedSender::appendSelective(
    size_t column_idx, const IColumn & source, const IColumn::Selector & selector, size_t from, size_t length)
{
    partition_buffer[column_idx]->insertRangeSelective(source, selector, from, length);
}

size_t ExchangeBufferedSender::bufferBytes() const
{
    size_t total = 0;
    for (size_t i = 0; i < column_num; ++i)
    {
        total += partition_buffer[i]->byteSize();
    }
    return total;
}

}
