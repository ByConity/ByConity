#include <cstddef>
#include <common/types.h>

#include <Columns/IColumn.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeHelpers.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
ExchangeBufferedSender::ExchangeBufferedSender(
    const Block & header_, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes_, UInt32 threshold_in_row_num_, UInt32 wait_receiver_timeout_ms_)
    : header(header_)
    , column_num(header_.getColumns().size())
    , sender(sender_)
    , threshold_in_bytes(threshold_in_bytes_)
    , threshold_in_row_num(threshold_in_row_num_)
    , wait_receiver_timeout_ms(wait_receiver_timeout_ms_)
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

    if (!is_receivers_ready)
    {
        sender->waitAllReceiversReady(wait_receiver_timeout_ms);
        is_receivers_ready = true;
    }
    Chunk chunk(std::move(partition_buffer), rows);
    sendAndCheckReturnStatus(*sender, std::move(chunk));
    resetBuffer();
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
