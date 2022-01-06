#pragma once

#include <vector>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Poco/Logger.h>


namespace DB
{
class ExchangeBufferedSender
{
public:
    ExchangeBufferedSender(const Block & header, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes, UInt64 threshold_in_row_num);
    void appendSelective(size_t column_idx, const IColumn & source, const IColumn::Selector & selector, size_t from, size_t length);
    void flush(bool force);

private:
    const Block & header;
    size_t column_num;
    BroadcastSenderPtr sender;
    UInt64 threshold_in_bytes;
    UInt64 threshold_in_row_num;
    MutableColumns partition_buffer;
    Poco::Logger * logger;
    void resetBuffer();
    inline size_t bufferBytes() const;
};

using ExchangeBufferedSenders = std::vector<ExchangeBufferedSender>;

}
