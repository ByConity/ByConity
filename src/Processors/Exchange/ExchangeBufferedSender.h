#pragma once

#include <vector>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Poco/Logger.h>


namespace DB
{
class ExchangeBufferedSender
{
public:
    ExchangeBufferedSender(const Block & header, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes, UInt64 threshold_in_row_num);
    void appendSelective(size_t column_idx, const IColumn & source, const IColumn::Selector & selector, size_t from, size_t length);
    void sendThrough(Chunk chunk);
    void flush(bool force);
    bool compareBufferChunkInfo(const ChunkInfoPtr & chunk_info) const;
    void updateBufferChunkInfo(ChunkInfoPtr chunk_info);

private:
    const Block & header;
    size_t column_num;
    BroadcastSenderPtr sender;
    UInt64 threshold_in_bytes;
    UInt64 threshold_in_row_num;
    MutableColumns partition_buffer;
    ChunkInfoPtr current_chunk_info;
    Poco::Logger * logger;
    void resetBuffer();
    inline size_t bufferBytes() const;
};

using ExchangeBufferedSenders = std::vector<ExchangeBufferedSender>;

}
