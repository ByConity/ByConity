#pragma once

#include <vector>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Poco/Logger.h>


namespace DB
{
class BufferChunk
{
public:
    BufferChunk(const Block & header, UInt64 threshold_in_bytes, UInt64 threshold_in_row_num);
    Chunk add(Chunk chunk);
    Chunk flush(bool force);
    void resetBuffer();

private:
    const Block & header;
    size_t column_num;
    UInt64 threshold_in_bytes;
    UInt64 threshold_in_row_num;
    MutableColumns buffer_columns;
    ChunkInfoPtr current_chunk_info;
    Poco::Logger * logger;
    inline size_t bufferBytes() const;
    inline bool compareBufferChunkInfo(const ChunkInfoPtr & chunk_info) const;
};

}
