#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <common/types.h>

namespace DB
{
class WriteBuffer;
class CompressedWriteBuffer;


/** Serializes the stream of chunk in their native binary format.
  */
class NativeChunkOutputStream
{
public:
    NativeChunkOutputStream(WriteBuffer & ostr_, UInt64 client_revision_, const Block & header_, bool remove_low_cardinality_ = false);

    void write(const Chunk & chunk);

private:
    WriteBuffer & ostr;
    UInt64 client_revision;
    Block header;
    bool remove_low_cardinality;
};

}
