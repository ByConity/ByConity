#pragma once

#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

class CompressedStatisticsCollectBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    CompressionCodecPtr codec;
    PODArray<char> compressed_buffer;
    size_t total_compressed_size = 0;

    void nextImpl() override;

public:
    CompressedStatisticsCollectBuffer(
        CompressionCodecPtr codec_ = CompressionCodecFactory::instance().getDefaultCodec(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        next();
        return total_compressed_size;
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes()
    {
        return count();
    }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

    ~CompressedStatisticsCollectBuffer() override;
};

}
