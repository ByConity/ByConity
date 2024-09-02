#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>

#include <zlib.h>


namespace DB
{

/// Performs compression using zlib library and writes compressed data to out_ WriteBuffer.
class ZlibDeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    ZlibDeflatingWriteBuffer(
            std::unique_ptr<WriteBuffer> out_,
            CompressionMethod compression_method,
            int compression_level,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

    void finalize() override { finish(); }

    ~ZlibDeflatingWriteBuffer() override;

    WriteBuffer * inplaceReconstruct([[maybe_unused]] const String & out_path, std::unique_ptr<WriteBuffer> nested) override
    {
        CompressionMethod method = this->compression_method;
        int level = this->compression_level;
        // Call the destructor explicitly but does not free memory
        this->~ZlibDeflatingWriteBuffer();
        new (this) ZlibDeflatingWriteBuffer(std::move(nested), method, level);
        return this;
    }

private:
    void nextImpl() override;

    void finishImpl();
    /// Flush all pending data and write zlib footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finish();

    CompressionMethod compression_method;
    int compression_level;
    std::unique_ptr<WriteBuffer> out;
    z_stream zstr;
    bool finished = false;
};

}
