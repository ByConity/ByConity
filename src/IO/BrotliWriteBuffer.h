#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

class BrotliWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    BrotliWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~BrotliWriteBuffer() override;

    void finalize() override { finish(); }

    WriteBuffer * inplaceReconstruct([[maybe_unused]] const String & out_path, std::unique_ptr<WriteBuffer> nested) override
    {
        int level = this->compression_level;
        // Call the destructor explicitly but does not free memory
        this->~BrotliWriteBuffer();
        new (this) BrotliWriteBuffer(std::move(nested), level);
        return this;
    }

private:
    void nextImpl() override;

    void finish();
    void finishImpl();

    int compression_level;

    class BrotliStateWrapper;
    std::unique_ptr<BrotliStateWrapper> brotli;

    size_t in_available;
    const uint8_t * in_data;

    size_t out_capacity;
    uint8_t * out_data;

    std::unique_ptr<WriteBuffer> out;

    bool finished = false;
};

}
