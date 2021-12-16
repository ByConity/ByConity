#pragma once

#include <IO/ReadBuffer.h>
#include <butil/iobuf.h>

namespace DB
{
class ReadBufferFromBrpcBuf : public ReadBuffer
{
public:
    ReadBufferFromBrpcBuf(const butil::IOBuf & buf_) : ReadBuffer(nullptr, 0), buf(buf_) { }

    bool nextImpl() override
    {
        if (processed + 1 == int64_t(buf.backing_block_num()))
            return false;
        auto block_view = buf.backing_block(++processed);
        working_buffer = Buffer(const_cast<Position>(block_view.data()), const_cast<Position>(block_view.data() + block_view.size()));
        return true;
    }

private:
    const butil::IOBuf & buf;
    int64_t processed = -1;
};

class ReadBufferFromBrpcBufArray : public ReadBuffer
{
public:
    using BufferArray = const butil::IOBuf * const *;

    ReadBufferFromBrpcBufArray(BufferArray buffers_, size_t size_) : ReadBuffer(nullptr, 0), buffers(buffers_), size(size_) { }

    bool nextImpl() override
    {
        while (index < size && processed_block + 1 == int64_t(buffers[index]->backing_block_num()))
        {
            index += 1; /// move to next buffer
            processed_block = -1; /// reset block index
        }

        if (index == size)
            return false;

        auto block_view = buffers[index]->backing_block(++processed_block);
        working_buffer = Buffer(const_cast<Position>(block_view.data()), const_cast<Position>(block_view.data() + block_view.size()));
        return true;
    }

private:
    BufferArray buffers;
    size_t size;
    size_t index = 0;
    int64_t processed_block = -1;
};

}
