#pragma once

#include <IO/BufferBase.h>
#include <Storages/NexusFS/NexusFS.h>

namespace DB
{

class NexusFSBufferWithHandle
{
public:
    NexusFSBufferWithHandle() = default;
    NexusFSBufferWithHandle(NexusFSBufferWithHandle && other) noexcept
        : handle(std::move(other.handle)), buffer(std::move(other.buffer)), insert_cxt(std::move(other.insert_cxt))
    {
    }
    NexusFSBufferWithHandle & operator=(NexusFSBufferWithHandle && other) noexcept
    {
        if (this == &other)
            return *this;

        reset();
        swap(handle, other.handle);
        swap(buffer, other.buffer);
        swap(insert_cxt, other.insert_cxt);
        return *this;
    }
    ~NexusFSBufferWithHandle() { reset(); }

    void reset()
    {
        if (handle)
        {
            handle->unpin();
            handle.reset();
        }
        if (buffer)
            buffer.reset();
        if (insert_cxt)
            insert_cxt.reset();
    }

    size_t getSize() const { return buffer ? buffer->available() : 0; }
    BufferBase::Position getData() { return buffer ? buffer->position() : nullptr; }

private:
    friend class NexusFS;

    std::shared_ptr<NexusFSComponents::BlockHandle> handle{nullptr};
    std::unique_ptr<BufferWithOwnMemory<ReadBuffer>> buffer{nullptr};
    std::shared_ptr<NexusFS::InsertCxt> insert_cxt{nullptr};
};
}
