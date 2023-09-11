#pragma once

#include <optional>
#include <Core/Types.h>
#include <IO/SeekableReadBuffer.h>

namespace DB
{

class LimitSeekableReadBuffer: public SeekableReadBuffer
{
public:
    // size == nullopt means this file has no limit
    LimitSeekableReadBuffer(SeekableReadBuffer& in, UInt64 beg_limit = 0,
        std::optional<UInt64> end_limit = std::nullopt, bool should_throw = false);

    virtual off_t getPosition() override
    {
        return buffer_end_offset_ - (working_buffer.end() - position());
    }

    virtual off_t seek(off_t offset, int whence) override;

private:
    virtual bool nextImpl() override;

    void adoptInputBuffer(size_t offset);

    SeekableReadBuffer& in_;
    UInt64 range_start_;
    // range_end_ == nullopt means in's end
    std::optional<UInt64> range_end_;
    bool should_throw_;

    // Underlying cursor of current working buffer end
    UInt64 buffer_end_offset_;
};

}