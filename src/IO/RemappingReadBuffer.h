#pragma once

#include <optional>
#include <IO/SeekableReadBuffer.h>
#include <IO/LimitSeekableReadBuffer.h>

namespace DB
{

class RemappingReadBuffer: public SeekableReadBuffer
{
public:
    explicit RemappingReadBuffer(std::unique_ptr<SeekableReadBuffer> in_,
        UInt64 offset_ = 0, std::optional<UInt64> size_ = std::nullopt);

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    size_t readBigAt(char* to, size_t n, size_t read_offset, const std::function<bool(size_t m)>& progress_callback) override;

    bool supportsReadAt() override { return in->supportsReadAt(); }

private:
    virtual bool nextImpl() override;

    void syncToUnderlyingBuffer();

    UInt64 offset;

    std::unique_ptr<SeekableReadBuffer> in;
    LimitSeekableReadBuffer limit_in;
};

}
