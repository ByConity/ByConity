#include <fcntl.h>
#include <IO/RemappingReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_PARAMETER;
}

RemappingReadBuffer::RemappingReadBuffer(std::unique_ptr<SeekableReadBuffer> in_,
    UInt64 offset_, std::optional<UInt64> size_):
        SeekableReadBuffer(nullptr, 0),
        offset(offset_), in(std::move(in_)),
        limit_in(*in, offset_, size_.has_value() ? std::optional<UInt64>(offset_ + size_.value()) : std::nullopt)
{
    syncToUnderlyingBuffer();
}

off_t RemappingReadBuffer::getPosition()
{
    limit_in.position() = pos;

    return limit_in.getPosition() - offset;
}

off_t RemappingReadBuffer::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
    {
        throw Exception("RemappingReadBuffer only support SEEK_SET by now",
            ErrorCodes::UNSUPPORTED_PARAMETER);
    }

    off_t target_off = off + offset;
    off_t result_off = limit_in.seek(target_off, whence);

    syncToUnderlyingBuffer();

    return result_off - offset;
}

size_t RemappingReadBuffer::readBigAt(char* to, size_t n, size_t read_offset, const std::function<bool(size_t m)>& progress_callback)
{
    return in->readBigAt(to, n, offset + read_offset, progress_callback);
}

bool RemappingReadBuffer::nextImpl()
{
    limit_in.position() = pos;

    bool more = limit_in.next();

    syncToUnderlyingBuffer();

    return more;
}

void RemappingReadBuffer::syncToUnderlyingBuffer()
{
    working_buffer = limit_in.buffer();
    pos = limit_in.position();
}

}
