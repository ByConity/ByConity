#include <optional>
#include <IO/LimitSeekableReadBuffer.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer& in,
    UInt64 beg_limit, std::optional<UInt64> end_limit, bool should_throw):
        SeekableReadBuffer(nullptr, 0), in_(in), range_start_(beg_limit),
        range_end_(end_limit), should_throw_(should_throw),
        buffer_end_offset_(0)
{
    in_.seek(range_start_, SEEK_SET);

    adoptInputBuffer(range_start_);
}

off_t LimitSeekableReadBuffer::seek(off_t offset, int whence)
{
    if (whence != SEEK_SET)
        throw Exception("Seek mode " + std::to_string(whence) + " not supported",
            ErrorCodes::BAD_ARGUMENTS);

    UInt64 under_buf_start = buffer_end_offset_ - working_buffer.size();
    UInt64 under_new_pos = offset;
    if (under_new_pos >= under_buf_start && under_new_pos <= buffer_end_offset_)
    {
        // Seek in current working buffer
        pos = working_buffer.begin() + (under_new_pos - under_buf_start);
        return offset;
    }
    else
    {
        if (under_new_pos < range_start_ || (range_end_.has_value() && under_new_pos > range_end_.value()))
            throw Exception("Try to seek to " + std::to_string(under_new_pos) \
                + ", which outside range " + std::to_string(range_start_) + "-" \
                + (range_end_.has_value() ? std::to_string(range_end_.value()) : "eof"), \
                ErrorCodes::BAD_ARGUMENTS);

        offset = in_.seek(under_new_pos, whence);

        adoptInputBuffer(offset);

        return offset;
    }
}

void LimitSeekableReadBuffer::adoptInputBuffer(size_t offset)
{
    UInt64 under_buf_end = in_.getPosition() + (in_.buffer().end() - in_.position());
    if (in_.hasPendingData())
    {
        UInt64 under_buf_start = under_buf_end - in_.buffer().size();

        UInt64 usable_start_offset = under_buf_start < range_start_ ? range_start_ - under_buf_start : 0;
        UInt64 usable_end_offset = in_.buffer().size();
        if (range_end_.has_value())
        {
            usable_end_offset = under_buf_end > range_end_.value() ? in_.buffer().size() - (under_buf_end - range_end_.value()) : in_.buffer().size();
        }

        working_buffer = Buffer(in_.buffer().begin() + usable_start_offset, in_.buffer().begin() + usable_end_offset);
        pos = working_buffer.begin() + (offset - under_buf_start - usable_start_offset);

        buffer_end_offset_ = under_buf_start + usable_end_offset;
    }
    else
    {
        working_buffer = in_.buffer();
        working_buffer.resize(0);
        pos = working_buffer.begin();

        buffer_end_offset_ = under_buf_end;
    }
}

bool LimitSeekableReadBuffer::nextImpl()
{
    in_.position() = in_.buffer().end();

    if (range_end_.has_value() && buffer_end_offset_ >= range_end_.value())
    {
        if (should_throw_)
            throw Exception("Attempt to read after eof", ErrorCodes::BAD_ARGUMENTS);
        else
            return false;
    }

    if (!in_.next())
        return false;

    UInt64 under_end_pos = in_.getPosition() + (in_.buffer().end() - in_.position());
    UInt64 usable_end_offset = in_.buffer().size();
    if (range_end_.has_value())
    {
        usable_end_offset = range_end_.value() < under_end_pos ? in_.buffer().size() - (under_end_pos - range_end_.value()) : in_.buffer().size();
    }

    working_buffer = Buffer(in_.position(), in_.buffer().begin() + usable_end_offset);
    pos = working_buffer.begin();
    buffer_end_offset_ = under_end_pos - in_.buffer().size() + usable_end_offset;
 
    return true;
}

}