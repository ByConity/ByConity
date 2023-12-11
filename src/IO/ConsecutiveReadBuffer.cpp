#include <IO/ConsecutiveReadBuffer.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ConsecutiveReadBuffer::ConsecutiveReadBuffer(const std::vector<ReadBuffer*>& buffers_):
        ReadBuffer(nullptr, 0), current_buffer(0), buffers(buffers_) {
    if (buffers.empty())
    {
        throw Exception("No buffer to read when construct ConsecutiveReadBuffer",
            ErrorCodes::LOGICAL_ERROR);
    }

    syncToBuffer();
}

size_t ConsecutiveReadBuffer::readBig(char * to, size_t n)
{
    if (current_buffer >= buffers.size())
    {
        return 0;
    }

    /// Update buffer cursor
    buffers[current_buffer]->position() = position();

    size_t read_bytes = 0;
    while (read_bytes < n)
    {
        size_t readed = buffers[current_buffer]->readBig(to + read_bytes, n - read_bytes);
        read_bytes += readed;

        if (readed == 0)
        {
            ++current_buffer;

            if (current_buffer == buffers.size())
            {
                resetWorkingBuffer();
                return read_bytes;
            }
        }
    }

    syncToBuffer();

    return read_bytes;
}

bool ConsecutiveReadBuffer::nextImpl()
{
    if (current_buffer >= buffers.size())
    {
        return false;
    }

    /// Sync buffer cursor
    buffers[current_buffer]->position() = position();
    bool more_to_read = buffers[current_buffer]->next();

    if (more_to_read)
    {
        syncToBuffer();
        return true;
    }
    else
    {
        ++current_buffer;

        if (current_buffer < buffers.size())
        {
            syncToBuffer();
            return true;
        }
        else
        {
            return false;
        }
    }
}

void ConsecutiveReadBuffer::syncToBuffer()
{
    working_buffer = buffers[current_buffer]->buffer();
    pos = buffers[current_buffer]->position();
}

}
