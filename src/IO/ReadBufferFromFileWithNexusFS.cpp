#include <IO/ReadBufferFromFileWithNexusFS.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromFileWithNexusFS::ReadBufferFromFileWithNexusFS(
    size_t buf_size,
    std::unique_ptr<ReadBufferFromFileBase> source_read_buffer_,
    NexusFS &nexus_fs_)
    : ReadBufferFromFileBase(buf_size, nullptr, 0)
    , file_name(source_read_buffer_->getFileName())
    , source_read_buffer(std::move(source_read_buffer_))
    , nexus_fs(nexus_fs_)
{
}

bool ReadBufferFromFileWithNexusFS::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
        }
    }

    /// If internal_buffer size is empty, then read() cannot be distinguished from EOF
    chassert(!internal_buffer.empty());

    size_t max_size_to_read = internal_buffer.size();
    if (read_until_position)
    {
        max_size_to_read = std::min(max_size_to_read, static_cast<size_t>(read_until_position - offset));
    }

    size_t bytes_read = nexus_fs.read(file_name, offset, max_size_to_read, source_read_buffer, internal_buffer.begin());

    if (bytes_read)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
        offset += bytes_read;
        return true;
    }
    
    return false;
}

off_t ReadBufferFromFileWithNexusFS::seek(off_t offset_, int whence)
{
    if (whence == SEEK_CUR)
        offset_ = getPosition() + offset_;
    else if (whence != SEEK_SET)
        throw Exception("Seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::BAD_ARGUMENTS);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (offset_ == getPosition())
        return offset_;

    if (!working_buffer.empty()
        && static_cast<size_t>(offset_) >= offset - working_buffer.size()
        && offset_ < offset)
    {
        pos = working_buffer.end() - (offset - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos < working_buffer.end());

        return getPosition();
    }

    resetWorkingBuffer();
    offset = offset_;

    return offset;
}

IAsynchronousReader::Result ReadBufferFromFileWithNexusFS::readInto(char * data, size_t size, size_t read_offset, size_t ignore_bytes)
{
    bool result = false;
    offset = read_offset;
    set(data, size);

    if (ignore_bytes)
    {
        ignore(ignore_bytes);
        result = hasPendingData();
        ignore_bytes = 0;
    }

    if (!result)
        result = next();

    if (result)
    {
        assert(available());
        return { working_buffer.size(), BufferBase::offset(), nullptr };
    }

    return {0, 0, nullptr};
}

size_t ReadBufferFromFileWithNexusFS::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback)
{
    if (n == 0)
        return 0;

    size_t bytes_read = nexus_fs.read(file_name, range_begin, n, source_read_buffer, to);

    if (bytes_read && progress_callback)
        progress_callback(bytes_read);
    return bytes_read;
}

void ReadBufferFromFileWithNexusFS::setReadUntilPosition(size_t position)
{
    if (position != static_cast<size_t>(read_until_position))
    {
        offset = getPosition();
        resetWorkingBuffer();
        read_until_position = position;
    }
}

void ReadBufferFromFileWithNexusFS::setReadUntilEnd()
{
    if (read_until_position)
    {
        offset = getPosition();
        resetWorkingBuffer();
        read_until_position = 0;
    }
}

}
