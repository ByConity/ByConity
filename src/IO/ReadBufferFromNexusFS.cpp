#include <IO/ReadBufferFromNexusFS.h>
#include "Common/Exception.h"
#include "common/logger_useful.h"
#include <Common/ElapsedTimeProfileEventIncrement.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}

namespace ProfileEvents
{
    extern const Event ReadFromNexusFSReadBytes;
    extern const Event ReadFromNexusFSAsynchronousWaitMicroseconds;
    extern const Event ReadFromNexusFSSynchronousWaitMicroseconds;
    extern const Event ReadFromNexusFSSeeks;
    extern const Event ReadFromNexusFSPrefetchRequests;
    extern const Event ReadFromNexusFSUnusedPrefetches;
    extern const Event ReadFromNexusFSPrefetchedReads;
    extern const Event ReadFromNexusFSPrefetchedBytes;
    extern const Event ReadFromNexusFSPrefetchTaskWait;
    extern const Event ReadFromNexusFSPrefetchTaskNotWait;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromNexusFS::ReadBufferFromNexusFS(
    size_t buf_size_,
    bool actively_prefetch_,
    std::unique_ptr<ReadBufferFromFileBase> source_read_buffer_,
    NexusFS &nexus_fs_)
    : ReadBufferFromFileBase(nexus_fs_.supportNonCopyingRead() ? 0 : buf_size_, nullptr, 0)
    , file_name(source_read_buffer_->getFileName())
    , source_read_buffer(std::move(source_read_buffer_))
    , nexus_fs(nexus_fs_)
    , buf_size(buf_size_)
    , read_to_internal_buffer(!nexus_fs_.supportNonCopyingRead())
    , actively_prefetch(actively_prefetch_)
{
}

ReadBufferFromNexusFS::~ReadBufferFromNexusFS()
{
    try
    {
        resetPrefetch();
    }
    catch (Exception & e)
    {
        LOG_WARNING(log, "resetPrefetch raises exception: {}", e.message());
    }
}

bool ReadBufferFromNexusFS::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    if (!read_to_internal_buffer)
    {
        // read from nexusfs by non-copying method
        nexusfs_buffer.reset();

        // first, check if there prefetched data
        if (prefetch_future.valid())
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadFromNexusFSAsynchronousWaitMicroseconds);
            CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};

            if (prefetch_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
                ProfileEvents::increment(ProfileEvents::ReadFromNexusFSPrefetchTaskNotWait);
            else
                ProfileEvents::increment(ProfileEvents::ReadFromNexusFSPrefetchTaskWait);

            nexusfs_buffer = prefetch_future.get();
            auto size = nexusfs_buffer.getSize();

            prefetch_future = {};

            ProfileEvents::increment(ProfileEvents::ReadFromNexusFSPrefetchedReads);
            ProfileEvents::increment(ProfileEvents::ReadFromNexusFSPrefetchedBytes, size);
        }
        else
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadFromNexusFSSynchronousWaitMicroseconds);
            size_t max_size_to_read = read_until_position ? read_until_position - offset : buf_size;
            nexusfs_buffer = nexus_fs.read(
                file_name,
                offset,
                max_size_to_read,
                source_read_buffer);
        }

        size_t bytes_read = nexusfs_buffer.getSize();
        if (bytes_read == 0)
            return false;

        ProfileEvents::increment(ProfileEvents::ReadFromNexusFSReadBytes, bytes_read);
        BufferBase::set(nexusfs_buffer.getData(), bytes_read, 0);
        offset += bytes_read;

        if (actively_prefetch)
            prefetch(Priority{0});

        return true;
    }

    size_t max_size_to_read = internal_buffer.size();
    if (read_until_position)
    {
        max_size_to_read = std::min(max_size_to_read, static_cast<size_t>(read_until_position - offset));
    }

    size_t total_bytes_read = 0;
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadFromNexusFSSynchronousWaitMicroseconds);
        do
        {
            size_t bytes_read = nexus_fs.read(
                file_name,
                offset + total_bytes_read,
                max_size_to_read - total_bytes_read,
                source_read_buffer,
                internal_buffer.begin() + total_bytes_read);

            if (bytes_read == 0)
                break;
            total_bytes_read += bytes_read;
        }
        while (total_bytes_read < max_size_to_read);
    }

    if (total_bytes_read)
    {
        ProfileEvents::increment(ProfileEvents::ReadFromNexusFSReadBytes, total_bytes_read);
        working_buffer = internal_buffer;
        working_buffer.resize(total_bytes_read);
        offset += total_bytes_read;
        return true;
    }
    
    return false;
}

off_t ReadBufferFromNexusFS::seek(off_t offset_, int whence)
{
    ProfileEvents::increment(ProfileEvents::ReadFromNexusFSSeeks);
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
    resetPrefetch();
    offset = offset_;

    return offset;
}

IAsynchronousReader::Result ReadBufferFromNexusFS::readInto(char * data, size_t size, size_t read_offset, size_t ignore_bytes)
{
    bool result = false;
    offset = read_offset;
    set(data, size);

    auto original_status = read_to_internal_buffer;
    read_to_internal_buffer = true;

    if (ignore_bytes)
    {
        ignore(ignore_bytes);
        result = hasPendingData();
        ignore_bytes = 0;
    }
    if (!result)
        result = next();

    read_to_internal_buffer = original_status;

    if (result)
    {
        assert(available());
        return { working_buffer.size(), BufferBase::offset(), nullptr };
    }

    return {0, 0, nullptr};
}

bool ReadBufferFromNexusFS::hasPendingDataToRead()
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

    return true;
}

void ReadBufferFromNexusFS::prefetch(Priority)
{
    if (!nexus_fs.supportPrefetch())
        return;

    chassert(!read_to_internal_buffer);

    if (prefetch_future.valid())
        return;

    if (!hasPendingDataToRead())
        return;

    size_t max_size_to_read = read_until_position ? read_until_position - offset : buf_size;

    prefetch_future = nexus_fs.prefetchToBuffer(file_name, offset, max_size_to_read, source_read_buffer);

    ProfileEvents::increment(ProfileEvents::ReadFromNexusFSPrefetchRequests);
}

size_t ReadBufferFromNexusFS::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback)
{
    if (n == 0)
        return 0;

    size_t bytes_read = nexus_fs.read(file_name, range_begin, n, source_read_buffer, to);

    if (bytes_read && progress_callback)
        progress_callback(bytes_read);
    return bytes_read;
}

void ReadBufferFromNexusFS::setReadUntilPosition(size_t position)
{
    if (position != static_cast<size_t>(read_until_position))
    {
        offset = getPosition();
        resetWorkingBuffer();
        resetPrefetch();
        read_until_position = position;
    }
}

void ReadBufferFromNexusFS::setReadUntilEnd()
{
    if (read_until_position)
    {
        offset = getPosition();
        resetWorkingBuffer();
        read_until_position = 0;
    }
}

void ReadBufferFromNexusFS::resetPrefetch()
{
    if (!prefetch_future.valid())
        return;

    auto bwh = prefetch_future.get();
    prefetch_future = {};

    ProfileEvents::increment(ProfileEvents::ReadFromNexusFSPrefetchedBytes, bwh.getSize());
    ProfileEvents::increment(ProfileEvents::ReadFromNexusFSUnusedPrefetches);
}

}
