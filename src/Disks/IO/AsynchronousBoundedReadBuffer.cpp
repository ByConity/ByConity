#include "AsynchronousBoundedReadBuffer.h"

#include <Common/Stopwatch.h>
#include <common/logger_useful.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Interpreters/Context.h>


namespace CurrentMetrics
{
    extern const Metric AsynchronousReadWait;
}

namespace ProfileEvents
{
    extern const Event RemoteFSAsynchronousReadWaitMicroseconds;
    extern const Event RemoteFSSynchronousReadWaitMicroseconds;
    extern const Event RemoteFSSeeks;
    extern const Event RemoteFSPrefetchRequests;
    extern const Event RemoteFSCancelledPrefetches;
    extern const Event RemoteFSUnusedPrefetches;
    extern const Event RemoteFSPrefetchedReads;
    extern const Event RemoteFSUnprefetchedReads;
    extern const Event RemoteFSPrefetchedBytes;
    extern const Event RemoteFSUnprefetchedBytes;
    extern const Event RemoteFSLazySeeks;
    extern const Event RemoteFSSeeksWithReset;
    extern const Event RemoteFSSeeksOverUntilPosition;
    extern const Event RemoteFSBuffers;

    extern const Event RemoteFSPrefetchTaskWait;
    extern const Event RemoteFSPrefetchTaskNotWait;

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

AsynchronousBoundedReadBuffer::AsynchronousBoundedReadBuffer(
    ImplPtr impl_,
    IAsynchronousReader & reader_,
    const ReadSettings & settings_,
    AsyncReadCountersPtr async_read_counters_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , impl(std::move(impl_))
    , read_settings(settings_)
    , reader(reader_)
    , prefetch_buffer(settings_.remote_fs_buffer_size)
    , log(getLogger("AsynchronousBoundedReadBuffer"))
    , async_read_counters(async_read_counters_)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSBuffers);
}

bool AsynchronousBoundedReadBuffer::hasPendingDataToRead()
{
    if (read_until_position)
    {
        if (file_offset_of_buffer_end == *read_until_position) /// Everything is already read.
            return false;

        if (file_offset_of_buffer_end > *read_until_position)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Read beyond last offset ({} > {})",
                file_offset_of_buffer_end, *read_until_position);
        }
    }

    return true;
}

std::future<IAsynchronousReader::Result>
AsynchronousBoundedReadBuffer::asyncReadInto(char * data, size_t size, Priority priority)
{
    IAsynchronousReader::Request request;
    request.descriptor = std::make_shared<RemoteFSFileDescriptor>(*impl, async_read_counters);
    request.buf = data;
    request.size = size;
    request.offset = file_offset_of_buffer_end;
    request.priority = priority;
    request.ignore = bytes_to_ignore;
    return reader.submit(request);
}

void AsynchronousBoundedReadBuffer::prefetch(Priority priority)
{
    if (prefetch_future.valid())
        return;

    if (!hasPendingDataToRead())
        return;

    // Current working buffer may not have been consumed up, but don't worry,
    // prefetch is execute based on end offset of previous buffer, and stored in prefetch buffer.
    // Only current buffer is consumed up, prefetch buffer will swap with memory.
    prefetch_future = asyncReadInto(prefetch_buffer.data(), prefetch_buffer.size(), priority);
    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchRequests);
}

void AsynchronousBoundedReadBuffer::setReadUntilPosition(size_t position)
{
    if (!read_until_position || position != *read_until_position)
    {
        if (position < file_offset_of_buffer_end)
        {
            /// file has been read beyond new read until position already
            if (available() >= file_offset_of_buffer_end - position)
            {
                /// new read until position is after the current position in the working buffer
                file_offset_of_buffer_end = position;
                working_buffer.resize(working_buffer.size() - (file_offset_of_buffer_end - position));
                pos = std::min(pos, working_buffer.end());
            }
            else
            {
                /// new read until position is before working buffer begin
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to set read until position {} before already read data position {})",
                    position,
                    getPosition());
            }
        }

        read_until_position = position;

        /// We must wait on future and reset the prefetch here, because otherwise there might be
        /// a race between reading the data in the threadpool and impl->setReadUntilPosition()
        /// which reinitializes internal remote read buffer (because if we have a new read range
        /// then we need a new range request) and in case of reading from cache we need to request
        /// and hold more file segment ranges from cache.
        resetPrefetch();

        impl->setReadUntilPosition(*read_until_position);
    }
}

bool AsynchronousBoundedReadBuffer::nextImpl()
{
    if (!hasPendingDataToRead())
        return false;

    chassert(file_offset_of_buffer_end <= impl->getFileSize());

    size_t size, offset;
    // If here, means existing data is consumed up
    if (prefetch_future.valid())
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::RemoteFSAsynchronousReadWaitMicroseconds);
        CurrentMetrics::Increment metric_increment{CurrentMetrics::AsynchronousReadWait};

        if (prefetch_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
            ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchTaskNotWait);
        else
            ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchTaskWait);

        auto result = prefetch_future.get();
        size = result.size;
        offset = result.offset;

        prefetch_future = {};
        prefetch_buffer.swap(memory);

        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, size);
    }
    else
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::RemoteFSSynchronousReadWaitMicroseconds);

        std::tie(size, offset) = impl->readInto(memory.data(), memory.size(), file_offset_of_buffer_end, bytes_to_ignore);

        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedReads);
        ProfileEvents::increment(ProfileEvents::RemoteFSUnprefetchedBytes, size);
    }

    bytes_to_ignore = 0;

    chassert(size >= offset);

    size_t bytes_read = size - offset;
    if (bytes_read)
    {
        /// Adjust the working buffer so that it ignores `offset` bytes.
        internal_buffer = Buffer(memory.data(), memory.data() + memory.size());
        working_buffer = Buffer(memory.data() + offset, memory.data() + size);
        pos = working_buffer.begin();
    }

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();

    return bytes_read;
}


off_t AsynchronousBoundedReadBuffer::seek(off_t offset, int whence)
{
    ProfileEvents::increment(ProfileEvents::RemoteFSSeeks);

    size_t new_pos;
    if (whence == SEEK_SET)
    {
        assert(offset >= 0);
        new_pos = offset;
    }
    else if (whence == SEEK_CUR)
    {
        new_pos = static_cast<size_t>(getPosition()) + offset;
    }
    else
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Expected SEEK_SET or SEEK_CUR as whence");
    }

    /// Position is unchanged.
    if (new_pos == static_cast<size_t>(getPosition()))
        return new_pos;

    while (true)
    {
        /// The first condition implies bytes_to_ignore = 0.
        if (!working_buffer.empty() && file_offset_of_buffer_end - working_buffer.size() <= new_pos &&
            new_pos <= file_offset_of_buffer_end)
        {
            /// Position is still inside the buffer.
            /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.

            pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return new_pos;
        }
        else if (prefetch_future.valid())
        {
            /// Read from prefetch buffer and recheck if the new position is valid inside.
            if (nextImpl())
            {
                /// Prefetch is not actually read because of seek.
                ProfileEvents::increment(ProfileEvents::RemoteFSCancelledPrefetches);
                continue;
            }
        }

        break;
    }

    assert(!prefetch_future.valid());

    /// First reset the buffer so the next read will fetch new data to the buffer.
    resetWorkingBuffer();
    bytes_to_ignore = 0;

    if (read_until_position && new_pos > *read_until_position)
    {
        if (!impl->isSeekCheap())
            ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
        ProfileEvents::increment(ProfileEvents::RemoteFSSeeksOverUntilPosition);
        file_offset_of_buffer_end = new_pos = *read_until_position; /// read_until_position is a non-included boundary.
        impl->seek(file_offset_of_buffer_end, SEEK_SET);
        return new_pos;
    }

    /**
    * Lazy ignore. Save number of bytes to ignore and ignore it either for prefetch buffer or current buffer.
    * Note: we read in range [file_offset_of_buffer_end, read_until_position).
    */
    if (impl && !impl->isSeekCheap() && file_offset_of_buffer_end && read_until_position && new_pos < *read_until_position
        && new_pos > file_offset_of_buffer_end && new_pos < file_offset_of_buffer_end + read_settings.remote_read_min_bytes_for_seek)
    {
        ProfileEvents::increment(ProfileEvents::RemoteFSLazySeeks);
        bytes_to_ignore = new_pos - file_offset_of_buffer_end;
    }
    else
    {
        if (!impl->isSeekCheap())
            ProfileEvents::increment(ProfileEvents::RemoteFSSeeksWithReset);
        file_offset_of_buffer_end = new_pos;
        impl->seek(file_offset_of_buffer_end, SEEK_SET);
    }

    return new_pos;
}


void AsynchronousBoundedReadBuffer::finalize()
{
    resetPrefetch();
}

AsynchronousBoundedReadBuffer::~AsynchronousBoundedReadBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void AsynchronousBoundedReadBuffer::resetPrefetch()
{
    if (!prefetch_future.valid())
        return;

    auto [size, offset, _] = prefetch_future.get();
    prefetch_future = {};

    ProfileEvents::increment(ProfileEvents::RemoteFSPrefetchedBytes, size);
    ProfileEvents::increment(ProfileEvents::RemoteFSUnusedPrefetches);
}
}
