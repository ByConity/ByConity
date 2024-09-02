#include "ThreadPoolRemoteFSReader.h"

#include <IO/AsyncReadCounters.h>
#include <IO/SeekableReadBuffer.h>
#include "Common/setThreadName.h"
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <common/getThreadId.h>

#include <future>
#include <memory>


namespace ProfileEvents
{
    extern const Event ThreadPoolReaderTaskMicroseconds;
    extern const Event ThreadPoolReaderReadBytes;
    extern const Event ThreadPoolReaderSubmit;
    extern const Event ThreadPoolReaderScheduleMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric RemoteRead;
    // TODO: add metric monitor for thread pool
    // See https://github.com/ClickHouse/ClickHouse/commit/f38a7aeabe83cff19fba7d727081da75c06912c7
    extern const Metric ThreadPoolRemoteFSReaderThreads;
    extern const Metric ThreadPoolRemoteFSReaderThreadsActive;
}

namespace DB
{

namespace
{
    struct AsyncReadIncrement : boost::noncopyable
    {
        explicit AsyncReadIncrement(std::shared_ptr<AsyncReadCounters> counters_)
            : counters(counters_)
        {
            std::lock_guard lock(counters->mutex);
            if (++counters->current_parallel_read_tasks > counters->max_parallel_read_tasks)
                counters->max_parallel_read_tasks = counters->current_parallel_read_tasks;
        }

        ~AsyncReadIncrement()
        {
            std::lock_guard lock(counters->mutex);
            --counters->current_parallel_read_tasks;
        }

        std::shared_ptr<AsyncReadCounters> counters;
    };
}

IAsynchronousReader::Result RemoteFSFileDescriptor::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    return read_buffer.readInto(data, size, offset, ignore);
}


ThreadPoolRemoteFSReader::ThreadPoolRemoteFSReader(size_t pool_size, size_t queue_size_)
    : pool(std::make_unique<ThreadPool>(pool_size, pool_size, queue_size_))
{
}


std::future<IAsynchronousReader::Result> ThreadPoolRemoteFSReader::submit(Request request)
{
    ProfileEvents::increment(ProfileEvents::ThreadPoolReaderSubmit);
    auto watch = std::make_unique<Stopwatch>(CLOCK_REALTIME);
    return scheduleFromThreadPool<Result>(
        [request, submit_watch = std::move(watch)]() -> Result {
            setThreadName("RemoteReadThr");
            submit_watch->stop();
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderScheduleMicroseconds, submit_watch->elapsedMicroseconds());
            CurrentMetrics::Increment metric_increment{CurrentMetrics::RemoteRead};

            auto * remote_fs_fd = assert_cast<RemoteFSFileDescriptor *>(request.descriptor.get());
            auto async_read_counters = remote_fs_fd->getReadCounters();
            std::optional<AsyncReadIncrement> increment
                = async_read_counters ? std::optional<AsyncReadIncrement>(async_read_counters) : std::nullopt;

            auto exec_watch = std::make_unique<Stopwatch>(CLOCK_REALTIME);
            Result result = remote_fs_fd->readInto(request.buf, request.size, request.offset, request.ignore);
            exec_watch->stop();

            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderTaskMicroseconds, exec_watch->elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderReadBytes, result.size);

            return Result{.size = result.size, .offset = result.offset, .execution_watch = std::move(exec_watch)};
        },
        *pool,
        "VFSRead",
        request.priority);
}

void ThreadPoolRemoteFSReader::wait()
{
    pool->wait();
}

}
