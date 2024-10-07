#pragma once

#include <Common/Logger.h>
#include <mutex>
#include <Common/config.h>

#if USE_LIBURING

#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <IO/AsynchronousReader.h>
#include <deque>
#include <unordered_map>
#include <liburing.h>

namespace Poco { class Logger; }

namespace DB
{

/** Perform asynchronous reads using io_uring.
  *
  * The class sets up a single io_uring that clients submit read requests to.
  * A monitor thread continuously polls the completion queue and completes the matching promise.
  */
class IOUringReader final : public IAsynchronousReader
{
public:
    explicit IOUringReader(uint32_t entries_, bool enable_sqpoll_ = false);

    bool isSupported() const { return is_supported; }
    void wait() override {}

    std::future<Result> submit(Request internel_request) override;

    ~IOUringReader() override;

private:
    struct IOUringRequest
    {
        std::promise<IAsynchronousReader::Result> promise;
        Request request;
        bool resubmitting; // resubmits can happen due to short reads or when io_uring returns -EAGAIN
        size_t bytes_read; // keep track of bytes already read in case short reads happen
    };

    int submitOrPending(IOUringRequest *request);

    // submit a request to liburing
    // return: >0 on success; 0 when SQ is full; <0 on error
    int submitToRing(const std::unique_lock<std::mutex> &/*lock*/, IOUringRequest *request);

    static void failAndfinalizeRequest(IOUringRequest* request, const Exception & ex);
    static void finalizeRequest(IOUringRequest* request);

    void monitorRing();


    // SQPoll creats a kernel thread to perform submission queue polling
    // it seems that this is a limitation of the files opened for reading
    // disabled for now
    const bool enable_sqpoll;

    // perform busy-waiting for an I/O completion instead of waiting for an interrupt
    // currently, this feature is usable only on a file descriptor opened using the O_DIRECT flag
    // disabled for now
    const bool enable_iopoll;
    
    bool is_supported;

    struct io_uring ring;
    uint32_t sq_entries;

    // monitor thread
    std::atomic<bool> cancelled{false};
    std::unique_ptr<ThreadFromGlobalPool> ring_completion_monitor;

    // submitToRing method and pending deque is protected by this lock
    std::mutex submit_lock;
    std::deque<IOUringRequest*> pending_requests;

    LoggerPtr log;
};

}
#endif
