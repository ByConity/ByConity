#include "IOUringReader.h"
#include <mutex>
#include <liburing.h>

#if USE_LIBURING

#include "Common/Priority.h"
#include <common/errnoToString.h>
#include <Common/assert_cast.h>
#include <Common/MemorySanitizer.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>
#include <future>
#include <memory>
#include <stdio.h>

namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;

    extern const Event IOUringSQEsSubmitted;
    extern const Event IOUringSQEsResubmits;
    extern const Event IOUringCQEsCompleted;
    extern const Event IOUringCQEsFailed;
    extern const Event IOUringReaderIgnoredBytes;
}

namespace CurrentMetrics
{
    extern const Metric IOUringPendingEvents;
    extern const Metric IOUringInFlightEvents;
    extern const Metric Read;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int IO_URING_INIT_FAILED;
    extern const int IO_URING_SUBMIT_ERROR;
    extern const int IO_URING_COMPLETE_ERROR;
}

IOUringReader::IOUringReader(uint32_t entries_, bool enable_sqpoll_)
    : enable_sqpoll(enable_sqpoll_), enable_iopoll(false), log(getLogger("IOUringReader"))
{
    struct io_uring_probe * probe = io_uring_get_probe();
    if (!probe)
    {
        is_supported = false;
        return;
    }

    is_supported = io_uring_opcode_supported(probe, IORING_OP_READ);
    io_uring_free_probe(probe);

    if (!is_supported)
        return;

    __u32 flags = 0;
    if (enable_sqpoll)
        flags |= IORING_SETUP_SQPOLL | IORING_FEAT_SQPOLL_NONFIXED;
    if (enable_iopoll)
        flags |= IORING_SETUP_IOPOLL;

    struct io_uring_params params =
    {
        .sq_entries = 0, // filled by the kernel, initializing to silence warning
        .cq_entries = 0, // filled by the kernel, initializing to silence warning
        .flags = flags,
        .sq_thread_cpu = 0, // Unused (IORING_SETUP_SQ_AFF isn't set). Silences warning
        .sq_thread_idle = (enable_sqpoll ? 2000u : 0u), // Unused (IORING_SETUP_SQPOL isn't set). Silences warning
        .features = 0, // filled by the kernel, initializing to silence warning
        .wq_fd = 0, // Unused (IORING_SETUP_ATTACH_WQ isn't set). Silences warning.
        .resv = {0, 0, 0}, // "The resv array must be initialized to zero."
        .sq_off = {}, // filled by the kernel, initializing to silence warning
        .cq_off = {}, // filled by the kernel, initializing to silence warning
    };

    int ret = io_uring_queue_init_params(entries_, &ring, &params);
    if (ret < 0)
        throw Exception(ErrorCodes::IO_URING_INIT_FAILED, "Failed initializing io_uring ret='{}'", ret);

    sq_entries = params.sq_entries;
    ring_completion_monitor = std::make_unique<ThreadFromGlobalPool>([this] { monitorRing(); });
}

std::future<IAsynchronousReader::Result> IOUringReader::submit(Request internel_request)
{
    assert(internel_request.buf);
    assert(internel_request.size);

    auto *request = new IOUringRequest
    {
        .request = internel_request,
        .resubmitting = false,
        .bytes_read = 0
    };
    if (!request)
    {
        LOG_ERROR(log, "Failed constructing IOUringRequest object");
        std::promise<IAsynchronousReader::Result> promise;
        promise.set_exception(std::make_exception_ptr(
            Exception(ErrorCodes::IO_URING_SUBMIT_ERROR, "Failed constructing IOUringRequest object")));
        return promise.get_future();
    }
    auto future = request->promise.get_future();

    int ret = submitOrPending(request);

    if (ret < 0)
    {
        failAndfinalizeRequest(request, Exception(ErrorCodes::IO_URING_SUBMIT_ERROR,
            "Failed submitting SQE: {}", errnoToString(ErrorCodes::IO_URING_SUBMIT_ERROR, -ret)));
    }

    return future;
}

int IOUringReader::submitOrPending(IOUringRequest *request)
{
    // take lock here because we are going to submitToRing and modify pending deque
    std::unique_lock lock{submit_lock};

    int ret = submitToRing(lock, request);

    if (ret >= 0)
    {
        if (ret == 0)
        {
            CurrentMetrics::add(CurrentMetrics::IOUringPendingEvents);
            pending_requests.push_back(request);
        }
        else
        {
            // check pending deque to see if there are any requests to submit
            while (!pending_requests.empty())
            {
                IOUringRequest *front_pending_request=pending_requests.front();
                int pending_request_ret = submitToRing(lock, front_pending_request);
                if (pending_request_ret == 0)
                {
                    // SQ is full
                    break;
                }
                else
                {
                    pending_requests.pop_front();
                    CurrentMetrics::sub(CurrentMetrics::IOUringPendingEvents);
                    if (pending_request_ret < 0)
                    {
                        // request failed, finalize it
                        failAndfinalizeRequest(front_pending_request, Exception(ErrorCodes::IO_URING_SUBMIT_ERROR,
                        "Failed submitting SQE: {}", errnoToString(ErrorCodes::IO_URING_SUBMIT_ERROR, -pending_request_ret)));
                    }
                }
            }

        }
    }

    return ret;
}

int IOUringReader::submitToRing(const std::unique_lock<std::mutex> &/*lock*/, IOUringRequest *request)
{
    struct io_uring_sqe * sqe = io_uring_get_sqe(&ring);
    if (!sqe)
    {
        // fail to get sqe, SQ is full
        return 0;
    }

    auto req = request->request;
    int fd = assert_cast<const LocalFileDescriptor &>(*req.descriptor).fd;
    bool is_resubmitting = request->resubmitting;
    int ret = 0;

    io_uring_sqe_set_data(sqe, static_cast<void*>(request));
    io_uring_prep_read(sqe, fd, req.buf, static_cast<unsigned>(req.size - request->bytes_read), req.offset + request->bytes_read);

    do
    {
        ret = io_uring_submit(&ring);
    } while (ret == -EINTR || ret == -EAGAIN);

    // if ret>0, DO NOT access this request any more, since it could have been completeed and freed by minitor thread

    if (ret == 0)
    {
        if (enable_sqpoll)
        {
            // when enable_sqpoll, ret could be 0, since kernel sq_thread could have fetched this sqe before we call io_uring_submit
            // TODO: need more tests here
            ret = 1;
        }
        else
        {
            LOG_ERROR(log, "io_uring_submit returns 0, it shouldn't happen");
            ret = -EIO;
        }
    }

    if (ret > 0 && !is_resubmitting)
    {
        ProfileEvents::increment(ProfileEvents::IOUringSQEsSubmitted);
        CurrentMetrics::add(CurrentMetrics::IOUringInFlightEvents);
        CurrentMetrics::add(CurrentMetrics::Read);
    }

    return ret;
}

void IOUringReader::failAndfinalizeRequest(IOUringRequest *request, const Exception & ex)
{
    ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
    request->promise.set_exception(std::make_exception_ptr(ex));

    finalizeRequest(request);
}

void IOUringReader::finalizeRequest(IOUringRequest *request)
{
    delete request;
}

void IOUringReader::monitorRing()
{
    setThreadName("IOUringMonitor");
    LOG_TRACE(log, "monitor thread starts to run");

    while (!cancelled.load(std::memory_order_relaxed))
    {
        // we can't use wait_cqe_* variants with timeouts as they can
        // submit timeout events in older kernels that do not support IORING_FEAT_EXT_ARG
        // and it is not safe to mix submission and consumption event threads.
        struct io_uring_cqe * cqe = nullptr;
        int ret = io_uring_wait_cqe(&ring, &cqe);

        if (ret == -EAGAIN || ret == -EINTR)
        {
            LOG_TRACE(log, "Restarting waiting for CQEs due to: {}", errnoToString(ErrorCodes::IO_URING_COMPLETE_ERROR, -ret));
            continue;
        }

        if (ret < 0)
        {
            LOG_ERROR(log, "Failed waiting for io_uring CQEs: {}", errnoToString(ErrorCodes::IO_URING_COMPLETE_ERROR, -ret));
            continue;
        }

        if (!cqe)
        {
            LOG_ERROR(log, "Unexpectedly got a null CQE, continuing");
            continue;
        }

        // user_data zero means a noop event sent from the destructor meant to interrupt this thread
        if (!io_uring_cqe_get_data(cqe))
        {
            LOG_TRACE(log, "Stopping IOUringMonitor thread");

            io_uring_cqe_seen(&ring, cqe);
            break;
        }

        // now we start to process a normal cqe
        assert(cqe && io_uring_cqe_get_data(cqe));
        IOUringRequest *request = static_cast<IOUringRequest*>(io_uring_cqe_get_data(cqe));

        if (cqe->res == -EAGAIN || cqe->res == -EINTR)
        {
            ProfileEvents::increment(ProfileEvents::IOUringSQEsResubmits);
            request->resubmitting = true;
            io_uring_cqe_seen(&ring, cqe);
            
            if (!cancelled.load(std::memory_order_relaxed))
            {
                int resubmit_ret = submitOrPending(request);
                if (resubmit_ret < 0)
                {
                    failAndfinalizeRequest(request, Exception(ErrorCodes::IO_URING_SUBMIT_ERROR,
                        "Failed re-submitting SQE: {}", errnoToString(ErrorCodes::IO_URING_COMPLETE_ERROR, -resubmit_ret)));
                }
            }
            else
            {
                failAndfinalizeRequest(request, Exception(ErrorCodes::IO_URING_SUBMIT_ERROR, "IOUringReader is deconstructing"));
            }

            continue;
        }

        if (cqe->res < 0)
        {
            auto req = request->request;
            int fd = assert_cast<const LocalFileDescriptor &>(*req.descriptor).fd;
            failAndfinalizeRequest(request, Exception(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR,
                "Failed reading {} bytes at offset {} to address {} from fd {}: {}",
                req.size, req.offset, static_cast<void*>(req.buf), fd, errnoToString(ErrorCodes::IO_URING_COMPLETE_ERROR, -cqe->res)
            ));

            ProfileEvents::increment(ProfileEvents::IOUringCQEsFailed);
            io_uring_cqe_seen(&ring, cqe);
            continue;
        }

        size_t bytes_read = cqe->res;
        size_t total_bytes_read = request->bytes_read + bytes_read;
        if (bytes_read > 0)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
        }

        if (bytes_read > 0 && total_bytes_read < request->request.size)
        {
            // potential short read, re-submit
            ProfileEvents::increment(ProfileEvents::IOUringSQEsResubmits);
            request->resubmitting = true;
            request->bytes_read += bytes_read;
            io_uring_cqe_seen(&ring, cqe);

            if (!cancelled.load(std::memory_order_relaxed))
            {
                int resubmit_ret = submitOrPending(request);
                if (resubmit_ret < 0)
                {
                    failAndfinalizeRequest(request, Exception(ErrorCodes::IO_URING_SUBMIT_ERROR,
                        "Failed re-submitting SQE for short read: {}", errnoToString(ErrorCodes::IO_URING_COMPLETE_ERROR, -resubmit_ret)));
                }
            }
            else
            {
                failAndfinalizeRequest(request, Exception(ErrorCodes::IO_URING_SUBMIT_ERROR, "IOUringReader is deconstructing"));
            }
            
            continue;
        }

        // successfully completed request
        ProfileEvents::increment(ProfileEvents::IOUringCQEsCompleted);
        ProfileEvents::increment(ProfileEvents::IOUringReaderIgnoredBytes, request->request.ignore);
        request->promise.set_value(Result{ .size = total_bytes_read, .offset = request->request.ignore });
        finalizeRequest(request);

        io_uring_cqe_seen(&ring, cqe);
    }
    LOG_TRACE(log, "monitor thread exists");
}

IOUringReader::~IOUringReader()
{
    cancelled.store(true, std::memory_order_relaxed);

    // interrupt the monitor thread by sending a noop event
    {
        std::unique_lock lock{submit_lock};

        struct io_uring_sqe * sqe = io_uring_get_sqe(&ring);
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ring); //TODO: do we need to handle the cacse where it returns a vaule <=0 ?
    }

    ring_completion_monitor->join();

    // some requests may still be in the ring, finalize them
    struct io_uring_cqe * cqe = nullptr;
    while (0 == io_uring_peek_cqe(&ring, &cqe))
    {
        if (!cqe)
        {
            // no cqe left
            break;
        }

        IOUringRequest *request = static_cast<IOUringRequest*>(io_uring_cqe_get_data(cqe));
        io_uring_cqe_seen(&ring, cqe);

        if (request)
        {
            failAndfinalizeRequest(request, Exception(ErrorCodes::IO_URING_COMPLETE_ERROR, "IOUringReader is deconstructing"));
        }
    }

    // there may be still some on-the-fly requests, we don't wait them any more 

    io_uring_queue_exit(&ring);
}

}
#endif
