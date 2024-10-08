#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <memory>
#include <map>
#include <mutex>
#include <set>
#include <list>
#include <future>
#include <sstream>
#include <vector>
#include <IO/Scheduler/Common.h>
#include <IO/Scheduler/IOScheduler.h>
#include <IO/RemoteFSReader.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

class DeadlineSchedulerTest;

namespace DB::IO::Scheduler {

class DeadlineEntry;

class DeadlineRawPayload {
public:
    String str() const;

    std::vector<std::list<DeadlineEntry>::iterator> user_dependency_;
};

using DeadlineRawRequest = RawRequest<DeadlineRawPayload>;

class DeadlineUserPayload {
public:
    DeadlineUserPayload(): pending_count_(0), max_readed_offset_(0), exception_(nullptr) {}

    String str() const;

    // TODO: Seems not required to be atomic, remove it if possible
    // the number of uncompleted raw requests in this user request
    std::atomic<uint32_t> pending_count_;
    std::atomic<uint64_t> max_readed_offset_;

    std::exception_ptr exception_;

    // offset -> deadlineRawRequest
    std::map<uint64_t, DeadlineRawRequest*> raw_dependency_;
};

using DeadlineUserRequest = UserRequest<DeadlineUserPayload>;

class DeadlineEntry {
public:
    DeadlineEntry(std::chrono::time_point<std::chrono::steady_clock> submit_time,
        uint64_t offset, uint64_t size, char* buffer,
        std::unique_ptr<FinishCallback> callback):
            submit_time_(submit_time), request_(offset, size, buffer, std::move(callback)) {}

    String str() const {
        return fmt::format("SubmitTime: {}, UR: {}", submit_time_.time_since_epoch().count(),
            request_.str());
    }

    // By default now, the timeout time of all requests is the same.
    // For convenience, record submit time instead of deadline time at present.
    std::chrono::time_point<std::chrono::steady_clock> submit_time_;
    DeadlineUserRequest request_;
};

class DeadlineScheduler: public IOScheduler {
public:
    struct Options {
        static Options parseFromConfig(const Poco::Util::AbstractConfiguration& cfg,
            const String& prefix);

        LogLevel log_level_ = LogLevel::NONE;
        uint64_t min_delay_ns_ = 100 * 1000;
        uint64_t max_delay_ns_ = 1 * 1000 * 1000;
        uint64_t max_request_size_ = 2 * 1024 * 1024;
        uint64_t request_deadline_ns_ = 50 * 1000 * 1000;
        uint64_t aligned_boundary_ = 128 * 1024;
    };

    using RawRequestType = DeadlineRawRequest;

    DeadlineScheduler(const Options& opts);
    virtual ~DeadlineScheduler() override = default;

    virtual OpenedFile open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) override;
    virtual void readAsync(FileMeta* file, uint64_t offset, uint64_t size,
        char* buffer, std::unique_ptr<FinishCallback> callback) override;

    virtual String status() const override;

    // Invoked by IOWorkerPool
    std::vector<std::unique_ptr<DeadlineRawRequest>> retrieveRequests(
        size_t max_request_num, size_t max_wait_time_ns) noexcept;
    void finalizeRequest(std::unique_ptr<DeadlineRawRequest> request,
        std::unique_ptr<char []> buffer, uint64_t readed);
    void finalizeRequest(std::unique_ptr<DeadlineRawRequest> request,
        const std::exception_ptr& e);

private:
    struct Status {
        Status(): pending_user_request_(0), pending_raw_request_(0),
            submitted_user_request_(0), finished_user_request_(0),
            retrieved_raw_request_(0), finalized_raw_request_(0) {}

        // Current status
        std::atomic<uint64_t> pending_user_request_;
        std::atomic<uint64_t> pending_raw_request_;

        // Statistics
        std::atomic<uint64_t> submitted_user_request_;
        std::atomic<uint64_t> finished_user_request_;
        std::atomic<uint64_t> retrieved_raw_request_;
        std::atomic<uint64_t> finalized_raw_request_;
    };

    friend class ::DeadlineSchedulerTest;

    std::unique_ptr<DeadlineRawRequest> clearRequestDependency(DeadlineRawRequest* raw_request,
        std::unique_lock<std::mutex>&);
    std::unique_ptr<DeadlineRawRequest> clearRequestDependency(
        std::map<RequestPos, std::unique_ptr<DeadlineRawRequest>>::iterator iter,
        std::unique_lock<std::mutex>&);

    String schedulerStatus() const;

    LoggerPtr logger_;

    const Options opts_;

    Status status_;

    FDMap files_;

    std::mutex mu_;
    std::condition_variable cv_;

    RequestPos last_retrieve_pos_;

    std::list<DeadlineEntry> ddl_queue_;
    std::map<RequestPos, std::unique_ptr<DeadlineRawRequest>> ordered_requests_;
};

}
