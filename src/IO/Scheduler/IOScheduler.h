#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <unordered_set>
#include <list>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <fmt/format.h>
#include <common/singleton.h>
#include <IO/Scheduler/ReaderCache.h>
#include <Core/Types.h>
#include <IO/RemoteFSReader.h>
#include <IO/Scheduler/FDs.h>
#include <IO/Scheduler/IOWorkerPool.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::IO::Scheduler {

// Callback for IOScheduler's async interface
class FinishCallback {
public:
    virtual ~FinishCallback() {}

    // After onSuccess or onFailure is invoked, all upper level resource(like buffer)
    // can release safely
    virtual void onSuccess(uint64_t readed) = 0;
    virtual void onFailure(const std::exception_ptr& except) = 0;
};

class NotifyFinishCallback: public FinishCallback {
public:
    virtual void onSuccess(uint64_t readed) override {
        promise_.set_value(readed);
    }

    virtual void onFailure(const std::exception_ptr& except) override {
        try {
            promise_.set_exception(except);
        } catch (const std::future_error& e) {
            if (e.code() != std::future_errc::promise_already_satisfied) {
                abort();
            }
        }
    }

    std::future<uint64_t> future() {
        return promise_.get_future();
    }

private:
    std::promise<uint64_t> promise_;
};

template <typename T>
class RawRequest;

// Represent one user request
template <typename T>
class UserRequest {
public:
    UserRequest(uint64_t offset, uint64_t size, char* buffer,
        std::unique_ptr<FinishCallback> callback):
            offset_(offset), size_(size), buffer_(buffer),
            callback_(std::move(callback)) {}

    String str() const {
        return fmt::format("UserReq: [Offset: {}, Size: {}, Payload: {{{}}}, This: {}]",
            offset_, size_, payload_.str(), static_cast<const void*>(this));
    }

    uint64_t offset_;
    uint64_t size_;
    char* buffer_;

    std::unique_ptr<FinishCallback> callback_;

    T payload_;
};

template <typename T>
class RawRequest {
public:
    RawRequest(FileMeta* file, uint64_t offset, uint64_t size):
        file_(file), offset_(offset), size_(size), target_mem_(nullptr) {}

    String str() const {
        return fmt::format("RawReq: [FD: {}, Offset: {}, Size: {}, Payload: {{{}}}, This: {}]",
            file_->fd_, offset_, size_, payload_.str(), static_cast<const void*>(this));
    }

    FileMeta* file_;
    uint64_t offset_;
    uint64_t size_;

    char* target_mem_;

    // Minimal submit time of UserRequest which depend on this RawRequest
    std::chrono::steady_clock::time_point min_reach_submit_time_;

    T payload_;
};

class IOScheduler {
public:
    virtual ~IOScheduler() {}

    virtual OpenedFile open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) = 0;
    // FileMeta need to remain valid before future released, may throw some exception
    virtual void readAsync(FileMeta* file, uint64_t offset, uint64_t size,
        char* buffer, std::unique_ptr<FinishCallback> callback) = 0;

    std::future<uint64_t> read(FileMeta* file, uint64_t offset, uint64_t size,
            char* buffer) {
        std::unique_ptr<NotifyFinishCallback> callback
            = std::make_unique<NotifyFinishCallback>();

        std::future<uint64_t> future = callback->future();

        readAsync(file, offset, size, buffer, std::move(callback));

        return future;
    }

    virtual String status() const {return "";}
};

struct RequestPos {
    RequestPos(): fd_(-1), offset_(0) {}
    RequestPos(uint64_t fd, uint64_t offset): fd_(fd), offset_(offset) {}

    bool operator==(const RequestPos& rhs) const {
        return fd_ == rhs.fd_ && offset_ == rhs.offset_;
    }

    bool operator<(const RequestPos& rhs) const {
        return fd_ < rhs.fd_ || (fd_ == rhs.fd_ && offset_ < rhs.offset_);
    }

    uint64_t fd_;
    uint64_t offset_;
};

class IOSchedulerSet {
public:
    struct Options {
        enum SchedulerType {
            NOOP,
            DEADLINE,
            FIFO
        };

        enum WorkerPoolType {
            FIXED_PARALLEL,
            DYNAMIC_PARALLEL,
            DISPATCHED,
            NONE
        };

        enum AllocationPolicyType {
            RANDOM,
            HASH,
            // Hybrid allocation policy is combined by hash and random, it will first
            // do a hash of path, and allocate it to a determinted set of io schedulers,
            // then do a random choice from these schedulers
            HYBRID
        };

        static SchedulerType str2SchedulerType(const String& str);
        static WorkerPoolType str2WorkerPoolType(const String& str);
        static AllocationPolicyType str2AllocationPolicy(const String& str);

        SchedulerType scheduler_type_ = NOOP;
        WorkerPoolType worker_pool_type_ = FIXED_PARALLEL;
        AllocationPolicyType allocation_policy_type_ = HASH;

        const void* scheduler_opts_ = nullptr;
        const void* worker_pool_opts_ = nullptr;

        size_t allocation_level0_buckets_ = 1;
        size_t allocation_level1_buckets_ = 1;
        
        ReaderCache::Options reader_cache_opts_ = ReaderCache::Options {.shard_size_ = 1, .max_cache_size_ = 1};
    };

    static IOSchedulerSet& instance();
    static IOScheduler* get(const String& path);

    IOSchedulerSet(): initialized_(false), enabled_(false) {}

    void initialize(const Poco::Util::AbstractConfiguration& cfg, const String& prefix);
    void initialize(const Options& opts);
    void uninitialize();

    bool enabled() const;

    IOScheduler* schedulerForPath(const String& path);

    template<typename SchedulerType>
    std::vector<SchedulerType*> schedulers() const {
        std::vector<SchedulerType*> result;
        for (auto& scheduler : schedulers_) {
            result.push_back(reinterpret_cast<SchedulerType*>(scheduler.get()));
        }
        return result;
    }

    const std::vector<std::unique_ptr<IOScheduler>>& rawSchedulers() const {
        return schedulers_;
    }

    IOWorkerPool* workerPool();

private:
    bool initialized_;
    bool enabled_;
    Options opts_;

    size_t scheduler_count_;

    std::vector<std::unique_ptr<IOScheduler>> schedulers_;
    std::unique_ptr<IOWorkerPool> worker_pool_;
};

template <>
class UserRequest<void> {
public:
    UserRequest(uint64_t offset, uint64_t size, char* buffer,
        std::unique_ptr<FinishCallback> callback):
            offset_(offset), size_(size), buffer_(buffer),
            callback_(std::move(callback)) {}

    String str() const {
        return fmt::format("UserReq: [Offset: {}, Size: {}, This: {}]",
            offset_, size_, static_cast<const void*>(this));
    }

    uint64_t offset_;
    uint64_t size_;
    char* buffer_;

    std::unique_ptr<FinishCallback> callback_;
};

template <>
class RawRequest<void> {
public:
    RawRequest(FileMeta* file, uint64_t offset, uint64_t size):
        file_(file), offset_(offset), size_(size), target_mem_(nullptr) {}

    String str() const {
        return fmt::format("RawReq: [FD: {}, Offset: {}, Size: {}, This: {}]",
            file_->fd_, offset_, size_, static_cast<const void*>(this));
    }

    FileMeta* file_;
    uint64_t offset_;
    uint64_t size_;

    char* target_mem_;

    std::chrono::steady_clock::time_point min_reach_time_;
};

}
