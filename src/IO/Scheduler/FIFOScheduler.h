#pragma once

#include <chrono>
#include <list>
#include <IO/Scheduler/IOScheduler.h>
#include <IO/RemoteFSReader.h>
#include <IO/Scheduler/FDs.h>

namespace DB::IO::Scheduler {

using FIFOUserRequest = UserRequest<void>;

class FIFORawPayload {
public:
    String str() const {
        return fmt::format("Reach: {}-{}@{}", user_req_iter_->offset_,
            user_req_iter_->size_, static_cast<void*>(&(*user_req_iter_)));
    }

    std::chrono::time_point<std::chrono::steady_clock> create_time_;
    std::list<FIFOUserRequest>::iterator user_req_iter_;
};

using FIFORawRequest = RawRequest<FIFORawPayload>;

class FIFOScheduler: public IOScheduler {
public:
    using RawRequestType = FIFORawRequest;

    virtual OpenedFile open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) override;
    virtual void readAsync(FileMeta* file, uint64_t offset, uint64_t size,
        char* buffer, std::unique_ptr<FinishCallback> callback) override;

    std::vector<std::unique_ptr<FIFORawRequest>> retrieveRequests(
        size_t max_request_num, size_t max_wait_time_ns) noexcept;
    void finalizeRequest(std::unique_ptr<FIFORawRequest> request,
        std::unique_ptr<char []> buffer, uint64_t readed);
    void finalizeRequest(std::unique_ptr<FIFORawRequest> request,
        const std::exception_ptr& e);

private:
    FDMap files_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::list<FIFOUserRequest> user_requests_;
    std::list<std::unique_ptr<FIFORawRequest>> raw_requests_;
};

}
