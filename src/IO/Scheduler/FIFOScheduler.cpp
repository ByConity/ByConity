#include <chrono>
#include <mutex>
#include <IO/Scheduler/FIFOScheduler.h>

namespace DB::IO::Scheduler {

OpenedFile FIFOScheduler::open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) {
    return files_.ref(file_name, opts);
}

void FIFOScheduler::readAsync(FileMeta* file, uint64_t offset, uint64_t size,
        char* buffer, std::unique_ptr<FinishCallback> callback) {
    std::lock_guard<std::mutex> lock(mu_);

    user_requests_.emplace_back(offset, size, buffer, std::move(callback));
    auto user_req_iter = std::prev(user_requests_.end());

    raw_requests_.emplace_back(std::make_unique<FIFORawRequest>(file, offset, size));
    raw_requests_.back()->payload_.user_req_iter_ = user_req_iter;
    raw_requests_.back()->payload_.create_time_ = std::chrono::steady_clock::now(); 
}

std::vector<std::unique_ptr<FIFORawRequest>> FIFOScheduler::retrieveRequests(
        size_t max_request_num, size_t max_wait_time_ns) noexcept {
    std::unique_lock<std::mutex> lock(mu_);
    if (!cv_.wait_for(lock, std::chrono::nanoseconds(max_wait_time_ns),
        [this]() {
            return !raw_requests_.empty();
        })
    ) {
        return {};
    }

    std::vector<std::unique_ptr<FIFORawRequest>> results;
    for (size_t i = 0, item_count = std::min(max_request_num, raw_requests_.size());
            i < item_count; ++i) {
        results.push_back(std::move(std::move(raw_requests_.front())));
        raw_requests_.pop_front();
    }

    return results;
}

void FIFOScheduler::finalizeRequest(std::unique_ptr<FIFORawRequest> request,
        std::unique_ptr<char []> buffer, uint64_t readed) {
    FIFOUserRequest& user_req = *(request->payload_.user_req_iter_);
    memcpy(user_req.buffer_, buffer.get(), readed);

    user_req.callback_->onSuccess(readed);

    std::lock_guard<std::mutex> lock(mu_);
    user_requests_.erase(request->payload_.user_req_iter_);
}

void FIFOScheduler::finalizeRequest(std::unique_ptr<FIFORawRequest> request,
        const std::exception_ptr& e) {
    FIFOUserRequest& user_req = *(request->payload_.user_req_iter_);
    user_req.callback_->onFailure(e);

    std::lock_guard<std::mutex> lock(mu_);
    user_requests_.erase(request->payload_.user_req_iter_);
}

}
