#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ratio>
#include <cassert>
#include <common/scope_guard.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <IO/Scheduler/Common.h>
#include <IO/Scheduler/IOScheduler.h>
#include <IO/Scheduler/DeadlineScheduler.h>
#include <Poco/JSON/Object.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace CurrentMetrics {
    extern const Metric IOSchUserRequests;
    extern const Metric IOSchRawRequests;
}

namespace ProfileEvents {
    extern const Event IOSchedulerOpenFileMicro;
    extern const Event IOSchedulerScheduleMicro;
    extern const Event IOSchedulerSubmittedUserRequests;
    extern const Event IOSchedulerExecutedRawReuqests;
}

namespace DB {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace IO::Scheduler {

String DeadlineRawPayload::str() const {
    std::stringstream ss;
    for (const auto& entry : user_dependency_) {
        ss << entry->request_.offset_ << "-" << entry->request_.size_ << "@" << &(entry->request_) << ",";
    }
    return fmt::format("Reach: ({})", ss.str());
}

String DeadlineUserPayload::str() const {
    std::stringstream ss;
    for (const auto& entry : raw_dependency_) {
        ss << entry.first << "-" << entry.second->size_ << "@" << entry.second << ",";
    }
    return fmt::format("Pending: {}, MaxOff: {}, Dependency: ({})", pending_count_,
        max_readed_offset_, ss.str());
}

DeadlineScheduler::Options DeadlineScheduler::Options::parseFromConfig(
        const Poco::Util::AbstractConfiguration& cfg, const String& prefix) {
    return Options {
        .log_level_ = str2LogLevel(cfg.getString(prefix + ".log_level", "none")),
        .min_delay_ns_ = cfg.getUInt64(prefix + ".min_delay_ns", 100 * 1000),
        .max_delay_ns_ = cfg.getUInt64(prefix + ".max_delay_ns", 1000 * 1000),
        .max_request_size_ = cfg.getUInt64(prefix + ".max_request_size", 2 * 1024 * 1024),
        .request_deadline_ns_ = cfg.getUInt64(prefix + ".request_deadline_ns", 50 * 1000 * 1000),
        .aligned_boundary_ = cfg.getUInt64(prefix + ".aligned_boundary", 128 * 1024),
    };
}

DeadlineScheduler::DeadlineScheduler(const Options& opts):
    logger_(getLogger("DeadlineScheduler")), opts_(opts),
        last_retrieve_pos_(0, 0) {
    if (opts_.max_request_size_ % opts_.aligned_boundary_ != 0) {
        throw Exception(fmt::format("Max request size {} is not aligned to request boundary {}",
            opts_.max_request_size_, opts_.aligned_boundary_), ErrorCodes::BAD_ARGUMENTS);
    }
}

OpenedFile DeadlineScheduler::open(const String& file_name,
        const std::shared_ptr<RemoteFSReaderOpts>& opts) {
    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::IOSchedulerOpenFileMicro,
        watch.elapsedMicroseconds());});

    return files_.ref(file_name, opts);
}

/**
 * @brief Based on given file and <offset, size>, merge with exsiting requests if overlapped,
 *        then split by max_request_size_ and replace original requests.
 */
void DeadlineScheduler::readAsync(FileMeta* file, uint64_t offset, uint64_t size, char* buffer,
        std::unique_ptr<FinishCallback> callback) {
    if (size == 0) {
        callback->onSuccess(0);
        return;
    }

    CurrentMetrics::add(CurrentMetrics::IOSchUserRequests, 1);
    ProfileEvents::increment(ProfileEvents::IOSchedulerSubmittedUserRequests);
    Stopwatch watch;
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::IOSchedulerScheduleMicro,
        watch.elapsedMicroseconds());});

    std::lock_guard<std::mutex> lock(mu_);

    if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
        LOG_INFO(logger_, fmt::format("[Scheduler: {}] Read fd: {}, offset: {}, size: {}",
            static_cast<void*>(this), file->fd_, offset, size));
    }

    // Assign deadline for new request and insert it into deadline queue
    ddl_queue_.emplace_back(std::chrono::steady_clock::now(), offset, size, buffer,
        std::move(callback));
    auto req_ddl_iter = std::prev(ddl_queue_.end());
    req_ddl_iter->request_.payload_.max_readed_offset_ = offset;

    // Align request to certain boundary
    uint64_t aligned_offset = offset - offset % opts_.aligned_boundary_;
    uint64_t aligned_size = (offset + size + opts_.aligned_boundary_ - 1) / opts_.aligned_boundary_ * opts_.aligned_boundary_ - aligned_offset;

    // Search for raw requests which is overlapping with new request;
    // If file which is the same as request exsits, returns the minimum offset >= current lower bound,
    // else return the other file whose fd > current file.
    auto lower_bound_iter = ordered_requests_.lower_bound(RequestPos(file->fd_,
        aligned_offset));
    // If file which is the same as request exsits, returns the maximum offset <= current upper bound,
    // else return the other file whose fd > current file.
    auto upper_bound_iter = ordered_requests_.upper_bound(RequestPos(file->fd_,
        aligned_offset + aligned_size));
    // backward search
    for (; lower_bound_iter != ordered_requests_.begin(); ) {
        auto prev_iter = std::prev(lower_bound_iter);
        if (prev_iter->first.fd_ != file->fd_
                || prev_iter->first.offset_ + prev_iter->second->size_ < aligned_offset) {
            break;
        }
        lower_bound_iter = prev_iter;
    }

    // Calculate final merged range
    uint64_t range_begin = aligned_offset;
    uint64_t range_end = aligned_offset + aligned_size;
    if (lower_bound_iter != upper_bound_iter) {
        range_begin = std::min(range_begin, lower_bound_iter->first.offset_);
        // if lower_bound != upper_bound, prev(upper_bound) will be the same fd as lower_bound
        auto back_iter = std::prev(upper_bound_iter);
        range_end = std::max(range_end, back_iter->first.offset_ + back_iter->second->size_);
    }

    // Remove all old requests from sorted request list
    std::vector<std::unique_ptr<DeadlineRawRequest>> requests_to_merge;
    for (auto iter = lower_bound_iter; iter != upper_bound_iter;) {
        requests_to_merge.emplace_back(std::move(iter->second));
        iter = ordered_requests_.erase(iter);
    }

    assert((range_begin % opts_.aligned_boundary_ == 0)
        && (range_end % opts_.aligned_boundary_ == 0)
        && "Range begin/end isn't aligned to boundary");

    // Insert new DeadlineRawRequest
    std::map<RequestPos, std::unique_ptr<DeadlineRawRequest>>::iterator inserted_begin;
    std::map<RequestPos, std::unique_ptr<DeadlineRawRequest>>::iterator inserted_end;
    // Split requests by max_request_size_
    for (uint64_t merging_offset = range_begin; merging_offset < range_end; merging_offset += opts_.max_request_size_) {
        // Construct new request, the size of last request should be the remaining size
        std::unique_ptr<DeadlineRawRequest> new_request = std::make_unique<DeadlineRawRequest>(
            file, merging_offset, std::min(range_end - merging_offset, opts_.max_request_size_));
        new_request->min_reach_submit_time_ = req_ddl_iter->submit_time_;

        // Add dependency for both raw user request and new requests
        if ((offset + size > new_request->offset_)
                && (offset < new_request->offset_ + new_request->size_)) {
            // req_ddl_iter represent the raw user request, maybe rename it
            new_request->payload_.user_dependency_.emplace_back(req_ddl_iter);
            auto insert_res = req_ddl_iter->request_.payload_.raw_dependency_.emplace(
                merging_offset, new_request.get());
            assert("UserRequest already have a DeadlineRawRequest with same offset"
                && insert_res.second);
            // Avoid compiler warning in release build
            insert_res.second = insert_res.second;

            ++req_ddl_iter->request_.payload_.pending_count_;
        }

        // insert new request, maybe merged, maybe not merged
        auto ordered_insert_res = ordered_requests_.emplace(RequestPos(file->fd_, merging_offset),
            std::move(new_request));
        assert("ordered_requests_ already have a RawReqeust with same offset"
            && ordered_insert_res.second);

        if (merging_offset == range_begin) {
            inserted_begin = ordered_insert_res.first;
        }
        inserted_end = ordered_insert_res.first;
    }
    ++inserted_end;

    if (unlikely(opts_.log_level_ >= LogLevel::TRACE)) {
        std::stringstream old_ss;
        for (auto iter = requests_to_merge.begin(); iter != requests_to_merge.end(); ++iter) {
            old_ss << "R: " << (*iter)->str() << ";";
        }
        std::stringstream new_ss;
        for (auto iter = inserted_begin; iter != inserted_end; ++iter) {
            new_ss << "R: " << iter->second->str() << ";";
        }

        LOG_INFO(logger_, fmt::format("[Scheduler: {}] OldRawRequest: {{{}}}, NewRawRequest: {{{}}}",
            static_cast<void*>(this), old_ss.str(), new_ss.str()));
    }

    // Update affected UserRequest
    // NOTE(wsy): What if request with same offset?
    auto old_raw_req_iter = requests_to_merge.begin();
    auto new_raw_req_iter = inserted_begin;
    while (old_raw_req_iter != requests_to_merge.end() && new_raw_req_iter != inserted_end) {
        uint64_t old_req_begin = old_raw_req_iter->get()->offset_;
        uint64_t old_req_end = old_req_begin + old_raw_req_iter->get()->size_;
        uint64_t new_req_begin = new_raw_req_iter->first.offset_;
        uint64_t new_req_end = new_req_begin + new_raw_req_iter->second->size_;

        assert((old_req_begin % opts_.aligned_boundary_ == 0)
            && (old_req_end % opts_.aligned_boundary_ == 0) && "Some old raw request not aligned to boundary");
        assert((new_req_begin % opts_.aligned_boundary_ == 0)
            && (new_req_end % opts_.aligned_boundary_ == 0) && "Some new raw request not aligned to boundary");

        if (old_req_begin >= new_req_end) {
            ++new_raw_req_iter;
        } else if (new_req_begin >= old_req_end) {
            ++old_raw_req_iter;
        } else {
            for (auto& dependent : old_raw_req_iter->get()->payload_.user_dependency_) {
                // Remove old raw request from user request's dependency list
                // There maybe multiple old raw request related to same user request, so
                // the erased_num may be 0 here
                size_t erased_num = dependent->request_.payload_.raw_dependency_.erase(
                    old_raw_req_iter->get()->offset_);
                dependent->request_.payload_.pending_count_.fetch_sub(erased_num);

                // Find out which range did UserRequest is overlapping with this
                // DeadlineRawRequest
                uint64_t user_req_overlapping_begin = std::max(old_req_begin,
                    dependent->request_.offset_);
                uint64_t user_req_overlapping_end = std::min(old_req_end,
                    dependent->request_.offset_ + dependent->request_.size_);
                assert("DeadlineRawRequest and UserRequest didn't overlapping"
                    && ((old_req_begin < dependent->request_.offset_ + dependent->request_.size_)
                        && (dependent->request_.offset_ < old_req_end)));

                if (new_req_end > user_req_overlapping_begin && new_req_begin < user_req_overlapping_end) {
                    auto res = dependent->request_.payload_.raw_dependency_.emplace(
                        new_raw_req_iter->first.offset_, new_raw_req_iter->second.get());
                    if (res.second) {
                        ++dependent->request_.payload_.pending_count_;

                        new_raw_req_iter->second->min_reach_submit_time_ = std::min(
                            new_raw_req_iter->second->min_reach_submit_time_,
                            dependent->submit_time_);

                        new_raw_req_iter->second->payload_.user_dependency_.emplace_back(dependent);
                    }
                }
            }

            if (old_req_end <= new_req_end) {
                ++old_raw_req_iter;
            }
            if (old_req_end >= new_req_end) {
                ++new_raw_req_iter;
            }
        }
    }

    // Update scheduler status
    ++status_.pending_user_request_;
    ++status_.submitted_user_request_;
    status_.pending_raw_request_ = ordered_requests_.size();

    CurrentMetrics::set(CurrentMetrics::IOSchRawRequests, ordered_requests_.size());

    cv_.notify_one();

    if (unlikely(opts_.log_level_ >= LogLevel::TRACE)) {
        LOG_INFO(logger_, fmt::format("[Scheduler: {}] {}", static_cast<void*>(this),
            schedulerStatus()));
    }
}

String DeadlineScheduler::status() const {
    Poco::JSON::Object obj;
    obj.set("type", "deadline");
    obj.set("pending_user_request", status_.pending_user_request_.load());
    obj.set("pending_raw_request", status_.pending_raw_request_.load());
    obj.set("submitted_user_request", status_.submitted_user_request_.load());
    obj.set("finished_user_request", status_.finished_user_request_.load());
    obj.set("retrieved_raw_request", status_.retrieved_raw_request_.load());
    obj.set("finalized_raw_request", status_.finalized_raw_request_.load());
    std::ostringstream os;
    obj.stringify(os);
    return os.str();
}

std::vector<std::unique_ptr<DeadlineRawRequest>> DeadlineScheduler::retrieveRequests(
        size_t max_request_num, size_t max_wait_time_ns) noexcept {
    std::vector<std::unique_ptr<DeadlineRawRequest>> results;
    {
        std::unique_lock<std::mutex> lock(mu_);
        // wait until ordered_requests_ is not empty
        if (!cv_.wait_for(lock, std::chrono::nanoseconds(max_wait_time_ns),
            [this]() {
                return !ordered_requests_.empty();
            })
        ) {
            return {};
        }

         // Get the submit time which is going to timeout
        auto ddl_threshold = std::chrono::steady_clock::now()
            - std::chrono::nanoseconds(opts_.max_delay_ns_);

        // Check ddl queue and see if we can wait for a while
        for (auto iter = ddl_queue_.begin(); iter != ddl_queue_.end(); ++iter) {
            if (!iter->request_.payload_.raw_dependency_.empty()) {
                if (iter->submit_time_ > ddl_threshold) {
                    // Wait for a while
                    auto wait_time = std::min(iter->submit_time_ - ddl_threshold,
                        std::chrono::nanoseconds(opts_.max_delay_ns_));
                    if (wait_time > std::chrono::nanoseconds(opts_.min_delay_ns_)) {
                        lock.unlock();
                        std::this_thread::sleep_for(wait_time);
                        lock.lock();
                    }
                }
                break;
            }
        }

        // Check deadline queue first
        for (auto ddl_iter = ddl_queue_.begin(); max_request_num > 0 && ddl_iter != ddl_queue_.end() && ddl_iter->submit_time_ <= ddl_threshold;) {
            auto& raw_requests = ddl_iter->request_.payload_.raw_dependency_;
            assert("Some UserRequest have 0 dependency and no ongoing request"
                && !(raw_requests.empty() && ddl_iter->request_.payload_.pending_count_ == 0));
            for (auto raw_iter = raw_requests.begin(); max_request_num > 0 && raw_iter != raw_requests.end();) {
                auto next_iter = std::next(raw_iter);
                // raw_iter will become invalid when retrieve raw request
                results.push_back(clearRequestDependency(raw_iter->second, lock));
                raw_iter = next_iter;
                --max_request_num;
            }
            assert("Some UserRequest have 0 dependency and no ongoing request after retrieve raw request"
                && (!raw_requests.empty() || ddl_iter->request_.payload_.pending_count_ != 0));

            ++ddl_iter;
        }

        // Cannot retrieve anything from ddl queue, try by position
        if (results.empty()) {
            if (ordered_requests_.empty()) {
                return results;
            }

            max_request_num = std::min(max_request_num, ordered_requests_.size());
            // Give priority to the last retrieve position where prefetch data may exist
            auto iter = ordered_requests_.lower_bound(last_retrieve_pos_);
            iter = (iter == ordered_requests_.end() ? ordered_requests_.begin() : iter);
            for (size_t i = 0; i < max_request_num; ++i) {
                auto next_iter = std::next(iter);
                results.push_back(clearRequestDependency(iter, lock));
                iter = next_iter;
                if (iter == ordered_requests_.end()) {
                    iter = ordered_requests_.begin();
                }
            }
        }
        // Update last retrieve position
        if (!results.empty()) {
            DeadlineRawRequest* last_req = results.back().get();
            last_retrieve_pos_.fd_ = last_req->file_->fd_;
            last_retrieve_pos_.offset_ = last_req->offset_ + last_req->size_;
        }

        // If requests left are not empty after this request retreives, notify another thread
        if (!ordered_requests_.empty()) {
            cv_.notify_one();
        }

        if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
            LOG_INFO(logger_, fmt::format("[Scheduler: {}] Retrieve {} requests",
                static_cast<void*>(this), results.size()));

            if (opts_.log_level_ >= LogLevel::TRACE) {
                std::stringstream ss;
                for (const auto& request : results) {
                    ss << request->str() << ";";
                }
                LOG_INFO(logger_, fmt::format("[Scheduler: {}] Retrieve request {}, status: {}",
                    static_cast<void*>(this), ss.str(), schedulerStatus()));
            }
        }

        status_.pending_raw_request_ = ordered_requests_.size();
        status_.retrieved_raw_request_ += results.size();

        CurrentMetrics::set(CurrentMetrics::IOSchRawRequests, ordered_requests_.size());
    }

    {
        // Trying to elimate unnecessary io request
        for (std::unique_ptr<DeadlineRawRequest>& request : results) {
            assert(!request->payload_.user_dependency_.empty()
                && "Some RawRequest have on corresponding UserRequest");
            if (request->payload_.user_dependency_.size() == 1) {
                DeadlineUserRequest& user_request = request->payload_.user_dependency_.front()->request_;

                request->offset_ = std::max(request->offset_, user_request.offset_);
                request->size_ = std::min(user_request.offset_ + user_request.size_,
                    request->offset_ + request->size_) - request->offset_;
                request->target_mem_ = user_request.buffer_ + (request->offset_ - user_request.offset_);
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::IOSchedulerExecutedRawReuqests, results.size());
    return results;
}

std::unique_ptr<DeadlineRawRequest> DeadlineScheduler::clearRequestDependency(
        DeadlineRawRequest* raw_request, std::unique_lock<std::mutex>& lock) {
    auto iter = ordered_requests_.find(RequestPos(raw_request->file_->fd_,
        raw_request->offset_));
    assert("DeadlineRawRequest not found in ordered_requests_" && iter != ordered_requests_.end());

    return clearRequestDependency(iter, lock);
}

std::unique_ptr<DeadlineRawRequest> DeadlineScheduler::clearRequestDependency(
    std::map<RequestPos, std::unique_ptr<DeadlineRawRequest>>::iterator iter,
        std::unique_lock<std::mutex>&) {
    std::unique_ptr<DeadlineRawRequest> raw_request = std::move(iter->second);
    ordered_requests_.erase(iter);

    // Remove raw dependency from user request which depends on this raw request
    for (auto dependent : raw_request->payload_.user_dependency_) {
        auto it = dependent->request_.payload_.raw_dependency_.find(raw_request->offset_);
        assert("UserRequest and RawRequest's dependency mismatch"
            && it != dependent->request_.payload_.raw_dependency_.end());
        assert("UserRequest have a different DeadlineRawRequest in same position"
            && (it->second == raw_request.get()));

        dependent->request_.payload_.raw_dependency_.erase(it);
    }

    return raw_request;
}

/**
 * @brief Clear request when it was successful.
 */
void DeadlineScheduler::finalizeRequest(std::unique_ptr<DeadlineRawRequest> request,
        std::unique_ptr<char []> buffer, uint64_t readed) {
    // Copy from temporary buffer[source_offset, source_offset + copy_size] to all pending UserRequests
    for (const auto& dependent : request->payload_.user_dependency_) {
        uint64_t source_offset = 0;
        uint64_t target_offset = 0;
        if (request->offset_ >= dependent->request_.offset_) {
            target_offset = request->offset_ - dependent->request_.offset_;
        } else {
            source_offset = dependent->request_.offset_ - request->offset_;
        }
        uint64_t copy_size = 0;
        if (readed > source_offset) {
            copy_size = std::min(readed, dependent->request_.offset_ + dependent->request_.size_ - request->offset_)
                - source_offset;
        }

        if (copy_size == 0) {
            continue;
        }

        // Update request's max readed offset atomically
        uint64_t end_offset = std::min(dependent->request_.offset_ + dependent->request_.size_,
            request->offset_ + readed);
        for (uint64_t prev_max_offset = dependent->request_.payload_.max_readed_offset_;
            readed != 0 && prev_max_offset < end_offset
            && !dependent->request_.payload_.max_readed_offset_.compare_exchange_weak(prev_max_offset, end_offset); ) {}

        if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
            LOG_INFO(logger_, fmt::format("[Scheduler: {}] Fill to UserRequest {} with buffer offset {}, "
                "from source buffer offset {}, with size {}, abs: {} -> {}, MaxOffset: {}, EndOffset: {}",
                static_cast<void*>(this), static_cast<void*>(&(dependent->request_)), target_offset, source_offset,
                copy_size, request->offset_ + source_offset, dependent->request_.offset_ + target_offset,
                dependent->request_.payload_.max_readed_offset_, end_offset));
        }

        // When target_mem_ is specified, io worker can either write directly
        // to target memory, or create a temporary buffer and copy again here
        if (buffer != nullptr) {
            memcpy(dependent->request_.buffer_ + target_offset, buffer.get() + source_offset,
                copy_size);
        } else {
            assert(request->target_mem_ != nullptr && "IOWorker can only write to target memory "
                "when scheduler enable it");
        }
    }

    std::lock_guard<std::mutex> lock(mu_);

    if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
        LOG_INFO(logger_, fmt::format("[Scheduler: {}] Finish raw request {}",
            static_cast<void*>(this), request->str()));
    }

    // Update user request which depends on this raw request
    for (auto dependent : request->payload_.user_dependency_) {
        DeadlineUserRequest& user_request = dependent->request_;
        --user_request.payload_.pending_count_;

        if (user_request.payload_.pending_count_ == 0) {
            assert("UserRequest have no DeadlineRawRequest to execute but positiove pending count"
                && user_request.payload_.raw_dependency_.empty());

            CurrentMetrics::sub(CurrentMetrics::IOSchUserRequests, 1);

            if (user_request.payload_.exception_ != nullptr) {
                user_request.callback_->onFailure(user_request.payload_.exception_);
            } else {
                user_request.callback_->onSuccess(
                    user_request.payload_.max_readed_offset_ - user_request.offset_);
            }

            ddl_queue_.erase(dependent);

            --status_.pending_user_request_;
            ++status_.finished_user_request_;
        }
    }

    ++status_.finalized_raw_request_;
}

/**
 * @brief Clear request when it failed.
 */
void DeadlineScheduler::finalizeRequest(std::unique_ptr<DeadlineRawRequest> request,
        const std::exception_ptr& except) {
    std::lock_guard<std::mutex> lock(mu_);

    if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
        LOG_INFO(logger_, fmt::format("[Scheduler: {}] Finish raw request {} with exception",
            static_cast<void*>(this), request->str()));
    }

    // Update user request which depends on this raw request
    for (auto dependent : request->payload_.user_dependency_) {
        DeadlineUserRequest& user_request = dependent->request_;

        if (user_request.payload_.exception_ == nullptr) {
            // Only retain first exception
            user_request.payload_.exception_ = except;
        }

        // traverse all raw requests of this user request
        // If one raw request only depends on this user request, then erase it from ordered_requests_.
        for (auto raw_dependency_iter : user_request.payload_.raw_dependency_) {
            DeadlineRawRequest* raw_request = raw_dependency_iter.second;

            auto temp = find(raw_request->payload_.user_dependency_.begin(), raw_request->payload_.user_dependency_.end(), dependent);
            raw_request->payload_.user_dependency_.erase(temp);
            if (raw_request->payload_.user_dependency_.empty()) {

                if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
                    LOG_INFO(logger_, fmt::format("[Scheduler: {}] Finish raw request {} together with {}",
                    static_cast<void*>(this), raw_request->str(), request->str()));
                }
                // clear from all raw requests
                ordered_requests_.erase(RequestPos(raw_request->file_->fd_, raw_request->offset_));
                --status_.pending_raw_request_;
                ++status_.finalized_raw_request_;
            }
        }

        // clear all elements from this failed user request's dependency
        user_request.payload_.pending_count_ -= user_request.payload_.raw_dependency_.size() + 1;
        user_request.payload_.raw_dependency_.clear();

        if (user_request.payload_.pending_count_ == 0) {
            CurrentMetrics::sub(CurrentMetrics::IOSchUserRequests, 1);

            // Only trigger failure callback when all on the fly requests are finished.
            // Otherwise user may release before request finished
            user_request.callback_->onFailure(user_request.payload_.exception_);

            ddl_queue_.erase(dependent);

            --status_.pending_user_request_;
            ++status_.finished_user_request_;
        }
    }

    ++status_.finalized_raw_request_;
}

String DeadlineScheduler::schedulerStatus() const {
    std::stringstream ss;
    ss << "DDLQueue: {";
    for (const auto& entry : ddl_queue_) {
        ss << "DDLEntry: " << entry.str() << ";";
    }
    ss << "}, ReuqestQueue: {";
    for (const auto& entry : ordered_requests_) {
        ss << "R: <" << entry.first.fd_ << "," << entry.first.offset_ << ">," << entry.second->str() << ";";
    }
    ss << "}";
    return ss.str();
}

}

}
