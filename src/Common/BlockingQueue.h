#pragma once

#include <queue>
#include <chrono>
#include <atomic>
#include <optional>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <cassert>

class BlockingQueueTest;

namespace DB {

template <typename T>
class BlockingQueue {
public:
    BlockingQueue(size_t max_queue_size): max_queue_size_(max_queue_size), size_(0) {}

    // Insert elements to Queue, if max_wait_time_ns is std::nullopt, wait indefinitely,
    // otherwise it may timeout and only insert partial of requests into queue
    void insertBatch(std::vector<T>& requests, std::optional<uint64_t> max_wait_time_ns) {
        if (requests.empty()) {
            return;
        }

        std::unique_lock<std::mutex> lock(mu_);

        assert("Queue already exceeded max size" && max_queue_size_ >= container_.size());

        size_t consumed_request = 0;

        if (max_wait_time_ns.has_value() && max_wait_time_ns.value() == 0) {
            size_t before_insert_size = container_.size();

            for (size_t i = 0, num_to_insert = std::min(max_queue_size_ - container_.size(), requests.size());
                    i < num_to_insert; ++i) {
                container_.push_back(std::move(requests[consumed_request++]));
                ++size_;
            }

            if (before_insert_size == 0) {
                not_empty_cv_.notify_all();
            }
        } else {
            std::chrono::time_point<std::chrono::steady_clock> end_time =
                std::chrono::steady_clock::now() + std::chrono::nanoseconds(max_wait_time_ns.value_or(0));

            while (consumed_request < requests.size()) {
                if (max_wait_time_ns.has_value()) {
                    not_full_cv_.wait_until(lock, end_time, [&]() {
                        return container_.size() < max_queue_size_;
                    });

                    if (container_.size() >= max_queue_size_) {
                        break;
                    }
                } else {
                    not_full_cv_.wait(lock, [&]() {
                        return container_.size() < max_queue_size_;
                    });
                }

                size_t before_insert_size = container_.size();

                for (size_t i = 0, num_to_insert = std::min(max_queue_size_ - container_.size(), requests.size() - consumed_request);
                        i < num_to_insert; ++i) {
                    container_.push_back(std::move(requests[consumed_request++]));
                    ++size_;
                }

                if (before_insert_size == 0) {
                    not_empty_cv_.notify_all();
                }
            }
        }

        size_t remain_element_size = requests.size() - consumed_request;
        for (size_t i = 0; i < remain_element_size; ++i) {
            std::swap(requests[i], requests[i + consumed_request]);
        }
        requests.resize(remain_element_size);
    }

    // Extract elements up to request_count from Queue, if max_wait_time is std::nullopt
    // wait indefinitely, other wise may timeout and return as many as it can get
    std::vector<T> extractBatch(size_t request_count, std::optional<uint64_t> max_wait_time_ns) {
        if (request_count == 0) {
            return {};
        }

        std::unique_lock<std::mutex> lock(mu_);

        std::vector<T> result;

        assert("Queue already exceeded max size" && max_queue_size_ >= container_.size());

        if (max_wait_time_ns.has_value() && max_wait_time_ns.value() == 0) {
            size_t before_extract_size = container_.size();

            for (size_t i = 0, num_to_extract = std::min(request_count, container_.size());
                    i < num_to_extract; ++i) {
                result.emplace_back(std::move(container_.front()));
                container_.pop_front();
                --size_;
            }

            if (before_extract_size >= max_queue_size_) {
                not_full_cv_.notify_all();
            }
        } else {
            std::chrono::time_point<std::chrono::steady_clock> end_time =
                std::chrono::steady_clock::now() + std::chrono::nanoseconds(max_wait_time_ns.value_or(0));

            while (result.size() < request_count) {
                if (max_wait_time_ns.has_value()) {
                    not_empty_cv_.wait_until(lock, end_time, [&]() {
                        return !container_.empty();
                    });

                    if (container_.empty()) {
                        break;
                    }
                } else {
                    not_empty_cv_.wait(lock, [&]() {
                        return !container_.empty();
                    });
                }

                size_t before_extract_size = container_.size();

                for (size_t i = 0, num_to_extract = std::min(container_.size(), request_count - result.size());
                        i < num_to_extract; ++i) {
                    result.push_back(std::move(container_.front()));
                    container_.pop_front();
                    --size_;
                }

                if (before_extract_size >= max_queue_size_) {
                    not_full_cv_.notify_all();
                }
            }
        }

        return result;
    }

    size_t size() const {
        return size_;
    }

private:
    friend class ::BlockingQueueTest;

    size_t max_queue_size_;

    std::mutex mu_;

    std::atomic<size_t> size_;

    std::condition_variable not_empty_cv_;
    std::condition_variable not_full_cv_;

    std::deque<T> container_;
};

}
