#pragma once

#include <Common/Logger.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <thread>
#include <vector>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/JSON/JSON.h>
#include <Common/Stopwatch.h>
#include <common/scope_guard.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/BlockingQueue.h>
#include <IO/RemoteFSReader.h>
#include <IO/Scheduler/Common.h>
#include <IO/Scheduler/IOWorkerPool.h>
#include <sys/types.h>
#include <common/scope_guard.h>

namespace DB::IO::Scheduler {

struct DynamicParallelIOWorkerPoolOptions {
    static DynamicParallelIOWorkerPoolOptions parseFromConfig(const Poco::Util::AbstractConfiguration& cfg,
            const String& prefix) {
        size_t hwc = std::thread::hardware_concurrency();
        size_t worker_group_count = cfg.getUInt(prefix + ".worker_group_size", std::max(1ul, hwc / 4));

        return DynamicParallelIOWorkerPoolOptions {
            .log_level_ = str2LogLevel(cfg.getString(prefix + ".log_level", "none")),
            .worker_group_size_ = worker_group_count,
            .worker_group_queue_length = cfg.getUInt64(prefix + ".worker_group_queue_length", 16),
            .worker_group_target_request_per_worker_ = cfg.getUInt(prefix + ".worker_group_target_request_per_worker", 1),
            .min_thread_per_worker_group_ = cfg.getUInt(prefix + ".min_thread_per_worker_group", 32),
            .max_thread_per_worker_group_ = cfg.getUInt(prefix + ".max_thread_per_worker_group", 128),
            .dispatcher_max_wait_ns_ = cfg.getUInt64(prefix + ".dispatcher_max_wait_ns", 1000 * 1000 * 1000),
            .dispatcher_max_retrieve_count_ = cfg.getUInt(prefix + ".dispatcher_max_retrieve_count", 3),
            .worker_extract_request_count_ = cfg.getUInt(prefix + ".worker_extract_request_count", 1),
            .worker_max_wait_ns_ = cfg.getUInt64(prefix + ".worker_max_wait_ns", 2 * 1000 * 1000 * 1000),
            .worker_idle_exit_ns_ = cfg.getUInt64(prefix + ".worker_idle_exit_ns", 10ul * 1000 * 1000 * 1000),
        };
    }

    LogLevel log_level_ = LogLevel::NONE;

    size_t worker_group_size_ = 16;
    size_t worker_group_queue_length = 16;
    size_t worker_group_target_request_per_worker_ = 1;

    size_t min_thread_per_worker_group_ = 16;
    size_t max_thread_per_worker_group_ = 128;

    uint64_t dispatcher_max_wait_ns_ = 1000 * 1000 * 1000;
    size_t dispatcher_max_retrieve_count_ = 3;

    size_t worker_extract_request_count_ = 1;
    uint64_t worker_max_wait_ns_ = 1000 * 1000 * 1000;
    uint64_t worker_idle_exit_ns_ = 10ul * 1000 * 1000 * 1000;
};

template <typename SchedulerType>
class DynamicParallelIOWorkerPool: public IOWorkerPool {
public:
    using Options = DynamicParallelIOWorkerPoolOptions;

    DynamicParallelIOWorkerPool(const Options& opts,
        const std::vector<SchedulerType*>& schedulers):
            opts_(opts), logger_(getLogger("DynamicParallelIOWorkerPool")),
            schedulers_(schedulers), worker_pool_(nullptr), dispatcher_pool_(nullptr) {}

    virtual void startup() override {
        size_t max_threads = opts_.max_thread_per_worker_group_ * opts_.worker_group_size_;
        size_t min_threads = opts_.min_thread_per_worker_group_ * opts_.worker_group_size_;
        worker_pool_ = std::make_unique<ThreadPool>(max_threads,
            max_threads - min_threads, max_threads);

        for (size_t i = 0; i < opts_.worker_group_size_; ++i) {
            worker_groups_.emplace_back(std::make_unique<WorkerGroup>(opts_, logger_,
                worker_pool_.get()));
        }

        dispatcher_pool_ = std::make_unique<ThreadPool>(schedulers_.size());
        for (SchedulerType* scheduler : schedulers_) {
            dispatchers_.emplace_back(std::make_unique<Dispatcher>(opts_,
                logger_, scheduler, worker_groups_));
        }
        for (size_t i = 0; i < dispatchers_.size(); ++i) {
            dispatcher_pool_->scheduleOrThrow([dispatcher = dispatchers_[i].get()]() {
                dispatcher->run();
            });
        }
    }

    virtual void shutdown() override {
        for (auto& dispatcher : dispatchers_) {
            dispatcher->shutdown_ = true;
        }

        for (auto& worker_group : worker_groups_) {
            worker_group->shutdown_ = true;
        }

        dispatcher_pool_->wait();
        worker_pool_->wait();
    }

    virtual String status() const override {
        Poco::JSON::Object status_obj;
        status_obj.set("type", "dynamic_parallel");

        Poco::JSON::Array dispatcher_status;
        for (const auto& dispatcher : dispatchers_) {
            Poco::JSON::Object dispatcher_obj;
            dispatcher_obj.set("dispatched_requests", dispatcher->status_.dispatched_requests_.load());
            dispatcher_obj.set("dispatch_delay_ns", dispatcher->status_.dispatch_delay_ns_.load());
            dispatcher_obj.set("dispatch_time_ns", dispatcher->status_.dispatch_time_ns_.load());
            dispatcher_status.add(dispatcher_obj);
        }
        status_obj.set("dispatcher_status", dispatcher_status);

        Poco::JSON::Array worker_group_status;
        for (const auto& worker_group : worker_groups_) {
            Poco::JSON::Object wg_status_obj;
            wg_status_obj.set("executed_requests", worker_group->status_.executed_requests_.load());
            wg_status_obj.set("start_execute_delay_ns", worker_group->status_.start_execute_delay_ns_.load());
            wg_status_obj.set("finish_execute_delay_ns", worker_group->status_.finish_execute_delay_ns_.load());
            wg_status_obj.set("finalize_delay_ns", worker_group->status_.finalize_delay_ns_.load());
            wg_status_obj.set("s3_read_count", worker_group->status_.s3_read_count_.load());
            wg_status_obj.set("s3_read_bytes", worker_group->status_.s3_read_bytes_.load());
            wg_status_obj.set("s3_read_time_ns", worker_group->status_.s3_read_time_ns_.load());
            wg_status_obj.set("get_reader_ns", worker_group->status_.get_reader_ns_.load());
            wg_status_obj.set("reader_seek_ns", worker_group->status_.reader_seek_ns_.load());
            worker_group_status.add(wg_status_obj);
        }
        status_obj.set("worker_group_status", worker_group_status);

        std::ostringstream os;
        status_obj.stringify(os);
        return os.str();
    }

private:
    struct PendingRawRequest {
        PendingRawRequest(): scheduler_(nullptr), request_(nullptr) {}
        PendingRawRequest(SchedulerType* scheduler,
            std::unique_ptr<typename SchedulerType::RawRequestType> request):
                scheduler_(scheduler), request_(std::move(request)) {}

        SchedulerType* scheduler_;
        std::unique_ptr<typename SchedulerType::RawRequestType> request_;
    };

    struct WorkerGroup;

    struct WorkerGroupStatus {
        WorkerGroupStatus(): executed_requests_(0), start_execute_delay_ns_(0),
            finish_execute_delay_ns_(0), finalize_delay_ns_(0), s3_read_count_(0),
            s3_read_bytes_(0), s3_read_time_ns_(0), get_reader_ns_(0),
            reader_seek_ns_(0) {}

        std::atomic<uint64_t> executed_requests_;

        std::atomic<uint64_t> start_execute_delay_ns_;
        std::atomic<uint64_t> finish_execute_delay_ns_;
        std::atomic<uint64_t> finalize_delay_ns_;

        std::atomic<uint64_t> s3_read_count_;
        std::atomic<uint64_t> s3_read_bytes_;
        std::atomic<uint64_t> s3_read_time_ns_;
        std::atomic<uint64_t> get_reader_ns_;
        std::atomic<uint64_t> reader_seek_ns_;
    };

    struct Worker {
        Worker(WorkerGroup& worker_group, LoggerPtr logger,
            typename std::list<std::unique_ptr<Worker>>::iterator worker_iter):
                worker_group_(worker_group), logger_(logger), opts_(worker_group.opts_),
                shutdown_(worker_group.shutdown_), status_(worker_group.status_),
                requests_queue_(worker_group.requests_queue_), worker_iter_(worker_iter) {}

        void run() {
            std::chrono::steady_clock::time_point last_request_time_
                = std::chrono::steady_clock::now();

            while (!shutdown_) {
                std::vector<PendingRawRequest> requests = requests_queue_.extractBatch(
                    opts_.worker_extract_request_count_, opts_.worker_max_wait_ns_);

                if (!requests.empty()) {
                    if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
                        LOG_DEBUG(logger_, fmt::format("Worker {} retrieve {} requests",
                            static_cast<void*>(this), requests.size()));
                    }

                    status_.executed_requests_ += requests.size();

                    last_request_time_ = std::chrono::steady_clock::now();

                    for (auto& pending_request : requests) {
                        status_.start_execute_delay_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - pending_request.request_->min_reach_submit_time_).count();

                        auto request_min_reach_time = pending_request.request_->min_reach_submit_time_;

                        std::unique_ptr<typename SchedulerType::RawRequestType> raw_request =
                            std::move(pending_request.request_);

                        std::unique_ptr<char []> temp_buffer(raw_request->target_mem_ != nullptr ?
                            nullptr : new char[raw_request->size_]);
                        uint64_t readed = 0;

                        SCOPE_EXIT({
                            status_.finalize_delay_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - request_min_reach_time).count();
                        });

                        try {
                            SCOPE_EXIT({
                                status_.finish_execute_delay_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now() - request_min_reach_time).count();
                            });

                            ++status_.s3_read_count_;
                            status_.s3_read_bytes_ += raw_request->size_;

                            Stopwatch watch;
                            SCOPE_EXIT({
                                status_.s3_read_time_ns_ += watch.elapsedNanoseconds();
                            });

                            Stopwatch op_watch;
                            std::unique_ptr<RemoteFSReader> reader = raw_request->file_->borrowReader(
                                raw_request->offset_);

                            status_.get_reader_ns_ += op_watch.elapsedNanoseconds();
                            op_watch.restart();

                            reader->seek(raw_request->offset_);

                            status_.reader_seek_ns_ += op_watch.elapsedNanoseconds();

                            char* target_location = raw_request->target_mem_ != nullptr ?
                                raw_request->target_mem_ : temp_buffer.get();
                            readed = reader->read(target_location, raw_request->size_);

                            raw_request->file_->returnReader(std::move(reader));
                        } catch (...) {
                            pending_request.scheduler_->finalizeRequest(std::move(raw_request),
                                std::current_exception());

                            continue;
                        }

                        pending_request.scheduler_->finalizeRequest(std::move(raw_request),
                            std::move(temp_buffer), readed);
                    }
                } else {
                    if (std::chrono::steady_clock::now() - last_request_time_
                            > std::chrono::nanoseconds(opts_.worker_idle_exit_ns_)) {
                        if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
                            LOG_DEBUG(logger_, fmt::format("Worker {} exit since it's idle too long",
                                static_cast<void*>(this)));
                        }

                        if (worker_group_.removeWorker(worker_iter_)) {
                            return;
                        }
                    }
                }
            }
        }

        WorkerGroup& worker_group_;

        LoggerPtr logger_;

        const Options& opts_;
        std::atomic<bool>& shutdown_;
        WorkerGroupStatus& status_;
        BlockingQueue<PendingRawRequest>& requests_queue_;

        typename std::list<std::unique_ptr<Worker>>::iterator worker_iter_;
    };

    struct WorkerGroup {
        WorkerGroup(const Options& opts, LoggerPtr logger, ThreadPool* pool):
                opts_(opts), shutdown_(false), logger_(logger),
                requests_queue_(opts.worker_group_queue_length), worker_pool_(pool), worker_num_(0) {
            for (size_t i = 0; i < opts_.min_thread_per_worker_group_; ++i) {
                addWorker();
            }
        }

        void insertRequests(std::vector<PendingRawRequest>& requests,
                std::optional<uint64_t> wait_time) {
            requests_queue_.insertBatch(requests, wait_time);

            while (requests_queue_.size() >= worker_num_ * opts_.worker_group_target_request_per_worker_
                    && addWorker()) {}
        }

        bool addWorker() {
            if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
                LOG_INFO(logger_, fmt::format("WorkerGroup {} add new worker",
                    static_cast<void*>(this)));
            }

            std::lock_guard<std::mutex> lock(mu_);

            if (workers_.size() >= opts_.max_thread_per_worker_group_) {
                return false;
            }

            auto worker_iter = workers_.insert(workers_.end(), nullptr);
            *worker_iter = std::make_unique<Worker>(*this, logger_, worker_iter);
            worker_num_ = workers_.size();

            return worker_pool_->trySchedule([wrk_iter = worker_iter]() {
                (*wrk_iter)->run();
            });
        }

        bool removeWorker(typename std::list<std::unique_ptr<Worker>>::iterator worker_iter) {
            if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
                LOG_INFO(logger_, fmt::format("WorkerGroup {} remove worker",
                    static_cast<void*>(this)));
            }

            std::lock_guard<std::mutex> lock(mu_);

            if (workers_.size() <= opts_.min_thread_per_worker_group_) {
                return false;
            }

            workers_.erase(worker_iter);
            worker_num_ = workers_.size();

            return true;
        }

        const Options& opts_;

        std::atomic<bool> shutdown_;

        LoggerPtr logger_;

        WorkerGroupStatus status_;

        BlockingQueue<PendingRawRequest> requests_queue_;

        ThreadPool* worker_pool_;

        std::mutex mu_;
        std::atomic<size_t> worker_num_;
        std::list<std::unique_ptr<Worker>> workers_;
    };

    struct Dispatcher {
        struct Status {
            Status(): dispatched_requests_(0), dispatch_delay_ns_(0), dispatch_time_ns_(0) {}

            std::atomic<uint64_t> dispatched_requests_;
            std::atomic<uint64_t> dispatch_delay_ns_;
            std::atomic<uint64_t> dispatch_time_ns_;
        };

        Dispatcher(const Options& opts, LoggerPtr logger, SchedulerType* scheduler,
            std::vector<std::unique_ptr<WorkerGroup>>& worker_groups):
                opts_(opts), logger_(logger), shutdown_(false), scheduler_(scheduler),
                worker_groups_(worker_groups) {}

        void run() {
            std::default_random_engine re(rand());
            std::uniform_int_distribution<size_t> dist(0, worker_groups_.size() - 1);

            while (!shutdown_) {
                auto requests = scheduler_->retrieveRequests(
                    opts_.dispatcher_max_retrieve_count_, opts_.dispatcher_max_wait_ns_);

                if (requests.empty()) {
                    continue;
                }

                status_.dispatched_requests_ += requests.size();

                auto now = std::chrono::steady_clock::now();

                std::vector<PendingRawRequest> pending_requests;
                pending_requests.reserve(requests.size());
                for (size_t i = 0; i < requests.size(); ++i) {
                    status_.dispatch_delay_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        now - requests[i]->min_reach_submit_time_).count();

                    pending_requests.emplace_back(scheduler_, std::move(requests[i]));
                }
                requests.clear();

                Stopwatch dispatch_time_watch;

                size_t worker_group_offset = dist(re);
                for (size_t i = 0; !pending_requests.empty() && i < worker_groups_.size(); ++i) {
                    size_t worker_group_idx = (i + worker_group_offset) % worker_groups_.size();

                    worker_groups_[worker_group_idx]->insertRequests(pending_requests, 0);
                }

                if (!pending_requests.empty()) {
                    worker_groups_[worker_group_offset]->insertRequests(pending_requests, std::nullopt);
                }

                status_.dispatch_time_ns_ += dispatch_time_watch.elapsedNanoseconds();
            }
        }

        const Options& opts_;
        LoggerPtr logger_;

        std::atomic<bool> shutdown_;

        SchedulerType* scheduler_;

        Status status_;

        std::vector<std::unique_ptr<WorkerGroup>>& worker_groups_;
    };

    const Options opts_;

    LoggerPtr logger_;

    std::vector<SchedulerType*> schedulers_;

    std::unique_ptr<ThreadPool> worker_pool_;
    std::vector<std::unique_ptr<WorkerGroup>> worker_groups_;

    std::unique_ptr<ThreadPool> dispatcher_pool_;
    std::vector<std::unique_ptr<Dispatcher>> dispatchers_;
};

}
