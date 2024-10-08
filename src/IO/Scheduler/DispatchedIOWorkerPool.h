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

namespace DB::IO::Scheduler {

struct DispatchedIOWorkerPoolOptions {
    static DispatchedIOWorkerPoolOptions parseFromConfig(const Poco::Util::AbstractConfiguration& cfg,
            const String& prefix) {
        return DispatchedIOWorkerPoolOptions {
            .log_level_ = str2LogLevel(cfg.getString(prefix + ".log_level", "none")),
            .worker_thread_num_ = cfg.getUInt64(prefix + ".worker_thread_num", std::thread::hardware_concurrency() * 2),
            .worker_queue_max_length_ = cfg.getUInt64(prefix + ".worker_queue_max_length", 128),
            .global_queue_max_length_ = cfg.getUInt64(prefix + ".global_queue_max_length", 1024),
            .worker_extract_local_max_count_ = cfg.getUInt64(prefix + ".worker_extract_local_max_count", 1),
            .worker_extract_local_max_wait_ns_ = cfg.getUInt64(prefix + ".worker_extract_local_max_wait_ns", 5 * 1000 * 1000),
            .worker_extract_global_max_count_ = cfg.getUInt64(prefix + ".worker_extract_global_max_count", 64),
            .worker_extract_global_max_wait_ns_ = cfg.getUInt64(prefix + ".worker_extract_global_max_wait_ns", 0),
            .steal_candidate_threshold = cfg.getUInt64(prefix + ".steal_candidate_threshold", 2),
            .steal_percentage = cfg.getUInt64(prefix + ".steal_percentage", 50),
            .retrieve_nothing_wait = cfg.getUInt64(prefix + ".retrieve_nothing_wait", 0),
            .dispatcher_max_retrieve_count_ = cfg.getUInt64(prefix + ".dispatcher_max_retrieve_count", 1),
            .dispatcher_max_wait_ns_ = cfg.getUInt64(prefix + ".dispatcher_max_wait_ns", 1000 * 1000 * 1000),
        };
    }

    LogLevel log_level_ = LogLevel::NONE;

    size_t worker_thread_num_ = 64;
    size_t worker_queue_max_length_ = 128;
    size_t global_queue_max_length_ = 1024;

    size_t worker_extract_local_max_count_ = 1;
    size_t worker_extract_local_max_wait_ns_ = 5 * 1000 * 1000;
    size_t worker_extract_global_max_count_ = 64;
    size_t worker_extract_global_max_wait_ns_ = 0;

    size_t steal_candidate_threshold = 2;
    size_t steal_percentage = 50;

    size_t retrieve_nothing_wait = 0;

    size_t dispatcher_max_retrieve_count_ = 1;
    size_t dispatcher_max_wait_ns_ = 1000 * 1000 * 1000;
};

template <typename SchedulerType>
class DispatchedIOWorkerPool: public IOWorkerPool {
public:
    using Options = DispatchedIOWorkerPoolOptions;

    DispatchedIOWorkerPool(const Options& opts,
        const std::vector<SchedulerType*>& schedulers):
            opts_(opts), logger_(getLogger("DispatchedIOWorkerPool")),
            schedulers_(schedulers), global_context_(opts.global_queue_max_length_),
            worker_pool_(nullptr), dispatcher_pool_(nullptr) {}

    virtual void startup() override {
        for (size_t i = 0; i < opts_.worker_thread_num_; ++i) {
            local_contexts_.emplace_back(std::make_unique<IOWorkerContext>(
                opts_, logger_, global_context_, local_contexts_));
        }

        for (size_t i = 0; i < schedulers_.size(); ++i) {
            dispatcher_context_.emplace_back(std::make_unique<DispatcherContext>(
                opts_, logger_, schedulers_[i], global_context_, local_contexts_));
        }

        worker_pool_ = std::make_unique<ThreadPool>(opts_.worker_thread_num_);
        for (auto iter = local_contexts_.begin(); iter != local_contexts_.end(); ++iter) {
            worker_pool_->scheduleOrThrow([ctx = (*iter).get()]() {
                ctx->run();
            });
        }

        dispatcher_pool_ = std::make_unique<ThreadPool>(schedulers_.size());
        for (auto iter = dispatcher_context_.begin(); iter != dispatcher_context_.end(); ++iter) {
            dispatcher_pool_->scheduleOrThrow([ctx = (*iter).get()]() {
                ctx->run();
            });
        }
    }

    virtual void shutdown() override {
        for (auto& ctx : dispatcher_context_) {
            ctx->shutdown_ = true;
        }

        for (auto& ctx : local_contexts_) {
            ctx->shutdown_ = true;
        }

        dispatcher_pool_->wait();
        dispatcher_pool_ = nullptr;
        worker_pool_->wait();
        worker_pool_ = nullptr;
    }

    virtual String status() const override {
        Poco::JSON::Object status_obj;
        status_obj.set("type", "dispatched");
        Poco::JSON::Array worker_status;
        for (size_t i = 0; i < local_contexts_.size(); ++i) {
            IOWorkerContext& ctx = *(local_contexts_[i]);
            Poco::JSON::Object worker_status_obj;
            worker_status_obj.set("total_executed_request", ctx.status_.total_executed_request_.load());
            worker_status_obj.set("total_executed_ns", ctx.status_.total_executed_ns_.load());
            worker_status_obj.set("retrieve_from_local_queue_count", ctx.status_.retrieve_from_local_queue_count_.load());
            worker_status_obj.set("total_request_from_local_queue", ctx.status_.total_request_from_local_queue_.load());
            worker_status_obj.set("retrieve_from_global_queue_count", ctx.status_.retrieve_from_global_queue_count_.load());
            worker_status_obj.set("total_request_from_global_queue", ctx.status_.total_request_from_global_queue_.load());
            worker_status_obj.set("retrieve_from_other_queue_count", ctx.status_.retrieve_from_other_queue_count_.load());
            worker_status_obj.set("total_request_from_other_queue", ctx.status_.total_request_from_other_queue_.load());
            worker_status_obj.set("request_execute_wait_time_ns", ctx.status_.request_wait_time_ns_.load());
            worker_status_obj.set("retrieve_non_local_ns", ctx.status_.retrieve_non_local_ns_.load());
            worker_status.add(worker_status_obj);
        }
        status_obj.set("worker_status", worker_status);
        Poco::JSON::Array dispatcher_status;
        for (size_t i = 0; i < dispatcher_context_.size(); ++i) {
            DispatcherContext& ctx = *(dispatcher_context_[i]);
            Poco::JSON::Object dispatcher_status_obj;
            dispatcher_status_obj.set("dispatched_request", ctx.status_.dispatched_request_.load());
            dispatcher_status_obj.set("retrieve_count", ctx.status_.retrieve_count_.load());
            dispatcher_status_obj.set("retrieve_time_ns", ctx.status_.retrieve_time_ns_.load());
            dispatcher_status_obj.set("dispatch_local_count", ctx.status_.dispatch_local_count_.load());
            dispatcher_status_obj.set("dispatch_local_time_ns", ctx.status_.dispatch_local_time_ns_.load());
            dispatcher_status_obj.set("dispatch_global_count", ctx.status_.dispatch_global_count_.load());
            dispatcher_status_obj.set("dispatch_global_time_ns", ctx.status_.dispatch_global_time_ns_.load());
            dispatcher_status_obj.set("request_dispatch_wait_time_ns", ctx.status_.request_wait_time_ns_.load());
            dispatcher_status.add(dispatcher_status_obj);
        }
        status_obj.set("dispatcher_status", dispatcher_status);
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

    struct GlobalIOWorkerContext {
        explicit GlobalIOWorkerContext(size_t max_queue_length): request_queue_(max_queue_length) {}

        BlockingQueue<PendingRawRequest> request_queue_;
    };

    struct IOWorkerContext {
        struct Status {
            Status(): total_executed_request_(0), total_executed_ns_(0),
                retrieve_from_local_queue_count_(0), total_request_from_local_queue_(0),
                retrieve_from_global_queue_count_(0), total_request_from_global_queue_(0),
                retrieve_from_other_queue_count_(0), total_request_from_other_queue_(0),
                request_wait_time_ns_(0), retrieve_non_local_ns_(0) {}

            std::atomic<uint64_t> total_executed_request_;
            std::atomic<uint64_t> total_executed_ns_;

            std::atomic<uint64_t> retrieve_from_local_queue_count_;
            std::atomic<uint64_t> total_request_from_local_queue_;
            std::atomic<uint64_t> retrieve_from_global_queue_count_;
            std::atomic<uint64_t> total_request_from_global_queue_;
            std::atomic<uint64_t> retrieve_from_other_queue_count_;
            std::atomic<uint64_t> total_request_from_other_queue_;

            // Total time for request from submit to execute
            std::atomic<uint64_t> request_wait_time_ns_;

            std::atomic<uint64_t> retrieve_non_local_ns_;
        };

        IOWorkerContext(const Options& opts, LoggerPtr logger, GlobalIOWorkerContext& global_ctx,
            std::vector<std::unique_ptr<IOWorkerContext>>& local_ctx):
                opts_(opts), logger_(logger), shutdown_(false), global_context_(global_ctx),
                local_contexts_(local_ctx), request_queue_(opts.worker_queue_max_length_) {}

        void run() {
            LOG_INFO(logger_, fmt::format("IOWorker {} startup", static_cast<const void*>(this)));
            SCOPE_EXIT(LOG_INFO(logger_, fmt::format("IOWorker {} shutdown", static_cast<const void*>(this))));

            std::default_random_engine re(rand());
            std::uniform_int_distribution<size_t> dist(0, local_contexts_.size() - 1);

            while (!shutdown_) {
                std::vector<PendingRawRequest> requests = request_queue_.extractBatch(
                    opts_.worker_extract_local_max_count_,
                    opts_.worker_extract_local_max_wait_ns_);
                ++status_.retrieve_from_local_queue_count_;

                if (unlikely(opts_.log_level_ >= LogLevel::TRACE)) {
                    LOG_INFO(logger_, fmt::format("Retrieve {} requests from local ctx",
                        requests.size()));
                }

                if (requests.empty()) {
                    Stopwatch watch;
                    SCOPE_EXIT({status_.retrieve_non_local_ns_ += watch.elapsedNanoseconds();});

                    requests = global_context_.request_queue_.extractBatch(
                        opts_.worker_extract_global_max_count_,
                        opts_.worker_extract_global_max_wait_ns_);
                    ++status_.retrieve_from_global_queue_count_;

                    if (unlikely(opts_.log_level_ >= LogLevel::TRACE)) {
                        LOG_INFO(logger_, fmt::format("Retrieve {} requests from global ctx",
                            requests.size()));
                    }

                    if (requests.empty()) {
                        size_t ctx_idx_offset = dist(re);
                        for (size_t i = 0; i < local_contexts_.size(); ++i) {
                            IOWorkerContext* ctx = local_contexts_[(ctx_idx_offset + i) % local_contexts_.size()].get();
                            size_t total_request_count = ctx->request_queue_.size();
                            if (ctx != this && total_request_count >= opts_.steal_candidate_threshold) {
                                size_t steal_count = std::max(1ul, total_request_count * opts_.steal_percentage / 100);
                                requests = ctx->request_queue_.extractBatch(
                                    steal_count, 0);
                                ++status_.retrieve_from_other_queue_count_;

                                if (requests.size() > 0) {
                                    status_.retrieve_from_other_queue_count_ += requests.size();
                                    break;
                                }

                                if (unlikely(opts_.log_level_ >= LogLevel::TRACE)) {
                                    LOG_INFO(logger_, fmt::format("Retrieve {} requests from other worker {}",
                                        requests.size(), static_cast<void*>(ctx)));
                                }
                            }
                        }
                    } else {
                        status_.total_request_from_global_queue_ += requests.size();
                    }

                    if (!requests.empty()) {
                        request_queue_.insertBatch(requests, std::nullopt);
                    } else {
                        if (opts_.retrieve_nothing_wait != 0) {
                            std::this_thread::sleep_for(std::chrono::nanoseconds(opts_.retrieve_nothing_wait));
                        }
                    }
                    continue;
                } else {
                    status_.total_request_from_local_queue_ += requests.size();
                }

                for (auto& pending_request : requests) {
                    std::unique_ptr<typename SchedulerType::RawRequestType> raw_request =
                        std::move(pending_request.request_);

                    ++status_.total_executed_request_;
                    status_.request_wait_time_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - raw_request->min_reach_submit_time_).count();

                    std::unique_ptr<char []> temp_buffer(new char[raw_request->size_]);
                    size_t readed = 0;

                    try {
                        Stopwatch watch;
                        SCOPE_EXIT({status_.total_executed_ns_ += watch.elapsedNanoseconds();});

                        std::unique_ptr<RemoteFSReader> reader = raw_request->file_->borrowReader(
                            raw_request->offset_);
                        reader->seek(raw_request->offset_);

                        readed = reader->read(temp_buffer.get(), raw_request->size_);

                        raw_request->file_->returnReader(std::move(reader));
                    } catch (...) {
                        pending_request.scheduler_->finalizeRequest(std::move(raw_request),
                            std::current_exception());

                        continue;
                    }

                    pending_request.scheduler_->finalizeRequest(std::move(raw_request),
                        std::move(temp_buffer), readed);
                }
            }
        }

        const Options& opts_;

        LoggerPtr logger_;

        std::atomic<bool> shutdown_;

        Status status_;

        GlobalIOWorkerContext& global_context_;
        std::vector<std::unique_ptr<IOWorkerContext>>& local_contexts_;

        BlockingQueue<PendingRawRequest> request_queue_;
    };

    struct DispatcherContext {
        struct Status {
            Status(): dispatched_request_(0), retrieve_count_(0), retrieve_time_ns_(0),
                dispatch_local_count_(0), dispatch_local_time_ns_(0),
                dispatch_global_count_(0), dispatch_global_time_ns_(0),
                request_wait_time_ns_(0) {}

            std::atomic<uint64_t> dispatched_request_;
            std::atomic<uint64_t> retrieve_count_;
            std::atomic<uint64_t> retrieve_time_ns_;
            std::atomic<uint64_t> dispatch_local_count_;
            std::atomic<uint64_t> dispatch_local_time_ns_;
            std::atomic<uint64_t> dispatch_global_count_;
            std::atomic<uint64_t> dispatch_global_time_ns_;

            // Total wait time from submit to dispatch
            std::atomic<uint64_t> request_wait_time_ns_;
        };

        DispatcherContext(const Options& opts, LoggerPtr logger, SchedulerType* scheduler,
            GlobalIOWorkerContext& global_ctx, std::vector<std::unique_ptr<IOWorkerContext>>& local_ctx):
                opts_(opts), logger_(logger), shutdown_(false), scheduler_(scheduler),
                global_ctx_(global_ctx), local_ctx_(local_ctx) {}

        void run() {
            LOG_INFO(logger_, fmt::format("IODispatcher {} for scheduler {} startup",
                static_cast<const void*>(this), static_cast<const void*>(scheduler_)));
            SCOPE_EXIT(LOG_INFO(logger_, fmt::format("IODispatcher {} for scheduler {} shutdown",
                static_cast<const void*>(this), static_cast<const void*>(scheduler_))));

            std::default_random_engine re(rand());
            std::uniform_int_distribution<size_t> dist(0, local_ctx_.size() - 1);

            while (!shutdown_) {
                Stopwatch watch;
                auto requests = scheduler_->retrieveRequests(
                    opts_.dispatcher_max_retrieve_count_, opts_.dispatcher_max_wait_ns_);
                ++status_.retrieve_count_;
                status_.retrieve_time_ns_ += watch.elapsedNanoseconds();

                if (requests.empty()) {
                    continue;
                }

                status_.dispatched_request_ += requests.size();

                std::vector<PendingRawRequest> pending_requests;
                pending_requests.reserve(requests.size());
                for (size_t i = 0; i < requests.size(); ++i) {
                    status_.request_wait_time_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - requests[i]->min_reach_submit_time_).count();
                    pending_requests.emplace_back(scheduler_, std::move(requests[i]));
                }
                requests.clear();

                size_t total_request_count = pending_requests.size();
                watch.restart();
                // TODO: Make request spread to multiple scheduler more even
                size_t scheduler_offset = dist(re);
                for (size_t i = 0; !pending_requests.empty() && i < local_ctx_.size(); ++i) {
                    size_t scheduler_idx = (i + scheduler_offset) % local_ctx_.size();
                    local_ctx_[scheduler_idx]->request_queue_.insertBatch(pending_requests, 0);
                }
                status_.dispatch_local_time_ns_ += watch.elapsedNanoseconds();
                status_.dispatch_local_count_ += total_request_count - pending_requests.size();

                if (!pending_requests.empty()) {
                    watch.restart();
                    status_.dispatch_global_count_ += pending_requests.size();
                    global_ctx_.request_queue_.insertBatch(pending_requests, std::nullopt);
                    status_.dispatch_global_time_ns_ += watch.elapsedNanoseconds();
                }
            }
        }

        const Options& opts_;

        LoggerPtr logger_;

        std::atomic<bool> shutdown_;

        Status status_;

        SchedulerType* scheduler_;
        GlobalIOWorkerContext& global_ctx_;
        std::vector<std::unique_ptr<IOWorkerContext>>& local_ctx_;
    };

    const Options opts_;

    LoggerPtr logger_;

    std::vector<SchedulerType*> schedulers_;

    // Woker local context and global context
    GlobalIOWorkerContext global_context_;
    std::vector<std::unique_ptr<IOWorkerContext>> local_contexts_;
    std::unique_ptr<ThreadPool> worker_pool_;

    // Dispatcher pool, there will be one dispatcher per io scheduler
    std::vector<std::unique_ptr<DispatcherContext>> dispatcher_context_;
    std::unique_ptr<ThreadPool> dispatcher_pool_;
};

}
