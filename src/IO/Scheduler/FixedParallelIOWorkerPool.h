#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <random>
#include <sstream>
#include "common/defines.h"
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <IO/Scheduler/Common.h>
#include <IO/Scheduler/FixedParallelIOWorkerPool.h>
#include <IO/RemoteFSReader.h>
#include <IO/Scheduler/IOWorkerPool.h>
#include <IO/Scheduler/IOScheduler.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::IO::Scheduler {

struct FixedParallelIOWorkerPoolOptions {
public:
    static FixedParallelIOWorkerPoolOptions parseFromConfig(const Poco::Util::AbstractConfiguration& cfg,
            const String& prefix) {
        return FixedParallelIOWorkerPoolOptions {
            .log_level_ = str2LogLevel(cfg.getString(prefix + ".log_level", "none")),
            .max_io_threads_ = cfg.getUInt(prefix + ".max_io_threads", std::thread::hardware_concurrency() * 2),
            .worker_max_request_batch_size_ = cfg.getUInt(prefix + ".worker_max_request_batch_size", 1),
            .worker_wait_ns_before_switch_ = cfg.getUInt64(prefix + ".worker_wait_ns_before_switch", 1 * 1000 * 1000)
        };
    }

    LogLevel log_level_ = LogLevel::NONE;

    size_t max_io_threads_ = 64;

    size_t worker_max_request_batch_size_ = 3;
    uint64_t worker_wait_ns_before_switch_ = 1 * 1000 * 1000;
};

template <typename SchedulerType>
class FixedParallelIOWorkerPool: public IOWorkerPool {
public:
    using Options = FixedParallelIOWorkerPoolOptions;

    FixedParallelIOWorkerPool(const Options& opts,
        const std::vector<SchedulerType*>& schedulers):
            opts_(opts), logger_(getLogger("FixedParallelIOWorkerPool")),
            schedulers_(schedulers), shutdown_(false), workers_(nullptr) {}

    virtual void startup() override {
        LOG_INFO(logger_, "FixedParallelIOWorkerPool starting up");

        workers_ = std::make_unique<ThreadPool>(opts_.max_io_threads_);
        for (size_t i = 0; i < opts_.max_io_threads_; ++i) {
            worker_infos_.emplace_back(i, schedulers_, opts_);
        }

        for (auto iter = worker_infos_.begin(); iter != worker_infos_.end(); ++iter) {
            workers_->scheduleOrThrow([&worker_info = *iter, this]() {
                workerRountine(worker_info, logger_);
            });
        }

        LOG_INFO(logger_, "FixedParallelIOWorkerPool started up");
    }

    virtual void shutdown() override {
        LOG_INFO(logger_, "FixedParallelIOWorkerPool start shutting down");

        for (auto& worker_info : worker_infos_) {
            worker_info.shutdown_ = true;
        }

        workers_->wait();
        workers_ = nullptr;

        LOG_INFO(logger_, "FixedParallelIOWorkerPool shutted down");
    }

    virtual String status() const override {
        Poco::JSON::Object obj;
        obj.set("type", "fixed_parallel");
        obj.set("thread_num", worker_infos_.size());
        Poco::JSON::Array worker_status;
        for (const auto& worker_info : worker_infos_) {
            Poco::JSON::Object worker_info_obj;
            worker_info_obj.set("last_wait_time", worker_info.status_.last_wait_time_.load());
            worker_info_obj.set("total_wait_time", worker_info.status_.total_wait_time_.load());
            worker_info_obj.set("total_wait_count", worker_info.status_.total_wait_count_.load());
            worker_info_obj.set("total_executed_request", worker_info.status_.total_executed_request_.load());
            worker_info_obj.set("request_wait_time", worker_info.status_.request_total_wait_.load());
            worker_status.add(worker_info_obj);
        }
        obj.set("worker_status", worker_status);
        std::ostringstream os;
        obj.stringify(os);
        return os.str();
    }

private:
    struct WorkerInfo {
        struct Status {
            Status(): last_wait_time_(0), total_wait_time_(0), total_wait_count_(0),
                total_executed_request_(0), request_total_wait_(0) {}

            // Current status
            std::atomic<uint64_t> last_wait_time_;

            // Total statistics
            std::atomic<uint64_t> total_wait_time_;
            std::atomic<uint64_t> total_wait_count_;
            std::atomic<uint64_t> total_executed_request_;
            std::atomic<uint64_t> request_total_wait_;
        };

        WorkerInfo(uint32_t worker_id,
            const std::vector<SchedulerType*>& schedulers,
            const Options& opts):
                worker_id_(worker_id), schedulers_(schedulers), opts_(opts),
                shutdown_(false) {}

        const uint32_t worker_id_;
        const std::vector<SchedulerType*> schedulers_;
        const Options& opts_;

        std::atomic<bool> shutdown_;

        Status status_;
    };

    void workerRountine(WorkerInfo& worker_info, LoggerPtr logger) {
        LOG_DEBUG(logger, fmt::format("Worker {} started up", worker_info.worker_id_));
        SCOPE_EXIT({LOG_DEBUG(logger, fmt::format("Worker {} shutted down", worker_info.worker_id_));});

        const Options& opts = worker_info.opts_;
        const std::vector<SchedulerType*>& schedulers = worker_info.schedulers_;
        size_t last_choose_scheduler_idx = worker_info.worker_id_ % schedulers.size();
        SchedulerType* last_choose_scheduler = schedulers[last_choose_scheduler_idx];

        std::random_device r;
        std::default_random_engine re(r());
        std::uniform_int_distribution<size_t> scheduler_bias_dist(0, schedulers.size() - 1);

        Stopwatch wait_watch;

        while (!worker_info.shutdown_) {
            wait_watch.restart();
            auto requests = last_choose_scheduler->retrieveRequests(
                opts.worker_max_request_batch_size_, opts.worker_wait_ns_before_switch_);
            if (requests.empty()) {
                // Choose a random offset, and start search for scheduler for requests
                size_t scheduler_offset = scheduler_bias_dist(re);
                for (size_t i = 0; i < schedulers.size(); ++i) {
                    size_t scheduler_idx = (scheduler_offset + i) % schedulers.size();
                    requests = schedulers[scheduler_idx]->retrieveRequests(opts.worker_max_request_batch_size_,
                        opts.worker_wait_ns_before_switch_);

                    if (!requests.empty()) {
                        last_choose_scheduler_idx = scheduler_idx;
                        last_choose_scheduler = schedulers[scheduler_idx];
                        break;
                    }
                }
                if (requests.empty()) {
                    continue;
                }
            }

            worker_info.status_.last_wait_time_ = wait_watch.elapsedNanoseconds();
            worker_info.status_.total_wait_time_ += worker_info.status_.last_wait_time_;
            ++worker_info.status_.total_wait_count_;

            for (auto& request : requests) {
                ++worker_info.status_.total_executed_request_;
                worker_info.status_.request_total_wait_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - request->min_reach_submit_time_).count();

                // TODO: direct read to target buffer or buffer pool
                std::unique_ptr<char []> temp_buffer(new char[request->size_]);
                uint64_t readed = 0;
                try {
                    std::unique_ptr<RemoteFSReader> reader = request->file_->borrowReader(
                        request->offset_);
                    reader->seek(request->offset_);

                    readed = reader->read(temp_buffer.get(), request->size_);

                    request->file_->returnReader(std::move(reader));
                } catch (...) {
                    last_choose_scheduler->finalizeRequest(std::move(request), std::current_exception());

                    continue;
                }

                if (unlikely(opts_.log_level_ >= LogLevel::DEBUG)) {
                    LOG_INFO(logger, fmt::format("Finish RawRequest {}", request->str()));
                }

                last_choose_scheduler->finalizeRequest(std::move(request),
                    std::move(temp_buffer), readed);
            }
        }
    }

    const Options opts_;

    LoggerPtr logger_;

    std::vector<SchedulerType*> schedulers_;

    std::atomic<bool> shutdown_;
    std::unique_ptr<ThreadPool> workers_;
    std::list<WorkerInfo> worker_infos_;
};

}
