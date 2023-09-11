#include <IO/Scheduler/IOScheduler.h>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <fmt/format.h>
#include <base/defines.h>
#include <common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Common/Exception.h>
#include <Common/Exception.h>
#include "IO/Scheduler/ReaderCache.h"
#include <IO/Scheduler/NoopScheduler.h>
#include <IO/Scheduler/DeadlineScheduler.h>
#include <IO/Scheduler/FIFOScheduler.h>
#include <IO/Scheduler/FixedParallelIOWorkerPool.h>
#include <IO/Scheduler/DynamicParallelIOWorkerPool.h>
#include <IO/Scheduler/DispatchedIOWorkerPool.h>
#include <Poco/Types.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace IO::Scheduler {

IOSchedulerSet::Options::SchedulerType IOSchedulerSet::Options::str2SchedulerType(
        const String& str) {
    if (str == "noop") {
        return IOSchedulerSet::Options::SchedulerType::NOOP;
    } else if (str == "deadline") {
        return IOSchedulerSet::Options::SchedulerType::DEADLINE;
    } else if (str == "fifo") {
        return IOSchedulerSet::Options::SchedulerType::FIFO;
    } else {
        throw Exception("Unknown scheduler type str " + str,
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}

IOSchedulerSet::Options::WorkerPoolType IOSchedulerSet::Options::str2WorkerPoolType(
        const String& str) {
    if (str == "fixed_parallel") {
        return IOSchedulerSet::Options::WorkerPoolType::FIXED_PARALLEL;
    } else if (str == "dynamic_parallel") {
        return IOSchedulerSet::Options::WorkerPoolType::DYNAMIC_PARALLEL;
    } else if (str == "dispatched") {
        return IOSchedulerSet::Options::WorkerPoolType::DISPATCHED;
    } else if (str == "none") {
        return IOSchedulerSet::Options::WorkerPoolType::NONE;
    } else {
        throw Exception("Unknown worker pool type str " + str,
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}

IOSchedulerSet::Options::AllocationPolicyType IOSchedulerSet::Options::str2AllocationPolicy(
        const String& str) {
    if (str == "random") {
        return IOSchedulerSet::Options::AllocationPolicyType::RANDOM;
    } else if (str == "hash") {
        return IOSchedulerSet::Options::AllocationPolicyType::HASH;
    } else if (str == "hybrid") {
        return IOSchedulerSet::Options::AllocationPolicyType::HYBRID;
    } else {
        throw Exception("Unknown allocation policy type str " + str,
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}

IOSchedulerSet& IOSchedulerSet::instance() {
    static IOSchedulerSet scheduler_set;
    return scheduler_set;
}

IOScheduler* IOSchedulerSet::get(const String& path) {
    return instance().schedulerForPath(path);
}

void IOSchedulerSet::initialize(const Poco::Util::AbstractConfiguration& cfg,
        const String& prefix) {
    if (initialized_) {
        throw Exception("IOSchedulerSet already initialized", ErrorCodes::LOGICAL_ERROR);
    }

    if (enabled_ =  cfg.getBool(prefix + ".enabled", false); !enabled_) {
        initialized_ = true;
        return;
    }

    String scheduler_type_str = cfg.getString(prefix + ".scheduler_type", "deadline");
    String worker_pool_type_str = cfg.getString(prefix + ".worker_pool_type", "dynamic_parallel");
    String allocation_policy_str = cfg.getString(prefix + ".allocation_policy_type", "hash");

    Options opts;
    
    opts.scheduler_type_ = Options::str2SchedulerType(scheduler_type_str);
    opts.worker_pool_type_ = Options::str2WorkerPoolType(worker_pool_type_str);
    opts.allocation_policy_type_ = Options::str2AllocationPolicy(allocation_policy_str);

    DeadlineScheduler::Options ddl_opts_holder;
    switch (opts.scheduler_type_) {
        case Options::SchedulerType::DEADLINE: {
            ddl_opts_holder = DeadlineScheduler::Options::parseFromConfig(cfg, prefix + ".scheduler.deadline");
            opts.scheduler_opts_ = reinterpret_cast<void*>(&ddl_opts_holder);
            break;
        }
        case Options::SchedulerType::FIFO:
        case Options::SchedulerType::NOOP: {
            break;
        }
    }

    FixedParallelIOWorkerPoolOptions ddl_fixed_worker_pool_opts_holder;
    DynamicParallelIOWorkerPoolOptions ddl_dynamic_worker_pool_opts_holder;
    DispatchedIOWorkerPoolOptions dispatched_worker_pool_opts_holder;
    switch (opts.worker_pool_type_) {
        case Options::WorkerPoolType::FIXED_PARALLEL: {
            ddl_fixed_worker_pool_opts_holder = FixedParallelIOWorkerPoolOptions::parseFromConfig(
                cfg, prefix + ".worker_pool.fixed_parallel");
            opts.worker_pool_opts_ = reinterpret_cast<void*>(&ddl_fixed_worker_pool_opts_holder);
            break;
        }
        case Options::WorkerPoolType::DYNAMIC_PARALLEL: {
            ddl_dynamic_worker_pool_opts_holder = DynamicParallelIOWorkerPoolOptions::parseFromConfig(
                cfg, prefix + ".worker_pool.dynamic_parallel");
            opts.worker_pool_opts_ = reinterpret_cast<void*>(&ddl_dynamic_worker_pool_opts_holder);
            break;
        }
        case Options::WorkerPoolType::DISPATCHED: {
            dispatched_worker_pool_opts_holder = DispatchedIOWorkerPoolOptions::parseFromConfig(
                cfg, prefix + ".worker_pool.dispatched");
            opts.worker_pool_opts_ = reinterpret_cast<void*>(&dispatched_worker_pool_opts_holder);
            break;
        }
        case Options::WorkerPoolType::NONE: {
            break;
        }
    }

    switch (opts.allocation_policy_type_) {
        case Options::AllocationPolicyType::HASH: {
            opts.allocation_level0_buckets_ = cfg.getUInt(
                prefix + ".allocation.hash_allocation_policy.buckets", 128);
            break;
        }
        case Options::AllocationPolicyType::RANDOM: {
            opts.allocation_level0_buckets_ = cfg.getUInt(
                prefix + ".allocation.random_allocation_policy.buckets", 128);
            break;
        }
        case Options::AllocationPolicyType::HYBRID: {
            opts.allocation_level0_buckets_ = cfg.getUInt(
                prefix + ".allocation.hybrid_allocation_policy.level0_buckets", 8);
            opts.allocation_level1_buckets_ = cfg.getUInt(
                prefix + ".allocation.hybrid_allocation_policy.level1_buckets", 16);
            break;
        }
    }

    opts.reader_cache_opts_ = ReaderCache::Options::parseFromConfig(cfg, prefix + ".reader_cache");

    initialize(opts);
}

void IOSchedulerSet::initialize(const Options& opts) {
    if (initialized_) {
        throw Exception("IOSchedulerSet already initialized", ErrorCodes::LOGICAL_ERROR);
    }

    ReaderCache::instance().initialize(opts.reader_cache_opts_);

    opts_ = opts;

    scheduler_count_ = opts.allocation_level0_buckets_ * opts.allocation_level1_buckets_;

    for (size_t i = 0; i < scheduler_count_; ++i) {
        switch(opts.scheduler_type_) {
            case Options::SchedulerType::NOOP: {
                schedulers_.emplace_back(std::make_unique<NoopScheduler>());
                break;
            }
            case Options::SchedulerType::DEADLINE: {
                schedulers_.emplace_back(std::make_unique<DeadlineScheduler>(
                    *reinterpret_cast<const DeadlineScheduler::Options*>(opts.scheduler_opts_)
                ));
                break;
            }
            case Options::SchedulerType::FIFO: {
                schedulers_.emplace_back(std::make_unique<FIFOScheduler>());
                break;
            }
        }
    }

    switch(opts.worker_pool_type_) {
        case Options::WorkerPoolType::FIXED_PARALLEL: {
            switch(opts.scheduler_type_) {
                case Options::SchedulerType::DEADLINE: {
                    worker_pool_ = std::make_unique<FixedParallelIOWorkerPool<DeadlineScheduler>>(
                        *reinterpret_cast<const FixedParallelIOWorkerPoolOptions*>(opts.worker_pool_opts_),
                        schedulers<DeadlineScheduler>()
                    );
                    break;
                }
                case Options::SchedulerType::FIFO: {
                    worker_pool_ = std::make_unique<FixedParallelIOWorkerPool<FIFOScheduler>>(
                        *reinterpret_cast<const FixedParallelIOWorkerPoolOptions*>(opts.worker_pool_opts_),
                        schedulers<FIFOScheduler>()
                    );
                    break;
                }
                default:
                    throw Exception("Unsupported scheduler type when construct FixedParallelIOWorkerPool",
                        ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
            break;
        }
        case Options::WorkerPoolType::DYNAMIC_PARALLEL: {
            switch(opts.scheduler_type_) {
                case Options::SchedulerType::DEADLINE: {
                    worker_pool_ = std::make_unique<DynamicParallelIOWorkerPool<DeadlineScheduler>>(
                        *reinterpret_cast<const DynamicParallelIOWorkerPoolOptions*>(opts.worker_pool_opts_),
                        schedulers<DeadlineScheduler>()
                    );
                    break;
                }
                case Options::SchedulerType::FIFO: {
                    worker_pool_ = std::make_unique<DynamicParallelIOWorkerPool<FIFOScheduler>>(
                        *reinterpret_cast<const DynamicParallelIOWorkerPoolOptions*>(opts.worker_pool_opts_),
                        schedulers<FIFOScheduler>()
                    );
                    break;
                }
                default:
                    throw Exception("Unsupported scheduler type when construct DynamicParallelIOWorkerPool",
                        ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
            break;
        }
        case Options::WorkerPoolType::DISPATCHED: {
            switch(opts.scheduler_type_) {
                case Options::SchedulerType::DEADLINE: {
                    worker_pool_ = std::make_unique<DispatchedIOWorkerPool<DeadlineScheduler>>(
                        *reinterpret_cast<const DispatchedIOWorkerPoolOptions*>(opts.worker_pool_opts_),
                        schedulers<DeadlineScheduler>()
                    );
                    break;
                }
                case Options::SchedulerType::FIFO: {
                    worker_pool_ = std::make_unique<DispatchedIOWorkerPool<FIFOScheduler>>(
                        *reinterpret_cast<const DispatchedIOWorkerPoolOptions*>(opts.worker_pool_opts_),
                        schedulers<FIFOScheduler>()
                    );
                    break;
                }
                default:
                    throw Exception("Unsupported scheduler type when construct DispatchedIOWorkerPool",
                        ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
            break;
        }
        case Options::WorkerPoolType::NONE: {
            if (opts.worker_pool_type_ == Options::WorkerPoolType::NONE) {
                worker_pool_ = nullptr;
            } else {
                throw Exception("Unsupported scheduler type when construct None worker pool",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
            break;
        }
    }

    enabled_ = true;
    initialized_ = true;
}

void IOSchedulerSet::uninitialize() {
    schedulers_.clear();
    worker_pool_ = nullptr;
}

bool IOSchedulerSet::enabled() const {
    if (unlikely(!initialized_)) {
        return false;
    }

    return enabled_;
}

IOScheduler* IOSchedulerSet::schedulerForPath(const String& path) {
    switch(opts_.allocation_policy_type_) {
        case Options::AllocationPolicyType::HASH: {
            std::hash<String> hasher;
            return schedulers_[hasher(path) % scheduler_count_].get();
        }
        case Options::AllocationPolicyType::RANDOM: {
            std::uniform_int_distribution<size_t> dist(0, scheduler_count_ - 1);
            return schedulers_[dist(thread_local_rng)].get();
        }
        case Options::AllocationPolicyType::HYBRID: {
            std::hash<String> hasher;
            size_t bucket = hasher(path) % opts_.allocation_level0_buckets_; 
            std::uniform_int_distribution<size_t> dist(
                bucket * opts_.allocation_level0_buckets_,
                (bucket + 1) * opts_.allocation_level1_buckets_ - 1);

            return schedulers_[dist(thread_local_rng)].get();
        }
        default:
            throw Exception("Invalid IOSchedulerSet allocation policy",
                ErrorCodes::LOGICAL_ERROR);
    }
}

IOWorkerPool* IOSchedulerSet::workerPool() {
    return worker_pool_ == nullptr ? nullptr : worker_pool_.get();
}

}

}
