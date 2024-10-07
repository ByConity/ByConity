#pragma once

#include <Common/Logger.h>
#include <Storages/DiskCache/ConditionVariable.h>
#include <Common/Exception.h>
#include <common/defines.h>

#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <Poco/Logger.h>

#include <atomic>
#include <memory>

namespace DB::HybridCache
{

class FiberThread;

// @return the current FiberThread context if running on FiberThread. nullptr
// otherwise.
FiberThread * getCurrentFiberThread();

/**
 * FiberThread is a wrapper class that wraps folly::ScopedEventBaseThread and
 * FiberManager. The purpose of FiberThread is to start a new thread running
 * an EventBase loop and FiberManager loop along with providing an deligate
 * interface to add tasks to the FiberManager.
 *
 * FiberThread is not CopyConstructible nor CopyAssignable nor
 * MoveConstructible nor MoveAssignable.
 */
class FiberThread
{
public:
    struct Options
    {
        static constexpr size_t kDefaultStackSize{64 * 1024};
        constexpr Options() { }

        explicit Options(size_t size) : stack_size(size) { }

        /**
         * Maximum stack size for fibers which will be used for executing all the
         * tasks.
         */
        size_t stack_size{kDefaultStackSize};
    };

    /**
     * Initializes with current EventBaseManager and passed-in thread name.
     */
    explicit FiberThread(String name, Options options = Options());

    ~FiberThread();

    /**
     * Add the passed-in task to the FiberManager.
     *
     * @param func Task functor; must have a signature of `void func()`.
     *             The object will be destroyed once task execution is complete.
     */
    void addTaskRemote(folly::Func func);

    /**
     * Add the passed-in task to the FiberManager.
     * Must be called from FiberManager's thread.
     *
     * @param func Task functor; must have a signature of `void func()`.
     *             The object will be destroyed once task execution is complete.
     */
    void addTask(folly::Func func);

    /**
     * Wait until tasks are drained
     */
    void drain();

    FiberThread(FiberThread && other) = delete;
    FiberThread & operator=(FiberThread && other) = delete;

    FiberThread(const FiberThread & other) = delete;
    FiberThread & operator=(const FiberThread & other) = delete;

private:
    LoggerPtr log = getLogger("FiberThread");

    String name;

    // Actual worker thread running EventBase and FiberManager loop
    std::unique_ptr<folly::ScopedEventBaseThread> th;

    // FiberManager which are driven by the thread
    folly::fibers::FiberManager * fm;

    size_t num_running{0};
    folly::fibers::TimedMutex drain_mutex;
    ConditionVariable drain_cond;
};

}
