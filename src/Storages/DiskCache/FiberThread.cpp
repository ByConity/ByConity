#include <Storages/DiskCache/FiberThread.h>
#include <common/logger_useful.h>

namespace DB::HybridCache
{

static thread_local FiberThread * current_fiber_thread = nullptr;

FiberThread * getCurrentFiberThread()
{
    return current_fiber_thread;
}

FiberThread::FiberThread(String name_, Options options_)
    : name(name_)
{
    th = std::make_unique<folly::ScopedEventBaseThread>(name);

    folly::fibers::FiberManager::Options opts;
    opts.stackSize = options_.stack_size ? options_.stack_size : Options::kDefaultStackSize;
    auto & eb = *th->getEventBase();
    fm = &folly::fibers::getFiberManager(eb, opts);

    eb.runInEventBaseThreadAndWait([this]() { current_fiber_thread = this; });

    addTaskRemote([this]() { LOG_INFO(log, "[{}] Started", name); });
}

void FiberThread::addTaskRemote(folly::Func func)
{
    {
        std::unique_lock<folly::fibers::TimedMutex> lk(drain_mutex);
        num_running++;
    }

    fm->addTaskRemote([this, func = std::move(func)]() mutable {
        func();
        std::unique_lock<folly::fibers::TimedMutex> lk(drain_mutex);
        chassert(num_running >= 0u);
        if (--num_running == 0u)
        {
            drain_cond.notifyAll();
        }
    });
}

void FiberThread::addTask(folly::Func func)
{
    {
        std::unique_lock<folly::fibers::TimedMutex> lk(drain_mutex);
        num_running++;
    }

    fm->addTask([this, func = std::move(func)]() mutable {
        func();
        std::unique_lock<folly::fibers::TimedMutex> lk(drain_mutex);
        chassert(num_running >= 0u);
        if (--num_running == 0u)
        {
            drain_cond.notifyAll();
        }
    });
}

void FiberThread::drain()
{
    std::unique_lock<folly::fibers::TimedMutex> lk(drain_mutex);
    if (num_running == 0)
    {
        return;
    }
    drain_cond.wait(lk);
}


FiberThread::~FiberThread()
{
    th.reset();
}

}
