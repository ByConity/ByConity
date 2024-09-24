#pragma once

#include <Common/ThreadPool.h>
#include <common/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>

#include <future>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using Priority = int;

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously.
template <typename Result, typename Callback = std::function<Result()>>
using ThreadPoolCallbackRunnerUnsafe = std::function<std::future<Result>(Callback &&, Priority)>;

/// NOTE When using ThreadPoolCallbackRunnerUnsafe you MUST ensure that all async tasks are finished
/// before any objects they may use are destroyed.
/// A common mistake is capturing some some local objects in lambda and passing it to the runner.
/// In case of exception, these local objects will be destroyed before scheduled tasks are finished.

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrowOnError()'.
template <typename Result, typename Callback = std::function<Result()>>
ThreadPoolCallbackRunnerUnsafe<Result, Callback> threadPoolCallbackRunnerUnsafe(ThreadPool & pool, const std::string & thread_name)
{
    return [my_pool = &pool, thread_group = CurrentThread::getGroup(), thread_name](Callback && callback, Priority priority) mutable -> std::future<Result>
    {
        auto task = std::make_shared<std::packaged_task<Result()>>([thread_group, thread_name, my_callback = std::move(callback)]() mutable -> Result
        {
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);

            SCOPE_EXIT(
            {
                {
                    /// Release all captured resources before detaching thread group
                    /// Releasing has to use proper memory tracker which has been set here before callback

                    [[maybe_unused]] auto tmp = std::move(my_callback);
                }

                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
            });

            setThreadName(thread_name.data());

            return my_callback();
        });

        auto future = task->get_future();

        /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
        /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
        /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
        my_pool->scheduleOrThrowOnError([my_task = std::move(task)]{ (*my_task)(); }, priority);

        return future;
    };
}

template <typename Result, typename T>
std::future<Result> scheduleFromThreadPoolUnsafe(T && task, ThreadPool & pool, const std::string & thread_name, Priority priority = {})
{
    auto schedule = threadPoolCallbackRunnerUnsafe<Result, T>(pool, thread_name);
    return schedule(std::move(task), priority); /// NOLINT
}

}
