#pragma once

#include <folly/IntrusiveList.h>
#include <folly/Synchronized.h>
#include <folly/fibers/Baton.h>
#include <folly/fibers/TimedMutex.h>

#include <Common/Exception.h>
#include <common/defines.h>

namespace DB::HybridCache
{

using folly::fibers::Baton;
using folly::fibers::TimedMutex;
using SharedMutex = folly::fibers::TimedRWMutex<Baton>;

/**
 * ConditionVariable is a synchronization primitive that can be used to block
 * until a condition is satisfied. This is the implementation of
 * ConditionVariable supporting both thread and fiber waiters.
 */
class ConditionVariable
{
public:
    struct Waiter
    {
        Waiter() = default;

        // The baton will be signalled when this condition variable is notified
        Baton baton;

    private:
        friend ConditionVariable;
        folly::SafeIntrusiveListHook hook;
    };

    // addWaiter adds a waiter to the wait list with the waitList lock held,
    // such that the actual wait can be made asynchronously with the baton
    void addWaiter(Waiter * waiter)
    {
        chassert(waiter);
        auto wait_list_lock = wait_list.wlock();
        wait_list_lock->push_back(*waiter);
        num_waiters++;
    }

    size_t numWaiters() const { return num_waiters; }

    void wait(std::unique_lock<TimedMutex> & lock)
    {
        Waiter waiter;
        addWaiter(&waiter);
        lock.unlock();
        waiter.baton.wait();
        lock.lock();
    }

    // Expect to be protected by the same lock
    void notifyOne()
    {
        auto wait_list_lock = wait_list.wlock();
        auto & list = *wait_list_lock;

        if (list.empty())
            return;

        auto waiter = &list.front();
        list.pop_front();
        chassert(num_waiters > 0u);
        num_waiters--;
        waiter->baton.post();
    }

    // Expect to be protected by the same lock
    void notifyAll()
    {
        auto wait_list_lock = wait_list.wlock();
        auto & list = *wait_list_lock;

        if (list.empty())
            return;

        while (!list.empty())
        {
            auto waiter = &list.front();
            list.pop_front();
            chassert(num_waiters > 0u);
            num_waiters--;
            waiter->baton.post();
        }
    }

private:
    using WaiterList = folly::SafeIntrusiveList<Waiter, &Waiter::hook>;
    folly::Synchronized<WaiterList, SharedMutex> wait_list;
    size_t num_waiters = 0;
};

}
