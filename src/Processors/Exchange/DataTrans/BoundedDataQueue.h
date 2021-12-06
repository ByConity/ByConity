#pragma once

#include <queue>
#include <type_traits>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>


namespace boundedQueue
{
template <typename T, bool is_nothrow_move_assignable = std::is_nothrow_move_assignable_v<T>>
struct MoveOrCopyIfThrow;

template <typename T>
struct MoveOrCopyIfThrow<T, true>
{
    void operator()(T && src, T & dst) const { dst = std::forward<T>(src); }
};

template <typename T>
struct MoveOrCopyIfThrow<T, false>
{
    void operator()(T && src, T & dst) const { dst = src; }
};

template <typename T>
void moveOrCopyIfThrow(T && src, T & dst)
{
    MoveOrCopyIfThrow<T>()(std::forward<T>(src), dst);
}
}

namespace CurrentMetrics
{
extern const Metric MemoryTrackingForExchange;
}

namespace DB
{
/// If memory track set as true, data type T should have function size() to calculate type size
template <typename T>
class BoundedDataQueue
{
public:
    explicit BoundedDataQueue(size_t capacity_ = 20) : full_cv(), empty_cv(), capacity(capacity_) { }

    void push(const T & x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            full_cv.wait(lock);
        }
        queue.push(x);
        empty_cv.notify_all();
    }

    void pop(T & x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty())
        {
            empty_cv.wait(lock);
        }

        boundedQueue::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        full_cv.notify_all();
    }

    bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        queue.push(x);
        empty_cv.notify_all();
        return true;
    }

    bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty())
        {
            if (ETIMEDOUT == empty_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        boundedQueue::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        full_cv.notify_all();
        return true;
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            full_cv.wait(lock);
        }

        queue.emplace(std::forward<Args>(args)...);
        empty_cv.notify_all();
    }

    template <typename... Args>
    bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        queue.emplace(std::forward<Args>(args)...);
        empty_cv.notify_all();
        return true;
    }

    size_t size()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return queue.size();
    }

    bool empty()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return queue.empty();
    }

    void clear()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        size_t data_size = 0;
        while (!queue.empty())
        {
            data_size += queue.front().size();
            queue.pop();
        }
        std::queue<T> empty_queue;
        std::swap(empty_queue, queue);
    }

    void setCapacity(size_t queue_capacity)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        capacity = queue_capacity;
    }

private:
    std::queue<T> queue;
    bthread::Mutex mutex;
    bthread::ConditionVariable full_cv;
    bthread::ConditionVariable empty_cv;
    size_t capacity;
};


template <typename T>
class TrackBoundedQueue
{
public:
    explicit TrackBoundedQueue(std::shared_ptr<MemoryTracker> memory_tracker_ = nullptr, size_t capacity_ = 20)
        : full_cv(), empty_cv(), capacity(capacity_), memory_tracker(memory_tracker_)
    {
        if (memory_tracker)
            memory_tracker->setMetric(CurrentMetrics::MemoryTrackingForExchange);
        else
        {
            if (auto tracker = CurrentThread::getMemoryTracker())
                tracker->setMetric(CurrentMetrics::MemoryTrackingForExchange);
        }
    }

    ~TrackBoundedQueue()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        size_t data_size = 0;
        while (!queue.empty())
        {
            data_size += queue.front().size();
            queue.pop();
        }

        if (memory_tracker)
            memory_tracker->free(data_size);
        else
            CurrentMemoryTracker::free(data_size);
    }

    void push(const T & x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            full_cv.wait(lock);
        }
        queue.push(x);
        if (memory_tracker)
            memory_tracker->alloc(x.size());
        else
            CurrentMemoryTracker::alloc(x.size());
        empty_cv.notify_all();
    }

    void pop(T & x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty())
        {
            empty_cv.wait(lock);
        }

        boundedQueue::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        if (memory_tracker)
            memory_tracker->free(x.size());
        else
            CurrentMemoryTracker::free(x.size());
        full_cv.notify_all();
    }

    bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        queue.push(x);
        if (memory_tracker)
            memory_tracker->alloc(x.size());
        else
            CurrentMemoryTracker::alloc(x.size());
        empty_cv.notify_all();
        return true;
    }

    bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty())
        {
            if (ETIMEDOUT == empty_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        boundedQueue::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        if (memory_tracker)
            memory_tracker->free(x.size());
        else
            CurrentMemoryTracker::free(x.size());
        full_cv.notify_all();
        return true;
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            full_cv.wait(lock);
        }

        queue.emplace(std::forward<Args>(args)...);
        if (memory_tracker)
            memory_tracker->alloc(queue.back().size());
        else
            CurrentMemoryTracker::alloc(queue.back().size());
        empty_cv.notify_all();
    }

    template <typename... Args>
    bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() == capacity)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        queue.emplace(std::forward<Args>(args)...);
        if (memory_tracker)
            memory_tracker->alloc(queue.back().size());
        else
            CurrentMemoryTracker::alloc(queue.back().size());
        empty_cv.notify_all();
        return true;
    }

    size_t size()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return queue.size();
    }

    bool empty()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return queue.empty();
    }

    void clear()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        size_t data_size = 0;
        while (!queue.empty())
        {
            data_size += queue.front().size();
            queue.pop();
        }
        std::queue<T> empty_queue;
        std::swap(empty_queue, queue);

        if (memory_tracker)
            memory_tracker->free(data_size);
        else
            CurrentMemoryTracker::free(data_size);
    }

    void setCapacity(size_t queue_capacity)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        capacity = queue_capacity;
    }

private:
    std::queue<T> queue;
    bthread::Mutex mutex;
    bthread::ConditionVariable full_cv;
    bthread::ConditionVariable empty_cv;
    size_t capacity;
    std::shared_ptr<MemoryTracker> memory_tracker;
};
}
