/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <queue>
#include <type_traits>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include "common/logger_useful.h"
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/time.h>
#include <common/MoveOrCopyIfThrow.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int STD_EXCEPTION;
}

template <typename T, typename Controller = std::nullptr_t>
class BoundedDataQueue
{
    static constexpr bool use_controller = !std::is_same_v<Controller, std::nullptr_t>;

public:
    explicit BoundedDataQueue(size_t capacity_ = 20) : full_cv(), empty_cv(), capacity(capacity_)
    {
        static_assert(!use_controller);
    }
    BoundedDataQueue(size_t capacity_, Controller memory_controller_)
        : full_cv(), empty_cv(), capacity(capacity_), memory_controller(std::move(memory_controller_))
    {
        static_assert(use_controller);
    }

    inline void push(const T & x)
    {
        pushImpl(x);
    }
    inline void push(T && x)
    {
        pushImpl(std::move(x));
    }

    inline void pop(T & x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty() && !is_closed)
        {
            empty_cv.wait(lock);
        }
        if (is_closed)
            throw Exception("Queue is closed", ErrorCodes::STD_EXCEPTION);
        ::detail::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->decrease(x);
        }
        lock.unlock();
        full_cv.notify_one();
    }

    inline bool tryPush(const T & x, UInt64 timeout_ms = 0)
    {
        return tryPushImpl(x, timeout_ms);
    }
    inline bool tryPush(T && x, UInt64 timeout_ms = 0)
    {
        return tryPushImpl(std::move(x), timeout_ms);
    }

    inline bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        UInt64 timeout_ms_ts = time_in_milliseconds(std::chrono::system_clock::now()) + milliseconds;
        timespec timestamp{.tv_sec = time_t(timeout_ms_ts / 1000), .tv_nsec = long(timeout_ms_ts % 1000) * 1000000};
        return tryPopUntil(x, timestamp);
    }

    bool tryPopUntil(T & x, timespec timestamp)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty() && !is_closed)
        {
            if (ETIMEDOUT == empty_cv.wait_until(lock, timestamp))
                return false;
        }

        if (is_closed)
            return false;

        ::detail::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->decrease(x);
        }
        lock.unlock();
        full_cv.notify_one();
        return true;
    }

    template <typename... Args>
    inline bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        UInt64 timeout_ms_ts = time_in_milliseconds(std::chrono::system_clock::now()) + milliseconds;
        timespec timestamp{.tv_sec = time_t(timeout_ms_ts / 1000), .tv_nsec = long(timeout_ms_ts % 1000) * 1000000};
        return tryEmplaceUntil(timestamp, std::forward<Args>(args)...);
    }

    template <typename... Args>
    bool tryEmplaceUntil(timespec timestamp, Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (exceedLimit() && !is_closed)
        {
            if (ETIMEDOUT == full_cv.wait_until(lock, timestamp))
                return false;
        }
        if (is_closed)
            return false;
        if constexpr (use_controller)
        {
            if (memory_controller)
                (memory_controller->increase(std::forward<Args>(args)), ...);
        }
        queue.emplace(std::forward<Args>(args)...);
        lock.unlock();
        empty_cv.notify_one();
        return true;
    }

    inline size_t size()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return queue.size();
    }

    inline bool empty()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return queue.empty();
    }

    inline void clear()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (!queue.empty())
        {
            T x;
            ::detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop();
            if constexpr (use_controller)
            {
                if (memory_controller)
                    memory_controller->decrease(x);
            }
        }
        std::queue<T> empty_queue;
        std::swap(empty_queue, queue);
    }

    inline void setCapacity(size_t queue_capacity)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        capacity = queue_capacity;
    }

    inline bool close()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (is_closed)
            return false;
        is_closed = true;
        lock.unlock();
        empty_cv.notify_all();
        full_cv.notify_all();
        return true;
    }

    inline bool tryWaitUntilEmpty(UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() > 0 && !is_closed)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }
        return queue.size() == 0;
    }

    bool closed()
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        return is_closed;
    }

private:
    template <typename E>
    inline void pushImpl(E && x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (exceedLimit() && !is_closed)
        {
            LOG_TRACE(getLogger("BoundedDataQueue"), fmt::format("Queue is full and waiting, current size: {}, max size: {}", queue.size(), capacity));
            full_cv.wait(lock);
        }
        if (is_closed)
            throw Exception("Queue is closed", ErrorCodes::STD_EXCEPTION);
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->increase(x);
        }
        queue.push(std::forward<E>(x));
        lock.unlock();
        empty_cv.notify_one();
    }

    template <typename E>
    inline bool tryPushImpl(E && x, UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (exceedLimit() && !is_closed)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }
        if (is_closed)
            return false;
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->increase(x);
        }
        queue.push(std::forward<E>(x));
        lock.unlock();
        empty_cv.notify_one();
        return true;
    }

    ALWAYS_INLINE bool exceedLimit() const
    {
        if constexpr (use_controller)
            return queue.size() >= capacity || (queue.size() != 0 && memory_controller && memory_controller->exceedLimit());
        else
            return queue.size() >= capacity;
    }

    ALWAYS_INLINE void increase(T & x)
    {
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->increase(x);
        }
    }

    ALWAYS_INLINE void decrease(T & x)
    {
        if constexpr (use_controller)
        {
            if (memory_controller)
                memory_controller->decrease(x);
        }
    }

    std::queue<T> queue;
    bthread::Mutex mutex;
    bthread::ConditionVariable full_cv;
    bthread::ConditionVariable empty_cv;
    size_t capacity;
    Controller memory_controller = nullptr;
    bool is_closed = false;
};

}
