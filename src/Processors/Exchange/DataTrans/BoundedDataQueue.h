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
#include <common/MoveOrCopyIfThrow.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int STD_EXCEPTION;
}

template <typename T>
class BoundedDataQueue
{
private:
    template <typename E>
    inline void pushImpl(E && x)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() >= capacity && !is_closed)
        {
            LOG_TRACE(&Poco::Logger::get("BoundedDataQueue"), fmt::format("Queue is full and waiting, current size: {}, max size: {}", queue.size(), capacity));
            full_cv.wait(lock);
        }
        if (is_closed)
            throw Exception("Queue is closed", ErrorCodes::STD_EXCEPTION);
        queue.push(std::forward<E>(x));
        lock.unlock();
        empty_cv.notify_one();
    }

    template <typename E>
    inline bool tryPushImpl(E && x, UInt64 milliseconds = 0)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() >= capacity && !is_closed)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }
        if (is_closed)
            return false;
        queue.push(std::forward<E>(x));
        lock.unlock();
        empty_cv.notify_one();
        return true;
    }

public:
    explicit BoundedDataQueue(size_t capacity_ = 20) : capacity(capacity_)
    {
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
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty() && !is_closed)
        {
            if (ETIMEDOUT == empty_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }

        if (is_closed)
            return false;

        ::detail::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        lock.unlock();
        full_cv.notify_one();
        return true;
    }

    bool tryPopUntil(T & x, timespec millis_timestamp)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.empty() && !is_closed)
        {
            if (ETIMEDOUT == empty_cv.wait_until(lock, millis_timestamp))
                return false;
        }

        if (is_closed)
            return false;

        ::detail::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();
        lock.unlock();
        full_cv.notify_one();
        return true;
    }

    template <typename... Args>
    inline bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() >= capacity && !is_closed)
        {
            if (ETIMEDOUT == full_cv.wait_for(lock, milliseconds * 1000))
                return false;
        }
        if (is_closed)
            return false;
        queue.emplace(std::forward<Args>(args)...);
        lock.unlock();
        empty_cv.notify_one();
        return true;
    }

    template <typename... Args>
    bool tryEmplaceUntil(timespec millis_timestamp, Args &&... args)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        while (queue.size() >= capacity && !is_closed)
        {
            if (ETIMEDOUT == full_cv.wait_until(lock, millis_timestamp))
                return false;
        }
        if (is_closed)
            return false;
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
            queue.pop();
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
    std::queue<T> queue;
    bthread::Mutex mutex;
    bthread::ConditionVariable full_cv;
    bthread::ConditionVariable empty_cv;
    size_t capacity;
    bool is_closed = false;
};

}
