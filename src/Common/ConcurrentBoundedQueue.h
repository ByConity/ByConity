/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <queue>
#include <type_traits>
#include <condition_variable>
#include <mutex>
#include <optional>

#include <common/MoveOrCopyIfThrow.h>

/** A very simple thread-safe queue of limited size.
  * If you try to pop an item from an empty queue, the thread is blocked until the queue becomes nonempty or queue is finished.
  * If you try to push an element into an overflowed queue, the thread is blocked until space appears in the queue or queue is finished.
  */
template <typename T>
class ConcurrentBoundedQueue
{
private:
    std::queue<T> queue;

    mutable std::mutex queue_mutex;
    std::condition_variable push_condition;
    std::condition_variable pop_condition;

    bool is_finished = false;

    size_t max_fill = 0;

    template <typename ...Args>
    bool emplaceImpl(std::optional<UInt64> timeout_milliseconds, Args &&...args)
    {
        {
            std::unique_lock<std::mutex> queue_lock(queue_mutex);

            auto predicate = [&]() { return is_finished || queue.size() < max_fill; };

            if (timeout_milliseconds.has_value())
            {
                bool wait_result = push_condition.wait_for(queue_lock, std::chrono::milliseconds(timeout_milliseconds.value()), predicate);

                if (!wait_result)
                    return false;
            }
            else
            {
                push_condition.wait(queue_lock, predicate);
            }

            if (is_finished)
                return false;

            queue.emplace(std::forward<Args>(args)...);
        }

        pop_condition.notify_one();
        return true;
    }

    bool popImpl(T & x, std::optional<UInt64> timeout_milliseconds)
    {
        {
            std::unique_lock<std::mutex> queue_lock(queue_mutex);

            auto predicate = [&]() { return is_finished || !queue.empty(); };

            if (timeout_milliseconds.has_value())
            {
                bool wait_result = pop_condition.wait_for(queue_lock, std::chrono::milliseconds(timeout_milliseconds.value()), predicate);

                if (!wait_result)
                    return false;
            }
            else
            {
                pop_condition.wait(queue_lock, predicate);
            }

            if (is_finished && queue.empty())
                return false;

            detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop();
        }

        push_condition.notify_one();
        return true;
    }

public:
     explicit ConcurrentBoundedQueue(size_t max_fill_)
        : max_fill(max_fill_)
    {}

    /// Returns false if queue is finished
    [[nodiscard]] bool push(const T & x)
    {
        return emplace(x);
    }

    /// Returns false if queue is finished
    template <typename... Args>
    [[nodiscard]] bool emplace(Args &&... args)
    {
        emplaceImpl(std::nullopt /* timeout in milliseconds */, std::forward<Args...>(args...));
        return true;
    }

    /// Returns false if queue is finished and empty
    [[nodiscard]] bool pop(T & x)
    {
        return popImpl(x, std::nullopt /*timeout in milliseconds*/);
    }

    /// Returns false if queue is finished or object was not pushed during timeout
    [[nodiscard]] bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        return emplaceImpl(milliseconds, x);
    }

    /// Returns false if queue is finished or object was not emplaced during timeout
    template <typename... Args>
    [[nodiscard]] bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        return emplaceImpl(milliseconds, std::forward<Args...>(args...));
    }

    template <typename... Args>
    bool tryEmplaceUntil(timespec millis_timestamp, Args &&... args)
    {
        return emplaceUntilImpl(millis_timestamp, std::forward<Args...>(args...));
    }

    /// Returns false if queue is (finished and empty) or (object was not popped during timeout)
    [[nodiscard]] bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        return popImpl(x, milliseconds);
    }

    bool tryPopUntil(T & x, timespec millis_timestamp)
    {
        return popUntilImpl(x, millis_timestamp);
    }

    /// Returns size of queue
    size_t size() const
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        return queue.size();
    }

    /// Returns if queue is empty
    bool empty() const
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        return queue.empty();
    }

    /** Clear and finish queue
      * After that push operation will return false
      * pop operations will return values until queue become empty
      * Returns true if queue was already finished
      */
    bool finish()
    {
        bool was_finished_before = false;

        {
            std::lock_guard<std::mutex> lock(queue_mutex);

            if (is_finished)
                return true;

            was_finished_before = is_finished;
            is_finished = true;
        }

        pop_condition.notify_all();
        push_condition.notify_all();

        return was_finished_before;
    }

    /// Returns if queue is finished
    bool isFinished() const
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        return is_finished;
    }

    /// Returns if queue is finished and empty
    bool isFinishedAndEmpty() const
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        return is_finished && queue.empty();
    }

    /// Clear queue
    void clear()
    {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);

            if (is_finished)
                return;

            std::queue<T> empty_queue;
            queue.swap(empty_queue);
        }

        push_condition.notify_all();
    }

    /// Clear and finish queue
    void clearAndFinish()
    {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);

            std::queue<T> empty_queue;
            queue.swap(empty_queue);
            is_finished = true;
        }

        pop_condition.notify_all();
        push_condition.notify_all();

    }
};
