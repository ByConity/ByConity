#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>
#include <common/types.h>


namespace DB::HybridCache
{

using TaskQueue = std::queue<std::function<void()>>;

class WorkerPool
{
public:
    WorkerPool(UInt32 num_workers, TaskQueue task_queue)
        : num_workers_(num_workers), is_running_(false), task_queue_(std::move(task_queue)), busy_workers_(0)
    {
    }

    ~WorkerPool()
    {
        std::unique_lock<std::mutex> lock(task_lock_);
        is_running_ = false;
        task_cv_.notify_all();
        lock.unlock();
        for (auto & thread : workers_)
            thread.join();
    }

    void startup()
    {
        {
            std::lock_guard lock(task_lock_);
            is_running_ = true;
        }

        while (workers_.size() < num_workers_)
        {
            addThread();
        }

        if (workers_.size() > num_workers_)
            workers_.resize(num_workers_);
    }

    void shutdown()
    {
        {
            std::lock_guard<std::mutex> lock(task_lock_);
            is_running_ = false;
        }

        task_cv_.notify_all();
        for (auto & worker : workers_)
            worker.join();

        workers_.clear();
    }

    template <typename F>
    void submitTask(const F & func)
    {
        {
            std::lock_guard<std::mutex> lock(task_lock_);
            task_queue_.emplace(std::move(func));
        }
        task_cv_.notify_one();
    }

    void waitUntilAllFinished()
    {
        std::unique_lock<std::mutex> lock(task_lock_);
        finished_cv_.wait(lock, [&] { return busy_workers_ == 0 && task_queue_.empty(); });
    }

    UInt32 numWorkers() const { return num_workers_; }

    void setNumWorkers(UInt32 num) { num_workers_ = num; }

private:
    std::vector<std::thread> workers_;
    UInt32 num_workers_;
    bool is_running_;

    TaskQueue task_queue_;

    UInt32 busy_workers_;

    std::mutex task_lock_;

    std::condition_variable task_cv_;
    std::condition_variable finished_cv_;

    void addThread()
    {
        workers_.emplace_back([this] {
            std::function<void(void)> task;

            while (true)
            {
                {
                    std::unique_lock<std::mutex> lock(task_lock_);
                    task_cv_.wait(lock, [&] { return !is_running_ || !task_queue_.empty(); });
                    if (!is_running_)
                        return;

                    task = std::move(task_queue_.front());
                    task_queue_.pop();
                    ++busy_workers_;
                }

                task();
                {
                    std::lock_guard<std::mutex> lock(task_lock_);
                    --busy_workers_;
                }

                finished_cv_.notify_one();
            }
        });
    }
};

}
