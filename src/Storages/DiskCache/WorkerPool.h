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
    WorkerPool(UInt32 num_workers, TaskQueue task_queue_)
        : workers_count(num_workers), is_running(false), task_queue(std::move(task_queue_)), busy_workers(0)
    {
    }

    ~WorkerPool()
    {
        std::unique_lock<std::mutex> lock(task_lock);
        is_running = false;
        task_cv.notify_all();
        lock.unlock();
        for (auto & thread : workers)
            thread.join();
    }

    void startup()
    {
        {
            std::lock_guard lock(task_lock);
            is_running = true;
        }

        while (workers.size() < workers_count)
            addThread();

        if (workers.size() > workers_count)
            workers.resize(workers_count);
    }

    void shutdown()
    {
        {
            std::lock_guard<std::mutex> lock(task_lock);
            is_running = false;
        }

        task_cv.notify_all();
        for (auto & worker : workers)
            worker.join();

        workers.clear();
    }

    template <typename F>
    void submitTask(const F & func)
    {
        {
            std::lock_guard<std::mutex> lock(task_lock);
            task_queue.emplace(std::move(func));
        }
        task_cv.notify_one();
    }

    void waitUntilAllFinished()
    {
        std::unique_lock<std::mutex> lock(task_lock);
        finished_cv.wait(lock, [&] { return busy_workers == 0 && task_queue.empty(); });
    }

    UInt32 numWorkers() const { return workers_count; }

    void setNumWorkers(UInt32 num) { workers_count = num; }

private:
    std::vector<std::thread> workers;
    UInt32 workers_count;
    bool is_running;

    TaskQueue task_queue;

    UInt32 busy_workers;

    std::mutex task_lock;

    std::condition_variable task_cv;
    std::condition_variable finished_cv;

    void addThread()
    {
        workers.emplace_back([this] {
            std::function<void(void)> task;

            while (true)
            {
                {
                    std::unique_lock<std::mutex> lock(task_lock);
                    task_cv.wait(lock, [&] { return !is_running || !task_queue.empty(); });
                    if (!is_running)
                        return;

                    task = std::move(task_queue.front());
                    task_queue.pop();
                    ++busy_workers;
                }

                task();
                {
                    std::lock_guard<std::mutex> lock(task_lock);
                    --busy_workers;
                }

                finished_cv.notify_one();
            }
        });
    }
};

}
