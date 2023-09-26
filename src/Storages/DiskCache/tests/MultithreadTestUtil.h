#pragma once

#include <thread>

#include <Storages/DiskCache/WorkerPool.h>

namespace DB::HybridCache
{
struct MultithreadTestUtil
{
    static void runThreadUntilFinish(WorkerPool * pool, UInt32 num_threads, const std::function<void(UInt32)> & workload, UInt32 repeat = 1)
    {
        pool->shutdown();
        pool->setNumWorkers(num_threads);
        for (UInt32 i = 0; i < repeat; i++)
        {
            pool->startup();

            for (UInt32 j = 0; j < pool->numWorkers(); j++)
                pool->submitTask([j, &workload] { workload(j); });

            pool->waitUntilAllFinished();
            pool->shutdown();
        }
    }

    static UInt32 hardwareConcurrency() noexcept { return std::thread::hardware_concurrency(); }
};
}
