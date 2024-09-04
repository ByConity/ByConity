#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <linux/fs.h>
#include "Common/Priority.h"
#include "common/defines.h"
#include "Parsers/ExpressionElementParsers.h"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <benchmark/benchmark.h>
#pragma clang diagnostic pop

#include <Disks/DiskLocal.h>
#include <Disks/VolumeJBOD.h>
#include <IO/ReadBuffer.h>
#include <Storages/DiskCache/BigHash.h>
#include <Storages/DiskCache/BlockCache.h>
#include <Storages/DiskCache/BloomFilter.h>
#include <Storages/DiskCache/Bucket.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/DiskCacheSimpleStrategy.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/LruPolicy.h>
#include <Storages/DiskCache/NvmCache.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/WorkerPool.h>
#include <Storages/DiskCache/tests/BufferGen.h>
#include <Storages/DiskCache/tests/MockJobScheduler.h>
#include <Storages/DiskCache/tests/MultithreadTestUtil.h>
#include <benchmark/benchmark_util/BenchMarkConfig.h>
#include <Common/Stopwatch.h>
#include <Common/tests/gtest_global_context.h>
#include <common/types.h>
#include <Disks/IO/IOUringReader.h>
#include <IO/AsynchronousReadBufferFromFile.h>

namespace DB
{
namespace fs = std::filesystem;

class DiskIOBenchmark : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) final
    {
        generator.seed(100);

        file_sizes.resize(num_files);
        for (UInt32 i = 0; i < num_files; i++)
            file_sizes[i] = pages_per_file * page_size;

        total_file_size = 0;
        for (UInt32 i = 0; i < num_files; i++)
            total_file_size += file_sizes[i];

        ctx = getContext().context;
        fs::create_directory(dictionary_path);
    }

    void TearDown(const benchmark::State &) final 
    {
        ctx->shutdown();
    }

    const UInt32 num_files = 100'000;
    const UInt32 pages_to_read = 6'400'000;
    const UInt32 pages_per_buffer = 100'000;
    const UInt64 page_size = 4096;
    const UInt64 pages_per_file = 512;

    BenchmarkConfig benchmark_config;
    std::shared_ptr<Context> ctx;

    void *buffer;

    std::default_random_engine generator;
    std::vector<UInt64> file_sizes;
    UInt64 total_file_size;

    std::string dictionary_path = "./tmp_disk_randread_benchmark";
};


// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskIOBenchmark, WriteFiles)(benchmark::State & state)
{
    HybridCache::WorkerPool pool(benchmark_config.num_threads, {});
    pool.startup();

    for (auto _ : state)
    {
        auto workload = [&](UInt32 id) {
            std::default_random_engine local_generator;
            local_generator.seed(id);
            void *buffer = aligned_alloc(page_size,2 * pages_per_file * page_size);
            chassert(buffer);
            Int32 *buffer_as_int32 = static_cast<Int32*>(buffer);
            char *buffer_as_char = static_cast<char*>(buffer);
            for (UInt32 i = 0; i < 2 * pages_per_file * page_size / sizeof(Int32); i++)
                buffer_as_int32[i] = generator();

            UInt32 start_key = num_files / benchmark_config.num_threads * id;
            UInt32 end_key = start_key + num_files / benchmark_config.num_threads;
            if (id == benchmark_config.num_threads - 1)
                end_key = num_files;

            for (UInt32 i = start_key; i < end_key; i++)
            {
                auto file_path = fmt::format("{}/{}", dictionary_path, i);
                auto file = File(file_path, O_RDWR | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR);
                pwrite(file.getFd(), buffer_as_char + (generator() % pages_per_file) * page_size, file_sizes[i], 0);
                file.close();
            }

            free(buffer);
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, benchmark_config.num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());
    }
    state.SetItemsProcessed(state.iterations() * total_file_size);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskIOBenchmark, PreadReadFiles)(benchmark::State & state)
{
    HybridCache::WorkerPool pool(benchmark_config.num_threads, {});
    pool.startup();

    chassert(pages_to_read % pages_per_buffer == 0);
    chassert((pages_to_read / pages_per_buffer) % benchmark_config.num_threads == 0);

    std::vector<OpenedFileCache::OpenedFilePtr> files;
    std::vector<int> fds;
    for (UInt32 i = 0; i < num_files; i++)
    {
        files.emplace_back(OpenedFileCache::instance().get(fmt::format("{}/{}", dictionary_path, i), -1));
        fds.push_back(files[i]->getFD());
        posix_fadvise(files[i]->getFD(), 0, 0, POSIX_FADV_DONTNEED); // avoid the influence of os kernel page cache
    }

    for (auto _ : state)
    {
        auto workload = [&](UInt32 id) {
            std::default_random_engine local_generator;
            local_generator.seed(id);
            char *local_buffer = static_cast<char*>(aligned_alloc(page_size, pages_per_buffer * page_size));

            for (UInt32 i = 0; i < pages_to_read / pages_per_buffer / benchmark_config.num_threads; i++)
            {
                char *ptr = local_buffer;
                for (UInt32 j = 0; j < pages_per_buffer; j++)
                {
                    int rand_fd = local_generator() % num_files;
                    int offset = (local_generator() % pages_per_file) * page_size;
                    pread(fds[rand_fd], ptr, page_size, offset);
                    ptr += page_size;
                }
            }

            free(local_buffer);
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, benchmark_config.num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());
    }
    state.SetItemsProcessed(state.iterations() * pages_to_read * page_size);
}

#if USE_LIBURING
// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskIOBenchmark, SingleIOUringReadFiles)(benchmark::State & state)
{
    HybridCache::WorkerPool pool(benchmark_config.num_threads, {});
    pool.startup();

    chassert(pages_to_read % pages_per_buffer == 0);
    chassert((pages_to_read / pages_per_buffer) % benchmark_config.num_threads == 0);

    auto reader = std::make_unique<IOUringReader>(1024, false);
    chassert(reader->isSupported());

    std::vector<OpenedFileCache::OpenedFilePtr> files;
    std::vector<int> fds;
    for (UInt32 i = 0; i < num_files; i++)
    {
        files.emplace_back(OpenedFileCache::instance().get(fmt::format("{}/{}", dictionary_path, i), -1));
        fds.push_back(files[i]->getFD());
        posix_fadvise(files[i]->getFD(), 0, 0, POSIX_FADV_DONTNEED); // avoid the influence of os kernel page cache
    }

    for (auto _ : state)
    {
        auto workload = [&](UInt32 id) {
            std::default_random_engine local_generator;
            local_generator.seed(id);
            char *local_buffer = static_cast<char*>(aligned_alloc(page_size, pages_per_buffer * page_size));

            for (UInt32 i = 0; i < pages_to_read / pages_per_buffer / benchmark_config.num_threads; i++)
            {
                std::vector<std::future<IAsynchronousReader::Result>> results;
                char *ptr = local_buffer;
                for (UInt32 j = 0; j < pages_per_buffer; j++)
                {
                    int rand_fd = local_generator() % num_files;
                    int offset = (local_generator() % pages_per_file) * page_size;

                    IAsynchronousReader::Request request;
                    request.descriptor = std::make_shared<IAsynchronousReader::LocalFileDescriptor>(fds[rand_fd]);
                    request.buf = ptr;
                    request.size = page_size;
                    request.offset = offset;
                    request.priority = Priority{0};
                    request.ignore = 0;

                    results.emplace_back(reader->submit(request));
                    ptr += page_size;
                }

                for (auto &res:results) res.get();
            }

            free(local_buffer);
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, benchmark_config.num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());
    }
    state.SetItemsProcessed(state.iterations() * pages_to_read * page_size);
}


// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskIOBenchmark, MultiIOUringReadFiles)(benchmark::State & state)
{
    HybridCache::WorkerPool pool(benchmark_config.num_threads, {});
    pool.startup();

    chassert(pages_to_read % pages_per_buffer == 0);
    chassert((pages_to_read / pages_per_buffer) % benchmark_config.num_threads == 0);

    std::vector<OpenedFileCache::OpenedFilePtr> files;
    std::vector<int> fds;
    for (UInt32 i = 0; i < num_files; i++)
    {
        files.emplace_back(OpenedFileCache::instance().get(fmt::format("{}/{}", dictionary_path, i), -1));
        fds.push_back(files[i]->getFD());
        posix_fadvise(files[i]->getFD(), 0, 0, POSIX_FADV_DONTNEED); // avoid the influence of os kernel page cache
    }

    for (auto _ : state)
    {
        auto workload = [&](UInt32 id) {
            auto reader = std::make_unique<IOUringReader>(1024, false);
            chassert(reader->isSupported());

            std::default_random_engine local_generator;
            local_generator.seed(id);
            char *local_buffer = static_cast<char*>(aligned_alloc(page_size, pages_per_buffer * page_size));

            for (UInt32 i = 0; i < pages_to_read / pages_per_buffer / benchmark_config.num_threads; i++)
            {
                std::vector<std::future<IAsynchronousReader::Result>> results;
                char *ptr = local_buffer;
                for (UInt32 j = 0; j < pages_per_buffer; j++)
                {
                    int rand_fd = local_generator() % num_files;
                    int offset = (local_generator() % pages_per_file) * page_size;

                    IAsynchronousReader::Request request;
                    request.descriptor = std::make_shared<IAsynchronousReader::LocalFileDescriptor>(fds[rand_fd]);
                    request.buf = ptr;
                    request.size = page_size;
                    request.offset = offset;
                    request.priority = Priority{0};
                    request.ignore = 0;

                    results.emplace_back(reader->submit(request));
                    ptr += page_size;
                }

                for (auto &res:results) res.get();
            }

            free(local_buffer);
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, benchmark_config.num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());
    }
    state.SetItemsProcessed(state.iterations() * pages_to_read * page_size);
}
#endif


// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskIOBenchmark, CleanUpFiles)(benchmark::State & state)
{
    for (auto _ : state)
    {
        Stopwatch watch;
        fs::remove_all(fs::path{dictionary_path});
        state.SetIterationTime(watch.elapsedSeconds());
    }
    state.SetItemsProcessed(1);
}


// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(DiskIOBenchmark, WriteFiles)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->Iterations(1);
BENCHMARK_REGISTER_F(DiskIOBenchmark, PreadReadFiles)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
#if USE_LIBURING
BENCHMARK_REGISTER_F(DiskIOBenchmark, SingleIOUringReadFiles)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
BENCHMARK_REGISTER_F(DiskIOBenchmark, MultiIOUringReadFiles)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);
#endif
BENCHMARK_REGISTER_F(DiskIOBenchmark, CleanUpFiles)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->Iterations(1);
/*
CLICKHOUSE_BENCHMARK_THREADS=8 ~/ClickHouse/build/src/disk_randread_benchmark
Running /data24/huangzichun.0/ClickHouse/build/src/disk_randread_benchmark
Run on (256 X 3205.33 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x128)
  L1 Instruction 32 KiB (x128)
  L2 Unified 512 KiB (x128)
  L3 Unified 32768 KiB (x16)
Load Average: 14.44, 14.98, 26.84
----------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                  Time             CPU   Iterations UserCounters...
----------------------------------------------------------------------------------------------------------------------------
DiskIOBenchmark/WriteFiles/iterations:1/manual_time                    59347 ms        0.464 ms            1 items_per_second=3.5337G/s
DiskIOBenchmark/PreadReadFiles/min_time:3.000/manual_time              54221 ms        0.823 ms            1 items_per_second=483.473M/s
DiskIOBenchmark/SingleIOUringReadFiles/min_time:3.000/manual_time      43130 ms        0.703 ms            1 items_per_second=607.798M/s
DiskIOBenchmark/MultiIOUringReadFiles/min_time:3.000/manual_time        6524 ms        0.822 ms            1 items_per_second=4.0179G/s
DiskIOBenchmark/CleanUpFiles/iterations:1/manual_time                   6114 ms         6080 ms            1 items_per_second=0.163562/s
*/
// clang-format on
}
