#include <algorithm>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
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

namespace DB
{
using HybridCache::BigHash;
using HybridCache::BlockCache;
using HybridCache::BloomFilter;
using HybridCache::BucketEntry;
using HybridCache::Buffer;
using HybridCache::Device;

namespace fs = std::filesystem;

class BigHashBenchmark : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) final
    {
        key_permutation.resize(num_keys);
        for (UInt32 i = 0; i < num_keys; i++)
            key_permutation[i] = i;
        std::shuffle(key_permutation.begin(), key_permutation.end(), generator);

        HybridCache::BufferGen buffer_generator;
        for (UInt32 i = 0; i < 100; i++)
            buffer_permutation.emplace_back(buffer_generator.gen(100, 1024));

        file_path = "./Benchmark-Test";

        fs::create_directory(file_path);

        file_path = fmt::format("{}/BigHashBenchmark-{}", file_path, ::getpid());

        std::vector<File> f_vec;
        f_vec.emplace_back(File(file_path, O_RDWR | O_CREAT, S_IRWXU));

        fs::resize_file(fs::path{file_path}, cache_size);

        device = HybridCache::createDirectIoFileDevice(std::move(f_vec), cache_size, 4096, 0, 0);
    }

    void TearDown(const benchmark::State &) final { fs::remove_all(fs::path{file_path}); }

    const UInt32 num_keys = 1'000'000;

    std::string file_path;

    const UInt64 cache_size = 1 * GiB;

    std::unique_ptr<Device> device;

    std::default_random_engine generator;
    std::vector<Int64> key_permutation;
    std::vector<Buffer> buffer_permutation;
};

class NvmCacheBenchmark : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) final
    {
        key_permutation.resize(num_keys);
        for (UInt32 i = 0; i < num_keys; i++)
            key_permutation[i] = i;
        std::shuffle(key_permutation.begin(), key_permutation.end(), generator);

        HybridCache::BufferGen buffer_generator;
        for (UInt32 i = 0; i < 100; i++)
            buffer_permutation.emplace_back(buffer_generator.gen(100, 2000)); // 1M ~ 4M

        file_path = "./Benchmark-Test";

        fs::create_directory(file_path);

        file_path = fmt::format("{}/NvmCacheBenchmark-{}", file_path, ::getpid());
    }

    void TearDown(const benchmark::State &) final { fs::remove_all(fs::path{file_path}); }

    void simpleInsertBase(
        benchmark::State & state,
        std::function<std::unique_ptr<HybridCache::Device>(std::vector<File> &, const UInt64)> create_device,
        std::function<std::unique_ptr<HybridCache::JobScheduler>()> create_scheduler)
    {
        HybridCache::WorkerPool pool(BenchmarkConfig::num_threads, {});
        pool.startup();

        for (auto _ : state)
        {
            std::vector<File> f_vec;
            f_vec.emplace_back(File(file_path, O_RDWR | O_CREAT | O_DIRECT, S_IRWXU));
            fs::resize_file(fs::path{file_path}, cache_size);
            auto config = NvmCache::Config();
            config.device = create_device(f_vec, cache_size);
            config.scheduler = create_scheduler();
            config.max_concurrent_inserts = 1'000'000;
            config.max_parcel_memory = 1'000 * GiB;

            auto bh_config = BigHash::Config();
            bh_config.cache_size = 5 * GiB;
            bh_config.cache_start_offset = 45 * GiB;
            bh_config.device = config.device.get();

            auto bc_config = BlockCache::Config();
            bc_config.cache_size = 45 * GiB;
            bc_config.num_in_mem_buffers = 2;
            bc_config.device = config.device.get();
            bc_config.scheduler = config.scheduler.get();
            bc_config.eviction_policy = std::make_unique<HybridCache::LruPolicy>(bc_config.getNumberRegions());

            NvmCache::Pair pair{nullptr, nullptr, 2048};
            pair.small_item_cache = std::make_unique<BigHash>(std::move(bh_config));
            pair.large_item_cache = std::make_unique<BlockCache>(std::move(bc_config));
            config.pairs.push_back(std::move(pair));

            auto * nvm_cache = new NvmCache(std::move(config));

            auto workload = [&](UInt32 id) {
                UInt32 start_key = num_keys / BenchmarkConfig::num_threads * id;
                UInt32 end_key = start_key + num_keys / BenchmarkConfig::num_threads;

                for (UInt32 i = start_key; i < end_key; i++)
                    nvm_cache->insertAsync(
                        HybridCache::makeHashKey(&key_permutation[i], sizeof(Int64)),
                        buffer_permutation[i % 100].view(),
                        [](HybridCache::Status status, HybridCache::HashedKey) {
                            if (status != HybridCache::Status::Ok)
                                printf("insert failed %d\n", status);
                        },
                        HybridCache::EngineTag::UncompressedCache);
            };

            Stopwatch watch;
            HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, BenchmarkConfig::num_threads, workload);
            nvm_cache->flush();
            state.SetIterationTime(watch.elapsedSeconds());
            delete nvm_cache;
        }
        state.SetItemsProcessed(state.iterations() * num_keys);
    }

    void simpleLookupBase(
        benchmark::State & state,
        std::function<std::unique_ptr<HybridCache::Device>(std::vector<File> &, const UInt64)> create_device,
        std::function<std::unique_ptr<HybridCache::JobScheduler>()> create_scheduler)
    {
        HybridCache::WorkerPool pool(BenchmarkConfig::num_threads, {});
        pool.startup();

        for (auto _ : state)
        {
            std::vector<File> f_vec;
            f_vec.emplace_back(File(file_path, O_RDWR | O_CREAT | O_DIRECT, S_IRWXU));
            fs::resize_file(fs::path{file_path}, cache_size);
            auto config = NvmCache::Config();
            config.device = create_device(f_vec, cache_size);
            config.scheduler = create_scheduler();
            config.max_concurrent_inserts = 1'000'000;
            config.max_parcel_memory = 1'000 * GiB;

            auto bh_config = BigHash::Config();
            bh_config.cache_size = 5 * GiB;
            bh_config.cache_start_offset = 45 * GiB;
            bh_config.device = config.device.get();

            auto bc_config = BlockCache::Config();
            bc_config.cache_size = 45 * GiB;
            bc_config.num_in_mem_buffers = 2;
            bc_config.device = config.device.get();
            bc_config.scheduler = config.scheduler.get();
            bc_config.eviction_policy = std::make_unique<HybridCache::LruPolicy>(bc_config.getNumberRegions());

            NvmCache::Pair pair{nullptr, nullptr, 2048};
            pair.small_item_cache = std::make_unique<BigHash>(std::move(bh_config));
            pair.large_item_cache = std::make_unique<BlockCache>(std::move(bc_config));
            config.pairs.push_back(std::move(pair));

            auto * nvm_cache = new NvmCache(std::move(config));

            auto insert_workload = [&](UInt32 id) {
                UInt32 start_key = num_keys / BenchmarkConfig::num_threads * id;
                UInt32 end_key = start_key + num_keys / BenchmarkConfig::num_threads;

                for (UInt32 i = start_key; i < end_key; i++)
                    nvm_cache->insertAsync(
                        HybridCache::makeHashKey(&key_permutation[i], sizeof(Int64)),
                        buffer_permutation[i % 100].view(),
                        [](HybridCache::Status status, HybridCache::HashedKey) {
                            if (status != HybridCache::Status::Ok)
                                printf("insert faield %d\n", status);
                        },
                        HybridCache::EngineTag::UncompressedCache);
            };

            auto lookup_workload = [&](UInt32 id) {
                UInt32 start_key = num_keys / BenchmarkConfig::num_threads * id;
                UInt32 end_key = start_key + num_keys / BenchmarkConfig::num_threads;

                for (UInt32 i = start_key; i < end_key; i++)
                    nvm_cache->lookupAsync(
                        HybridCache::makeHashKey(&key_permutation[i], sizeof(Int64)),
                        [](HybridCache::Status status, HybridCache::HashedKey, HybridCache::Buffer) {
                            if (status != HybridCache::Status::Ok)
                                printf("lookup failed %d\n", status);
                        },
                        HybridCache::EngineTag::UncompressedCache);
            };

            HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, BenchmarkConfig::num_threads, insert_workload);
            nvm_cache->flush();

            Stopwatch watch;
            HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, BenchmarkConfig::num_threads, lookup_workload);
            nvm_cache->flush();
            state.SetIterationTime(watch.elapsedSeconds());
            delete nvm_cache;
        }
        state.SetItemsProcessed(state.iterations() * num_keys);
    }

    const UInt32 num_keys = 1'000'000;

    std::string file_path;

    const UInt64 cache_size = 50 * GiB;

    std::default_random_engine generator;
    std::vector<Int64> key_permutation;
    std::vector<Buffer> buffer_permutation;
};

class NvmCacheBenchmarkPsync : public NvmCacheBenchmark
{
};

class NvmCacheBenchmarkIoUring : public NvmCacheBenchmark
{
};

class DiskCacheLRUBenchmark : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) final
    {
        key_permutation.resize(num_keys);
        for (UInt32 i = 0; i < num_keys; i++)
            key_permutation[i] = i;
        std::shuffle(key_permutation.begin(), key_permutation.end(), generator);

        HybridCache::BufferGen buffer_generator;
        for (UInt32 i = 0; i < 100; i++)
            buffer_permutation.emplace_back(buffer_generator.gen(100, 1024));

        ctx = getContext().context;
        fs::create_directory("./tmp");
        fs::create_directory("./tmp/disk_cache_v1/");
    }

    void TearDown(const benchmark::State &) final { ctx->shutdown(); }

    static DB::VolumePtr newDiskVolume()
    {
        fs::create_directory("./tmp/local1/");
        DB::Disks disks;
        disks.emplace_back(std::make_shared<DB::DiskLocal>("local1", "./tmp/local1/", DB::DiskStats{}));
        return std::make_shared<DB::VolumeJBOD>("local_disk", disks, disks.front()->getName(), 0, false);
    }

    const UInt32 num_keys = 1'000'000;

    std::shared_ptr<Context> ctx;

    std::default_random_engine generator;
    std::vector<Int64> key_permutation;
    std::vector<Buffer> buffer_permutation;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(NvmCacheBenchmarkPsync, SimpleInsert)(benchmark::State & state)
{
    auto create_device = [](std::vector<File> & f_vec, const UInt64 cache_size) {
        return HybridCache::createDirectIoFileDevice(std::move(f_vec), cache_size, 4096, 0, 0);
    };
    auto create_scheduler = [] { return HybridCache::createOrderedThreadPoolJobScheduler(1, 1, 10); };

    simpleInsertBase(state, create_device, create_scheduler);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(NvmCacheBenchmarkIoUring, SimpleInsert)(benchmark::State & state)
{
    auto create_device = [](std::vector<File> & f_vec, const UInt64 cache_size) {
        return HybridCache::createDirectIoFileDevice(std::move(f_vec), cache_size, 4096, 0, 0, HybridCache::IoEngine::IoUring, 256);
    };
    auto create_scheduler = [] { return HybridCache::createFiberRequestScheduler(1, 1, 256, 256, 0, 10); };

    simpleInsertBase(state, create_device, create_scheduler);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(NvmCacheBenchmarkPsync, SimpleLookup)(benchmark::State & state)
{
    auto create_device = [](std::vector<File> & f_vec, const UInt64 cache_size) {
        return HybridCache::createDirectIoFileDevice(std::move(f_vec), cache_size, 4096, 0, 0);
    };
    auto create_scheduler = [] { return HybridCache::createOrderedThreadPoolJobScheduler(1, 1, 10); };

    simpleLookupBase(state, create_device, create_scheduler);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(NvmCacheBenchmarkIoUring, SimpleLookup)(benchmark::State & state)
{
    auto create_device = [](std::vector<File> & f_vec, const UInt64 cache_size) {
        return HybridCache::createDirectIoFileDevice(std::move(f_vec), cache_size, 4096, 0, 0, HybridCache::IoEngine::IoUring, 256);
    };
    auto create_scheduler = [] { return HybridCache::createFiberRequestScheduler(1, 1, 256, 256, 0, 10); };

    simpleLookupBase(state, create_device, create_scheduler);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BigHashBenchmark, SimpleInsert)(benchmark::State & state)
{
    HybridCache::WorkerPool pool(BenchmarkConfig::num_threads, {});
    pool.startup();

    for (auto _ : state)
    {
        auto config = BigHash::Config();
        config.cache_size = cache_size;
        config.device = device.get();

        const uint32_t item_min_size = BucketEntry::computeSize(sizeof(Int64), 100);

        auto bf = BloomFilter::makeBloomFilter(cache_size / config.bucket_size, config.bucket_size / item_min_size, 0.1);
        config.bloom_filters = std::make_unique<BloomFilter>(std::move(bf));

        auto * big_hash = new BigHash(std::move(config));

        auto workload = [&](UInt32 id) {
            UInt32 start_key = num_keys / BenchmarkConfig::num_threads * id;
            UInt32 end_key = start_key + num_keys / BenchmarkConfig::num_threads;

            for (UInt32 i = start_key; i < end_key; i++)
                big_hash->insert(HybridCache::makeHashKey(&key_permutation[i], sizeof(Int64)), buffer_permutation[i % 100].view());
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, BenchmarkConfig::num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());
        delete big_hash;
    }
    state.SetItemsProcessed(state.iterations() * num_keys);
}


// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskCacheLRUBenchmark, SimpleInsert)(benchmark::State & state)
{
    DB::DiskCacheSettings settings;

    HybridCache::WorkerPool pool(BenchmarkConfig::num_threads, {});
    pool.startup();

    for (auto _ : state)
    {
        fs::remove_all("./tmp/");
        fs::create_directories("./tmp/");
        auto volume = newDiskVolume();
        IDiskCache::init(*getContext().context);

        DiskCacheLRU * cache = new DiskCacheLRU(
            "test", volume, getContext().context->getDiskCacheThrottler(), settings, std::make_shared<DiskCacheSimpleStrategy>(settings));
        cache->load();

        auto workload = [&](UInt32 id) {
            UInt32 start_key = num_keys / BenchmarkConfig::num_threads * id;
            UInt32 end_key = start_key + num_keys / BenchmarkConfig::num_threads;

            for (UInt32 i = start_key; i < end_key; i++)
            {
                auto & buffer = buffer_permutation[i % 100];
                auto read_buffer = ReadBuffer(reinterpret_cast<char *>(buffer.data()), buffer.size());
                cache->set(fmt::format("{}/", key_permutation[i]), read_buffer, buffer.size(), false);
            }
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, BenchmarkConfig::num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());


        cache->shutdown();
        delete cache;

        fs::remove_all("./tmp/");
        DB::IDiskCache::close();
    }
    state.SetItemsProcessed(state.iterations() * num_keys);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
BENCHMARK_REGISTER_F(NvmCacheBenchmarkPsync, SimpleInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

BENCHMARK_REGISTER_F(NvmCacheBenchmarkIoUring, SimpleInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

BENCHMARK_REGISTER_F(NvmCacheBenchmarkPsync, SimpleLookup)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

BENCHMARK_REGISTER_F(NvmCacheBenchmarkIoUring, SimpleLookup)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

BENCHMARK_REGISTER_F(BigHashBenchmark, SimpleInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

BENCHMARK_REGISTER_F(DiskCacheLRUBenchmark, SimpleInsert)
    ->Unit(benchmark::kMillisecond)
    ->UseManualTime()
    ->MinTime(3);

/*
Run on (80 X 2800 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x40)
  L1 Instruction 32 KiB (x40)
  L2 Unified 1024 KiB (x40)
  L3 Unified 28160 KiB (x2)
Load Average: 2.71, 2.21, 2.04

Case Info:
Cache size 1G, Bucket size: 4096, Key Num: 1'000'000, Value size: [100, 1024]
-------------------------------------------------------------------------------------------------------------------------------------
Benchmark                                                               Time             CPU   Iterations UserCounters...
-------------------------------------------------------------------------------------------------------------------------------------
BigHashBenchmark/SimpleInsert/min_time:3.000/manual_time             6473 ms         3.66 ms            1 items_per_second=154.48k/s
BigHashBenchmark/SimpleInsert/4 threads                              2483 ms         1.44 ms            2 items_per_second=402.761k/s
BigHashBenchmark/SimpleInsert/8 threads                              2315 ms         1.98 ms            2 items_per_second=431.923k/s
DiskCacheLRUBenchmark/SimpleInsert/min_time:3.000/manual_time       29336 ms         13.9 ms            1 items_per_second=34.0877k/s
DiskCacheLRUBenchmark/SimpleInsert/4 threads                        25275 ms         10.4 ms            1 items_per_second=39.5654k/s
DiskCacheLRUBenchmark/SimpleInsert/8 threads                        25109 ms         10.8 ms            1 items_per_second=39.8267k/s
NvmCacheBenchmark/SimpleInsert/min_time:3.000/manual_time            240 ms          197 ms            17 items_per_second=4.16463M/s  (ASYNC API)
*/
// clang-format on
}
