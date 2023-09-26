#include <algorithm>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <benchmark/benchmark.h>

#include <Disks/DiskLocal.h>
#include <Disks/VolumeJBOD.h>
#include <Disks/registerDisks.h>
#include <IO/ReadBuffer.h>
#include <Storages/DiskCache/BigHash.h>
#include <Storages/DiskCache/BloomFilter.h>
#include <Storages/DiskCache/Bucket.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/File.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/WorkerPool.h>
#include <Storages/DiskCache/tests/BufferGen.h>
#include <Storages/DiskCache/tests/MultithreadTestUtil.h>
#include <benchmark/benchmark_util/BenchMarkConfig.h>
#include <Common/Stopwatch.h>
#include <Common/tests/gtest_global_context.h>
#include <common/types.h>

namespace DB
{

using HybridCache::BigHash;
using HybridCache::BloomFilter;
using HybridCache::BucketEntry;
using HybridCache::Buffer;
using HybridCache::Device;
using HybridCache::File;

namespace fs = std::filesystem;

class BigHashBenchmark : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) final
    {
        key_permutation_.reserve(num_keys_);
        for (UInt32 i = 0; i < num_keys_; i++)
            key_permutation_[i] = i;
        std::shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);

        HybridCache::BufferGen generator;
        for (UInt32 i = 0; i < 100; i++)
            buffer_permutation_.emplace_back(generator.gen(100, 1024));

        file_path_ = "./Benchmark-Test";

        fs::create_directory(file_path_);

        file_path_ = fmt::format("{}/BigHashBenchmark-{}", file_path_, ::getpid());

        std::vector<File> f_vec;
        f_vec.emplace_back(File(file_path_, O_RDWR | O_CREAT, S_IRWXU));

        fs::resize_file(fs::path{file_path_}, cache_size_);

        device_ = HybridCache::createDirectIoFileDevice(std::move(f_vec), cache_size_, 4096, 0, 0);
    }

    void TearDown(const benchmark::State &) final { fs::remove_all(fs::path{file_path_}); }

    const UInt32 num_keys_ = 1'000'000;

    std::string file_path_;

    const UInt32 cache_size_ = 1 * 1024 * 1024 * 1024;

    std::unique_ptr<Device> device_;

    std::default_random_engine generator_;
    std::vector<Int64> key_permutation_;
    std::vector<Buffer> buffer_permutation_;
};

class DiskCacheLRUBenchmark : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) final
    {
        key_permutation_.reserve(num_keys_);
        for (UInt32 i = 0; i < num_keys_; i++)
            key_permutation_[i] = i;
        std::shuffle(key_permutation_.begin(), key_permutation_.end(), generator_);

        HybridCache::BufferGen generator;
        for (UInt32 i = 0; i < 100; i++)
            buffer_permutation_.emplace_back(generator.gen(100, 1024));

        ctx_ = getContext().context;

        fs::create_directories("./disks/");

        fs::create_directory("./disk_cache/");
        fs::create_directory("./disk_cache_v1/");
        fs::create_directory("./tmp");
        fs::create_directory("./tmp/disk_cache/");
        fs::create_directory("./tmp/disk_cache_v1/");
        registerDisks();
    }

    void TearDown(const benchmark::State &) final { ctx_->shutdown(); }

    template <bool IS_V1_FORMAT>
    static void generateData(const DiskPtr & disk, int depth, int num_per_level, Strings & names, Strings & partial_key)
    {
        static int counter = 0;
        if (depth == 0)
        {
            for (int i = 0; i < num_per_level; i++)
            {
                partial_key.push_back(std::to_string(counter++));

                String cache_name;
                std::filesystem::path rel_path;
                cache_name = fmt::format("{}.{}", fmt::join(partial_key, "/"), "bin");
                if constexpr (IS_V1_FORMAT)
                    rel_path = DiskCacheLRU::getPath(DiskCacheLRU::hash(cache_name), "disk_cache_v1");
                else
                    rel_path = std::filesystem::path("disk_cache") / cache_name;
                partial_key.pop_back();

                disk->createDirectories(rel_path.parent_path());
                WriteBufferFromFile writer(std::filesystem::path(disk->getPath()) / rel_path);
                String content = String(std::abs(random()) % 100, 'a');
                writer.write(content.data(), content.size());
                names.push_back(cache_name);
            }
        }
        else
        {
            for (int i = 0; i < num_per_level; i++)
            {
                partial_key.push_back(std::to_string(counter++));
                generateData<IS_V1_FORMAT>(disk, depth - 1, num_per_level, names, partial_key);
                partial_key.pop_back();
            }
        }
    }

    static Strings generateData(const DiskPtr & disk, int depth, int num_per_level)
    {
        Strings seg_names;
        Strings partial_name;
        generateData<true>(disk, depth, num_per_level, seg_names, partial_name);
        return seg_names;
    }

    static DB::VolumePtr newDualDiskVolume()
    {
        fs::create_directory("./tmp/local1/");
        fs::create_directory("./tmp/local2/");
        DB::Disks disks;
        disks.emplace_back(std::make_shared<DB::DiskLocal>("local1", "./tmp/local1/", 0));
        disks.emplace_back(std::make_shared<DB::DiskLocal>("local2", "./tmp/local2/", 0));
        return std::make_shared<DB::VolumeJBOD>("dual_disk", disks, disks.front()->getName(), 0, false);
    }

    const UInt32 num_keys_ = 1'000'000;

    std::string file_path_;

    const UInt32 cache_size_ = 1 * 1024 * 1024 * 1024;

    std::shared_ptr<Context> ctx_;

    std::default_random_engine generator_;
    std::vector<Int64> key_permutation_;
    std::vector<Buffer> buffer_permutation_;
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(BigHashBenchmark, SimpleInsert)(benchmark::State & state)
{
    HybridCache::WorkerPool pool(BenchmarkConfig::num_threads, {});
    pool.startup();

    for (auto _ : state)
    {
        auto config = BigHash::Config();
        config.cache_size = cache_size_;
        config.device = device_.get();

        const uint32_t item_min_size = BucketEntry::computeSize(sizeof(Int64), 100);

        auto bf = BloomFilter::makeBloomFilter(cache_size_ / config.bucket_size, config.bucket_size / item_min_size, 0.1);
        config.bloom_filters = std::make_unique<BloomFilter>(std::move(bf));

        auto * big_hash = new BigHash(std::move(config));

        auto workload = [&](UInt32 id) {
            UInt32 start_key = num_keys_ / BenchmarkConfig::num_threads * id;
            UInt32 end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

            for (UInt32 i = start_key; i < end_key; i++)
                big_hash->insert(HybridCache::makeHashKey(&key_permutation_[i], sizeof(Int64)), buffer_permutation_[i % 100].view());
        };

        Stopwatch watch;
        HybridCache::MultithreadTestUtil::runThreadUntilFinish(&pool, BenchmarkConfig::num_threads, workload);
        state.SetIterationTime(watch.elapsedSeconds());
        delete big_hash;
    }
    state.SetItemsProcessed(state.iterations() * num_keys_);
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(DiskCacheLRUBenchmark, SimpleInsert)(benchmark::State & state)
{
    auto volume = newDualDiskVolume();
    std::vector<std::pair<DiskPtr, std::vector<std::string>>> metas;
    for (const DiskPtr & disk : volume->getDisks())
    {
        std::vector<String> metas_in_disk = generateData(disk, 3, 4);
        metas.push_back({disk, metas_in_disk});
    }

    DB::DiskCacheSettings settings;

    HybridCache::WorkerPool pool(BenchmarkConfig::num_threads, {});
    pool.startup();

    for (auto _ : state)
    {
        fs::remove_all("./tmp/");
        fs::create_directories("./tmp/");
        IDiskCache::init(*getContext().context);

        DiskCacheLRU * cache = new DiskCacheLRU(volume, getContext().context->getDiskCacheThrottler(), settings);
        cache->load();

        auto workload = [&](UInt32 id) {
            UInt32 start_key = num_keys_ / BenchmarkConfig::num_threads * id;
            UInt32 end_key = start_key + num_keys_ / BenchmarkConfig::num_threads;

            for (UInt32 i = start_key; i < end_key; i++)
            {
                auto & buffer = buffer_permutation_[i % 100];
                auto read_buffer = ReadBuffer(reinterpret_cast<char *>(buffer.data()), buffer.size());
                cache->set(fmt::format("{}/", key_permutation_[i]), read_buffer, buffer.size());
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
    state.SetItemsProcessed(state.iterations() * num_keys_);
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off
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
*/
// clang-format on
}
