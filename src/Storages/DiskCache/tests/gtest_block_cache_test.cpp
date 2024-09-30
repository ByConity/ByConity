#include <climits>
#include <memory>
#include <semaphore>
#include <thread>

#include <fmt/core.h>
#include <gmock/gmock.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>
#include <Poco/Logger.h>

#include <Storages/DiskCache/AbstractCache.h>
#include <Storages/DiskCache/BlockCache.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/RecordIO.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/tests/BufferGen.h>
#include <Storages/DiskCache/tests/Callbacks.h>
#include <Storages/DiskCache/tests/MockDevice.h>
#include <Storages/DiskCache/tests/MockJobScheduler.h>
#include <Storages/DiskCache/tests/MockPolicy.h>
#include <Storages/DiskCache/tests/SeqPoints.h>
#include <Common/Exception.h>
#include <Common/InjectPause.h>
#include <common/logger_useful.h>

namespace DB::HybridCache
{
using testing::_;
using testing::Invoke;
using testing::NiceMock;
using testing::Return;

constexpr UInt64 kDeviceSize{64 * 1024};
constexpr UInt64 kRegionSize{16 * 1024};

std::unique_ptr<JobScheduler> makeJobScheduler()
{
    return std::make_unique<MockSingleThreadJobScheduler>();
}

BlockCacheReinsertionConfig makeHitsReinsertionConfig(UInt8 hitsReinsertThreshold)
{
    BlockCacheReinsertionConfig config{};
    config.enableHitsBased(hitsReinsertThreshold);
    return config;
}

BlockCache::Config
makeConfig(JobScheduler & scheduler, std::unique_ptr<EvictionPolicy> policy, Device & device, UInt64 cache_size = kDeviceSize)
{
    BlockCache::Config config;
    config.scheduler = &scheduler;
    config.region_size = kRegionSize;
    config.cache_size = cache_size;
    config.device = &device;
    config.eviction_policy = std::move(policy);
    return config;
}

std::unique_ptr<BlockCache> makeCache(BlockCache::Config && config, size_t metadata_size = 0)
{
    config.cache_base_offset = metadata_size;
    return std::make_unique<BlockCache>(std::move(config));
}

Buffer strzBuffer(const char * strz)
{
    return Buffer{makeView(strz)};
}

class CacheEntry
{
public:
    CacheEntry(Buffer k, Buffer v) : key{std::move(k)}, value{std::move(v)} { }
    CacheEntry(HashedKey k, Buffer v) : key{makeView(k.key())}, value{std::move(v)} { }

    CacheEntry(CacheEntry &&) = default;
    CacheEntry & operator=(CacheEntry &&) = default;

    HashedKey getKey() const { return makeHashKey(key); }

    BufferView getValue() const { return value.view(); }

private:
    Buffer key, value;
};

void finishAllJobs(MockJobScheduler & ex)
{
    while (ex.getQueueSize())
    {
        ex.runFirst();
    }
}


class Driver
{
public:
    Driver(
        std::unique_ptr<BlockCache> cache_,
        std::unique_ptr<JobScheduler> scheduler_,
        std::unique_ptr<Device> device_,
        size_t metadata_size_ = 0)
        : cache{std::move(cache_)}, scheduler{std::move(scheduler_)}, device{std::move(device_)}, metadata_size{metadata_size_}
    {
    }

    ~Driver()
    {
        drain();
        cache.reset();
        scheduler.reset();
        device.reset();
    }

    void persist()
    {
        auto stream = createMetadataOutputStream(*device, metadata_size);
        cache->persist(stream.get());
    }

    void reset()
    {
        scheduler->finish();
        cache->reset();
    }

    bool recover()
    {
        auto stream = createMetadataInputStream(*device, metadata_size);
        return cache->recover(stream.get());
    }

    void flush()
    {
        scheduler->finish();
        cache->flush();
    }

    void drain()
    {
        scheduler->finish();
        cache->drain();
    }

    Status insertAsync(const HashedKey & key, const BufferView & value, InsertCallback cb = nullptr)
    {
        scheduler->enqueue(
            [this, cb = std::move(cb), key, value]() {
                auto status = cache->insert(key, value);
                if (status == Status::Retry)
                    return JobExitCode::Reschedule;

                if (cb)
                    cb(status, key);

                return JobExitCode::Done;
            },
            "insert",
            JobType::Write);
        return Status::Ok;
    }

    Status lookup(const HashedKey & key, Buffer & value)
    {
        Status status;
        do
        {
            status = cache->lookup(key, value);
            std::this_thread::yield();
        } while (status == Status::Retry);
        return status;
    }

    Status remove(const HashedKey & key)
    {
        Status status;
        do
        {
            status = cache->remove(key);
            std::this_thread::yield();
        } while (status == Status::Retry);
        return status;
    }

    Status insert(const HashedKey & key, const BufferView & value)
    {
        std::binary_semaphore done{0};
        Status status{Status::Ok};
        scheduler->enqueueWithKey(
            [&]() mutable {
                status = cache->insert(key, value);
                if (status == Status::Retry)
                    return JobExitCode::Reschedule;

                done.release();
                return JobExitCode::Done;
            },
            "insert",
            JobType::Write,
            key.keyHash());

        done.acquire();
        return status;
    }

    bool couldExist(const HashedKey & key) { return cache->couldExist(key); }

    std::pair<Status, std::string> getRandomAlloc(Buffer & value) { return cache->getRandomAlloc(value); }

private:
    std::unique_ptr<BlockCache> cache;
    std::unique_ptr<JobScheduler> scheduler;
    std::unique_ptr<Device> device;
    size_t metadata_size;
};

TEST(BlockCache, InsertLookup)
{
    std::vector<CacheEntry> log;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto js = makeJobScheduler();
    auto config = makeConfig(*js, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(js), std::move(device));

    BufferGen generator;
    {
        CacheEntry e{generator.gen(8), generator.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        driver->flush();
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(e.getKey(), value));

        EXPECT_EQ(e.getValue(), value.view());
        log.push_back(std::move(e));
        EXPECT_EQ(1, hits[0]);
    }

    for (size_t i = 0; i < 16; i++)
    {
        CacheEntry e{generator.gen(8), generator.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 17; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
    EXPECT_EQ(2, hits[0]);
    EXPECT_EQ(16, hits[1]);
    EXPECT_EQ(0, hits[2]);
    EXPECT_EQ(0, hits[3]);
}

TEST(BlockCache, InsertLookupSync)
{
    std::vector<CacheEntry> log;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 1;
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen generator;
    for (size_t i = 0; i < 17; i++)
    {
        CacheEntry e{generator.gen(8), generator.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(e.getKey(), value));
        EXPECT_EQ(e.getValue(), value.view());
        log.push_back(std::move(e));
    }
    EXPECT_EQ(0, hits[0]);
    EXPECT_EQ(0, hits[1]);
    EXPECT_EQ(0, hits[2]);
    EXPECT_EQ(0, hits[3]);

    for (size_t i = 0; i < 17; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
    EXPECT_EQ(16, hits[0]);
    EXPECT_EQ(0, hits[1]);
    EXPECT_EQ(0, hits[2]);
    EXPECT_EQ(0, hits[3]);
}

TEST(BlockCache, CouldExist)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen gen;
    for (size_t i = 0; i < 17; i++)
    {
        CacheEntry e{gen.gen(8 + i), gen.gen(800)};
        EXPECT_FALSE(driver->couldExist(e.getKey()));
        EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
        EXPECT_TRUE(driver->couldExist(e.getKey()));
        EXPECT_EQ(Status::Ok, driver->remove(e.getKey()));
        EXPECT_FALSE(driver->couldExist(e.getKey()));
    }
}

TEST(BlockCache, Remove)
{
    std::vector<CacheEntry> log;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));
    BufferGen bg;
    {
        CacheEntry e{strzBuffer("cat"), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    {
        CacheEntry e{strzBuffer("dog"), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[0].getKey(), value));
    EXPECT_EQ(log[0].getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
    EXPECT_EQ(log[1].getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->remove(makeHashKey("dog")));
    EXPECT_EQ(Status::NotFound, driver->remove(makeHashKey("fox")));
    EXPECT_EQ(Status::Ok, driver->lookup(log[0].getKey(), value));
    EXPECT_EQ(log[0].getValue(), value.view());
    EXPECT_EQ(Status::NotFound, driver->lookup(log[1].getKey(), value));
    EXPECT_EQ(Status::NotFound, driver->remove(makeHashKey("dog")));
    EXPECT_EQ(Status::NotFound, driver->remove(makeHashKey("fox")));
    EXPECT_EQ(Status::Ok, driver->remove(makeHashKey("cat")));
    EXPECT_EQ(Status::NotFound, driver->lookup(log[0].getKey(), value));
    EXPECT_EQ(Status::NotFound, driver->lookup(log[1].getKey(), value));
}

TEST(BlockCache, RemoveAndInsert)
{
    std::vector<CacheEntry> log;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));
    BufferGen bg;
    {
        CacheEntry e{strzBuffer("cat"), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    {
        CacheEntry e{strzBuffer("dog"), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(log[0].getKey(), value));
    EXPECT_EQ(log[0].getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
    EXPECT_EQ(log[1].getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->remove(makeHashKey("cat")));
    EXPECT_EQ(Status::NotFound, driver->lookup(log[0].getKey(), value));
    EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
    EXPECT_EQ(log[1].getValue(), value.view());

    {
        CacheEntry e{strzBuffer("cat"), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
    EXPECT_EQ(log[1].getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
    EXPECT_EQ(log[2].getValue(), value.view());
}

TEST(BlockCache, SimpleReclaim)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t j = 0; j < 3; j++)
    {
        for (size_t i = 0; i < 16; i++)
        {
            CacheEntry e{bg.gen(8), bg.gen(800)};
            EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
        driver->flush();
    }

    for (size_t i = 0; i < 16; i++)
    {
        CacheEntry e{bg.gen(8), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 16; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }
    for (size_t i = 16; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, StackAlloc)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = std::make_unique<MockSingleThreadJobScheduler>();
    auto * ex_ptr = ex.get();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.read_buffer_size = 2048;
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));
    BufferGen bg;

    CacheEntry e1{bg.gen(8), bg.gen(1800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e1.getKey(), e1.getValue()));
    ex_ptr->finish();

    CacheEntry e2{bg.gen(8), bg.gen(100)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e2.getKey(), e2.getValue()));
    ex_ptr->finish();

    CacheEntry e3{bg.gen(8), bg.gen(3000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e3.getKey(), e3.getValue()));
    ex_ptr->finish();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e1.getKey(), value));
    EXPECT_EQ(e1.getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(e2.getKey(), value));
    EXPECT_EQ(e2.getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(e3.getKey(), value));
    EXPECT_EQ(e3.getValue(), value.view());

    EXPECT_EQ(0, ex_ptr->getQueueSize());
}

TEST(BlockCache, RegionUnderflow)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
    EXPECT_CALL(*device, writeImpl(0, 16 * 1024, _));
    // Although 2k read buffer, shouldn't underflow the region!
    EXPECT_CALL(*device, readImpl(0, 1024, _));
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 4;
    config.read_buffer_size = 2048;

    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;

    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
    driver->flush();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.getKey(), value));
    EXPECT_EQ(e.getValue(), value.view());
}

TEST(BlockCache, SmallReadBuffer)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 4096);
    EXPECT_CALL(*device, writeImpl(0, 16 * 1024, _));
    EXPECT_CALL(*device, readImpl(0, 8192, _));
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 4;
    config.read_buffer_size = 5120;

    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    CacheEntry e{bg.gen(8), bg.gen(5800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
    driver->flush();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e.getKey(), value));
    EXPECT_EQ(e.getValue(), value.view());
}


TEST(BlockCache, SmallAllocAlignment)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 4;
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    std::vector<CacheEntry> log;
    BufferGen bg;
    Status status;
    for (size_t i = 0; i < 96; i++)
    {
        CacheEntry e{bg.gen(8), bg.gen(200)};
        status = driver->insert(e.getKey(), e.getValue());
        EXPECT_EQ(Status::Ok, status);
        log.push_back(std::move(e));
    }
    for (size_t i = 0; i < 96; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
    return;

    {
        CacheEntry e{bg.gen(8), bg.gen(10)};
        status = driver->insert(e.getKey(), e.getValue());
        EXPECT_EQ(Status::Ok, status);
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 32; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }
    for (size_t i = 32; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}


TEST(BlockCache, MultipleAllocAlignmentSizeItems)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 4;
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    std::vector<CacheEntry> log;
    BufferGen bg;
    Status status;
    for (size_t i = 0; i < 24; i++)
    {
        CacheEntry e{bg.gen(8), bg.gen(1700)};
        status = driver->insert(e.getKey(), e.getValue());
        EXPECT_EQ(Status::Ok, status);
        log.push_back(std::move(e));
    }
    for (size_t i = 0; i < 24; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
    return;

    {
        CacheEntry e{bg.gen(8), bg.gen(10)};
        status = driver->insert(e.getKey(), e.getValue());
        EXPECT_EQ(Status::Ok, status);
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 8; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }
    for (size_t i = 8; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, StackAllocReclaim)
{
    std::vector<CacheEntry> log;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    { // 2k
        CacheEntry e{bg.gen(8), bg.gen(2000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    { // 10k
        CacheEntry e{bg.gen(8), bg.gen(10'000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();
    // Fill region 1
    { // 8k
        CacheEntry e{bg.gen(8), bg.gen(8000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    { // 6k
        CacheEntry e{bg.gen(8), bg.gen(6000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();
    // Fill region 2
    { // 4k
        CacheEntry e{bg.gen(8), bg.gen(4000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    { // 4k
        CacheEntry e{bg.gen(8), bg.gen(4000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    { // 4k
        CacheEntry e{bg.gen(8), bg.gen(4000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();
    // Fill region 3
    { // 15k
        CacheEntry e{bg.gen(8), bg.gen(15'000)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 2; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }
    for (size_t i = 2; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, ReadRegionDuringEviction)
{
    std::vector<CacheEntry> log;
    SeqPoints sp;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);

    EXPECT_CALL(*device, readImpl(0, 16 * 1024, _));
    EXPECT_CALL(*device, readImpl(8192, 4096, _)).Times(2);
    EXPECT_CALL(*device, readImpl(4096, 4096, _)).WillOnce(Invoke([md = device.get(), &sp](UInt64 offset, UInt32 size, void * buffer) {
        sp.reached(0);
        sp.wait(1);
        return md->getRealDeviceRef().read(offset, size, buffer);
    }));

    auto ex = std::make_unique<MockJobScheduler>();
    auto * ex_ptr = ex.get();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    for (size_t j = 0; j < 3; j++)
    {
        for (size_t i = 0; i < 4; i++)
        {
            CacheEntry e{bg.gen(8), bg.gen(3800)};
            driver->insertAsync(e.getKey(), e.getValue());
            log.push_back(std::move(e));
            finishAllJobs(*ex_ptr);
            driver->drain();
        }
    }
    driver->flush();

    std::thread lookup_thread([&driver, &log] {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
        EXPECT_EQ(log[2].getValue(), value.view());
        EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
        EXPECT_EQ(log[1].getValue(), value.view());
    });

    sp.wait(0);

    ENABLE_INJECT_PAUSE_IN_SCOPE();

    injectPauseSet("pause_reclaim_begin");
    injectPauseSet("pause_reclaim_done");

    CacheEntry e{bg.gen(8), bg.gen(1000)};
    EXPECT_EQ(0, ex_ptr->getQueueSize());
    driver->insertAsync(e.getKey(), e.getValue());
    EXPECT_TRUE(ex_ptr->runFirstIf("insert"));

    EXPECT_TRUE(injectPauseWait("pause_reclaim_begin", 1 /* numThreads */, false /* wakeup */));

    Buffer value;

    EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
    EXPECT_EQ(log[2].getValue(), value.view());

    injectPauseClear("pause_reclaim_begin");
    EXPECT_FALSE(injectPauseWait("pause_reclaim_done", 1, false, 5000));

    std::thread lookup_thread2([&driver, &log] {
        Buffer value2;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[2].getKey(), value2));
    });

    EXPECT_EQ(Status::Ok, driver->remove(log[2].getKey()));

    EXPECT_FALSE(injectPauseWait("pause_reclaim_done", 1, false, 5000));

    sp.reached(1);

    EXPECT_TRUE(injectPauseWait("pause_reclaim_done", 1, true, 5000));

    finishAllJobs(*ex_ptr);
    driver->drain();

    EXPECT_EQ(Status::NotFound, driver->remove(log[1].getKey()));
    EXPECT_EQ(Status::NotFound, driver->remove(log[2].getKey()));

    lookup_thread.join();
    lookup_thread2.join();

    driver->flush();
}

TEST(BlockCache, DeviceFailure)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
    {
        testing::InSequence seq;
        EXPECT_CALL(*device, writeImpl(0, kRegionSize, _)).WillOnce(Return(false));
        EXPECT_CALL(*device, writeImpl(0, kRegionSize, _));
        EXPECT_CALL(*device, writeImpl(kRegionSize, kRegionSize, _));
        EXPECT_CALL(*device, writeImpl(kRegionSize * 2, kRegionSize, _));

        EXPECT_CALL(*device, readImpl(0, 1024, _));
        EXPECT_CALL(*device, readImpl(kRegionSize, 1024, _)).WillOnce(Return(false));
        EXPECT_CALL(*device, readImpl(kRegionSize * 2, 1024, _));
    }

    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    auto value1 = bg.gen(800);
    auto value2 = bg.gen(800);
    auto value3 = bg.gen(800);

    EXPECT_EQ(Status::Ok, driver->insert(makeHashKey("key1"), value1.view()));
    driver->flush();
    EXPECT_EQ(Status::Ok, driver->insert(makeHashKey("key2"), value2.view()));
    driver->flush();
    EXPECT_EQ(Status::Ok, driver->insert(makeHashKey("key3"), value3.view()));
    driver->flush();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(makeHashKey("key1"), value));
    EXPECT_EQ(value1.view(), value.view());
    EXPECT_EQ(Status::DeviceError, driver->lookup(makeHashKey("key2"), value));
    EXPECT_EQ(Status::Ok, driver->lookup(makeHashKey("key3"), value));
    EXPECT_EQ(value3.view(), value.view());
}

TEST(BlockCache, Flush)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

    auto device = std::make_unique<NiceMock<MockDevice>>(kDeviceSize, 1024);
    EXPECT_CALL(*device, flushImpl());

    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    CacheEntry e{bg.gen(8), bg.gen(800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
    driver->flush();
}

TEST(BlockCache, RegionLastOffset)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));


    folly::fibers::TimedMutex mutex;
    bool reclaim_started = false;
    size_t num_clean_regions = 0;
    ConditionVariable cv;
    ENABLE_INJECT_PAUSE_IN_SCOPE();
    injectPauseSet("pause_blockcache_clean_alloc_locked", [&]() {
        std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
        EXPECT_GT(num_clean_regions, 0u);
        num_clean_regions--;
    });
    injectPauseSet("pause_blockcache_clean_free_locked", [&]() {
        std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
        if (num_clean_regions++ == 0u)
        {
            cv.notifyAll();
        }
        reclaim_started = true;
    });
    injectPauseSet("pause_blockcache_insert_entry", [&]() {
        std::unique_lock<folly::fibers::TimedMutex> lk(mutex);
        if (num_clean_regions == 0u && reclaim_started)
        {
            cv.wait(lk);
        }
    });

    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t j = 0; j < 3; j++)
    {
        for (size_t i = 0; i < 7; i++)
        {
            CacheEntry e{bg.gen(8), bg.gen(1800)};
            EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
        driver->flush();
    }

    for (size_t i = 0; i < 7; i++)
    {
        CacheEntry e{bg.gen(8), bg.gen(1800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 7; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }
    for (size_t i = 7; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, SmallerSlotSizes)
{
    std::vector<UInt32> hits(4);
    UInt32 io_align_size = 4096;

    {
        auto device = createMemoryDevice(kDeviceSize);
        auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

        size_t metadata_size = 3 * 1024 * 1024;
        auto ex = makeJobScheduler();
        auto config = makeConfig(*ex, std::move(policy), *device);
        try
        {
            auto cache = makeCache(std::move(config), metadata_size);
        }
        catch (const std::invalid_argument & e)
        {
            EXPECT_EQ(e.what(), std::string("invalid size class: 3072"));
        }
    }
    const UInt64 my_device_size = 16 * 1024 * 1024;
    auto device = createMemoryDevice(my_device_size, io_align_size);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    size_t metadata_size = 3 * 1024 * 1024;
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, my_device_size);
    config.num_in_mem_buffers = 4;
    auto cache = makeCache(std::move(config), metadata_size);
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t j = 0; j < 5; j++)
    {
        CacheEntry e{bg.gen(8), bg.gen(2700)};
        EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    for (size_t j = 0; j < 3; j++)
    {
        CacheEntry e{bg.gen(8), bg.gen(5700)};
        EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    for (size_t i = 0; i < 8; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }

    driver->flush();
    for (size_t i = 0; i < 8; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }

    driver->persist();
    driver->reset();
    EXPECT_TRUE(driver->recover());
    for (size_t i = 0; i < 8; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, NoJobsOnStartup)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = std::make_unique<MockJobScheduler>();
    auto * ex_ptr = ex.get();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    EXPECT_EQ(0, ex_ptr->getQueueSize());
}

TEST(BlockCache, Checksum)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    constexpr UInt32 k_io_align_size = 1024;
    auto device = createMemoryDevice(kDeviceSize, k_io_align_size);
    auto & device_obj = *device;
    auto ex = std::make_unique<MockSingleThreadJobScheduler>();
    auto * ex_ptr = ex.get();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.read_buffer_size = 2048;
    config.checksum = true;
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    CacheEntry e1{bg.gen(8), bg.gen(1800)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e1.getKey(), e1.getValue()));
    ex_ptr->finish();
    CacheEntry e2{bg.gen(8), bg.gen(100)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e2.getKey(), e2.getValue()));
    ex_ptr->finish();
    CacheEntry e3{bg.gen(8), bg.gen(3000)};
    EXPECT_EQ(Status::Ok, driver->insertAsync(e3.getKey(), e3.getValue()));
    ex_ptr->finish();

    Buffer value;
    EXPECT_EQ(Status::Ok, driver->lookup(e1.getKey(), value));
    EXPECT_EQ(e1.getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(e2.getKey(), value));
    EXPECT_EQ(e2.getValue(), value.view());
    EXPECT_EQ(Status::Ok, driver->lookup(e3.getKey(), value));
    EXPECT_EQ(e3.getValue(), value.view());
    driver->flush();

    Buffer buf{2 * k_io_align_size, k_io_align_size};
    memcpy(buf.data() + buf.size() - 4, "hack", 4);
    EXPECT_TRUE(device_obj.write(0, std::move(buf)));
    EXPECT_EQ(Status::DeviceError, driver->lookup(e1.getKey(), value));

    const char corruption[k_io_align_size]{"hack"};
    // EXPECT_TRUE(device->write(
    //     2 * k_io_align_size, Buffer{BufferView{k_io_align_size, reinterpret_cast<const UInt8 *>(corruption)}, k_io_align_size}));
    // EXPECT_EQ(Status::NotFound, driver->lookup(e2.getKey(), value));

    EXPECT_TRUE(
        device_obj.write(3 * k_io_align_size, Buffer{BufferView{1024, reinterpret_cast<const UInt8 *>(corruption)}, k_io_align_size}));
    EXPECT_EQ(Status::DeviceError, driver->lookup(e3.getKey(), value));

    EXPECT_EQ(0, ex_ptr->getQueueSize());
}

TEST(BlockCache, HitsReinsertionPolicy)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.reinsertion_config = makeHitsReinsertionConfig(1);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t j = 0; j < 3; j++)
    {
        for (size_t i = 0; i < 4; i++)
        {
            CacheEntry e{bg.gen(8), bg.gen(800)};
            EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
        driver->flush();
    }

    for (size_t i = 0; i < 3; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }

    EXPECT_EQ(Status::Ok, driver->remove(log[0].getKey()));

    {
        CacheEntry e{bg.gen(8), bg.gen(800)};
        EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->drain();

    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[0].getKey(), value));
    }

    for (size_t i = 1; i < 3; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }

    for (size_t i = 3; i < 4; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }

    for (size_t i = 4; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, UsePriorities)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto & mp = *policy;
    size_t metadata_size = 3 * 1024 * 1024;
    auto device_size = metadata_size + kRegionSize * 6;
    auto device = createMemoryDevice(device_size);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, kRegionSize * 6);
    config.num_priorities = 3;
    config.reinsertion_config = makeHitsReinsertionConfig(1);
    config.num_in_mem_buffers = 3;
    config.clean_regions_pool = 3;
    auto cache = makeCache(std::move(config), metadata_size);
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    std::vector<CacheEntry> log;
    BufferGen bg;

    EXPECT_CALL(mp, track(_)).Times(4);
    EXPECT_CALL(mp, track(EqRegionPri(1)));
    EXPECT_CALL(mp, track(EqRegionPri(2)));

    for (size_t i = 0; i < 4; i++)
    {
        for (size_t j = 0; j < 4; j++)
        {
            CacheEntry e{bg.gen(8), bg.gen(3800)};
            EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
        driver->flush();
        driver->drain();
        if (i == 0)
        {
            Buffer value;
            EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
            EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
            EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
            EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
        }
    }
    driver->flush();
    driver->drain();

    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[0].getKey(), value));
        EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
        EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
    }
}

TEST(BlockCache, UsePrioritiesSizeClass)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto & mp = *policy;
    size_t metadata_size = 3 * 1024 * 1024;
    auto device_size = metadata_size + kRegionSize * 6;
    auto device = createMemoryDevice(device_size);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device, kRegionSize * 6);
    config.num_priorities = 3;
    config.reinsertion_config = makeHitsReinsertionConfig(1);
    config.num_in_mem_buffers = 3;
    config.clean_regions_pool = 3;
    auto cache = makeCache(std::move(config), metadata_size);
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    std::vector<CacheEntry> log;
    BufferGen bg;

    EXPECT_CALL(mp, track(_)).Times(4);
    EXPECT_CALL(mp, track(EqRegionPri(1)));
    EXPECT_CALL(mp, track(EqRegionPri(2)));

    for (size_t i = 0; i < 2; i++)
    {
        for (size_t j = 0; j < 4; j++)
        {
            CacheEntry e{bg.gen(8), bg.gen(3800)};
            EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
        driver->flush();
        driver->drain();
        for (size_t j = 0; j < 8; j++)
        {
            CacheEntry e{bg.gen(8), bg.gen(1800)};
            EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
        driver->flush();
        driver->drain();
        if (i == 0)
        {
            Buffer value;
            EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
            EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
            EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
            EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
        }
    }
    driver->flush();
    driver->drain();

    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[0].getKey(), value));
        EXPECT_EQ(Status::Ok, driver->lookup(log[1].getKey(), value));
        EXPECT_EQ(Status::Ok, driver->lookup(log[2].getKey(), value));
    }
}

inline void mockRegionsEvicted(MockPolicy & policy, std::vector<UInt32> regionIds, bool sticky = true)
{
    testing::InSequence s;
    for (auto id : regionIds)
        EXPECT_CALL(policy, evict()).WillOnce(testing::Return(RegionId{id})).RetiresOnSaturation();

    if (sticky)
        EXPECT_CALL(policy, evict()).Times(0);
    else
        EXPECT_CALL(policy, evict()).Times(testing::AtLeast(0));
}

TEST(BlockCache, testItemDestructor)
{
    std::vector<CacheEntry> log;
    {
        BufferGen bg;
        // 1st region, 12k
        log.emplace_back(Buffer{makeView("key_000")}, bg.gen(5'000));
        log.emplace_back(Buffer{makeView("key_001")}, bg.gen(7'000));
        // 2nd region, 14k
        log.emplace_back(Buffer{makeView("key_002")}, bg.gen(5'000));
        log.emplace_back(Buffer{makeView("key_003")}, bg.gen(3'000));
        log.emplace_back(Buffer{makeView("key_004")}, bg.gen(6'000));
        // 3rd region, 16k, overwrites
        log.emplace_back(log[0].getKey(), bg.gen(8'000));
        log.emplace_back(log[3].getKey(), bg.gen(8'000));
        // 4th region, 15k
        log.emplace_back(Buffer{makeView("key_007")}, bg.gen(9'000));
        log.emplace_back(Buffer{makeView("key_008")}, bg.gen(6'000));
        ASSERT_EQ(9, log.size());
    }

    MockDestructor cb;
    ON_CALL(cb, call(_, _, _)).WillByDefault(Invoke([](HashedKey key, BufferView val, DestructorEvent) {
        LOG_ERROR(&Poco::Logger::root(), "cb key: {}, val: {}", key.key(), toStringRef(val.slice(0, 20)));
    }));

    {
        testing::InSequence in_seq;
        EXPECT_CALL(cb, call(log[2].getKey(), log[2].getValue(), DestructorEvent::Removed));
        EXPECT_CALL(cb, call(log[0].getKey(), log[5].getValue(), DestructorEvent::Removed));
        EXPECT_CALL(cb, call(log[4].getKey(), log[4].getValue(), DestructorEvent::Recycled));
        EXPECT_CALL(cb, call(log[3].getKey(), log[3].getValue(), _)).Times(0);
        EXPECT_CALL(cb, call(log[2].getKey(), log[2].getValue(), _)).Times(0);
    }

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto & mp = *policy;
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = makeJobScheduler();
    auto * ex_ptr = ex.get();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 9;
    config.destructor_callback = toCallback(cb);
    config.item_destructor_enabled = true;
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    mockRegionsEvicted(mp, {0, 1, 2, 3, 1});
    for (size_t i = 0; i < 7; i++)
    {
        LOG_ERROR(&Poco::Logger::root(), "insert {}", log[i].getKey().key());
        EXPECT_EQ(Status::Ok, driver->insert(log[i].getKey(), log[i].getValue()));
    }

    LOG_ERROR(&Poco::Logger::root(), "remove {}", log[2].getKey().key());
    EXPECT_EQ(Status::Ok, driver->remove(log[2].getKey()));

    LOG_ERROR(&Poco::Logger::root(), "remove {}", log[0].getKey().key());
    EXPECT_EQ(Status::Ok, driver->remove(log[0].getKey()));

    EXPECT_EQ(Status::NotFound, driver->remove(log[2].getKey()));
    EXPECT_EQ(Status::NotFound, driver->remove(log[0].getKey()));

    LOG_ERROR(&Poco::Logger::root(), "insert {}", log[7].getKey().key());
    EXPECT_EQ(Status::Ok, driver->insert(log[7].getKey(), log[7].getValue()));

    LOG_ERROR(&Poco::Logger::root(), "insert {}", log[8].getKey().key());
    EXPECT_EQ(Status::Ok, driver->insert(log[8].getKey(), log[8].getValue()));

    Buffer value;
    EXPECT_EQ(Status::NotFound, driver->lookup(log[0].getKey(), value));
    EXPECT_EQ(Status::NotFound, driver->lookup(log[5].getKey(), value));

    ex_ptr->finish();
}

TEST(BlockCache, RandomAlloc)
{
    std::unordered_map<std::string, CacheEntry> log;
    SeqPoints sp;

    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    auto device = createMemoryDevice(kDeviceSize);
    auto ex = std::make_unique<MockJobScheduler>();
    auto * ex_ptr = ex.get();
    auto config = makeConfig(*ex, std::move(policy), *device);
    auto cache = makeCache(std::move(config));
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device));

    BufferGen bg;
    for (size_t j = 0; j < 3; j++)
    {
        for (size_t i = 0; i < 4; i++)
        {
            auto key = fmt::format("{}:{}", j, i);
            CacheEntry e{makeHashKey(key.c_str()), bg.gen(3800)};
            driver->insertAsync(e.getKey(), e.getValue());
            log.emplace(key, std::move(e));
            finishAllJobs(*ex_ptr);
        }
    }
    driver->flush();

    size_t succ_cnt = 0;
    std::unordered_map<std::string, size_t> get_cnts;
    static constexpr size_t loopCnt = 10000;
    for (size_t i = 0; i < loopCnt; i++)
    {
        Buffer value;
        auto [status, keyStr] = driver->getRandomAlloc(value);
        if (status != Status::Ok)
            continue;
        succ_cnt++;
        get_cnts[keyStr]++;
        auto it = log.find(keyStr);
        EXPECT_NE(it, log.end());
        EXPECT_EQ(it->second.getValue(), value.view());
    }
    std::vector<size_t> cnts;
    std::transform(
        get_cnts.begin(), get_cnts.end(), std::back_inserter(cnts), [](const std::pair<std::string, size_t> & p) { return p.second; });
    auto [avg, stddev] = getMeanDeviation(cnts);

    EXPECT_GT(succ_cnt, (size_t)((double)loopCnt * 3.0 * 0.8 / 4.0));
    EXPECT_LT(succ_cnt, (size_t)((double)loopCnt * 3.0 * 1.2 / 4.0));
    EXPECT_LT(stddev, avg * 0.2);
}

TEST(BlockCache, SizeAndAlignment)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);

    const UInt32 align_size = 1024;
    auto device = std::make_unique<SizeMockDevice>(align_size * (static_cast<UInt64>(1) << 32));
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 9;

    config.item_destructor_enabled = true;
    auto cache = makeCache(std::move(config));
    BufferGen bg;
    auto small_value = bg.gen(16);
    EXPECT_EQ(cache->estimateWriteSize(HashedKey{"key"}, small_value.view()), align_size);

    auto large_value = bg.gen(align_size - 24 - 3);
    EXPECT_EQ(cache->estimateWriteSize(HashedKey{"key"}, large_value.view()), align_size);

    auto huge_value = bg.gen(align_size - 24 - 3 + 1);
    EXPECT_EQ(cache->estimateWriteSize(HashedKey{"key"}, huge_value.view()), align_size * 2);
}

TEST(BlockCache, Recovery)
{
    std::vector<UInt32> hits(4);
    UInt32 io_align_size = 4096;
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    // auto & mp = *policy;
    size_t metadata_size = 3 * 1024 * 1024;
    auto device_size = metadata_size + kDeviceSize;
    auto device = createMemoryDevice(device_size, io_align_size);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.num_in_mem_buffers = 2;
    auto cache = makeCache(std::move(config), metadata_size);
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

    // expectRegionsTracked(mp, {0, 1, 2});
    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t i = 0; i < 3; i++)
    {
        for (size_t j = 0; j < 4; j++)
        {
            CacheEntry e{bg.gen(8), bg.gen(3200)};
            EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
    }

    driver->flush();
    driver->persist();

    // {
    //     testing::InSequence s;
    //     EXPECT_CALL(mp, reset());
    //     expectRegionsTracked(mp, {0, 1, 2, 3});
    //     EXPECT_CALL(mp, reset());
    //     expectRegionsTracked(mp, {3, 0, 1, 2, 3});
    // }
    EXPECT_TRUE(driver->recover());

    for (auto & entry : log)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(entry.getKey(), value));
        EXPECT_EQ(entry.getValue(), value.view());
    }

    for (size_t i = 0; i < 3; i++)
    {
        CacheEntry e{bg.gen(8), bg.gen(3200)};
        EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
        log.push_back(std::move(e));
    }
    driver->flush();

    for (size_t i = 0; i < 4; i++)
    {
        Buffer value;
        EXPECT_EQ(Status::NotFound, driver->lookup(log[i].getKey(), value));
    }
    for (size_t i = 4; i < log.size(); i++)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(log[i].getKey(), value));
        EXPECT_EQ(log[i].getValue(), value.view());
    }
}

TEST(BlockCache, RecoveryWithDifferentCacheSize)
{
    std::vector<UInt32> hits(4);
    size_t metadata_size = 3 * 1024 * 1024;
    auto device_size = metadata_size + kDeviceSize;
    auto device = createMemoryDevice(device_size);
    auto ex = makeJobScheduler();

    std::vector<std::string> keys;
    Buffer original_metadata(INT_MAX);
    {
        auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits), *device);
        auto cache = makeCache(std::move(config), metadata_size);

        auto num_items = kDeviceSize / kRegionSize;
        for (UInt64 i = 0; i < num_items; i++)
        {
            auto key = fmt::format("key_{}", i);
            while (true)
            {
                if (Status::Ok
                    == cache->insert(makeHashKey(BufferView{key.size(), reinterpret_cast<UInt8 *>(key.data())}), makeView("value")))
                {
                    break;
                }
                ex->finish();
            }
            if (i > 0)
            {
                keys.push_back(key);
            }
        }
        ex->finish();
        cache->flush();

        for (auto & key : keys)
        {
            Buffer buffer;
            ASSERT_EQ(Status::Ok, cache->lookup(makeHashKey(BufferView{key.size(), reinterpret_cast<UInt8 *>(key.data())}), buffer));
        }

        auto ostream = google::protobuf::io::ArrayOutputStream(original_metadata.data(), INT_MAX);
        cache->persist(&ostream);
    }

    {
        auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits), *device);
        auto cache = makeCache(std::move(config), metadata_size);

        auto istream = google::protobuf::io::ArrayInputStream(original_metadata.data(), INT_MAX);
        ASSERT_TRUE(cache->recover(&istream));

        for (auto & key : keys)
        {
            Buffer buffer;
            ASSERT_EQ(Status::Ok, cache->lookup(makeHashKey(BufferView{key.size(), reinterpret_cast<UInt8 *>(key.data())}), buffer));
        }
    }

    {
        auto config = makeConfig(*ex, std::make_unique<NiceMock<MockPolicy>>(&hits), *device);
        config.cache_size += 4096;
        ASSERT_ANY_THROW(makeCache(std::move(config), metadata_size));
    }
}

TEST(BlockCache, RecoveryBadConfig)
{
    {
        std::vector<UInt32> hits(4);
        auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
        auto & mp = *policy;
        size_t metadata_size = 3 * 1024 * 1024;
        auto device_size = metadata_size + kDeviceSize;
        auto device = createMemoryDevice(device_size);
        auto ex = makeJobScheduler();
        auto config = makeConfig(*ex, std::move(policy), *device);
        auto cache = makeCache(std::move(config), metadata_size);
        auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

        expectRegionsTracked(mp, {0, 1, 2});
        BufferGen bg;
        std::vector<CacheEntry> log;
        for (size_t i = 0; i < 3; i++)
        {
            for (size_t j = 0; j < 4; j++)
            {
                CacheEntry e{bg.gen(8), bg.gen(3200)};
                EXPECT_EQ(Status::Ok, driver->insertAsync(e.getKey(), e.getValue()));
                log.push_back(std::move(e));
            }
            driver->flush();
            driver->drain();
        }

        driver->persist();
    }

    {
        std::vector<UInt32> hits(4);
        auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
        size_t metadata_size = 3 * 1024 * 1024;
        auto device_size = metadata_size + kDeviceSize;
        auto device = createMemoryDevice(device_size);
        auto ex = makeJobScheduler();
        auto config = makeConfig(*ex, std::move(policy), *device);
        config.region_size = 8 * 1024;
        auto cache = makeCache(std::move(config), metadata_size);
        auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

        EXPECT_FALSE(driver->recover());
    }
    {
        std::vector<UInt32> hits(4);
        auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
        size_t metadata_size = 3 * 1024 * 1024;
        auto device_size = metadata_size + kDeviceSize;
        auto device = createMemoryDevice(device_size);
        auto ex = makeJobScheduler();
        auto config = makeConfig(*ex, std::move(policy), *device);
        config.checksum = true;
        auto cache = makeCache(std::move(config));
        auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

        EXPECT_FALSE(driver->recover());
    }
}

TEST(BlockCache, RecoveryCorruptedData)
{
    std::vector<UInt32> hits(4);
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    std::unique_ptr<Driver> driver;
    {
        size_t metadata_size = 3 * 1024 * 1024;
        auto device_size = metadata_size + kDeviceSize;
        auto device = createMemoryDevice(device_size);
        auto & device_obj = *device;
        auto ex = makeJobScheduler();
        auto config = makeConfig(*ex, std::move(policy), *device);
        auto cache = makeCache(std::move(config), metadata_size);
        driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

        driver->persist();

        BufferGen gen;
        auto buffer = gen.gen(512);

        device_obj.write(0, buffer.view());
    }

    EXPECT_FALSE(driver->recover());
}

TEST(BlockCache, HitsReinsertionPolicyRecovery)
{
    std::vector<UInt32> hits(4);
    UInt32 io_align_size = 4096;
    auto policy = std::make_unique<NiceMock<MockPolicy>>(&hits);
    size_t metadata_size = 3 * 1024 * 1024;
    auto device_size = metadata_size + kDeviceSize;
    auto device = createMemoryDevice(device_size, io_align_size);
    auto ex = makeJobScheduler();
    auto config = makeConfig(*ex, std::move(policy), *device);
    config.reinsertion_config = makeHitsReinsertionConfig(1);
    auto cache = makeCache(std::move(config), metadata_size);
    auto driver = std::make_unique<Driver>(std::move(cache), std::move(ex), std::move(device), metadata_size);

    BufferGen bg;
    std::vector<CacheEntry> log;
    for (size_t i = 0; i < 3; i++)
    {
        for (size_t j = 0; j < 4; j++)
        {
            CacheEntry e{bg.gen(8), bg.gen(3200)};
            EXPECT_EQ(Status::Ok, driver->insert(e.getKey(), e.getValue()));
            log.push_back(std::move(e));
        }
    }

    driver->flush();
    driver->persist();
    EXPECT_TRUE(driver->recover());

    for (auto & entry : log)
    {
        Buffer value;
        EXPECT_EQ(Status::Ok, driver->lookup(entry.getKey(), value));
        EXPECT_EQ(entry.getValue(), value.view());
    }
}

}
