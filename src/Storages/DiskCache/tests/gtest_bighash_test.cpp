#include <climits>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <thread>
#include <gmock/gmock-nice-strict.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/BigHash.h>
#include <Storages/DiskCache/BloomFilter.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/tests/BufferGen.h>
#include <Storages/DiskCache/tests/Callbacks.h>
#include <Storages/DiskCache/tests/MockDevice.h>
#include <Common/thread_local_rng.h>
#include <common/defines.h>

namespace DB::HybridCache
{

namespace
{
    void setLayout(BigHash::Config & config, uint32_t bs, uint32_t numBuckets)
    {
        config.bucket_size = bs;
        config.cache_size = uint64_t{bs} * numBuckets;
    }

    // Generate key for given bucket index
    std::string genKey(uint32_t num_buckets, uint32_t bid)
    {
        char key_buf[64];
        while (true)
        {
            std::uniform_int_distribution<UInt32> dist(INT_MIN, INT_MAX);
            auto id = dist(thread_local_rng);
            sprintf(key_buf, "key_%08X", id);
            HashedKey hk(key_buf);
            if ((hk.keyHash() % num_buckets) == bid)
            {
                break;
            }
        }
        return key_buf;
    }

    template <typename T>
    std::pair<double, double> getMeanDeviation(std::vector<T> v)
    {
        double sum = std::accumulate(v.begin(), v.end(), 0.0);
        double mean = sum / v.size();

        double accum = 0.0;
        std::for_each(v.begin(), v.end(), [&](const T & d) { accum += (static_cast<double>(d) - mean) * (static_cast<double>(d) - mean); });

        return std::make_pair(mean, sqrt(accum / v.size()));
    }
}

using testing::_;
using testing::InSequence;
using testing::NiceMock;
using testing::StrictMock;

TEST(BigHash, InsertAndRemove)
{
    BigHash::Config config;
    setLayout(config, 128, 2);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 128);
    config.device = device.get();

    BigHash bh(std::move(config));

    Buffer value;
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    EXPECT_EQ(Status::Ok, bh.remove(makeHashKey("key")));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(Status::NotFound, bh.remove(makeHashKey("key")));
}

TEST(BigHash, CouldExistWithoutBF)
{
    BigHash::Config config;
    setLayout(config, 128, 2);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 128);
    config.device = device.get();

    BigHash bh(std::move(config));

    Buffer value;
    EXPECT_EQ(true, bh.couldExist(makeHashKey("key")));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(true, bh.couldExist(makeHashKey("key")));
    EXPECT_EQ(Status::Ok, bh.remove(makeHashKey("key")));
    EXPECT_EQ(true, bh.couldExist(makeHashKey("key")));
}

TEST(BigHash, CouldExistWithBF)
{
    BigHash::Config config;
    size_t bucket_count = 2;
    setLayout(config, 128, bucket_count);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 128);
    config.device = device.get();

    auto bfs = std::make_unique<BloomFilter>(bucket_count, 2, 4);
    config.bloom_filters = std::move(bfs);

    BigHash bh(std::move(config));

    Buffer value;
    // bloom filter is initially un-initialized. So it will only return true for
    // keys that were inserted.
    EXPECT_EQ(false, bh.couldExist(makeHashKey("key")));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(true, bh.couldExist(makeHashKey("key")));
    EXPECT_EQ(Status::Ok, bh.remove(makeHashKey("key")));
    EXPECT_EQ(false, bh.couldExist(makeHashKey("key")));
}

TEST(BigHash, DoubleInsert)
{
    BigHash::Config config;
    setLayout(config, 128, 2);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 128);
    config.device = device.get();

    MockDestructor helper;
    config.destructor_callback = toCallback(helper);

    BigHash bh(std::move(config));

    Buffer value;

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    EXPECT_CALL(helper, call(makeHashKey("key"), makeView("12345"), DestructorEvent::Removed));

    // Insert the same key a second time will overwrite the previous value.
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("45678")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("45678"), value.view());
}

TEST(BigHash, DestructorCallback)
{
    BigHash::Config config;
    setLayout(config, 64, 1);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 64);
    config.device = device.get();

    MockDestructor helper;
    EXPECT_CALL(helper, call(makeHashKey("key 1"), makeView("value 1"), DestructorEvent::Recycled));
    EXPECT_CALL(helper, call(makeHashKey("key 2"), makeView("value 2"), DestructorEvent::Removed));
    config.destructor_callback = toCallback(helper);

    BigHash bh(std::move(config));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 1"), makeView("value 1")));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 2"), makeView("value 2")));
    EXPECT_EQ(Status::Ok, bh.remove(makeHashKey("key 2")));
}

TEST(BigHash, Reset)
{
    BigHash::Config config;
    size_t bucket_count = 2;
    constexpr UInt32 align_size = 128;
    setLayout(config, align_size, bucket_count);

    auto bfs = std::make_unique<BloomFilter>(bucket_count, 2, 4);
    config.bloom_filters = std::move(bfs);

    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, align_size);
    auto read_firtst_bucket = [&dev = device->getRealDeviceRef()] {
        Buffer buf{align_size, align_size};
        dev.read(0, align_size, buf.data());
        buf.trimStart(8);
        buf.shrink(92);
        return buf;
    };
    config.device = device.get();

    BigHash bh(std::move(config));

    Buffer value;

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    auto old_bucket_content = read_firtst_bucket();

    bh.reset();
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    auto new_bucket_content = read_firtst_bucket();
    EXPECT_EQ(old_bucket_content.view(), new_bucket_content.view());
}

TEST(BigHash, WriteInTwoBuckets)
{
    BigHash::Config config;
    config.cache_start_offset = 256;
    setLayout(config, 128, 2);
    auto device = std::make_unique<StrictMock<MockDevice>>(config.cache_start_offset + config.cache_size, 128);
    {
        InSequence in_seq;
        EXPECT_CALL(*device, readImpl(256, 128, _));
        EXPECT_CALL(*device, writeImpl(256, 128, _));
        EXPECT_CALL(*device, readImpl(384, 128, _));
        EXPECT_CALL(*device, writeImpl(384, 128, _));
        EXPECT_CALL(*device, readImpl(256, 128, _));
        EXPECT_CALL(*device, writeImpl(256, 128, _));
    }
    config.device = device.get();

    BigHash bh(std::move(config));

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("1"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("2"), makeView("45678")));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("3"), makeView("67890")));
}

TEST(BigHash, RemoveNotFound)
{
    // Remove that is not found will not write to disk, but rebuild bloom filter
    // and second remove would not read the device.
    BigHash::Config config;
    setLayout(config, 128, 1);
    auto device = std::make_unique<StrictMock<MockDevice>>(config.cache_size, 128);
    {
        InSequence in_seq;
        EXPECT_CALL(*device, readImpl(0, 128, _));
        EXPECT_CALL(*device, writeImpl(0, 128, _));
        EXPECT_CALL(*device, readImpl(0, 128, _));
        EXPECT_CALL(*device, writeImpl(0, 128, _));
        EXPECT_CALL(*device, readImpl(0, 128, _));
    }
    config.device = device.get();

    BigHash bh(std::move(config));

    bh.insert(makeHashKey("key"), makeView("12345"));
    bh.remove(makeHashKey("key"));
    bh.remove(makeHashKey("key"));
}

TEST(BigHash, CorruptBucket)
{
    BigHash::Config config;
    constexpr UInt32 align_size = 128;
    setLayout(config, align_size, 1);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, align_size);
    config.device = device.get();

    BigHash bh(std::move(config));

    bh.insert(makeHashKey("key"), makeView("12345"));

    Buffer value;
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    UInt8 bad_bytes[4] = {[0] = 13, [1] = 17, [2] = 19, [3] = 23};

    Buffer bad_buf{align_size, align_size};
    std::memcpy(bad_buf.data(), bad_bytes, sizeof(bad_bytes));
    device->getRealDeviceRef().write(0, std::move(bad_buf));

    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));
}

TEST(BigHash, Recovery)
{
    BigHash::Config config;
    config.cache_size = 16 * 1024;
    auto device = createMemoryDevice(config.cache_size);
    config.device = device.get();

    BigHash bh(std::move(config));

    Buffer value;
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    std::stringstream ss;
    bh.persist(&ss);

    ASSERT_TRUE(bh.recover(&ss));

    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());
}

TEST(BigHash, RecoveryBadConfig)
{
    std::stringstream ss;
    {
        BigHash::Config config;
        config.cache_size = 16 * 1024;
        config.bucket_size = 4 * 1024;
        auto device = createMemoryDevice(config.cache_size);
        config.device = device.get();

        BigHash bh(std::move(config));

        Buffer value;
        EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
        EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
        EXPECT_EQ(makeView("12345"), value.view());

        bh.persist(&ss);
    }

    {
        BigHash::Config config;
        config.cache_size = 32 * 1024;
        config.bucket_size = 8 * 1024;
        auto device = createMemoryDevice(config.cache_size);
        config.device = device.get();

        BigHash bh(std::move(config));
        ASSERT_FALSE(bh.recover(&ss));
    }
}

TEST(BigHash, RecoveryCorruptedData)
{
    BigHash::Config config;
    config.cache_size = 1024 * 1024;
    auto device = createMemoryDevice(config.cache_size);
    config.device = device.get();

    BigHash bh(std::move(config));

    Buffer value;

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key"), makeView("12345")));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("key"), value));
    EXPECT_EQ(makeView("12345"), value.view());

    std::stringstream ss;
    Buffer buffer(512);
    std::generate(buffer.data(), buffer.data() + buffer.size(), std::minstd_rand());

    ss.write(reinterpret_cast<const char *>(buffer.data()), buffer.size());

    ASSERT_FALSE(bh.recover(&ss));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("key"), value));
}


TEST(BigHash, ConcurrentRead)
{
    BigHash::Config config;
    setLayout(config, 128, 1);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 128);
    config.device = device.get();

    BigHash bh(std::move(config));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 1"), makeView("1")));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 2"), makeView("2")));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 3"), makeView("3")));

    struct MockLookupHelper
    {
        MOCK_METHOD2(call, void(HashedKey, BufferView));
    };
    MockLookupHelper helper;
    EXPECT_CALL(helper, call(makeHashKey("key 1"), makeView("1")));
    EXPECT_CALL(helper, call(makeHashKey("key 2"), makeView("2")));
    EXPECT_CALL(helper, call(makeHashKey("key 3"), makeView("3")));

    auto run_read = [&bh, &helper](HashedKey key) {
        Buffer value;
        EXPECT_EQ(Status::Ok, bh.lookup(key, value));
        helper.call(key, value.view());
    };

    auto t1 = std::thread(run_read, makeHashKey("key 1"));
    auto t2 = std::thread(run_read, makeHashKey("key 2"));
    auto t3 = std::thread(run_read, makeHashKey("key 3"));
    t1.join();
    t2.join();
    t3.join();
}

TEST(BigHash, BloomFilter)
{
    BigHash::Config config;
    setLayout(config, 128, 1);
    auto device = createMemoryDevice(config.cache_size);
    config.device = device.get();
    auto bfs = std::make_unique<BloomFilter>(1, 1, 4);
    config.bloom_filters = std::move(bfs);

    MockDestructor helper;
    EXPECT_CALL(helper, call(makeHashKey("100"), _, DestructorEvent::Recycled));
    EXPECT_CALL(helper, call(makeHashKey("101"), _, DestructorEvent::Removed));
    config.destructor_callback = toCallback(helper);

    BigHash bh(std::move(config));
    BufferGen bg;

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("100"), bg.gen(20).view()));

    Buffer value;
    // Rejected by BF
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("101"), value));
    // False positive, rejected by key
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("102"), value));
    // False positive, rejected by key
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("103"), value));

    // Insert "101"
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("101"), bg.gen(20).view()));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("110"), value));

    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("110"), bg.gen(20).view()));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("101"), value));
    EXPECT_EQ(Status::Ok, bh.lookup(makeHashKey("110"), value));

    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("100"), value));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("102"), value));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("103"), value));

    // If we remove, BF will be rebuilt and will reject "101":
    EXPECT_EQ(Status::Ok, bh.remove(makeHashKey("101")));
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("101"), value));
}

TEST(BigHash, BloomFilterRecoveryFail)
{
    BigHash::Config config;
    setLayout(config, 128, 2);
    auto device = std::make_unique<StrictMock<MockDevice>>(config.cache_size, 128);
    EXPECT_CALL(*device, readImpl(_, _, _)).Times(0);
    config.device = device.get();
    config.bloom_filters = std::make_unique<BloomFilter>(2, 1, 4);

    BigHash bh(std::move(config));

    Buffer value;
    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("100"), value));

    std::stringstream ss;
    Buffer buffer(512);
    std::generate(buffer.data(), buffer.data() + buffer.size(), std::minstd_rand());


    ss.write(reinterpret_cast<const char *>(buffer.data()), buffer.size());
    ASSERT_FALSE(bh.recover(&ss));

    EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("100"), value));
}

TEST(BigHash, BloomFilterRecovery)
{
    std::unique_ptr<Device> actual;
    std::stringstream ss;

    {
        BigHash::Config config;
        setLayout(config, 128, 2);
        auto device = std::make_unique<StrictMock<MockDevice>>(config.cache_size, 128);
        EXPECT_CALL(*device, readImpl(128, 128, _));
        EXPECT_CALL(*device, writeImpl(128, 128, _));
        config.device = device.get();
        config.bloom_filters = std::make_unique<BloomFilter>(2, 1, 4);

        BigHash bh(std::move(config));
        EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("100"), makeView("cat")));
        Buffer value;
        EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("200"), value));
        bh.persist(&ss);

        actual = device->releaseRealDevice();
    }

    {
        BigHash::Config config;
        setLayout(config, 128, 2);
        auto device = std::make_unique<MockDevice>(0, 128);
        device->setRealDevice(std::move(actual));
        EXPECT_CALL(*device, readImpl(0, 128, _)).Times(0);
        EXPECT_CALL(*device, readImpl(0, 128, _)).Times(0);
        config.device = device.get();
        config.bloom_filters = std::make_unique<BloomFilter>(2, 1, 4);

        BigHash bh(std::move(config));
        ASSERT_TRUE(bh.recover(&ss));

        EXPECT_EQ(Status::NotFound, bh.remove(makeHashKey("200")));

        EXPECT_EQ(Status::NotFound, bh.remove(makeHashKey("200")));
        Buffer value;
        EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("200"), value));

        EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("201"), value));
        EXPECT_EQ(Status::NotFound, bh.lookup(makeHashKey("201"), value));

        actual = device->releaseRealDevice();
    }
}

TEST(BigHash, DestructorCallbackOutsideLock)
{
    BigHash::Config config;
    setLayout(config, 64, 1);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 64);
    config.device = device.get();

    std::atomic<bool> done = false, started = false;
    config.destructor_callback = [&](HashedKey, BufferView, DestructorEvent event) {
        started = true;
        // only hangs the insertion not removal
        while (!done && event == DestructorEvent::Recycled)
            ;
    };

    BigHash bh(std::move(config));
    EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 1"), makeView("value 1")));

    // insert will hang in the destructor, but lock should be released once
    // destructorCB starts
    std::thread t([&]() { EXPECT_EQ(Status::Ok, bh.insert(makeHashKey("key 1"), makeView("value 2"))); });

    // wait until destrcutor started, which means bucket lock is released
    while (!started)
        ;
    // remove should not be blocked since bucket lock has been released
    EXPECT_EQ(Status::Ok, bh.remove(makeHashKey("key 1")));

    done = true;
    t.join();
}

TEST(BigHash, RandomAlloc)
{
    BigHash::Config config;
    setLayout(config, 1024, 4);
    auto device = std::make_unique<NiceMock<MockDevice>>(config.cache_size, 128);
    config.device = device.get();

    BigHash bh(std::move(config));

    BufferGen bg;
    auto data = bg.gen(40);
    for (uint32_t bid = 0; bid < 4; bid++)
    {
        for (size_t i = 0; i < 20; i++)
        {
            auto key_str = genKey(4, bid);
            sprintf(reinterpret_cast<char *>(data.data()), "data_%8s PAYLOAD: ", &key_str[key_str.size() - 8]);
            EXPECT_EQ(Status::Ok, bh.insert(makeHashKey(key_str.c_str()), data.view()));
        }
    }

    size_t succ_cnt = 0;
    std::unordered_map<std::string, size_t> get_cnts;
    static constexpr size_t loopCnt = 10000;
    for (size_t i = 0; i < loopCnt; i++)
    {
        Buffer value;
        auto [status, key_str] = bh.getRandomAlloc(value);
        if (status != Status::Ok)
        {
            continue;
        }
        succ_cnt++;
        get_cnts[key_str]++;
    }

    std::vector<size_t> cnts;
    std::transform(
        get_cnts.begin(), get_cnts.end(), std::back_inserter(cnts), [](const std::pair<std::string, size_t> & p) { return p.second; });
    auto [avg, stddev] = getMeanDeviation(cnts);

    EXPECT_GT(succ_cnt, (size_t)((double)loopCnt * 0.8));
    EXPECT_LT(stddev, avg * 0.2);
}
}
