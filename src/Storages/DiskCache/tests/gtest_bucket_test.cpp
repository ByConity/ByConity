#include <cstdio>
#include <functional>
#include <random>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <pcg_random.hpp>

#include <Storages/DiskCache/Bucket.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/tests/Callbacks.h>
#include <Common/thread_local_rng.h>

namespace DB::HybridCache
{
using testing::_;

TEST(Bucket, SingleKey)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk = makeHashKey("key");
    auto value = makeView("value");

    EXPECT_EQ(0, bucket.size());
    EXPECT_TRUE(bucket.find(hk).isNull());

    bucket.insert(hk, value, nullptr, nullptr);
    EXPECT_EQ(1, bucket.size());
    EXPECT_EQ(value, bucket.find(hk));

    MockDestructor helper;
    EXPECT_CALL(helper, call(_, _, _)).Times(0);
    EXPECT_CALL(helper, call(makeHashKey("key"), makeView("value"), DestructorEvent::Removed));
    auto callback = toCallback(helper);
    EXPECT_EQ(1, bucket.remove(hk, callback));
    EXPECT_EQ(0, bucket.size());
    EXPECT_TRUE(bucket.find(hk).isNull());

    EXPECT_EQ(0, bucket.remove(hk, nullptr));
}

TEST(Bucket, CollisionKeys)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk = makeHashKey("key 1");
    auto value1 = makeView("value 1");

    EXPECT_EQ(0, bucket.size());
    EXPECT_TRUE(bucket.find(hk).isNull());

    bucket.insert(hk, value1, nullptr, nullptr);
    EXPECT_EQ(1, bucket.size());
    EXPECT_EQ(value1, bucket.find(hk));

    // Simulate a key that collides on the same hash
    const auto collided_hk = HashedKey::precomputed("key 2", hk.keyHash());
    auto value2 = makeView("value 2");
    bucket.insert(collided_hk, value2, nullptr, nullptr);

    EXPECT_EQ(value1, bucket.find(hk));
    EXPECT_EQ(value2, bucket.find(collided_hk));
}

TEST(Bucket, MultipleKeys)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk1 = makeHashKey("key 1");
    const auto hk2 = makeHashKey("key 2");
    const auto hk3 = makeHashKey("key 3");

    // Insert all the keys and verify we can find them
    bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
    EXPECT_EQ(1, bucket.size());

    bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
    EXPECT_EQ(2, bucket.size());

    bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);
    EXPECT_EQ(3, bucket.size());

    EXPECT_EQ(makeView("value 1"), bucket.find(hk1));
    EXPECT_EQ(makeView("value 2"), bucket.find(hk2));
    EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

    // Remove them one by one and verify removing one doesn't affect others
    EXPECT_EQ(1, bucket.remove(hk1, nullptr));
    EXPECT_EQ(2, bucket.size());
    EXPECT_TRUE(bucket.find(hk1).isNull());
    EXPECT_EQ(makeView("value 2"), bucket.find(hk2));
    EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

    EXPECT_EQ(1, bucket.remove(hk2, nullptr));
    EXPECT_EQ(1, bucket.size());
    EXPECT_TRUE(bucket.find(hk1).isNull());
    EXPECT_TRUE(bucket.find(hk2).isNull());
    EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

    EXPECT_EQ(1, bucket.remove(hk3, nullptr));
    EXPECT_EQ(0, bucket.size());
    EXPECT_TRUE(bucket.find(hk1).isNull());
    EXPECT_TRUE(bucket.find(hk2).isNull());
    EXPECT_TRUE(bucket.find(hk3).isNull());
}

TEST(Bucket, DuplicateKeys)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk = makeHashKey("key");
    auto value1 = makeView("value 1");
    auto value2 = makeView("value 2");
    auto value3 = makeView("value 3");

    // Bucket does not replace an existing key.
    // New one will be shadowed by the old one, unless it
    // has evicted the old key by chance. Here, we won't
    // allocate enough to trigger evictions.
    EXPECT_EQ(0, bucket.size());
    EXPECT_TRUE(bucket.find(hk).isNull());

    bucket.insert(hk, value1, nullptr, nullptr);
    EXPECT_EQ(1, bucket.size());
    EXPECT_EQ(value1, bucket.find(hk));

    bucket.insert(hk, value2, nullptr, nullptr);
    EXPECT_EQ(2, bucket.size());
    EXPECT_EQ(value1, bucket.find(hk));

    bucket.insert(hk, value3, nullptr, nullptr);
    EXPECT_EQ(3, bucket.size());
    EXPECT_EQ(value1, bucket.find(hk));

    // Now we'll start removing the keys. The order of
    // removing should follow the order of insertion.
    // A key inserted earlier will be removed before
    // the next one is removed.
    EXPECT_EQ(1, bucket.remove(hk, nullptr));
    EXPECT_EQ(2, bucket.size());
    EXPECT_EQ(value2, bucket.find(hk));

    EXPECT_EQ(1, bucket.remove(hk, nullptr));
    EXPECT_EQ(1, bucket.size());
    EXPECT_EQ(value3, bucket.find(hk));

    EXPECT_EQ(1, bucket.remove(hk, nullptr));
    EXPECT_EQ(0, bucket.size());
    EXPECT_TRUE(bucket.find(hk).isNull());
}

TEST(Bucket, EvictionNone)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    // Insert 3 small key/value just enough not to trigger
    // any evictions.
    MockDestructor helper;
    EXPECT_CALL(helper, call(_, _, _)).Times(0);
    auto callback = toCallback(helper);

    const auto hk1 = makeHashKey("key 1");
    const auto hk2 = makeHashKey("key 2");
    const auto hk3 = makeHashKey("key 3");

    ASSERT_EQ(0, bucket.insert(hk1, makeView("value 1"), nullptr, callback).first);
    EXPECT_EQ(1, bucket.size());
    EXPECT_EQ(makeView("value 1"), bucket.find(hk1));

    ASSERT_EQ(0, bucket.insert(hk2, makeView("value 2"), nullptr, callback).first);
    EXPECT_EQ(2, bucket.size());
    EXPECT_EQ(makeView("value 2"), bucket.find(hk2));

    ASSERT_EQ(0, bucket.insert(hk3, makeView("value 3"), nullptr, callback).first);
    EXPECT_EQ(3, bucket.size());
    EXPECT_EQ(makeView("value 3"), bucket.find(hk3));

    EXPECT_EQ(3, bucket.size());
}

TEST(Bucket, EvictionOne)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk1 = makeHashKey("key 1");
    const auto hk2 = makeHashKey("key 2");
    const auto hk3 = makeHashKey("key 3");
    const auto hk4 = makeHashKey("key 4");

    // Insert 3 small key/value.
    bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
    bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
    bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);

    // Insert one more will evict the very first key
    MockDestructor helper;
    EXPECT_CALL(helper, call(makeHashKey("key 1"), makeView("value 1"), DestructorEvent::Recycled));
    auto callback = toCallback(helper);
    ASSERT_EQ(1, bucket.insert(hk4, makeView("value 4"), nullptr, callback).first);

    EXPECT_EQ(makeView("value 4"), bucket.find(hk4));
    EXPECT_EQ(3, bucket.size());
}

TEST(Bucket, EvictionAll)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk1 = makeHashKey("key 1");
    const auto hk2 = makeHashKey("key 2");
    const auto hk3 = makeHashKey("key 3");
    const auto hk_big = makeHashKey("big key");

    // Insert 3 small key/value.
    bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
    bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
    bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);
    EXPECT_EQ(3, bucket.size());

    // Inserting this big value will evict all previous keys
    MockDestructor helper;
    EXPECT_CALL(helper, call(makeHashKey("key 1"), makeView("value 1"), DestructorEvent::Recycled));
    EXPECT_CALL(helper, call(makeHashKey("key 2"), makeView("value 2"), DestructorEvent::Recycled));
    EXPECT_CALL(helper, call(makeHashKey("key 3"), makeView("value 3"), DestructorEvent::Recycled));
    auto callback = toCallback(helper);

    Buffer big_value(50);
    ASSERT_EQ(3, bucket.insert(hk_big, big_value.view(), nullptr, callback).first);
    EXPECT_EQ(big_value.view(), bucket.find(hk_big));
    EXPECT_EQ(1, bucket.size());
}

TEST(Bucket, Checksum)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk = makeHashKey("key");

    // Setting a checksum does not affect checksum's outcome
    const uint32_t checksum1 = Bucket::computeChecksum(buf.view());
    bucket.setChecksum(checksum1);
    EXPECT_EQ(checksum1, bucket.getChecksum());
    EXPECT_EQ(checksum1, Bucket::computeChecksum(buf.view()));

    // Adding a new key/value will not update the existing checksum member,
    // but it will change the checksum computed.
    bucket.insert(hk, makeView("value"), nullptr, nullptr);
    EXPECT_EQ(checksum1, bucket.getChecksum());
    const uint64_t checksum2 = Bucket::computeChecksum(buf.view());
    EXPECT_NE(checksum1, checksum2);
    bucket.setChecksum(checksum2);
    EXPECT_EQ(checksum2, bucket.getChecksum());

    // Removing a key/value will change the checksum
    EXPECT_EQ(1, bucket.remove(hk, nullptr));
    EXPECT_EQ(checksum2, bucket.getChecksum());
    const uint64_t checksum3 = Bucket::computeChecksum(buf.view());
    EXPECT_NE(checksum2, checksum3);
    bucket.setChecksum(checksum3);
    EXPECT_EQ(checksum3, bucket.getChecksum());
}

TEST(Bucket, Iteration)
{
    Buffer buf(96 + sizeof(Bucket));
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);

    const auto hk1 = makeHashKey("key 1");
    const auto hk2 = makeHashKey("key 2");
    const auto hk3 = makeHashKey("key 3");

    // Insert 3 small key/value.
    bucket.insert(hk1, makeView("value 1"), nullptr, nullptr);
    bucket.insert(hk2, makeView("value 2"), nullptr, nullptr);
    bucket.insert(hk3, makeView("value 3"), nullptr, nullptr);
    EXPECT_EQ(3, bucket.size());

    {
        auto itr = bucket.getFirst();
        EXPECT_FALSE(itr.done());
        EXPECT_TRUE(itr.keyEqualsTo(hk1));
        EXPECT_EQ(makeView("value 1"), itr.value());

        itr = bucket.getNext(itr);
        EXPECT_FALSE(itr.done());
        EXPECT_TRUE(itr.keyEqualsTo(hk2));
        EXPECT_EQ(makeView("value 2"), itr.value());

        itr = bucket.getNext(itr);
        EXPECT_FALSE(itr.done());
        EXPECT_TRUE(itr.keyEqualsTo(hk3));
        EXPECT_EQ(makeView("value 3"), itr.value());

        itr = bucket.getNext(itr);
        EXPECT_TRUE(itr.done());
    }

    // Remove and retry
    EXPECT_EQ(1, bucket.remove(hk2, nullptr));
    {
        auto itr = bucket.getFirst();
        EXPECT_FALSE(itr.done());
        EXPECT_TRUE(itr.keyEqualsTo(hk1));
        EXPECT_EQ(makeView("value 1"), itr.value());

        itr = bucket.getNext(itr);
        EXPECT_FALSE(itr.done());
        EXPECT_TRUE(itr.keyEqualsTo(hk3));
        EXPECT_EQ(makeView("value 3"), itr.value());

        itr = bucket.getNext(itr);
        EXPECT_TRUE(itr.done());
    }
}

TEST(Bucket, EvictionExpired)
{
    constexpr uint32_t bucket_size = 1024;
    const uint32_t item_min_size = BucketEntry::computeSize(sizeof("key "), sizeof("value "));
    const uint32_t total_items = bucket_size / item_min_size + 1;
    Buffer buf(bucket_size);
    auto & bucket = Bucket::initNew(buf.mutableView(), 0);
    uint32_t num_item = 0;
    pcg64 rng;

    char key_str[64];
    char value_str[64];

    std::vector<bool> valid_items(total_items, false);
    ExpiredCheck exp_cb = [&valid_items, &num_item, &rng](BufferView v) -> bool {
        // expire by 50% of probability
        std::uniform_int_distribution<UInt32> dist(0, 100);
        if (dist(rng) > 50)
        {
            return false;
        }
        uint32_t idx;
        std::sscanf(reinterpret_cast<const char *>(v.data()), "value %d", &idx);
        EXPECT_LT(idx, num_item);
        valid_items[idx] = false;
        return true;
    };

    // insert items until the bucket is full and some items are evicted by
    // expiration
    std::pair<uint32_t, uint32_t> evicted;
    do
    {
        sprintf(key_str, "key %d", num_item);
        sprintf(value_str, "value %d", num_item);

        const auto hk = makeHashKey(key_str);
        evicted = bucket.insert(hk, makeView(value_str), exp_cb, nullptr);
        // all evicted items should be by expiration if any
        EXPECT_TRUE(!evicted.first || (evicted.first == evicted.second));
        valid_items[num_item] = true;
        num_item++;
    } while (!evicted.first && num_item < total_items);

    EXPECT_EQ(num_item, bucket.size() + evicted.first);

    for (uint32_t i = 0; i < num_item; i++)
    {
        sprintf(key_str, "key %d", i);
        const auto hk = makeHashKey(key_str);
        auto value = bucket.find(hk);

        if (!valid_items[i])
        {
            EXPECT_TRUE(value.isNull());
        }
        else
        {
            sprintf(value_str, "value %d", i);
            EXPECT_EQ(makeView(value_str), bucket.find(hk));
        }
    }
}
}
