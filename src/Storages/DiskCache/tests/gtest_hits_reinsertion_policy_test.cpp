#include <gtest/gtest.h>

#include <Storages/DiskCache/Index.h>
#include "Storages/DiskCache/HashKey.h"
#include "Storages/DiskCache/HitsReinsertionPolicy.h"

namespace DB::HybridCache
{
TEST(HitsReinsertionPolicy, Simple)
{
    Index index;
    HitsReinsertionPolicy policy{1, index};

    auto hk1 = makeHashKey("test_key_1");
    StringRef str_key{reinterpret_cast<const char *>(hk1.key().data), hk1.key().size};

    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_FALSE(lr.isFound());
    }

    index.insert(hk1.keyHash(), 0, 0);
    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_EQ(0, lr.getTotalHits());
        EXPECT_EQ(0, lr.getCurrentHits());
    }

    index.lookup(hk1.keyHash());
    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_EQ(1, lr.getTotalHits());
        EXPECT_EQ(1, lr.getCurrentHits());
    }
    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_TRUE(policy.shouldReinsert(str_key));
        EXPECT_EQ(1, lr.getTotalHits());
        EXPECT_EQ(1, lr.getCurrentHits());
    }

    index.lookup(hk1.keyHash());
    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_EQ(2, lr.getTotalHits());
        EXPECT_EQ(2, lr.getCurrentHits());
    }

    index.remove(hk1.keyHash());
    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_FALSE(lr.isFound());
    }
    index.remove(hk1.keyHash());
}

TEST(HitsReinsertionPolicy, UpperBound)
{
    Index index;
    auto hk1 = makeHashKey("test_key_1");

    index.insert(hk1.keyHash(), 0, 0);
    for (int i = 0; i < 1000; i++)
    {
        index.lookup(hk1.keyHash());
    }
    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_EQ(255, lr.getTotalHits());
        EXPECT_EQ(255, lr.getCurrentHits());
    }
}

TEST(HitsReinsertionPolicy, ThreadSafe)
{
    Index index;

    auto hk1 = makeHashKey("test_key_1");

    index.insert(hk1.keyHash(), 0, 0);

    auto lookup = [&]() { index.lookup(hk1.keyHash()); };

    std::vector<std::thread> threads;
    threads.reserve(159);
    for (int i = 0; i < 159; i++)
    {
        threads.emplace_back(lookup);
    }

    for (auto & t : threads)
    {
        t.join();
    }

    {
        auto lr = index.peek(hk1.keyHash());
        EXPECT_EQ(159, lr.getTotalHits());
        EXPECT_EQ(159, lr.getCurrentHits());
    }
}
}
