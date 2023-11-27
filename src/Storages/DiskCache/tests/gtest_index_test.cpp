#include <thread>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/Index.h>
#include <common/types.h>

namespace DB::HybridCache
{
TEST(Index, EntrySize)
{
    Index index;
    index.insert(111, 0, 11);
    EXPECT_EQ(11, index.lookup(111).getSizeHint());
    index.insert(222, 0, 150);
    EXPECT_EQ(150, index.lookup(222).getSizeHint());
    index.insert(333, 0, 303);
    EXPECT_EQ(303, index.lookup(333).getSizeHint());
}

TEST(Index, ReplaceExact)
{
    Index index;

    EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
    EXPECT_FALSE(index.lookup(111).isFound());

    index.insert(111, 4444, 123);
    EXPECT_TRUE(index.lookup(111).isFound());
    EXPECT_EQ(4444, index.lookup(111).getAddress());
    EXPECT_EQ(123, index.lookup(111).getSizeHint());

    EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
    EXPECT_EQ(4444, index.lookup(111).getAddress());
    EXPECT_EQ(123, index.lookup(111).getSizeHint());

    EXPECT_TRUE(index.replaceIfMatch(111, 3333, 4444));
    EXPECT_EQ(3333, index.lookup(111).getAddress());
}

TEST(Index, RemoveExact)
{
    Index index;

    EXPECT_FALSE(index.removeIfMatch(111, 4444));

    index.insert(111, 4444, 0);
    EXPECT_TRUE(index.lookup(111).isFound());
    EXPECT_EQ(4444, index.lookup(111).getAddress());

    EXPECT_FALSE(index.removeIfMatch(111, 2222));
    EXPECT_EQ(4444, index.lookup(111).getAddress());

    EXPECT_TRUE(index.removeIfMatch(111, 4444));
    EXPECT_FALSE(index.lookup(111).isFound());
}

TEST(Index, Hits)
{
    Index index;
    const UInt64 key = 9999;

    index.insert(key, 0, 0);
    EXPECT_EQ(0, index.peek(key).getTotalHits());
    EXPECT_EQ(0, index.peek(key).getCurrentHits());

    index.lookup(key);
    EXPECT_EQ(1, index.peek(key).getTotalHits());
    EXPECT_EQ(1, index.peek(key).getCurrentHits());

    index.setHits(key, 2, 5);
    EXPECT_EQ(5, index.peek(key).getTotalHits());
    EXPECT_EQ(2, index.peek(key).getCurrentHits());

    index.lookup(key);
    EXPECT_EQ(6, index.peek(key).getTotalHits());
    EXPECT_EQ(3, index.peek(key).getCurrentHits());

    index.remove(key);
    EXPECT_FALSE(index.lookup(key).isFound());

    index.remove(key);
    EXPECT_FALSE(index.lookup(key).isFound());
}

TEST(Index, HitsAfterUpdate)
{
    Index index;
    const UInt64 key = 9999;

    index.insert(key, 0, 0);
    EXPECT_EQ(0, index.peek(key).getTotalHits());
    EXPECT_EQ(0, index.peek(key).getCurrentHits());

    index.lookup(key);
    EXPECT_EQ(1, index.peek(key).getTotalHits());
    EXPECT_EQ(1, index.peek(key).getCurrentHits());

    index.insert(key, 3, 0);
    EXPECT_EQ(0, index.peek(key).getTotalHits());
    EXPECT_EQ(0, index.peek(key).getCurrentHits());

    index.lookup(key);
    EXPECT_EQ(1, index.peek(key).getTotalHits());
    EXPECT_EQ(1, index.peek(key).getCurrentHits());

    EXPECT_FALSE(index.replaceIfMatch(key, 100, 0));
    EXPECT_EQ(1, index.peek(key).getTotalHits());
    EXPECT_EQ(1, index.peek(key).getCurrentHits());

    EXPECT_TRUE(index.replaceIfMatch(key, 100, 3));
    EXPECT_EQ(1, index.peek(key).getTotalHits());
    EXPECT_EQ(0, index.peek(key).getCurrentHits());
}

TEST(Index, HitsUpperBound)
{
    Index index;
    const UInt64 key = 8888;

    index.insert(key, 0, 0);
    for (int i = 0; i < 1000; i++)
    {
        index.lookup(key);
    }

    EXPECT_EQ(255, index.peek(key).getTotalHits());
    EXPECT_EQ(255, index.peek(key).getCurrentHits());
}

TEST(Index, ThreadSafe)
{
    Index index;
    const UInt64 key = 1314;
    index.insert(key, 0, 0);

    auto lookup = [&]() { index.lookup(key); };

    std::vector<std::thread> threads;
    threads.reserve(200);
    for (int i = 0; i < 200; i++)
    {
        threads.emplace_back(lookup);
    }

    for (auto & t : threads)
    {
        t.join();
    }

    EXPECT_EQ(200, index.peek(key).getTotalHits());
    EXPECT_EQ(200, index.peek(key).getCurrentHits());
}

TEST(Index, Recovery)
{
    Index index;
    std::vector<std::pair<UInt64, UInt32>> log;
    for (UInt64 i = 0; i < 16; i++)
    {
        for (UInt64 j = 0; j < 10; j++)
        {
            UInt64 key = i << 32 | j;
            UInt32 val = j + i;
            index.insert(key, val, 0);
            log.emplace_back(key, val);
        }
    }

    Buffer metadata(INT_MAX);

    {
        google::protobuf::io::ArrayOutputStream raw_stream(metadata.data(), INT_MAX);
        google::protobuf::io::CodedOutputStream ostream(&raw_stream);

        index.persist(&ostream);
    }

    Index new_index;
    google::protobuf::io::ArrayInputStream raw_stream(metadata.data(), INT_MAX);
    google::protobuf::io::CodedInputStream istream(&raw_stream);
    new_index.recover(&istream);
    for (auto & entry : log)
    {
        auto lr = new_index.lookup(entry.first);
        EXPECT_EQ(entry.second, lr.getAddress());
    }
}

}
