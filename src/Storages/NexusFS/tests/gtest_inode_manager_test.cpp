#include <thread>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>

#include <IO/WriteHelpers.h>
#include <Storages/DiskCache/FifoPolicy.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/NexusFS/NexusFSInodeManager.h>
#include <common/types.h>

namespace DB::NexusFSComponents
{

using namespace HybridCache;

TEST(NexusFSInodeManager, GetAndSet)
{
    auto get_file_and_segment_size = []() { return std::make_pair(5, 1); };
    InodeManager index("/prefix/", "/data", 1);
    auto h1 = std::make_shared<BlockHandle>(RelAddress(RegionId(1), 2), 3);
    index.insert("/prefix//AA/BB/CC/data", 0, h1, get_file_and_segment_size);
    auto h2 = index.lookup("/prefix/AA/BB//CC//data", 0);
    EXPECT_EQ(h1.get(), h2.get());
    EXPECT_TRUE(index.lookup("/prefix/AA/BB/CC/data", 0));
    EXPECT_FALSE(index.lookup("/prefix/AA/BB/CC/data", 1));
    EXPECT_FALSE(index.lookup("/prefix/AA/BB/CC/data", 1));
    EXPECT_FALSE(index.lookup("/prefix/AA/BB/CCX/data", 0));
    EXPECT_FALSE(index.lookup("/prefix/AA/AA/BB/CC/data", 0));

    EXPECT_FALSE(index.lookup("/prefix/AA/BB/BB/data", 1));
    auto h3 = std::make_shared<BlockHandle>(RelAddress(RegionId(2), 3), 4);
    index.insert("/prefix/AA/BB/BB/data", 1, h3, get_file_and_segment_size);
    EXPECT_TRUE(index.lookup("/prefix/AA/BB/BB/data", 1));

    EXPECT_FALSE(index.lookup("/prefix/0/data", 1));
    auto h4 = std::make_shared<BlockHandle>(RelAddress(RegionId(5), 3), 4);
    index.insert("/prefix/0/data", 1, h4, get_file_and_segment_size);
    EXPECT_TRUE(index.lookup("/prefix/0/data", 1));
}

TEST(NexusFSInodeManager, InvalidPath)
{
    InodeManager index("/prefix/", "/data", 128);
    EXPECT_THROW({ index.lookup("", 0); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix1/AA/BB/CC/data", 0); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix/AA/BB/CC/data2", 0); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix/data", 0); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix//data", 0); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix///data", 0); }, Exception);
}

TEST(NexusFSInodeManager, InvalidSegmentId)
{
    auto get_file_and_segment_size = []() { return std::make_pair(5, 1); };
    InodeManager index("/prefix/", "/data", 1);
    EXPECT_FALSE(index.lookup("/prefix/AA/data", 1));
    auto h1 = std::make_shared<BlockHandle>(RelAddress(RegionId(1), 2), 3);
    index.insert("/prefix/AA/data", 1, h1, get_file_and_segment_size);
    EXPECT_TRUE(index.lookup("/prefix/AA/data", 1));
    EXPECT_THROW({ index.lookup("/prefix/AA/data", 5); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix/AA/data", 6); }, Exception);
    EXPECT_THROW({ index.lookup("/prefix/AA/data", 999); }, Exception);
    EXPECT_THROW({ index.insert("/prefix/AA/data", 5, h1, get_file_and_segment_size); }, Exception);
    EXPECT_THROW({ index.insert("/prefix/AA/data", 6, h1, get_file_and_segment_size); }, Exception);
    EXPECT_THROW({ index.insert("/prefix/AA/data", 999, h1, get_file_and_segment_size); }, Exception);
}

TEST(NexusFSInodeManager, ThreadSafe)
{
    auto get_file_and_segment_size = []() { return std::make_pair(20, 1); };
    InodeManager index("/prefix/", "/data", 128);
    const String file = "/prefix/AA/BB/CC/DD/EE/data";
    auto handle = std::make_shared<BlockHandle>(RelAddress(RegionId(1), 2), 3);
    index.insert(file, 10, handle, get_file_and_segment_size);

    auto lookup = [&]() { index.lookup(file, 10); };

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

    // TODO: hits
    // EXPECT_EQ(200, index.peek(key).getTotalHits());
    // EXPECT_EQ(200, index.peek(key).getCurrentHits());
}

TEST(NexusFSInodeManager, Recovery)
{
    auto get_file_and_segment_size = []() { return std::make_pair(10, 1); };
    InodeManager index("/prefix/", "/data", 1);
    std::vector<std::pair<String, UInt64>> log;
    for (UInt64 i = 0; i < 16; i++)
    {
        for (UInt64 j = 0; j < 10; j++)
        {
            String file = "/prefix/123/" + toString(i) + "/data";
            auto handle = std::make_shared<BlockHandle>(RelAddress(RegionId(i), j), 1);
            index.insert(file, j, handle, get_file_and_segment_size);
            log.emplace_back(file, j);
        }
    }
    for (UInt64 i = 16; i < 20; i++)
    {
        for (UInt64 j = 0; j < 10; j++)
        {
            String file = "/prefix/" + toString(i) + "/data";
            auto handle = std::make_shared<BlockHandle>(RelAddress(RegionId(i), j), 1);
            index.insert(file, j, handle, get_file_and_segment_size);
            log.emplace_back(file, j);
        }
    }
    {
        auto files = index.getFileCachedStates();
        EXPECT_EQ(20, files.size());
        for (auto & file : files)
        {
            EXPECT_EQ(10, file.cached_size);
            EXPECT_EQ(10, file.total_size);
            EXPECT_EQ(10, file.cached_segments);
            EXPECT_EQ(10, file.total_segments);
        }
    }

    Buffer metadata(INT_MAX);

    {
        google::protobuf::io::ArrayOutputStream raw_stream(metadata.data(), INT_MAX);
        google::protobuf::io::CodedOutputStream ostream(&raw_stream);

        index.persist(&ostream);
    }

    auto device = createMemoryDevice(4096 * 20, 4096);
    auto policy = std::make_unique<HybridCache::FifoPolicy>();
    RegionManager region_manager(20, 4096, 0, *device, 1, 1, {}, {}, std::move(policy), 2, 4, 10);
    std::atomic<UInt64> num_segments = 0;
    InodeManager new_index("/prefix/", "/data", 1);
    google::protobuf::io::ArrayInputStream raw_stream(metadata.data(), INT_MAX);
    google::protobuf::io::CodedInputStream istream(&raw_stream);
    new_index.recover(&istream, region_manager, num_segments);
    for (auto & entry : log)
    {
        EXPECT_TRUE(new_index.lookup(entry.first, entry.second));
    }
    for (UInt64 i = 20; i < 24; i++)
    {
        for (UInt64 j = 0; j < 10; j++)
        {
            String file = "/prefix/123/" + toString(i) + "/data";
            EXPECT_FALSE(new_index.lookup(file, j));
        }
    }
    {
        auto files = index.getFileCachedStates();
        EXPECT_EQ(20, files.size());
        for (auto & file : files)
        {
            EXPECT_EQ(10, file.cached_size);
            EXPECT_EQ(10, file.total_size);
            EXPECT_EQ(10, file.cached_segments);
            EXPECT_EQ(10, file.total_segments);
        }
    }
}

}
