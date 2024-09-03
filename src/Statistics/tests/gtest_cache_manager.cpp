#include <atomic>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatisticsCache.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsDummy.h>
#include <Statistics/tests/testing_utils.h>
#include <gtest/gtest.h>
#include <Poco/ExpireLRUCache.h>
#include "Common/Stopwatch.h"
#include <Common/LRUCache.h>
#include "Statistics/StatsColumnBasic.h"

using namespace DB;
using namespace DB::Statistics;

struct KeyHash
{
    auto operator()(const std::pair<UUID, String> & key) const
    {
        return UInt128TrivialHash()(key.first) ^ std::hash<String>()(key.second);
    }
};

TEST(StatsCacheManager, CacheTest)
{
    SUCCEED();

    LRUCache<std::pair<UUID, String>, StatisticsBase, KeyHash> cache(10);
    auto refA = std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 0, 1, 2, 3, 4, 5, 6, 7, 8, 0, 1};
    auto refB = std::vector<bool>{1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    for (int i = 0; i < 20; ++i)
    {
        auto index = i % 9;
        UInt128 u128{index, 0xBEEF};
        UUID uuid(u128);
        auto col_name = fmt::format(FMT_STRING("col{}"), index);
        auto [stats_val, created] = cache.getOrSet(
            std::make_pair(uuid, col_name),
            // wtf
            [=]() { return std::make_shared<StatsDummyAlpha>(i); });
        EXPECT_EQ(dynamic_pointer_cast<StatsDummyAlpha>(stats_val)->get(), refA[i]);
        EXPECT_EQ(created, refB[i]);
        // cout << dynamic_pointer_cast<StatsDummyAlpha>(res.first)->get() << " " << res.second << endl;
    }

    // flush the cache
    for (int i = 0; i < 20; ++i)
    {
        auto index = i % 9;
        UInt128 u128{index, 0xDEAD};
        UUID uuid(u128);
        auto col_name = fmt::format(FMT_STRING("col{}"), index);
        auto [stats_val, created] = cache.getOrSet(
            std::make_pair(uuid, col_name),
            // wtf
            [=]() { return std::make_shared<StatsDummyAlpha>(i + 10); });
        EXPECT_EQ(dynamic_pointer_cast<StatsDummyAlpha>(stats_val)->get(), refA[i] + 10);
        EXPECT_EQ(created, refB[i]);
        // cout << dynamic_pointer_cast<StatsDummyAlpha>(res.first)->get() << " " << res.second << endl;
    }

    // flush the cache again
    for (int i = 0; i < 20; ++i)
    {
        auto index = i % 9;
        UInt128 u128{index, 0xDEAD};
        UUID uuid(u128);
        auto col_name = fmt::format(FMT_STRING("colPlus{}"), index);
        auto [stats_val, created] = cache.getOrSet(
            std::make_pair(uuid, col_name),
            // wtf
            [=]() { return std::make_shared<StatsDummyAlpha>(i + 20); });
        EXPECT_EQ(dynamic_pointer_cast<StatsDummyAlpha>(stats_val)->get(), refA[i] + 20);
        EXPECT_EQ(created, refB[i]);
        // cout << dynamic_pointer_cast<StatsDummyAlpha>(res.first)->get() << " " << res.second << endl;
    }

    // flush the cache all
    for (int i = 0; i < 20; ++i)
    {
        auto index = i % 11;
        UInt128 u128{index, 0xDEAD};
        UUID uuid(u128);
        auto col_name = fmt::format(FMT_STRING("col{}"), index);
        auto [stats_val, created] = cache.getOrSet(
            std::make_pair(uuid, col_name),
            // wtf
            [=]() { return std::make_shared<StatsDummyAlpha>(i + 30); });

        EXPECT_EQ(dynamic_pointer_cast<StatsDummyAlpha>(stats_val)->get(), i + 30);
        EXPECT_EQ(created, true);
        // cout << dynamic_pointer_cast<StatsDummyAlpha>(res.first)->get() << " " << res.second << endl;
    }
}


// TEST(StatsCacheManager, StatisticsCache)
// {
//     SUCCEED();
//     StatisticsCache cache(chrono::seconds(10));
//     // the following test should be done in 10 seconds
//     auto uuid1 = UUID(UInt128{0, 1});
//     auto uuid2 = UUID(UInt128{0, 2});

//     int columns = 10;

//     int max_write = 1000;
//     int max_read = 100000;

//     Stopwatch watch;
//     std::vector<std::thread> threads;
//     for (int iter_ = 0; iter_ < 10; ++iter_)
//     {
//         threads.emplace_back(std::thread(
//             [&](int i) {
//                 for (auto j = 0; j < max_write; ++j)
//                 {
//                     std::optional<String> column;
//                     if (i != 0)
//                         column = std::to_string(i);
//                     auto cc = std::make_shared<StatsCollection>();
//                     cc->emplace(StatisticsTag::DummyAlpha, std::make_shared<StatsDummyAlpha>(i + j * columns));
//                     cache.update(uuid1, column, cc);
//                     cache.update(uuid2, column, cc);
//                 }
//             },
//             iter_));
//     }
//     for (auto & thread : threads)
//     {
//         thread.join();
//     }
//     threads.clear();

//     cache.invalidate(uuid2);

//     for (int iter_ = 0; iter_ < columns + 1; ++iter_)
//     {
//         threads.emplace_back(std::thread(
//             [&](int iter) {
//                 for (auto j = 0; j < max_read; ++j)
//                 {
//                     std::optional<String> column;
//                     std::default_random_engine e(iter);
//                     int i = e() % columns;
//                     if (i != 0)
//                         column = std::to_string(i);
//                     auto data1 = cache.get(uuid1, column);
//                     auto data2 = cache.get(uuid2, column);
//                     ASSERT_FALSE(!!data2);
//                     if (columns == i)
//                     {
//                         ASSERT_FALSE(!!data1);
//                     }
//                     else
//                     {
//                         ASSERT_TRUE(!!data1);
//                         ASSERT_TRUE(data1->contains(StatisticsTag::DummyAlpha));
//                         auto ptr = dynamic_pointer_cast<StatsDummyAlpha>((*data1)[StatisticsTag::DummyAlpha]);
//                         ASSERT_EQ(ptr->get(), (max_write - 1) * columns + i);
//                     }
//                 }
//             },
//             iter_));
//     }
//     for (auto & thread : threads)
//     {
//         thread.join();
//     }
//     threads.clear();
// }
