#include <chrono>
#include <condition_variable>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <gtest/gtest.h>
#include "Common/formatIPv6.h"
#include <Common/BucketLRUCache.h>

using namespace DB;

using TrivialBucketLRUCache = BucketLRUCache<String, String, DimensionBucketLRUWeight<2>,
    TrivialDimensionBucketLRUWeighter<2, String>>;

class BucketLRUCacheTest: public ::testing::Test {
public:
    static void SetUpTestCase() {
    }

    static void TearDownTestCase() {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }

    template<typename TWeight, typename WeightFunction, typename HashFunction>
    static void insertToCache(BucketLRUCache<String, String, TWeight, WeightFunction, HashFunction>& cache,
            int begin, int end) {
        for (int i = begin; i < end; ++i) {
            cache.insert(std::to_string(i), std::make_shared<String>(std::to_string(i)));
        }
    }

    template<typename TWeight, typename WeightFunction, typename HashFunction>
    static void verifyRangeExist(BucketLRUCache<String, String, TWeight, WeightFunction, HashFunction>& cache,
            int begin, int end) {
        for (int i = begin; i < end; i++) {
            ASSERT_EQ(*cache.get(std::to_string(i)), std::to_string(i));
        }
    }

    template<typename TWeight, typename WeightFunction, typename HashFunction>
    static void verifyRangeNotExist(BucketLRUCache<String, String, TWeight, WeightFunction, HashFunction>& cache,
            int begin, int end) {
        for (int i = begin; i < end; i++) {
            ASSERT_EQ(cache.get(std::to_string(i)), nullptr);
        }
    }

    template<typename TWeight, typename WeightFunction, typename HashFunction>
    static void verifyLRUOrder(BucketLRUCache<String, String, TWeight, WeightFunction, HashFunction>& cache,
            const std::vector<String>& order) {
        ASSERT_EQ(cache.queue.size(), order.size());
        auto iter = cache.queue.begin();
        for (const String& key : order) {
            ASSERT_EQ(*iter, key);
            ++iter;
        }
    }

    template<typename TWeight, typename WeightFunction, typename HashFunction>
    static void verifyCacheWeight(BucketLRUCache<String, String, TWeight, WeightFunction, HashFunction>& cache,
            const TWeight& cache_weight) {
        ASSERT_EQ(cache.weight(), cache_weight);
    }

    template<typename TWeight, typename WeightFunction, typename HashFunction>
    static void removeFromCache(BucketLRUCache<String, String, TWeight, WeightFunction, HashFunction>& cache,
            const String& key) {
        cache.erase(key);
    }

    static std::vector<String> generateRangeStr(int begin, int end) {
        std::vector<String> result;
        result.reserve(end - begin);
        for (int i = begin; i < end; i++) {
            result.push_back(std::to_string(i));
        }
        return result;
    }
};

#define VERIFY_CACHE_RANGE_EXIST(cache, begin, end) \
    ASSERT_NO_FATAL_FAILURE(verifyRangeExist(cache, begin, end))

#define VERIFY_CACHE_RANGE_NOT_EXIST(cache, begin, end) \
    ASSERT_NO_FATAL_FAILURE(verifyRangeNotExist(cache, begin, end))

#define VERIFY_CACHE_LRU_ORDER(cache, order) \
    ASSERT_NO_FATAL_FAILURE(verifyLRUOrder(cache, order))

#define VERIFY_CACHE_WEIGHT(cache, weight) \
    ASSERT_NO_FATAL_FAILURE(verifyCacheWeight(cache, weight))

TEST_F(BucketLRUCacheTest, Simple) {
    size_t cache_size = 5;
    // Set lru refresh interval to 24h, so it shouldn't refresh lru list
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, cache_size);

    int round = 10;
    VERIFY_CACHE_RANGE_NOT_EXIST(cache, cache_size, cache_size + round);

    for (int i = cache_size; i < cache_size + round; i++) {
        cache.insert(std::to_string(i), std::make_shared<String>(std::to_string(i)));

        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_RANGE_NOT_EXIST(cache, 0, i - cache_size);
        VERIFY_CACHE_RANGE_EXIST(cache, i - cache_size + 1, i + 1);
    }
}

TEST_F(BucketLRUCacheTest, AllInsert) {
    size_t cache_size = 5;

    // Set lru refresh interval to 24h, so it shouldn't refresh lru list
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, cache_size);

    // Emplace without insert
    {
        ASSERT_FALSE(cache.emplace("0", std::make_shared<String>("1")));
        ASSERT_EQ(*(cache.get("0")), "0");
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));
    }

    // Emplace with insert
    {
        ASSERT_TRUE(cache.emplace("5", std::make_shared<String>("5")));
        ASSERT_EQ(*(cache.get("5")), "5");
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "2", "3", "4", "5"}));
    }

    // Update existing value
    {
        ASSERT_NO_THROW(cache.update("1", std::make_shared<String>("2")));
        ASSERT_EQ(*(cache.get("1")), "2");
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"2", "3", "4", "5", "1"}));
    }

    // Update not existed value
    {
        ASSERT_THROW(cache.update("6", std::make_shared<String>("6")), DB::Exception);
        ASSERT_EQ(cache.get("6"), nullptr);
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"2", "3", "4", "5", "1"}));
    }

    // Upsert not existed value
    {
        ASSERT_NO_THROW(cache.upsert("6", std::make_shared<String>("6")));
        ASSERT_EQ(*(cache.get("6")), "6");
        ASSERT_EQ(cache.get("2"), nullptr);
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"3", "4", "5", "1", "6"}));
    }

    // Upsert existed value
    {
        ASSERT_NO_THROW(cache.upsert("3", std::make_shared<String>("6")));
        ASSERT_EQ(*(cache.get("3")), "6");
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"4", "5", "1", "6", "3"}));
    }

    // Insert non exist value
    {
        ASSERT_NO_THROW(cache.insert("7", std::make_shared<String>("7")));
        ASSERT_EQ(*(cache.get("7")), "7");
        ASSERT_EQ(cache.get("4"), nullptr);
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"5", "1", "6", "3", "7"}));
    }

    // Insert existed value
    {
        ASSERT_THROW(cache.insert("5", std::make_shared<String>("6")), DB::Exception);
        ASSERT_EQ(*(cache.get("5")), "5");
        VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
        VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"5", "1", "6", "3", "7"}));
    }
}

TEST_F(BucketLRUCacheTest, Erase) {
    size_t cache_size = 5;

    // Set lru refresh interval to 24h, so it shouldn't refresh lru list
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, cache_size);

    cache.erase("1");
    cache.erase("3");

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({3, 3}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "2", "4"}));

    cache.insert("1", std::make_shared<String>("1"));

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({4, 4}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "2", "4", "1"}));
}

TEST_F(BucketLRUCacheTest, StrictLRU) {
    size_t cache_size = 5;

    // Set lru refresh interval to 24h, so it shouldn't refresh lru list
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 0,
        .mapping_bucket_size = 2,
        .max_weight = DimensionBucketLRUWeight<2>({cache_size, cache_size}),
    });

    insertToCache(cache, 0, cache_size);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));

    cache.get("2");

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "3", "4", "2"}));

    cache.get("2");

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "3", "4", "2"}));

    cache.get("1");

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "3", "4", "2", "1"}));
}

struct TestWeightHandler {
    DimensionBucketLRUWeight<2> operator()(const String& val) {
        return DimensionBucketLRUWeight<2>({1, std::stoul(val)});
    }
};

TEST_F(BucketLRUCacheTest, CustomizeEvictHandler) {
    size_t cache_size = 5;

    auto evict_handler = [](const String&, const String&, const DimensionBucketLRUWeight<2>&) {
        return std::pair<bool, std::shared_ptr<String>>({false, std::make_shared<String>("0")});
    };

    // Set lru refresh interval to 24h, so it shouldn't refresh lru list
    BucketLRUCache<String, String, DimensionBucketLRUWeight<2>, TestWeightHandler> cache(BucketLRUCache<String, String, DimensionBucketLRUWeight<2>, TestWeightHandler>::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), 10}),
        .evict_handler = evict_handler,
    });

    insertToCache(cache, 0, cache_size);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 10}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));

    insertToCache(cache, cache_size, cache_size + 1);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({6, 9}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"4", "5", "0", "1", "2", "3"}));

    cache.erase("0");

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 9}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"4", "5", "1", "2", "3"}));
}

TEST_F(BucketLRUCacheTest, CustomizeEvictHandlerUpdateOrder) {
    auto evict_handler = [](const String&, const String&, const DimensionBucketLRUWeight<2>& sz) {
        if (sz[1] > 0) {
            return std::pair<bool, std::shared_ptr<String>>({false, std::make_shared<String>("0")});
        }
        return std::pair<bool, std::shared_ptr<String>>({true, nullptr});
    };

    // Set lru refresh interval to 24h, so it shouldn't refresh lru list
    BucketLRUCache<String, String, DimensionBucketLRUWeight<2>, TestWeightHandler> cache(BucketLRUCache<String, String, DimensionBucketLRUWeight<2>, TestWeightHandler>::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), 5}),
        .evict_handler = evict_handler,
    });

    cache.insert("0", std::make_shared<String>("1"));
    cache.insert("1", std::make_shared<String>("0"));
    cache.insert("2", std::make_shared<String>("0"));
    cache.insert("3", std::make_shared<String>("1"));
    cache.insert("4", std::make_shared<String>("1"));

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 3}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));

    cache.insert("5", std::make_shared<String>("5"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({4, 5}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"5", "0", "3", "4"}));
}

TEST_F(BucketLRUCacheTest, Evict) {
    size_t cache_size = 5;
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 0,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 1);

    insertToCache(cache, cache_size, cache_size + 1);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_RANGE_NOT_EXIST(cache, 1, 2);
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 1);
    VERIFY_CACHE_RANGE_EXIST(cache, 2, cache_size + 1);
}

TEST_F(BucketLRUCacheTest, EvictWithSkipInterval) {
    size_t cache_size = 5;
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 10,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(0, cache_size));

    cache.get("4");
    cache.get("3");
    cache.get("2");
    cache.get("1");
    cache.get("0");
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(0, cache_size));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));

    std::this_thread::sleep_for(std::chrono::seconds(11));
    cache.get("0");
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "2", "3", "4", "0"}));
}

TEST_F(BucketLRUCacheTest, InorderEvict) {
    size_t cache_size = 10;
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 1,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    int total_size = 10000;
    insertToCache(cache, 0, total_size);

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(total_size - cache_size, total_size));
    VERIFY_CACHE_RANGE_NOT_EXIST(cache, 0, total_size - cache_size);
    VERIFY_CACHE_RANGE_EXIST(cache, total_size - cache_size, total_size);
}

TEST_F(BucketLRUCacheTest, SetInsert) {
    size_t cache_size = 5;
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size - 1);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size - 1, cache_size - 1}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, cache_size - 1);
    VERIFY_CACHE_RANGE_NOT_EXIST(cache, cache_size - 1, cache_size);
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(0, cache_size - 1));

    insertToCache(cache, cache_size - 1, cache_size);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, cache_size);
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(0, cache_size));
}

TEST_F(BucketLRUCacheTest, SetUpdate) {
    size_t cache_size = 3;
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    insertToCache(cache, 0, cache_size);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));

    VERIFY_CACHE_RANGE_EXIST(cache, 0, cache_size);
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(0, cache_size));

    cache.update("0", std::make_shared<String>("3"));
    ASSERT_EQ(*cache.get("0"), "3");
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({cache_size, cache_size}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "2", "0"}));
}

TEST_F(BucketLRUCacheTest, Remove) {
    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 1,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), 5}),
    });

    insertToCache(cache, 0, 5);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 5}));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 5);
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(0, 5));

    removeFromCache(cache, String("0"));
    removeFromCache(cache, String("4"));

    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({3, 3}));
    VERIFY_CACHE_RANGE_EXIST(cache, 1, 4);
    VERIFY_CACHE_LRU_ORDER(cache, generateRangeStr(1, 4));

    removeFromCache(cache, String("2"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({2, 2}));
    VERIFY_CACHE_RANGE_EXIST(cache, 1, 2);
    VERIFY_CACHE_RANGE_EXIST(cache, 3, 4);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "3"}));

    insertToCache(cache, 5, 6);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({3, 3}));
    VERIFY_CACHE_RANGE_EXIST(cache, 1, 2);
    VERIFY_CACHE_RANGE_EXIST(cache, 3, 4);
    VERIFY_CACHE_RANGE_EXIST(cache, 5, 6);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "3", "5"}));

    insertToCache(cache, 4, 5);
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({4, 4}));
    VERIFY_CACHE_RANGE_EXIST(cache, 1, 2);
    VERIFY_CACHE_RANGE_EXIST(cache, 3, 6);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "3", "5", "4"}));
}

TEST_F(BucketLRUCacheTest, Fuzzy) {
    size_t cache_size = 10;
    int mapping_bucket_size = 2;
    int lru_update_interval = 1;
    int worker_num = 30;
    int range_begin = 0;
    int range_end = 100;
    int per_thread_op_count = 100000;

    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = static_cast<UInt32>(lru_update_interval),
        .mapping_bucket_size = static_cast<UInt32>(mapping_bucket_size),
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    auto worker = [=, &cache]() {
        std::default_random_engine re;
        std::uniform_int_distribution<int> num_dist(range_begin, range_end);
        std::uniform_int_distribution<int> op_dist(0, 3);
        for (int count = 0; count < per_thread_op_count; count++) {
            int op = op_dist(re);
            String key = std::to_string(num_dist(re));
            if (op < 2) {
                // Read op
                auto res = cache.get(key);
                if (res != nullptr) {
                    ASSERT_EQ(*res, key);
                }
            } else if (op == 2) {
                cache.upsert(key, std::make_shared<String>(key));
            } else if (op == 3) {
                cache.erase(key);
            }
        }
    };

    std::vector<std::thread> tasks;
    for (int i = 0; i < worker_num; i++)
    {
        tasks.emplace_back(worker);
    }
    for (auto& task : tasks)
    {
        task.join();
    }
}

using MultiWeightBucketLRUCache = BucketLRUCache<String, String,
    DimensionBucketLRUWeight<2>, TestWeightHandler>;

TEST_F(BucketLRUCacheTest, MultiWeight)
{
    MultiWeightBucketLRUCache cache(MultiWeightBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 2,
        .max_weight = DimensionBucketLRUWeight<2>({5, 7}),
    });

    // Verify limit on count
    for (int i = 0; i < 10; ++i)
    {
        cache.insert(std::to_string(i), std::make_shared<String>("0"));
    }
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 0}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"5", "6", "7", "8", "9"}));

    // Verify limit on weight
    for (int i = 10; i < 20; ++i)
    {
        cache.insert(std::to_string(i), std::make_shared<String>("2"));
    }
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({3, 6}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"17", "18", "19"}));

    // Verify limit on count again
    for (int i = 20; i < 23; ++i)
    {
        cache.insert(std::to_string(i), std::make_shared<String>("0"));
    }
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 4}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"18", "19", "20", "21", "22"}));

    // Verify limit on weight again
    cache.insert("23", std::make_shared<String>("2"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 4}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"19", "20", "21", "22", "23"}));

    cache.insert("24", std::make_shared<String>("1"));
    cache.insert("25", std::make_shared<String>("2"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 5}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"21", "22", "23", "24", "25"}));

    cache.insert("26", std::make_shared<String>("0"));
    cache.insert("27", std::make_shared<String>("3"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({4, 6}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"24", "25", "26", "27"}));

    cache.insert("28", std::make_shared<String>("7"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({1, 7}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"28"}));

    cache.insert("29", std::make_shared<String>("1"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({1, 1}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"29"}));

    cache.insert("30", std::make_shared<String>("8"));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({0, 0}));
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({}));
}

void verifyCacheLoad(TrivialBucketLRUCache& cache, bool loaded, const String& key,
    const String& value)
{
    bool load = false;
    ASSERT_EQ(
        value,
        *(cache.getOrSet(key, [&load, &value]() {
            load = true;
            return std::make_shared<String>(value);
        }))
    );
    ASSERT_EQ(loaded, load);
}

TEST_F(BucketLRUCacheTest, getOrSet)
{
    size_t cache_size = 5;

    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    for (size_t i = 0; i < 6; ++i)
    {
        ASSERT_NO_FATAL_FAILURE(verifyCacheLoad(cache, true, std::to_string(i),
            std::to_string(i)));
        ASSERT_NO_FATAL_FAILURE(verifyCacheLoad(cache, false, std::to_string(i),
            std::to_string(i)));
    }
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"1", "2", "3", "4", "5"}));

    ASSERT_NO_FATAL_FAILURE(verifyCacheLoad(cache, true, "0", "0"));
    ASSERT_NO_FATAL_FAILURE(verifyCacheLoad(cache, false, "0", "0"));

    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"2", "3", "4", "5", "0"}));
}

TEST_F(BucketLRUCacheTest, concurrentGetOrSet)
{
    size_t cache_size = 5;

    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
    });

    std::atomic<size_t> loaded = 0;
    auto worker = [&]() {
        cache.getOrSet("0", [&loaded]() {
            loaded.fetch_add(1);
            return std::make_shared<String>();
        });
    };

    std::vector<std::thread> tasks;
    for (size_t i = 0; i < 10; ++i)
    {
        tasks.emplace_back(worker);
    }
    for (auto& task : tasks)
    {
        task.join();
    }
    ASSERT_EQ(loaded, 1);
}

TEST_F(BucketLRUCacheTest, cacheTTL)
{
    size_t cache_size = 5;

    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
        .cache_ttl = 24 * 60 * 60,
    });

    insertToCache(cache, 0, 5);
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 5);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 5}));

    cache.insert("5", std::make_shared<String>("5"));
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 5);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 5}));

    std::shared_ptr<String> ret = cache.getOrSet("6", []() {
        return std::make_shared<String>("6");
    });
    ASSERT_EQ(*ret, "6");
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 5);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 5}));
}

TEST_F(BucketLRUCacheTest, multiGetOrSetWithTTL)
{
    size_t cache_size = 5;

    TrivialBucketLRUCache cache(TrivialBucketLRUCache::Options {
        .lru_update_interval = 24 * 60 * 60,
        .mapping_bucket_size = 3,
        .max_weight = DimensionBucketLRUWeight<2>({std::numeric_limits<size_t>::max(), cache_size}),
        .cache_ttl = 24 * 60 * 60,
    });

    insertToCache(cache, 0, 5);
    VERIFY_CACHE_RANGE_EXIST(cache, 0, 5);
    VERIFY_CACHE_LRU_ORDER(cache, std::vector<String>({"0", "1", "2", "3", "4"}));
    VERIFY_CACHE_WEIGHT(cache, DimensionBucketLRUWeight<2>({5, 5}));

    size_t loaded = 0;
    for (size_t i = 0; i < 10; ++i)
    {
        cache.getOrSet("10", [&]() {
            ++loaded;
            return std::make_shared<String>();
        });
    }

    ASSERT_EQ(loaded, 10);
    ASSERT_EQ(cache.get("10"), nullptr);
}
