#include <gtest/gtest.h>
#include <Common/LRUCache.h>


using namespace DB;

static const size_t LRUCacheSize = 5;
static const std::string COMMON_KEY = "KEY_";
static const std::shared_ptr<std::string> VALUE = std::make_shared<std::string>("value");


TEST(LRUCache, LRUWithoutExpiration)
{
    LRUCache<std::string, std::string> cache(LRUCacheSize);
    
    /// insert more elements than cache capacity
    for (int i=0; i<10; i++)
        cache.set(COMMON_KEY + std::to_string(i), VALUE);

    /// elements are evicted when cache size exceed the limit.
    ASSERT_EQ(cache.weight(), 5);
    /// trival weight function means count == weight
    ASSERT_EQ(cache.count(), 5);
}

TEST(LRUCache, LRUWithExpiration)
{
    LRUCache<std::string, std::string> cache(LRUCacheSize, std::chrono::seconds(300));
    
    /// insert more elements than cache capacity
    for (int i=0; i<10; i++)
        cache.set(COMMON_KEY + std::to_string(i), VALUE);

    /// no element is evicted because of the expiration is set.
    ASSERT_EQ(cache.weight(), 10);
    /// trival weight function means count == weight
    ASSERT_EQ(cache.count(), 10);
}
