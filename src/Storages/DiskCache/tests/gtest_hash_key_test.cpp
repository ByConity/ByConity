#include <gtest/gtest.h>

#include <Storages/DiskCache/HashKey.h>


namespace DB::HybridCache
{
TEST(HashKey, HashedKeyCollision)
{
    HashedKey key1{"key 1"};
    HashedKey key2{"key 2"};

    HashedKey key3 = HashedKey::precomputed(key2.key(), key1.keyHash());

    EXPECT_NE(key1, key2);
    EXPECT_NE(key1, key3);
    EXPECT_NE(key2, key3);
}
}
