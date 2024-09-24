#include <climits>
#include <random>
#include <utility>
#include <vector>
#include <stdint.h>
#include <sys/param.h>

#include <gmock/gmock.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>

#include <Functions/FunctionsHashing.h>
#include <Interpreters/BloomFilter.h>
#include <Storages/DiskCache/BloomFilter.h>
#include <Storages/DiskCache/Buffer.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <common/types.h>
#include <common/unit.h>

namespace DB::HybridCache
{
TEST(BloomFilter, OptimalParams)
{
    {
        auto bf = BloomFilter::makeBloomFilter(1000, 25, 0.1);

        EXPECT_EQ(1000, bf.numFilters());
        EXPECT_EQ(120, bf.numBitsPerFilter());
        EXPECT_EQ(3, bf.numHashes());
    }

    {
        auto bf = BloomFilter::makeBloomFilter(6, 200'000'000, 0.02);
        EXPECT_EQ(6, bf.numFilters());
        EXPECT_EQ(1630310280, bf.numBitsPerFilter());
        EXPECT_EQ(6, bf.numHashes());
    }
}

TEST(BloomFilter, Default)
{
    BloomFilter bf{};

    EXPECT_EQ(0, bf.numFilters());
    EXPECT_EQ(0, bf.numBitsPerFilter());
    EXPECT_EQ(0, bf.numHashes());
    EXPECT_EQ(0, bf.getByteSize());

    std::uniform_int_distribution<UInt64> dist(INT64_MIN, INT64_MAX);
    auto key = dist(thread_local_rng);
    EXPECT_TRUE(bf.couldExist(1, key));
    EXPECT_TRUE(bf.couldExist(1, key + 1));

    bf.set(1, key);
    EXPECT_TRUE(bf.couldExist(1, key));
    EXPECT_TRUE(bf.couldExist(1, key + 1));
}

TEST(BloomFilter, Move)
{
    UInt32 num_filters = 10;
    BloomFilter bf = BloomFilter::makeBloomFilter(num_filters, 1000, 0.01);
    auto num_hashes = bf.numHashes();
    auto num_bits = bf.numBitsPerFilter();
    std::vector<UInt64> added_keys;

    auto idx = [&](UInt64 key) { return key % num_hashes; };
    std::uniform_int_distribution<UInt64> dist(INT64_MIN, INT64_MAX);
    for (int i = 0; i < 1000; i++)
    {
        auto key = dist(thread_local_rng);
        bf.set(idx(key), key);
        EXPECT_TRUE(bf.couldExist(idx(key), key));
        added_keys.push_back(key);
    }

    std::vector<UInt64> non_existen_keys;
    while (non_existen_keys.size() < added_keys.size())
    {
        auto key = dist(thread_local_rng);
        if (!bf.couldExist(idx(key), key))
            non_existen_keys.push_back(key);
    }

    auto new_bf = std::move(bf);
    EXPECT_EQ(0, bf.numHashes());

    EXPECT_EQ(num_filters, new_bf.numFilters());
    EXPECT_EQ(num_bits, new_bf.numBitsPerFilter());
    EXPECT_EQ(num_hashes, new_bf.numHashes());

    for (auto k : added_keys)
        EXPECT_TRUE(new_bf.couldExist(idx(k), k));

    for (auto k : non_existen_keys)
        EXPECT_FALSE(new_bf.couldExist(idx(k), k));
}

TEST(BloomFilter, Reset)
{
    BloomFilter bf{4, 2, 4};
    EXPECT_EQ(4, bf.getByteSize());
    for (UInt32 i = 0; i < 4; i++)
    {
        for (UInt64 key = 0; key < 10; key++)
            bf.set(i, key);
    }

    for (UInt32 i = 0; i < 4; i++)
    {
        for (UInt64 key = 0; key < 10; key++)
            EXPECT_TRUE(bf.couldExist(i, key));
    }

    bf.reset();

    for (UInt32 i = 0; i < 4; i++)
    {
        for (UInt64 key = 0; key < 10; key++)
            EXPECT_FALSE(bf.couldExist(i, key));
    }
}

TEST(BloomFilter, SimpleCollision)
{
    BloomFilter bf{4, 2, 4};
    EXPECT_EQ(4, bf.getByteSize());
    for (UInt32 i = 0; i < 4; i++)
    {
        bf.set(i, 1);
        {
            UInt64 key = 1;
            EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(0)) % 4);
            EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(1)) % 4);
            EXPECT_TRUE(bf.couldExist(i, key));
        }
        {
            UInt64 key = 3;
            EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(0)) % 4);
            EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(1)) % 4);
            EXPECT_FALSE(bf.couldExist(i, key));
        }
        {
            UInt64 key = 11;
            EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(0)) % 4);
            EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(1)) % 4);
            EXPECT_TRUE(bf.couldExist(i, key));
        }

        if (i == 1)
        {
            bf.clear(0);
            bf.clear(2);
            bf.clear(3);
            EXPECT_TRUE(bf.couldExist(i, 1));
            EXPECT_FALSE(bf.couldExist(i, 2));
            EXPECT_FALSE(bf.couldExist(i, 3));
        }
        bf.clear(i);
        EXPECT_FALSE(bf.couldExist(i, 1));
    }
}

TEST(BloomFilter, SharedCollision)
{
    BloomFilter bf{1, 2, 4};
    EXPECT_EQ(1, bf.getByteSize());
    EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(1, IntHash64Impl::apply(0)) % 4);
    EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(1, IntHash64Impl::apply(1)) % 4);
    bf.set(0, 1);
    EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(3, IntHash64Impl::apply(0)) % 4);
    EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(3, IntHash64Impl::apply(1)) % 4);
    bf.set(0, 3);

    {
        UInt64 key = 7;
        EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(0)) % 4);
        EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(1)) % 4);
        EXPECT_TRUE(bf.couldExist(0, key));
    }
    {
        UInt64 key = 32;
        EXPECT_EQ(0, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(0)) % 4);
        EXPECT_EQ(2, MurmurHash3Impl64::combineHashes(key, IntHash64Impl::apply(1)) % 4);
        EXPECT_TRUE(bf.couldExist(0, key));
    }
}

TEST(BloomFilter, InvalidArgs)
{
    EXPECT_NO_THROW(BloomFilter(2, 2, 3));
    EXPECT_THROW(BloomFilter(0, 2, 2), DB::Exception);
    EXPECT_THROW(BloomFilter(2, 0, 2), DB::Exception);
    EXPECT_THROW(BloomFilter(2, 2, 0), DB::Exception);
}

TEST(BloomFilter, Clear)
{
    BloomFilter bf{2, 2, 4};

    for (UInt32 i = 0; i < 10; i++)
    {
        EXPECT_FALSE(bf.couldExist(1, 100 + i));
    }

    bf.clear(1);

    for (UInt32 i = 0; i < 10; i++)
    {
        EXPECT_FALSE(bf.couldExist(1, 100 + i));
    }

    bf.set(1, 100);

    EXPECT_TRUE(bf.couldExist(1, 100));
    EXPECT_TRUE(bf.couldExist(1, 110));
    for (UInt32 i = 1; i < 10; i++)
    {
        EXPECT_FALSE(bf.couldExist(1, 100 + i));
    }

    bf.clear(1);
    for (UInt32 i = 0; i < 10; i++)
    {
        EXPECT_FALSE(bf.couldExist(1, 100 + i));
    }
}

TEST(BloomFilter, PersistRecoverWithInvalidParams)
{
    const size_t num_filters = 10;
    const size_t bits_per_filter = 16;
    const size_t num_hash = 1;
    const int num_keys = 1024;

    std::stringstream ss;
    google::protobuf::io::OstreamOutputStream raw_stream(&ss);
    google::protobuf::io::CodedOutputStream stream(&raw_stream);

    auto make_bloom_filter = [=](google::protobuf::io::CodedOutputStream * os) {
        BloomFilter bf{num_filters, 1, 8};
        std::uniform_int_distribution<UInt64> dist(0, UINT64_MAX);
        for (int i = 0; i < num_keys; i++)
        {
            auto key = dist(thread_local_rng);
            auto idx = dist(thread_local_rng) % num_filters;
            bf.set(idx, key);
        }

        bf.persist(os);
    };

    {
        {
            make_bloom_filter(&stream);
            BloomFilter bf(num_filters + 1, num_hash, bits_per_filter);
            google::protobuf::io::IstreamInputStream raw_istream(&ss);
            google::protobuf::io::CodedInputStream istream(&raw_istream);
            ASSERT_THROW(bf.recover(&istream), Exception);
        }
        {
            make_bloom_filter(&stream);
            BloomFilter bf(num_filters, num_hash + 1, bits_per_filter);
            google::protobuf::io::IstreamInputStream raw_istream(&ss);
            google::protobuf::io::CodedInputStream istream(&raw_istream);
            ASSERT_THROW(bf.recover(&istream), Exception);
        }
        {
            make_bloom_filter(&stream);
            BloomFilter bf(num_filters, num_hash, bits_per_filter + 8);
            google::protobuf::io::IstreamInputStream raw_istream(&ss);
            google::protobuf::io::CodedInputStream istream(&raw_istream);
            ASSERT_THROW(bf.recover(&istream), Exception);
        }
    }
}

void testWithParameter(UInt32 num_filters, size_t bits_per_filter_hash, UInt32 num_hash)
{
    const int num_keys = 1024;

    std::vector<std::pair<size_t, UInt64>> keys_added;
    std::vector<std::pair<size_t, UInt64>> keys_not_present;
    std::stringstream ss;
    google::protobuf::io::OstreamOutputStream raw_stream(&ss);
    google::protobuf::io::CodedOutputStream stream(&raw_stream);

    {
        BloomFilter bf{num_filters, num_hash, bits_per_filter_hash};
        std::uniform_int_distribution<UInt64> dist(0, UINT64_MAX);
        for (int i = 0; i < num_keys; i++)
        {
            auto key = dist(thread_local_rng);
            auto idx = dist(thread_local_rng) % num_filters;
            bf.set(idx, key);
            keys_added.push_back(std::make_pair(idx, key));
        }

        while (keys_not_present.size() < num_keys)
        {
            auto key = dist(thread_local_rng);
            auto idx = dist(thread_local_rng) % num_filters;
            if (!bf.couldExist(idx, key))
                keys_not_present.push_back(std::make_pair(idx, key));
        }

        bf.persist(&stream);
    }

    BloomFilter bf(num_filters, num_hash, bits_per_filter_hash);
    google::protobuf::io::IstreamInputStream raw_istream(&ss);
    google::protobuf::io::CodedInputStream istream(&raw_istream);
    bf.recover(&istream);

    EXPECT_EQ(bf.numHashes(), num_hash);
    EXPECT_EQ(bf.numBitsPerFilter(), bits_per_filter_hash * num_hash);
    EXPECT_EQ(bf.numFilters(), num_filters);

    for (const auto & p : keys_added)
        EXPECT_TRUE(bf.couldExist(p.first, p.second));

    for (const auto & p : keys_not_present)
        EXPECT_FALSE(bf.couldExist(p.first, p.first));
}

void testPersistRecoveryWithParams(UInt32 num_filters, size_t bits_per_filter_hash, UInt32 num_hash)
{
    const int num_keys = 1024;

    std::vector<std::pair<size_t, UInt64>> keys_added;
    std::vector<std::pair<size_t, UInt64>> keys_not_present;
    std::uniform_int_distribution<UInt64> dist;
    size_t buffer_size = INT_MAX;
    Buffer metadata(buffer_size);
    {
        BloomFilter bf{num_filters, num_hash, bits_per_filter_hash};
        for (int i = 0; i < num_keys; i++)
        {
            auto key = dist(thread_local_rng);
            auto idx = dist(thread_local_rng) % num_filters;
            bf.set(idx, key);
            keys_added.push_back(std::make_pair(idx, key));
        }

        while (keys_not_present.size() < num_keys)
        {
            auto key = dist(thread_local_rng);
            auto idx = dist(thread_local_rng) % num_filters;
            if (!bf.couldExist(idx, key))
                keys_not_present.push_back(std::make_pair(idx, key));
        }

        google::protobuf::io::ArrayOutputStream raw_stream(metadata.data(), buffer_size);
        google::protobuf::io::CodedOutputStream stream(&raw_stream);
        bf.persist(&stream);
    }

    google::protobuf::io::ArrayInputStream raw_istream(metadata.data(), buffer_size);
    google::protobuf::io::CodedInputStream istream(&raw_istream);
    BloomFilter bf(num_filters, num_hash, bits_per_filter_hash);
    bf.recover(&istream);

    EXPECT_EQ(bf.numHashes(), num_hash);
    EXPECT_EQ(bf.numBitsPerFilter(), bits_per_filter_hash * num_hash);
    EXPECT_EQ(bf.numFilters(), num_filters);
    for (const auto & p : keys_added)
        EXPECT_TRUE(bf.couldExist(p.first, p.second));

    for (const auto & p : keys_not_present)
        EXPECT_FALSE(bf.couldExist(p.first, p.second));
}

TEST(BloomFilter, PersistRecoveryValidLarge)
{
    const size_t num_filters = 1000;
    const size_t bits_per_filter_hash = 1024;
    const size_t num_hash = 1;
    testPersistRecoveryWithParams(num_filters, bits_per_filter_hash, num_hash);
}

TEST(BloomFilter, PersisRecoveryValid)
{
    const size_t num_filters = 10;
    const size_t bits_per_filter_hash = 1024;
    const size_t num_hash = 1;
    testPersistRecoveryWithParams(num_filters, bits_per_filter_hash, num_hash);
}
}
