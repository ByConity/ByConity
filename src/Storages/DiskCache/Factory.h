#pragma once

#include <memory>
#include <utility>

#include <Storages/DiskCache/BigHash.h>
#include <Storages/DiskCache/BlockCache.h>
#include <Storages/DiskCache/BloomFilter.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/FifoPolicy.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/LruPolicy.h>
#include <Storages/DiskCache/NvmCache.h>
#include <Storages/DiskCache/Types.h>
#include <Common/Exception.h>

namespace ErrorCodes
{
const extern int BAD_ARGUMENTS;
}

namespace DB
{
struct BigHashProto
{
    HybridCache::BigHash::Config config;

    void setLayout(UInt64 base_offset, UInt64 size, UInt32 bucket_size)
    {
        config.cache_start_offset = base_offset;
        config.cache_size = size;
        config.bucket_size = bucket_size;
    }

    void setBloomFilter(UInt32 num_hashes_, UInt32 hash_table_bit_size_)
    {
        bloom_filter_enabled = true;
        num_hashes = num_hashes_;
        hash_table_bit_size = hash_table_bit_size_;
    }

    std::unique_ptr<HybridCache::CacheEngine> create(HybridCache::ExpiredCheck check_expired) &&
    {
        config.check_expired = std::move(check_expired);
        if (bloom_filter_enabled)
        {
            if (config.bucket_size == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "invalid bucket size");

            config.bloom_filters = std::make_unique<HybridCache::BloomFilter>(config.numBuckets(), num_hashes, hash_table_bit_size);
        }
        return std::make_unique<HybridCache::BigHash>(std::move(config));
    }

private:
    bool bloom_filter_enabled{false};
    UInt32 num_hashes{};
    UInt32 hash_table_bit_size{};
};


struct BlockCacheProto
{
    HybridCache::BlockCache::Config config;

    void setLayout(UInt64 base_offset, UInt64 size, UInt32 region_size)
    {
        if (size <= 0 || region_size <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "invalid layout. size: {}, region_size: {}.", size, region_size);

        config.cache_base_offset = base_offset;
        config.cache_size = size;
        config.region_size = region_size;
    }

    void setLruEvictionPolicy()
    {
        if (config.cache_size <= 0 || config.region_size <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "layout is not set");

        auto num_regions = config.getNumberRegions();
        if (config.eviction_policy)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already an eviction policy set");

        config.eviction_policy = std::make_unique<HybridCache::LruPolicy>(num_regions);
    }

    void setFifoEvictionPolicy()
    {
        if (config.eviction_policy)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already an eviction policy set");

        config.eviction_policy = std::make_unique<HybridCache::FifoPolicy>();
    }

    void setSegmentedFifoEvictionPolicy(std::vector<unsigned int> segmentRatio)
    {
        if (config.eviction_policy)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already an eviction policy set");

        config.num_priorities = static_cast<UInt16>(segmentRatio.size());
        config.eviction_policy = std::make_unique<HybridCache::SegmentedFifoPolicy>(std::move(segmentRatio));
    }

    std::unique_ptr<HybridCache::CacheEngine>
    create(HybridCache::JobScheduler & scheduler, HybridCache::ExpiredCheck check_expired, HybridCache::DestructorCallback call_back) &&
    {
        config.scheduler = &scheduler;
        config.check_expired = std::move(check_expired);
        config.destructor_callback = std::move(call_back);
        config.validate();
        return std::make_unique<HybridCache::BlockCache>(std::move(config));
    }
};

struct EnginePairProto
{
    std::unique_ptr<BigHashProto> big_hash_proto;
    std::unique_ptr<BlockCacheProto> block_cache_proto;
    UInt32 small_item_max_size;

    void setBigHash(std::unique_ptr<BigHashProto> proto, UInt32 small_item_max_size_)
    {
        big_hash_proto = std::move(proto);
        small_item_max_size = small_item_max_size_;
    }

    void setBlockCache(std::unique_ptr<BlockCacheProto> proto) { block_cache_proto = std::move(proto); }

    NvmCache::Pair create(
        HybridCache::Device * device,
        HybridCache::ExpiredCheck check_expired,
        HybridCache::DestructorCallback destructor_callback,
        HybridCache::JobScheduler & scheduler) const
    {
        std::unique_ptr<HybridCache::CacheEngine> bh;
        if (big_hash_proto)
        {
            big_hash_proto->config.device = device;
            big_hash_proto->config.destructor_callback = destructor_callback;
            bh = std::move(*big_hash_proto).create(check_expired);
        }

        std::unique_ptr<HybridCache::CacheEngine> bc;
        if (block_cache_proto)
        {
            block_cache_proto->config.device = device;
            bc = std::move(*block_cache_proto).create(scheduler, check_expired, destructor_callback);
        }

        return NvmCache::Pair{std::move(bc), std::move(bh), small_item_max_size};
    }
};

struct Proto
{
    std::vector<std::unique_ptr<EnginePairProto>> engine_pair_protos;
    NvmCache::Config config;
    HybridCache::ExpiredCheck check_expired;
    HybridCache::DestructorCallback destructor_callback;

    std::unique_ptr<NvmCache> create() &&
    {
        if (config.scheduler == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "scheduler is null");

        for (auto & p : engine_pair_protos)
            config.pairs.push_back(p->create(config.device.get(), check_expired, destructor_callback, *config.scheduler));

        return std::make_unique<NvmCache>(std::move(config));
    }
};
}
