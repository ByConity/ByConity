#pragma once

#include <Common/Logger.h>
#include <memory>
#include <utility>
#include <vector>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Storages/DiskCache/AbstractCache.h>
#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/Types.h>
#include <Common/Exception.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <common/unit.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int BAD_ARGUMENTS;
}

class BlockCacheReinsertionConfig
{
public:
    BlockCacheReinsertionConfig & enableHitsBased(UInt8 hit_threshold_)
    {
        if (pct_threshold > 0 || custom)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already set reinsertion percentage threshold, should not set hits threshold");

        hit_threshold = hit_threshold_;
        return *this;
    }

    BlockCacheReinsertionConfig & enablePctBased(unsigned int pct_threshold_)
    {
        if (hit_threshold > 0 || custom)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already set reinsertion hits threshold, should not set probability threshold");
        if (pct_threshold_ > 100)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "reinsertion percentage threshold should between 0 and 100, but {} is set", pct_threshold_);

        pct_threshold = pct_threshold_;
        return *this;
    }

    BlockCacheReinsertionConfig & enableCustom(std::shared_ptr<HybridCache::BlockCacheReinsertionPolicy> policy)
    {
        if (pct_threshold > 0 || hit_threshold > 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "alreay set hits threshold {}, or probability threshold {}",
                static_cast<UInt32>(hit_threshold),
                pct_threshold);
        custom = policy;
        return *this;
    }

    BlockCacheReinsertionConfig & validate()
    {
        if ((pct_threshold > 0) + (hit_threshold > 0) + (custom != nullptr) > 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "more than one configuration for reinsertion policy is specified, pctThreshold {} hisThreshold {} custom {}",
                pct_threshold,
                static_cast<UInt32>(hit_threshold),
                custom != nullptr);

        return *this;
    }

    UInt8 getHitsThreshold() const { return hit_threshold; }

    unsigned int getPctThreshold() const { return pct_threshold; }

    std::shared_ptr<HybridCache::BlockCacheReinsertionPolicy> getCustomPolicy() const { return custom; }

private:
    UInt8 hit_threshold{0};
    unsigned int pct_threshold{0};

    std::shared_ptr<HybridCache::BlockCacheReinsertionPolicy> custom{nullptr};
};

class BlockCacheConfig
{
public:
    BlockCacheConfig & enableFifo() noexcept
    {
        lru = false;
        return *this;
    }

    BlockCacheConfig & enableSegmentedFifo(std::vector<unsigned int> fifo_segment_ratio_) noexcept
    {
        fifo_segment_ratio = std::move(fifo_segment_ratio_);
        lru = false;
        return *this;
    }

    BlockCacheConfig & enableHitsBasedReinsertion(UInt8 hits_threshold)
    {
        reinsertion_config.enableHitsBased(hits_threshold);
        return *this;
    }

    BlockCacheConfig & enablePctBasedReinsertion(unsigned int pct_threshold)
    {
        reinsertion_config.enablePctBased(pct_threshold);
        return *this;
    }

    BlockCacheConfig & enableCustomReinsertion(std::shared_ptr<HybridCache::BlockCacheReinsertionPolicy> policy)
    {
        reinsertion_config.enableCustom(policy);
        return *this;
    }

    BlockCacheConfig & setCleanRegions(UInt32 clean_resions_, UInt32 clean_region_threads_ = 1)
    {
        if (!clean_region_threads_ || clean_region_threads_ > clean_resions_ + 1)
            throw Exception(
                "number of clean region threads should be in the range of [1, {}]", clean_resions_ + 1, ErrorCodes::BAD_ARGUMENTS);
        clean_regions = clean_resions_;
        num_in_mem_buffers = 2 * clean_resions_;
        clean_region_threads = clean_region_threads_;
        return *this;
    }

    BlockCacheConfig & setRegionSize(UInt32 region_size_) noexcept
    {
        region_size = region_size_;
        return *this;
    }

    BlockCacheConfig & setDataChecksum(bool data_checksum_) noexcept
    {
        data_checksum = data_checksum_;
        return *this;
    }

    BlockCacheConfig & setPreciseRemove(bool precise_remove_) noexcept
    {
        precise_remove = precise_remove_;
        return *this;
    }

    BlockCacheConfig & setSize(UInt64 size_) noexcept
    {
        size = size_;
        return *this;
    }

    bool isLruEnabled() const { return lru; }

    const std::vector<unsigned int> & getFifoSegmentRatio() const { return fifo_segment_ratio; }

    UInt32 getCleanRegions() const { return clean_regions; }

    UInt32 getCleanRegionThreads() const { return clean_region_threads; }

    UInt32 getNumInMemBuffers() const { return num_in_mem_buffers; }

    UInt32 getRegionSize() const { return region_size; }

    bool isDataChecksum() const { return data_checksum; }

    UInt64 getSize() const { return size; }

    const BlockCacheReinsertionConfig & getReinsertionConfig() const { return reinsertion_config; }

    bool isPreciseRemove() const { return precise_remove; }


private:
    bool lru{true};
    std::vector<unsigned int> fifo_segment_ratio;
    BlockCacheReinsertionConfig reinsertion_config;
    UInt32 clean_regions{1};
    UInt32 clean_region_threads{1};
    UInt32 num_in_mem_buffers{2};
    UInt32 region_size{16 * MiB};
    bool data_checksum{false};
    bool precise_remove{false};

    UInt64 size{0};

    friend class NvmCacheConfig;
};

class BigHashConfig
{
public:
    BigHashConfig & setSizePctAndMaxItemSize(unsigned int size_pct_, UInt64 small_item_max_size_)
    {
        if (size_pct_ > 100)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "bighash size pct should be in range of [0, 100), but {} is set", size_pct_);

        if (size_pct_ == 0)
            LOG_INFO(getLogger("NvmCacheConfig"), "BigHash is not configured");

        size_pct = size_pct_;
        small_item_max_size = small_item_max_size_;
        return *this;
    }

    BigHashConfig & setBucketSize(UInt32 bucket_size_) noexcept
    {
        bucket_size = bucket_size_;
        return *this;
    }

    BigHashConfig & setBucketBfSize(UInt64 bucket_bf_size_) noexcept
    {
        bucket_bf_size = bucket_bf_size_;
        return *this;
    }

    bool isBloomFilterEnabled() const { return bucket_bf_size > 0; }

    unsigned int getSizePct() const { return size_pct; }

    UInt32 getBucketSize() const { return bucket_size; }

    UInt64 getBucketBfSize() const { return bucket_bf_size; }

    UInt64 getSmallItemMaxSize() const { return small_item_max_size; }

private:
    unsigned int size_pct{0};
    UInt32 bucket_size{4096};
    UInt64 bucket_bf_size{8};
    UInt64 small_item_max_size{};
};

class EnginesConfig
{
public:
    const BigHashConfig & bigHash() const { return big_hash_config; }

    const BlockCacheConfig & blockCache() const { return block_cache_config; }

    BigHashConfig & bigHash() { return big_hash_config; }

    BlockCacheConfig & blockCache() { return block_cache_config; }

    bool isBigHashEnabled() const { return big_hash_config.getSizePct() > 0; }

    void loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & conf);

private:
    BlockCacheConfig block_cache_config;
    BigHashConfig big_hash_config;
};

class NvmCacheConfig
{
public:
    using EnginesSelector = std::function<size_t(HybridCache::EngineTag)>;

    static constexpr std::string_view FILE_NAME = "nvm_cache.bin";

    bool usesSimpleFile() const noexcept { return !file_name.empty(); }
    bool usesRaidFiles() const noexcept { return !raid_paths.empty(); }
    bool isBigHashEnabled() const { return engines_configs[0].isBigHashEnabled(); }

    UInt64 getBlockSize() const { return block_size; }
    const std::string & getFileName() const
    {
        chassert(usesSimpleFile());
        return file_name;
    }
    const std::vector<std::string> & getRaidPaths() const
    {
        chassert(usesRaidFiles());
        return raid_paths;
    }
    UInt64 getDeviceMetadataSize() const { return device_metadata_size; }
    UInt64 getFileSize() const { return file_size; }
    bool isTruncateFile() const { return truncate_file; }
    UInt32 getDeviceMaxWriteSize() const { return device_max_write_size; }
    HybridCache::IoEngine getIoEngine() const { return io_engine; }
    unsigned int getQDepth() const { return q_depth; }

    const BigHashConfig & bigHash() const
    {
        chassert(engines_configs.size() == 1);
        return engines_configs[0].bigHash();
    }

    const BlockCacheConfig & blockCache() const
    {
        chassert(engines_configs.size() == 1);
        return engines_configs[0].blockCache();
    }

    unsigned int getReaderThreads() const { return reader_threads; }
    unsigned int getWriterThreads() const { return writer_threads; }
    UInt64 getReqOrderingShards() const { return req_ordering_shards; }
    unsigned int getMaxNumReads() const { return max_num_reads; }
    unsigned int getMaxNumWrites() const { return max_num_writes; }
    unsigned int getStackSize() const { return stack_size; }

    UInt32 getMaxConcurrentInserts() const { return max_concurrent_inserts; }
    UInt64 getMaxParcelMemoryMB() const { return max_parcel_memory_mb; }

    void setBlockSize(UInt64 block_size_) noexcept { block_size = block_size_; }
    void setSimpleFile(const std::string & file_name_, UInt64 file_size_, bool truncate_file_ = false)
    {
        if (usesRaidFiles())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already set RAID files");
        file_name = file_name_;
        file_size = file_size_;
        truncate_file = truncate_file_;
    }
    void setRaidFiles(std::vector<std::string> raid_paths_, UInt64 file_size_, bool truncate_file_ = false)
    {
        if (usesSimpleFile())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "already set a simple file");
        if (raid_paths_.size() <= 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "RAID needs at least two paths, but {} is set", raid_paths_.size());
        raid_paths = std::move(raid_paths_);
        file_size = file_size_;
        truncate_file = truncate_file_;
    }
    void setMemoryFile(UInt64 file_size_) noexcept { file_size = file_size_; }
    void setDeviceMetadataSize(UInt64 device_metadata_size_) noexcept { device_metadata_size = device_metadata_size_; }
    void setDeviceMaxWriteSize(UInt32 device_max_write_size_) noexcept { device_max_write_size = device_max_write_size_; }

    void enableAsyncIo(unsigned int q_depth_)
    {
        if (q_depth_ == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "qdepth {} should be >=1 to use async IO", q_depth_);
        io_engine = HybridCache::IoEngine::IoUring;
        q_depth = q_depth_;
    }

    BlockCacheConfig & blockCache() noexcept { return engines_configs[0].blockCache(); }

    BigHashConfig & bigHash() noexcept { return engines_configs[0].bigHash(); }

    void addEnginePair(EnginesConfig config) { engines_configs.push_back(std::move(config)); }
    void setEnginesSelector(EnginesSelector selector_) { selector = std::move(selector_); }

    void setReaderAndWriterThreads(
        unsigned int reader_threads_,
        unsigned int writer_threads_,
        unsigned int max_num_reads_ = 0,
        unsigned int max_num_writes_ = 0,
        unsigned int stack_size_kb_ = 0)
    {
        reader_threads = reader_threads_;
        writer_threads = writer_threads_;
        max_num_reads = max_num_reads_;
        max_num_writes = max_num_writes_;
        if (stack_size_kb_ >= 1024)
            throw Exception("Maximum fiber stack size for each thread should be less than 1024 KB", ErrorCodes::BAD_ARGUMENTS);
        stack_size = stack_size_kb_ * KiB;

        if ((max_num_reads > 0 && max_num_writes == 0) || (max_num_reads == 0 && max_num_writes > 0))
            throw Exception("maxNumReads and maxNumWrites should be both 0 or both >0", ErrorCodes::BAD_ARGUMENTS);


        if (max_num_reads > 0 || max_num_writes > 0)
            if ((max_num_reads % reader_threads) || (max_num_writes % writer_threads))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "reader threads ({}) and writer threads ({}) should divide evenly into maxNumReads ({}) or maxNumWrites ({})",
                    reader_threads,
                    writer_threads,
                    max_num_reads,
                    max_num_writes);

        if (!q_depth)
        {
            q_depth = std::max(max_num_reads / reader_threads, max_num_writes / writer_threads);
            if (q_depth > 0)
            {
                chassert(io_engine == HybridCache::IoEngine::Sync);
                io_engine = HybridCache::IoEngine::IoUring;
            }
        }
    }
    void setReqOrderingShards(UInt64 req_ordering_shards_)
    {
        if (req_ordering_shards_ == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, " request ordering shards should always be non-zero");
        req_ordering_shards = req_ordering_shards_;
    }

    void setMaxConcurrentInserts(UInt32 max_concurrent_inserts_) noexcept { max_concurrent_inserts = max_concurrent_inserts_; }
    void setMaxParcelMemoryMB(UInt64 max_parcel_memory_mb_) noexcept { max_parcel_memory_mb = max_parcel_memory_mb_; }

    const std::vector<EnginesConfig> & enginesConfigs() const { return engines_configs; }

    EnginesSelector getEnginesSelector() const { return selector; }

    void loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & conf);

    bool isEnable() const { return enable; }

    const std::string & getPolicyName() const { return policy_name; }
    const std::string & getVolumeName() const { return volume_name; }

private:
    bool enable{};
    // =============== Device settings ==================
    UInt64 block_size{4096};
    std::string file_name;
    std::vector<std::string> raid_paths;
    UInt64 device_metadata_size{};
    UInt64 file_size{};
    bool truncate_file{false};
    UInt32 device_max_write_size{};

    HybridCache::IoEngine io_engine{HybridCache::IoEngine::Sync};

    unsigned int q_depth{0};

    std::string policy_name;
    std::string volume_name;

    // =============== Engines settings ==================
    std::vector<EnginesConfig> engines_configs{1};
    EnginesSelector selector{};

    // ============= Job Scheduler settings ==============
    unsigned int reader_threads{4};
    unsigned int writer_threads{4};
    UInt64 req_ordering_shards{20};
    unsigned int max_num_reads{64};
    unsigned int max_num_writes{32};
    unsigned int stack_size{64 * KiB};

    // ================= Use settings ====================
    UInt32 max_concurrent_inserts{1'000'000};
    UInt64 max_parcel_memory_mb{256};
};

std::unique_ptr<HybridCache::Device> createDevice(const NvmCacheConfig & config);

std::unique_ptr<HybridCache::AbstractCache> createNvmCache(
    const NvmCacheConfig & config,
    HybridCache::ExpiredCheck check_expired,
    HybridCache::DestructorCallback destructor_callback,
    bool truncate,
    bool item_destructor_enabled);

}
