#include <memory>
#include <system_error>
#include <utility>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <fmt/core.h>
#include <Poco/Logger.h>

#include <Storages/DiskCache/AbstractCache.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/Factory.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/NvmCache.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/Types.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int CANNOT_FSTAT;
    const extern int CANNOT_TRUNCATE_FILE;
}

using namespace HybridCache;

namespace
{
    UInt64 getRegionSize(const NvmCacheConfig & config)
    {
        const auto & configs = config.enginesConfigs();
        UInt64 region_size = configs[0].blockCache().getRegionSize();
        for (size_t idx = 1; idx < configs.size(); idx++)
        {
            if (region_size != configs[idx].blockCache().getRegionSize())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "blockcache {} region size: {}, not equals to block cache 0: {}",
                    idx,
                    configs[idx].blockCache().getRegionSize(),
                    region_size);
        }
        return region_size;
    }

    UInt64 alignDown(UInt64 num, UInt64 alignment)
    {
        return num - num % alignment;
    }

    UInt64 alignUp(UInt64 num, UInt64 alignment)
    {
        return alignDown(num + alignment - 1, alignment);
    }

    File openCacheFile(const std::string & file_name, UInt64 size, bool truncate)
    {
        LOG_INFO(getLogger("NvmCacheConfig"), "create file: {} sie: {}, truncate: {}", file_name, size, truncate);
        if (file_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "file name is empty");

        int flags{O_RDWR | O_CREAT};

        File f;

        try
        {
            f = File(file_name.c_str(), flags | O_DIRECT);
        }
        catch (const ErrnoException & e)
        {
            if (e.getErrno() == EINVAL)
            {
                LOG_ERROR(getLogger("NvmCacheConfig"), "failed to open with o_direct, error: {}", e.what());
                f = File(file_name.c_str(), flags);
            }
            else
                throw;
        }
        chassert(f.getFd() >= 0);

        struct stat file_stat;
        if (fstat(f.getFd(), &file_stat) < 0)
            throwFromErrno(fmt::format("failed to get the file stat for file {}", file_name), ErrorCodes::CANNOT_FSTAT);

        UInt64 cur_file_size = file_stat.st_size;

        if (truncate && cur_file_size < size)
        {
            if (::ftruncate(f.getFd(), size) < 0)
                throwFromErrno(
                    fmt::format("ftruncate failed with requested size {}, current size {}", size, cur_file_size),
                    ErrorCodes::CANNOT_TRUNCATE_FILE);

            LOG_INFO(
                getLogger("NvmCacheConfig"),
                "cache file {} is ftruncated from {} bytes to {} bytes",
                file_name,
                cur_file_size,
                size);
        }

        return f;
    }

    std::unique_ptr<Device> createFileDevice(
        std::vector<std::string> file_paths,
        UInt64 file_size,
        bool truncate_file,
        UInt32 block_size,
        UInt32 stripe_size,
        UInt32 max_device_write_size,
        IoEngine io_engine,
        UInt32 q_depth)
    {
        std::sort(file_paths.begin(), file_paths.end());
        std::vector<File> file_vec;
        for (const auto & path : file_paths)
        {
            File f;
            try
            {
                f = openCacheFile(path, file_size, truncate_file);
            }
            catch (const ErrnoException & e)
            {
                LOG_ERROR(
                    getLogger("NvmCacheConfig"), "Exception in openCacheFile {}, error: {} errno: {}", path, e.what(), errno);
                throw;
            }
            file_vec.push_back(std::move(f));
        }

        return createDirectIoFileDevice(std::move(file_vec), file_size, block_size, stripe_size, max_device_write_size, io_engine, q_depth);
    }

    std::unique_ptr<JobScheduler> createJobScheduler(const NvmCacheConfig & config)
    {
        auto reader_threads = config.getReaderThreads();
        auto writer_threads = config.getWriterThreads();
        auto req_ordering_shards = config.getReqOrderingShards();
        auto max_num_reads = config.getMaxNumReads();
        auto max_num_writes = config.getMaxNumWrites();
        auto stack_size = config.getStackSize();
        if (max_num_reads == 0 && max_num_writes == 0)
            return createOrderedThreadPoolJobScheduler(reader_threads, writer_threads, req_ordering_shards);
        return createFiberRequestScheduler(reader_threads, writer_threads, max_num_reads, max_num_writes, stack_size, req_ordering_shards);
    }

    UInt64 setupBigHash(
        const BigHashConfig & big_hash_config,
        UInt32 io_align_size,
        UInt64 big_hash_reserved_size,
        UInt64 big_hash_end_offset,
        UInt64 big_hash_start_offset_limit,
        EnginePairProto & proto)
    {
        auto bucket_size = big_hash_config.getBucketSize();
        if (bucket_size != alignUp(bucket_size, io_align_size))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "bucket size: {} is not aligned to io_align_size: {}", bucket_size, io_align_size);


        const UInt64 big_hash_cache_offset = alignUp(big_hash_end_offset - big_hash_reserved_size, bucket_size);
        const UInt64 big_hash_cache_size = alignDown(big_hash_end_offset - big_hash_cache_offset, bucket_size);

        auto big_hash = std::make_unique<BigHashProto>();
        big_hash->setLayout(big_hash_cache_offset, big_hash_cache_size, bucket_size);

        if (big_hash_config.isBloomFilterEnabled())
        {
            constexpr uint32_t k_num_hashes = 4;
            const uint32_t bits_per_hash = big_hash_config.getBucketBfSize() * 8 / k_num_hashes;
            big_hash->setBloomFilter(k_num_hashes, bits_per_hash);
        }

        proto.setBigHash(std::move(big_hash), big_hash_config.getSmallItemMaxSize());

        if (big_hash_cache_offset <= big_hash_start_offset_limit)
            throw std::invalid_argument("NVM cache size is not big enough");

        LOG_INFO(
            getLogger("NvmCacheConfig"),
            "big_hash_starting_limit: {}, big_hash_cache_offset: {}, big_hash_cache_size: {}",
            big_hash_start_offset_limit,
            big_hash_cache_offset,
            big_hash_cache_size);
        return big_hash_cache_offset;
    }

    UInt64 setupBlockCache(
        const BlockCacheConfig & block_cache_config,
        UInt64 block_cache_size,
        UInt32 io_align_size,
        UInt64 block_cache_offset,
        bool uses_raid_files,
        bool item_destructor_enabled,
        EnginePairProto & proto)
    {
        auto region_size = block_cache_config.getRegionSize();
        auto clean_regions = block_cache_config.getCleanRegions();
        auto clean_region_threads = block_cache_config.getCleanRegionThreads();
        if (region_size != alignUp(region_size, io_align_size))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "region size: {} is not aligned to io_align_size: {}", region_size, io_align_size);
        if (clean_region_threads == 0 || clean_region_threads > clean_regions + 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "number of clean region threads should be in the range of [1, {}]", clean_regions + 1);

        if (uses_raid_files)
        {
            auto adjusted_block_cache_offset = alignUp(block_cache_offset, region_size);
            auto cache_size_adjustment = adjusted_block_cache_offset - block_cache_offset;
            chassert(cache_size_adjustment < block_cache_size);
            block_cache_size -= cache_size_adjustment;
            block_cache_offset = adjusted_block_cache_offset;
        }
        block_cache_size = alignDown(block_cache_size, region_size);

        LOG_INFO(getLogger("NvmCacheConfig"), "blockcache: starting offset: {}, size: {}", block_cache_offset, block_cache_size);

        auto block_cache = std::make_unique<BlockCacheProto>();
        block_cache->setLayout(block_cache_offset, block_cache_size, region_size);
        block_cache->config.checksum = block_cache_config.isDataChecksum();

        auto segment_ratio = block_cache_config.getFifoSegmentRatio();
        if (!segment_ratio.empty())
            block_cache->setSegmentedFifoEvictionPolicy(std::move(segment_ratio));
        else if (block_cache_config.isLruEnabled())
            block_cache->setLruEvictionPolicy();
        else
            block_cache->setFifoEvictionPolicy();

        block_cache->config.clean_regions_pool = clean_regions;
        block_cache->config.clean_region_threads = clean_region_threads;
        block_cache->config.reinsertion_config = block_cache_config.getReinsertionConfig();
        block_cache->config.num_in_mem_buffers = block_cache_config.getNumInMemBuffers();
        block_cache->config.item_destructor_enabled = item_destructor_enabled;
        block_cache->config.precise_remove = block_cache_config.isPreciseRemove();

        proto.setBlockCache(std::move(block_cache));
        return block_cache_offset + block_cache_size;
    }

    constexpr double kDefaultMetadataPercent = 0.5;

    // layout:
    // |--------------------------------- Device -------------------------------|
    // |--- Metadata ---|--- BC-0 ---|--- BC-1 ---|...|--- BH-1 ---|--- BH-0 ---|
    void setupProtos(const NvmCacheConfig & config, const Device & device, Proto & proto, bool item_destructor_enabled)
    {
        auto get_default_metadata_size = [](size_t size, size_t alignment) {
            chassert(isPowerOf2(alignment));
            auto mask = ~(alignment - 1);
            return (static_cast<size_t>(kDefaultMetadataPercent * size / 100) & mask);
        };

        auto io_align_size = device.getIOAlignmentSize();
        const UInt64 total_cache_size = device.getSize();

        auto metadata_size = config.getDeviceMetadataSize();
        if (metadata_size == 0)
            metadata_size = get_default_metadata_size(total_cache_size, io_align_size);
        metadata_size = alignUp(metadata_size, io_align_size);
        if (metadata_size >= total_cache_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "invalid metadata size: {}, cache size: {}", metadata_size, total_cache_size);
        proto.config.metadata_size = metadata_size;

        UInt64 block_cache_start_offset = metadata_size;
        UInt64 block_cache_end_offset = 0;
        UInt64 big_hash_end_offset = total_cache_size;
        UInt64 big_hash_start_offset = 0;

        LOG_INFO(getLogger("NvmCacheConfig"), "metadata size: {}", metadata_size);
        for (size_t idx = 0; idx < config.enginesConfigs().size(); idx++)
        {
            LOG_INFO(getLogger("NvmCacheConfig"), "setting up engine pair {}", idx);
            const auto & engines_config = config.enginesConfigs()[idx];
            UInt64 block_cache_size = engines_config.blockCache().getSize();
            auto engine_pair_proto = std::make_unique<EnginePairProto>();

            if (engines_config.isBigHashEnabled())
            {
                UInt64 big_hash_size = total_cache_size * engines_config.bigHash().getSizePct() / 100ul;
                big_hash_start_offset = setupBigHash(
                    engines_config.bigHash(),
                    io_align_size,
                    big_hash_size,
                    big_hash_end_offset,
                    block_cache_start_offset,
                    *engine_pair_proto);
                LOG_INFO(getLogger("NvmCacheConfig"), "block cache size: {}", block_cache_size);
            }
            else
            {
                big_hash_start_offset = big_hash_end_offset;
                LOG_INFO(getLogger("NvmCacheConfig"), "-- no bighash. block cache size: {}", block_cache_size);
            }

            if (block_cache_size > 0)
                block_cache_end_offset = setupBlockCache(
                    engines_config.blockCache(),
                    block_cache_size,
                    io_align_size,
                    block_cache_start_offset,
                    config.usesRaidFiles(),
                    item_destructor_enabled,
                    *engine_pair_proto);

            if (block_cache_end_offset > big_hash_start_offset)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "invalid engine size. block cache ends at {}, big hash starts at {}",
                    block_cache_end_offset,
                    big_hash_start_offset);

            proto.engine_pair_protos.push_back(std::move(engine_pair_proto));
            big_hash_end_offset = big_hash_start_offset;
            block_cache_start_offset = block_cache_end_offset;
        }
        proto.config.selector = config.getEnginesSelector();
    }
}

std::unique_ptr<Device> createDevice(const NvmCacheConfig & config)
{
    auto block_size = config.getBlockSize();
    auto max_device_write_size = config.getDeviceMaxWriteSize();
    if (config.usesRaidFiles() || config.usesSimpleFile())
    {
        auto stripe_size = 0;
        auto file_size = config.getFileSize();
        std::vector<std::string> file_paths;
        if (config.usesSimpleFile())
            file_paths.emplace_back(config.getFileName());
        else
        {
            stripe_size = getRegionSize(config);
            file_paths = config.getRaidPaths();
            file_size = alignDown(file_size, stripe_size);
        }

        return createFileDevice(
            file_paths,
            file_size,
            config.isTruncateFile(),
            block_size,
            stripe_size,
            max_device_write_size > 0 ? alignDown(max_device_write_size, block_size) : 0,
            config.getIoEngine(),
            config.getQDepth());
    }
    else
        return createMemoryDevice(config.getFileSize());
}

std::unique_ptr<AbstractCache> createNvmCache(
    const NvmCacheConfig & config,
    HybridCache::ExpiredCheck check_expired,
    HybridCache::DestructorCallback destructor_callback,
    bool truncate,
    bool item_destructor_enabled)
{
    auto device = createDevice(config);

    auto proto = std::make_unique<Proto>();
    auto * device_ptr = device.get();
    proto->config.device = std::move(device);
    proto->config.scheduler = createJobScheduler(config);
    proto->config.max_concurrent_inserts = config.getMaxConcurrentInserts();
    proto->config.max_parcel_memory = config.getMaxParcelMemoryMB() * MiB;
    proto->check_expired = std::move(check_expired);
    proto->destructor_callback = std::move(destructor_callback);

    setupProtos(config, *device_ptr, *proto, item_destructor_enabled);

    auto cache = std::move(*proto).create();
    chassert(cache != nullptr);

    if (truncate)
    {
        cache->reset();
        return cache;
    }

    if (!cache->recover())
        LOG_WARNING(getLogger("NvmCacheConfig"), "no recovery data found. setup with clean cache.");

    return cache;
}

void NvmCacheConfig::loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & conf)
{
    enable = conf.getBool(config_elem + ".enable", false);
    if (!enable)
        return;

    policy_name = conf.getString(config_elem + ".policy", "default");
    volume_name = conf.getString(config_elem + ".volume", "local");

    device_metadata_size = conf.getUInt64(config_elem + ".device_metadata_size_mb", 0) * MiB;
    file_size = conf.getUInt64(config_elem + ".file_size", 10 * GiB);
    q_depth = conf.getUInt(config_elem + ".q_depth", 0);
    if (q_depth)
        enableAsyncIo(q_depth);

    setReqOrderingShards(conf.getUInt64(config_elem + ".req_ordering_shards", 20));
    setReaderAndWriterThreads(
        conf.getUInt(config_elem + ".reader_threads", 4),
        conf.getUInt(config_elem + ".writer_threads", 4),
        conf.getUInt(config_elem + ".max_num_reads", 64),
        conf.getUInt(config_elem + ".max_num_writes", 32),
        conf.getUInt(config_elem + ".stack_size_kb", 64));

    max_concurrent_inserts = conf.getUInt(config_elem + ".max_concurrent_inserts", 1'000'000);
    max_parcel_memory_mb = conf.getUInt64(config_elem + ".max_parcel_memory_mb", 256);
    UInt32 engines_size = static_cast<UInt32>(EngineTag::COUNT);
    engines_configs.resize(engines_size);
    for (unsigned int i = 0; i < engines_size; ++i)
        engines_configs[i].loadFromConfig(fmt::format("{}.{}", config_elem, getEngineTagName(static_cast<EngineTag>(i))), conf);
}

void EnginesConfig::loadFromConfig(const std::string & config_elem, const Poco::Util::AbstractConfiguration & conf)
{
    big_hash_config.setSizePctAndMaxItemSize(
        conf.getUInt(config_elem + ".bh_size_pct", 0), conf.getUInt64(config_elem + ".small_item_max_size", 2048));
    block_cache_config.setSize(conf.getUInt64(config_elem + ".bc_size_mb", 0) * MiB);
    block_cache_config.setCleanRegions(conf.getUInt(config_elem + ".bc_clean_regions", 1));
}
}
