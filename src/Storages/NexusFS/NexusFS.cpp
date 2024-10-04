#include <algorithm>
#include <exception>
#include <memory>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/NexusFS/NexusFS.h>
#include <Storages/NexusFS/HitsReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/PercentageReinsertionPolicy.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/DiskCache/FifoPolicy.h>
#include <Storages/DiskCache/RecordIO.h>
#include <sys/stat.h>
#include "common/unit.h"
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <common/defines.h>
#include <common/function_traits.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace ProfileEvents
{
extern const Event NexusFSDiskCacheHit;
extern const Event NexusFSDiskCacheHitInflightInsert;
extern const Event NexusFSDiskCacheMiss;
extern const Event NexusFSDiskCacheEvict;
extern const Event NexusFSDiskCachePreload;
extern const Event NexusFSDiskCacheLookupRetries;
extern const Event NexusFSDiskCacheInsertRetries;
extern const Event NexusFSDiskCacheError;
extern const Event NexusFSDiskCacheBytesRead;
extern const Event NexusFSDiskCacheBytesWrite;
extern const Event NexusFSMemoryBufferHit;
extern const Event NexusFSMemoryBufferMiss;
extern const Event NexusFSMemoryBufferError;
extern const Event NexusFSMemoryBufferBytesRead;
}

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int NOT_IMPLEMENTED;
extern const int TIMEOUT_EXCEEDED;
extern const int CANNOT_OPEN_FILE;
}

namespace DB
{

using namespace DB::NexusFSComponents;
using namespace DB::HybridCache;

UInt64 alignDown(UInt64 num, UInt64 alignment)
{
    return num - num % alignment;
}

UInt64 alignUp(UInt64 num, UInt64 alignment)
{
    return alignDown(num + alignment - 1, alignment);
}

File NexusFSConfig::openFile(const std::string & file_name, UInt64 size, bool truncate)
{
    LOG_INFO(log, "create file: {} sie: {}, truncate: {}", file_name, size, truncate);
    if (file_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "file name is empty");

    // TODO: use DIRECT_IO
    int flags{O_RDWR | O_CREAT};

    File f = File(file_name.c_str(), flags);
    chassert(f.getFd() >= 0);

    struct stat file_stat;
    if (fstat(f.getFd(), &file_stat) < 0)
        throw std::system_error(errno, std::system_category(), fmt::format("failed to get the file stat for file {}", file_name));

    UInt64 cur_file_size = file_stat.st_size;

    if (truncate && cur_file_size < size)
    {
        if (::ftruncate(f.getFd(), size) < 0)
            throw std::system_error(
                errno,
                std::system_category(),
                fmt::format("ftruncate failed with requested size {}, current size {}", size, cur_file_size));

        LOG_INFO(
            log,
            "cache file {} is ftruncated from {} bytes to {} bytes",
            file_name,
            cur_file_size,
            size);
    }

    return f;
}

void NexusFSConfig::loadFromConfig(const Poco::Util::AbstractConfiguration & conf)
{
    String config_name(CONFIG_NAME);
    cache_size = conf.getUInt64(config_name + ".cache_size", 10 * GiB);
    region_size = conf.getUInt64(config_name + ".region_size", 1 * MiB);
    segment_size = conf.getUInt64(config_name + ".segment_size", 128 * KiB);
    alloc_align_size = conf.getUInt(config_name + ".alloc_align_size", 4096);
    io_align_size = conf.getUInt(config_name + ".io_align_size", 4096);
    stripe_size = conf.getUInt(config_name + ".stripe_size", 4096);
    clean_regions_pool = conf.getUInt(config_name + ".clean_regions_pool", 4);
    clean_region_threads = conf.getUInt(config_name + ".clean_region_threads", 2);
    num_in_mem_buffers = conf.getUInt(config_name + ".num_in_mem_buffers", 8);
    memory_cache_size = conf.getUInt64(config_name + ".memory_cache_size", 1 * GiB);
    timeout_ms = conf.getUInt(config_name + ".timeout_ms", 10000);
    bool use_memory_device = conf.getBool(config_name + ".use_memory_device", false);
    bool enable_async_io = conf.getBool(config_name + ".enable_async_io", false);

    double metadata_percentage = conf.getDouble(config_name + ".metadata_percentage", 0.01);
    metadata_size = alignUp(static_cast<UInt64>(metadata_percentage * cache_size), region_size);
    if (metadata_size >= cache_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "invalid metadata size: {}, cache size: {}", metadata_size, cache_size);

    // TODO: other policies
    eviction_policy = std::make_unique<HybridCache::FifoPolicy>();

    if (use_memory_device)
        device = createMemoryDevice(cache_size, alloc_align_size);
    else
    {
        HybridCache::IoEngine io_engine = HybridCache::IoEngine::Sync;
        int q_depth = 0;
        if (enable_async_io)
        {
            io_engine = HybridCache::IoEngine::IoUring;
            q_depth = 128;
        }

        std::sort(file_paths.begin(), file_paths.end());
        for (const auto & path : file_paths)
        {
            File f;
            try
            {
                f = openFile(path, cache_size, true);
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(
                    &Poco::Logger::get("NexusFSConfig"), "Exception in openFile {}, error: {} errno: {}", path, e.what(), errno);
                throw;
            }
            file_vec.push_back(std::move(f));
        }
        device = createDirectIoFileDevice(
            std::move(file_vec),
            cache_size,
            io_align_size,
            stripe_size,
            0,
            io_engine,
            q_depth);
    }

    validate();
}

NexusFSConfig & NexusFSConfig::validate()
{
    if (!device || !eviction_policy)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "missing required param");
    if (region_size > 256u << 20)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "region is too large");
    if (cache_size <= 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "invalid size");
    if (cache_size % region_size != 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, 
            fmt::format("Cache size is not aligned to region size! cache size: {} region size: {]}", cache_size, region_size));
    if (getNumberRegions() < clean_regions_pool)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "not enough space on device");
    if (num_in_mem_buffers == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "there must be at least one in-mem buffers");
    if (num_priorities == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "allocator must have at least one priority");

    reinsertion_config.validate();

    return *this;
}


NexusFS::NexusFS(NexusFSConfig && config)
    : serialized_config{serializeConfig(config)}
    , device{std::move(config.device)}
    , alloc_align_size{config.alloc_align_size}
    , region_size{config.region_size}
    , metadata_size(config.metadata_size)
    , segment_size(config.segment_size)
    , timeout_ms(config.timeout_ms)
    // , num_priorities{config.num_priorities}
    // , check_expired{std::move(config.check_expired)}
    // , destructor_callback{std::move(config.destructor_callback)}
    // , checksum_data{config.checksum}
    // , item_destructor_enabled{config.item_destructor_enabled}
    , region_manager{
          config.getNumberRegions(),
          config.region_size,
          config.metadata_size,
          *device,
          config.clean_regions_pool,
          config.clean_region_threads,
          bindThis(&NexusFS::onRegionReclaim, *this),
          bindThis(&NexusFS::onRegionCleanup, *this),
          std::move(config.eviction_policy),
          config.num_in_mem_buffers,
          config.num_priorities,
          config.in_mem_buf_flush_retry_limit}
    , allocator{region_manager, config.num_priorities}
    , reinsertion_policy{makeReinsertionPolicy(config.reinsertion_config)}
    , enable_segment_cache(false)
    // , segment_cache(config.memory_cache_size / segment_size)
{
    LOG_TRACE(log, "NexusFS created");
}

void NexusFS::preload(const String &file, const OffsetAndSizeVector &offsets_and_sizes, std::unique_ptr<ReadBufferFromFileBase> &source)
{
    std::unordered_set<UInt64> segment_ids;
    for (const auto & [offset, size] : offsets_and_sizes)
    {
        UInt64 first_segment_id = getSegmentId(offset);
        UInt64 last_segment_id = getSegmentId(offset + size - 1);
        for (UInt64 id = first_segment_id; id <= last_segment_id; ++id)
            segment_ids.insert(id);
    }

    LOG_TRACE(log, "preload {} segments from {}", segment_ids.size(), file);
    ProfileEvents::increment(ProfileEvents::NexusFSDiskCachePreload, segment_ids.size());

    for (auto id : segment_ids)
    {
        String segment_name = getSegmentName(file, id);
        off_t offset_in_source = getOffsetInSourceFile(id);
        HashedKey key(segment_name);
        std::shared_ptr<InsertCxt> cxt;
        load(key, offset_in_source, source, cxt);
    }
}

NexusFSIndex::LookupResult NexusFS::load(const HybridCache::HashedKey &key, off_t offset_in_source, std::unique_ptr<ReadBufferFromFileBase> &source, std::shared_ptr<InsertCxt> &insert_cxt)
{
    LOG_TRACE(log, "try find {}({}) from index", key.key(), key.keyHash());

    auto lr = index.lookup(key.keyHash());
    if (!lr.isFound())
    {
        LOG_TRACE(log, "{}({}) not find, read from source and insert to cache", key.key(), key.keyHash());

        bool is_newly_created;
        insert_cxt = in_flight_inserts.getOrCreateContext(key.keyHash(), is_newly_created);
        if (is_newly_created)
        {
            ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheMiss);

            insert_cxt->buffer = Buffer(segment_size);
            size_t bytes_read = source->readBigAt(reinterpret_cast<char*>(insert_cxt->buffer.data()), segment_size, offset_in_source);
            LOG_TRACE(log, "read {} bytes from source", bytes_read);

            insert_cxt->buffer.shrink(bytes_read);
            insert_cxt->ready = true;

            lr = insert(key, insert_cxt->buffer.view());
            in_flight_inserts.removeContext(key.keyHash());

            //TODO: insert into memory cache
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheHitInflightInsert);
        }
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheHit);
    }
    return lr;
}

size_t NexusFS::read(const String &file, const off_t offset, const size_t max_size, std::unique_ptr<ReadBufferFromFileBase> &source, char *to)
{
    UInt64 segment_id = getSegmentId(offset);
    String segment_name = getSegmentName(file, segment_id);
    off_t offset_in_source = getOffsetInSourceFile(segment_id);
    off_t offset_in_segment = getOffsetInSegment(offset);
    HashedKey key(segment_name);

    Stopwatch watch;
    UInt32 num_tries = 0;
    for (; watch.elapsedMilliseconds() < timeout_ms; num_tries++)
    {
        if (num_tries > 0)
            ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheLookupRetries);

        std::shared_ptr<InsertCxt> cxt;
        const auto seq_number = region_manager.getSeqNumber();
        auto lr = load(key, offset_in_source, source, cxt);

        // read from InsertCxt
        if (cxt)
        {
            Stopwatch watch_cxt;
            while (!cxt->ready && watch_cxt.elapsedMilliseconds() < timeout_ms)
            {
                std::this_thread::yield();
            }

            if (!cxt->ready)
            {
                LOG_WARNING(log, "stop waiting for InsertCxt to get ready, because of timeout ({}ms)", timeout_ms);
                break;
            }

            size_t buffer_size = cxt->buffer.size();
            if (buffer_size == 0 || buffer_size <= static_cast<size_t>(offset_in_segment))
            {
                return 0;
            }
            size_t size = getReadSizeInSegment(offset_in_segment, buffer_size, max_size);
            chassert(size > 0);
            memcpy(to, cxt->buffer.data() + offset_in_segment, size);

            return size;
        }

        chassert(lr.isFound());
        ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheHit);

        if (lr.getSize() == 0)
            return 0;

        // read from memory buffer or disk
        if (enable_segment_cache)
        {
            // // load into memory cache and read
            // auto callback = [this, &lr]{ return std::make_shared<SegmentHandleMemoryCacheHolder>(loadToMemoryCache(lr)); };
            // auto &handle = segment_cache.getOrSet(key.keyHash(), callback);
            // auto view = handle.pinMemoryBuffer();
            // size_t size = getReadSizeInSegment(offset_in_segment, lr.getSize(), max_size);
            // ProfileEvents::increment(ProfileEvents::NexusFSMemoryBufferBytesRead);
            // memcpy(to, view.data() + offset_in_segment, size);
            // handle.unpinMemoryBuffer();
            // return size;
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "memory buffer of NexusFS is not implemented");
        }
        else
        {
            // directly read disk file
            ProfileEvents::increment(ProfileEvents::NexusFSMemoryBufferMiss);

            auto addr = lr.getAddress();
            RegionDescriptor desc = region_manager.openForRead(addr.rid(), seq_number);
            if (desc.getStatus() == OpenStatus::Retry)
            {
                // retry, go back to the for loop
                continue;
            }
            if (desc.getStatus() != OpenStatus::Ready)

                throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "fail to open region for read");

            addr = addr.add(offset_in_segment);
            size_t size = getReadSizeInSegment(offset_in_segment, lr.getSize(), max_size);
            size_t bytes_read = readEntry(desc, addr, size, to);
            LOG_TRACE(log, "read {} bytes from {}({})", bytes_read, key.key(), key.keyHash());
            if (bytes_read > 0)
            {
                region_manager.touch(addr.rid());
            }
            region_manager.close(std::move(desc));

            return bytes_read;
        }
    }

    LOG_WARNING(log, "read tries for {} times and timeout ({}ms), read directly from source", num_tries, timeout_ms);
    size_t bytes_read = source->readBigAt(to, max_size, offset);
    LOG_TRACE(log, "read {} bytes from source", bytes_read);
    return bytes_read;
}

NexusFSIndex::LookupResult NexusFS::insert(const HashedKey &key, BufferView buf_view)
{
    size_t size = buf_view.size();
    UInt32 aligned_size = alignedSize(size);

    if (size == 0)
        return index.insert(key.keyHash(), RelAddress(), 0);

    chassert(size > 0);
    chassert(size <= segment_size);
    chassert(aligned_size <= segment_size);

    auto [desc, slot_size, addr] = allocator.allocate(aligned_size, kDefaultItemPriority, false);
    while (desc.getStatus() == OpenStatus::Retry)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheInsertRetries);
        std::tie(desc, slot_size, addr) = allocator.allocate(aligned_size, kDefaultItemPriority, false);
    }

    if (desc.getStatus() == OpenStatus::Error)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheError);
        LOG_ERROR(log, "failed to insert {}({}), size={}", key.key(), key.keyHash(), slot_size);
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "failed to insert {}({}), size={}", key.key(), key.keyHash(), slot_size);
    }

    writeEntry(addr, slot_size, key, buf_view);
    auto lr = index.insert(key.keyHash(), addr, size);
    allocator.close(std::move(desc));

    return lr;
}

void NexusFS::writeEntry(RelAddress addr, UInt32 slot_size, const HashedKey &key, BufferView value)
{
    chassert(addr.offset() + slot_size <= region_manager.regionSize());
    chassert(slot_size % alloc_align_size == 0ULL);

    LOG_TRACE(log, "writeEntry rid={}, off={}, key={}({}), size={} ", addr.rid().index(), addr.offset(), key.key(), key.keyHash(), slot_size);

    auto rid = addr.rid();
    auto & region = region_manager.getRegion(rid);
    region.writeToBuffer(addr.offset(), value);
    region.addKey(key.keyHash());

    ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheBytesWrite, value.size());
}

size_t NexusFS::readEntry(const RegionDescriptor &rdesc, RelAddress addr, UInt32 size, char *to)
{
    chassert(addr.offset() + size <= region_manager.regionSize());

    LOG_TRACE(log, "readEntry rid={}, off={}, size={} ", addr.rid().index(), addr.offset(), size);
    ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheBytesRead, size);

    return region_manager.read(rdesc, addr, size, to);
}

std::shared_ptr<BlockCacheReinsertionPolicy> NexusFS::makeReinsertionPolicy(const BlockCacheReinsertionConfig & reinsertion_config)
{
    auto hits_threshold = reinsertion_config.getHitsThreshold();
    if (hits_threshold)
        return std::make_shared<NexusFSHitsReinsertionPolicy>(hits_threshold, index);

    auto pct_threshold = reinsertion_config.getPctThreshold();
    if (pct_threshold)
        return std::make_shared<PercentageReinsertionPolicy>(pct_threshold);

    return reinsertion_config.getCustomPolicy();
}

UInt32 NexusFS::onRegionReclaim(RegionId rid, BufferView buffer)
{
    UInt32 eviction_count = 0;
    auto & region = region_manager.getRegion(rid);
    std::vector<UInt64> keys;
    region.getKeys(keys);
    chassert(region.getNumItems() == keys.size());

    for (auto key : keys)
    {
        auto lr = index.lookup(key);
        if (!lr.isFound())
        {
            LOG_ERROR(log, "reclaim a key {} in from region {}, but it does not exist in index", key, rid.index());
            continue;
        }

        auto addr = lr.getAddress();
        auto size = lr.getSize();
        BufferView value{size, buffer.data() + addr.offset()};

        const auto reinsertion_res = reinsertOrRemoveItem(key, value, size, addr);
        switch (reinsertion_res)
        {
            case ReinsertionRes::kEvicted:
                ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheEvict);
                eviction_count++;
                break;
            case ReinsertionRes::kRemoved:
                ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheEvict);
                break;
            case ReinsertionRes::kReinserted:
                break;
        }

        // if (destructor_callback && reinsertion_res == ReinsertionRes::kEvicted)
        //     destructor_callback(key, value, DestructorEvent::Recycled);

    }

    region.resetKeys();
    chassert(region.getNumItems() >= eviction_count);
    return eviction_count;
}

void NexusFS::onRegionCleanup(RegionId rid, BufferView /*buffer*/)
{
    UInt32 eviction_count = 0;
    auto & region = region_manager.getRegion(rid);
    std::vector<UInt64> keys;
    region.getKeys(keys);
    chassert(region.getNumItems() == keys.size());

    for (auto key : keys)
    {
        auto lr = index.lookup(key);
        if (!lr.isFound())
        {
            LOG_ERROR(log, "reclaim a key {} in from region {}, but it does not exist in index", key, rid.index());
            continue;
        }
        auto addr = lr.getAddress();

        auto remove_res = removeItem(key, addr);

        if (remove_res)
            eviction_count++;

        // if (destructor_callback && remove_res)
        //     destructor_callback(key, value, DestructorEvent::Recycled);
    }

    region.resetKeys();
    chassert(region.getNumItems() >= eviction_count);
}

NexusFS::ReinsertionRes NexusFS::reinsertOrRemoveItem(UInt64 key, BufferView /*value*/, UInt32 /*entry_size*/, RelAddress addr)
{
    auto remove_item = [this, key, addr](bool /*expired*/) {
        if (index.removeIfMatch(key, addr))
        {
            // if (expired)
            //     ProfileEvents::increment(ProfileEvents::BlockCacheEvictionExpiredCount);
            return ReinsertionRes::kEvicted;
        }
        return ReinsertionRes::kRemoved;
    };

    const auto lr = index.peek(key);
    if (!lr.isFound() || lr.getAddress() != addr)
    {
        // ProfileEvents::increment(ProfileEvents::BlockCacheEvictionLookupMissCount);
        return ReinsertionRes::kRemoved;
    }

    return remove_item(true);
}

Protos::NexusFSConfig NexusFS::serializeConfig(const NexusFSConfig & config)
{
    Protos::NexusFSConfig serialized_config;
    serialized_config.set_version(kFormatVersion);
    serialized_config.set_cache_size(config.cache_size);
    serialized_config.set_metadata_size(config.metadata_size);
    serialized_config.set_alloc_align_size(config.alloc_align_size);
    serialized_config.set_region_size(config.region_size);
    serialized_config.set_segment_size(config.segment_size);
    
    return serialized_config;
}

bool NexusFS::removeItem(UInt64 key, RelAddress addr)
{
    return index.removeIfMatch(key, addr);
}

void NexusFS::persist()
{
    LOG_INFO(log, "Starting block cache persist");
    auto stream = createMetadataOutputStream(*device, metadata_size);
    Protos::NexusFSConfig config = serialized_config;
    config.set_alloc_align_size(alloc_align_size);
    config.set_region_size(region_size);
    config.set_segment_size(segment_size);
    config.set_reinsertion_policy_enabled(reinsertion_policy != nullptr);
    google::protobuf::io::CodedOutputStream ostream(stream.get());
    google::protobuf::util::SerializeDelimitedToCodedStream(config, &ostream);
    region_manager.persist(&ostream);
    index.persist(&ostream);

    LOG_INFO(log, "Finished block cache persist");
}

bool NexusFS::recover()
{
    LOG_INFO(log, "Starting block cache recovery");
    reset();
    try
    {
        auto stream = createMetadataInputStream(*device, metadata_size);
        Protos::NexusFSConfig config;
        google::protobuf::io::CodedInputStream istream(stream.get());
        google::protobuf::util::ParseDelimitedFromCodedStream(&config, &istream, nullptr);

        if (config.cache_size() != serialized_config.cache_size() || config.metadata_size() != serialized_config.metadata_size()
            || config.region_size() != serialized_config.region_size() || config.segment_size() != serialized_config.segment_size()
            || config.version() != serialized_config.version() || config.alloc_align_size() != serialized_config.alloc_align_size())
        {
            LOG_ERROR(log, "Recovery config: {}", config.DebugString());
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Recovery config does not match cache config");
        }

        region_manager.recover(&istream);
        index.recover(&istream);
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Exception: {}", e.what());
        LOG_ERROR(log, "Failed to recover block cache. Resetting cache.");
        reset();
        return false;
    }
    LOG_INFO(log, "Finished block cache recovery");
    return true;
}


void NexusFS::reset()
{
    LOG_INFO(log, "Reset NexusFS");
    index.reset();
    allocator.reset();
}


void NexusFS::flush()
{
    LOG_INFO(log, "Flush NexusFS");
    allocator.flush();
    region_manager.flush();
    drain();
}

void NexusFS::drain()
{
    region_manager.drain();
}


bool NexusFS::shutDown()
{
    try
    {
        flush();
        persist();
    }
    catch (std::exception & ex)
    {
        LOG_ERROR(log, "Got error persist cache: ", ex.what());
        return false;
    }
    LOG_INFO(log, "Cache recovery saved to the Flash Device");
    return true;
}

// std::shared_ptr<SegmentHandle> NexusFS::loadToMemoryCache(const NexusFSIndex::LookupResult &lr)
// {
//     RelAddress addr = lr.getAddress();
//     size_t size = lr.getSize();
//     RegionDescriptor desc(OpenStatus::Retry);
//     while (desc.getStatus() == OpenStatus::Retry)
//     {
//         const auto seq_number = region_manager.getSeqNumber(); //TODO: why we need this?
//         desc = region_manager.openForRead(addr.rid(), seq_number);
//     }
//     if (desc.getStatus() != OpenStatus::Ready)
//     {
//         // TODO: err codes
//         throw Exception("fail to open region for read", ErrorCodes::BAD_ARGUMENTS);
//     }
//     chassert(desc.getStatus() == OpenStatus::Ready);

//     Buffer buffer(size);
//     size_t bytes_read = readEntry(desc, addr, size, reinterpret_cast<char*>(buffer.data()));
//     chassert(size == bytes_read);
//     LOG_TRACE(log, "loadToMemoryCache, read {} bytes from addr=<{},{}>", bytes_read, addr.rid().index(), addr.offset());
//     region_manager.touch(addr.rid());
//     region_manager.close(std::move(desc));

//     auto handle = lr.getHandler();
//     handle->loadedToMemory(buffer);
//     return handle;
// }

}
