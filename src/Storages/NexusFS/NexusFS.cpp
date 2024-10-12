#include <Storages/NexusFS/NexusFS.h>

#include <algorithm>
#include <exception>
#include <memory>
#include <string.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/FifoPolicy.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/RecordIO.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/NexusFS/NexusFSBufferWithHandle.h>
#include <sys/stat.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <common/defines.h>
#include <common/function_traits.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <common/unit.h>

namespace ProfileEvents
{
extern const Event NexusFSHit;
extern const Event NexusFSHitInflightInsert;
extern const Event NexusFSMiss;
extern const Event NexusFSPreload;
extern const Event NexusFSDeepRetry;
extern const Event NexusFSDiskCacheEvict;
extern const Event NexusFSDiskCacheInsertRetries;
extern const Event NexusFSDiskCacheError;
extern const Event NexusFSDiskCacheBytesRead;
extern const Event NexusFSDiskCacheBytesWrite;
extern const Event NexusFSReadFromInsertCxt;
extern const Event NexusFSReadFromInsertCxtRetry;
extern const Event NexusFSReadFromInsertCxtDeepRetry;
extern const Event NexusFSReadFromInsertCxtBytesRead;
extern const Event NexusFSReadFromInsertCxtNonCopy;
extern const Event NexusFSReadFromInsertCxtNonCopyBytesRead;
extern const Event NexusFSReadFromDisk;
extern const Event NexusFSReadFromDiskRetry;
extern const Event NexusFSReadFromDiskDeepRetry;
extern const Event NexusFSReadFromDiskBytesRead;
extern const Event NexusFSReadFromBuffer;
extern const Event NexusFSReadFromBufferRetry;
extern const Event NexusFSReadFromBufferDeepRetry;
extern const Event NexusFSReadFromBufferBytesRead;
extern const Event NexusFSReadFromBufferNonCopy;
extern const Event NexusFSReadFromBufferNonCopyBytesRead;
extern const Event NexusFSReadFromSourceBytesRead;
extern const Event NexusFSReadFromSourceMicroseconds;
extern const Event NexusFSTimeout;
extern const Event NexusFSPrefetchToBuffer;
extern const Event NexusFSPrefetchToBufferBytesRead;
}

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int NOT_IMPLEMENTED;
extern const int TIMEOUT_EXCEEDED;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSTAT;
extern const int CANNOT_TRUNCATE_FILE;
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

File NexusFSConfig::openFile(const std::string & file_name, UInt64 size, bool truncate, bool direct_io)
{
    LOG_INFO(log, "create file: {} sie: {}, truncate: {}", file_name, size, truncate);
    if (file_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "file name is empty");

    int flags{O_RDWR | O_CREAT};
    if (direct_io)
        flags |= O_DIRECT;

    File f = File(file_name.c_str(), flags);
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

        LOG_INFO(log, "cache file {} is ftruncated from {} bytes to {} bytes", file_name, cur_file_size, size);
    }

    return f;
}

void NexusFSConfig::loadFromConfig(const Poco::Util::AbstractConfiguration & conf)
{
    String config_name(CONFIG_NAME);
    cache_size = conf.getUInt64(config_name + ".cache_size", 10 * GiB);
    region_size = conf.getUInt64(config_name + ".region_size", 2 * MiB);
    segment_size = conf.getUInt64(config_name + ".segment_size", 128 * KiB);
    source_buffer_size = conf.getUInt64(config_name + ".source_buffer_size", DBMS_DEFAULT_BUFFER_SIZE);
    alloc_align_size = conf.getUInt(config_name + ".alloc_align_size", 512);
    io_align_size = conf.getUInt(config_name + ".io_align_size", 4096);
    stripe_size = conf.getUInt(config_name + ".stripe_size", 4096);
    reader_threads = conf.getUInt(config_name + ".reader_threads", getNumberOfPhysicalCPUCores() >> 1);
    insert_threads = conf.getUInt(config_name + ".reader_threads", 4);
    clean_regions_pool = conf.getUInt(config_name + ".clean_regions_pool", 4);
    clean_region_threads = conf.getUInt(config_name + ".clean_region_threads", 2);
    num_in_mem_buffers = conf.getUInt(config_name + ".num_in_mem_buffers", 8);
    enable_memory_buffer = conf.getBool(config_name + ".enable_memory_buffer", false);
    support_prefetch = conf.getBool(config_name + ".support_prefetch", true);
    memory_buffer_size = conf.getUInt64(config_name + ".memory_buffer_size", 10 * GiB);
    memory_buffer_cooling_percent = conf.getDouble(config_name + ".memory_buffer_cooling_percent", 0.1);
    memory_buffer_freed_percent = conf.getDouble(config_name + ".memory_buffer_freed_percent", 0.05);
    timeout_ms = conf.getUInt(config_name + ".timeout_ms", 10000);
    filemeta_gc_interval_s = conf.getUInt(config_name + ".filemeta_gc_interval_s", 300);
    bool use_memory_device = conf.getBool(config_name + ".use_memory_device", false);
    bool enable_async_io = conf.getBool(config_name + ".enable_async_io", false);
    file_prefix = conf.getString(config_name + ".file_prefix", "");

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
                f = openFile(path, cache_size, true, false);
            }
            catch (const ErrnoException & e)
            {
                LOG_ERROR(getLogger("NexusFSConfig"), "Exception in openFile {}, error: {} errno: {}", path, e.what(), e.getErrno());
                throw;
            }
            file_vec.push_back(std::move(f));
        }
        device = createDirectIoFileDevice(std::move(file_vec), cache_size, io_align_size, stripe_size, 0, io_engine, q_depth);
    }

    validate();
}

NexusFSConfig & NexusFSConfig::validate()
{
    if (!device || !eviction_policy)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "missing required param");
    if (region_size > 256u << 20)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "region is too large");
    if (source_buffer_size % segment_size != 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "source_buffer_size must be aligned to segment_size");
    if (cache_size <= 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "invalid size");
    if (cache_size % region_size != 0)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            fmt::format("Cache size is not aligned to region size! cache size: {} region size: {}", cache_size, region_size));
    if (getNumberRegions() < clean_regions_pool)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "not enough space on device");
    if (num_in_mem_buffers == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "there must be at least one in-mem buffers");
    if (num_priorities == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "allocator must have at least one priority");
    if (reader_threads == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "reader threads must greater than 0");

    return *this;
}


NexusFS::NexusFS(NexusFSConfig && config)
    : serialized_config{serializeConfig(config)}
    , device{std::move(config.device)}
    , alloc_align_size{config.alloc_align_size}
    , metadata_size(config.metadata_size)
    , source_buffer_size(config.source_buffer_size)
    , segment_size(config.segment_size)
    , num_segments_per_source_buffer(source_buffer_size / segment_size)
    , timeout_ms(config.timeout_ms)
    , file_prefix(config.file_prefix)
    , index(getFilePrefix(), getSegmentSize())
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
    , enable_buffer(config.enable_memory_buffer)
    , support_prefetch(config.support_prefetch)
    , buffer_manager(
        config.enable_memory_buffer ? 
            BufferManager::initInstance(
            config.memory_buffer_size,
            config.segment_size,
            config.filemeta_gc_interval_s,
            config.memory_buffer_cooling_percent,
            config.memory_buffer_freed_percent,
            region_manager,
            index)
            : nullptr)
{
    if (support_prefetch)
    {
        for (uint32_t i = 0; i < config.reader_threads; i++)
        {
            auto name = fmt::format("NexusFS_read_worker_{}", i);
            reader_workers.emplace_back(std::make_unique<FiberThread>(name));
        }
        for (uint32_t i = 0; i < config.insert_threads; i++)
        {
            auto name = fmt::format("NexusFS_insert_worker_{}", i);
            insert_workers.emplace_back(std::make_unique<FiberThread>(name));
        }
    }
    LOG_TRACE(log, "NexusFS created");
}

NexusFS::~NexusFS()
{
    reader_workers.clear();
    insert_workers.clear();
    if (buffer_manager)
        buffer_manager->destroy();
    LOG_TRACE(log, "NexusFS destroyed");
}

void NexusFS::preload(const String & file, const OffsetAndSizeVector & offsets_and_sizes, std::unique_ptr<ReadBufferFromFileBase> & source)
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
    ProfileEvents::increment(ProfileEvents::NexusFSPreload, segment_ids.size());

    for (auto id : segment_ids)
    {
        String segment_name = getSegmentName(file, id);
        open(segment_name, file, id, source);
    }
}

std::tuple<std::shared_ptr<NexusFSComponents::BlockHandle>, std::shared_ptr<NexusFS::InsertCxt>, UInt64>
NexusFS::open(const String & segment_name, const String & file, const UInt64 segment_id, std::unique_ptr<ReadBufferFromFileBase> & source)
{
    LOG_TRACE(log, "try to find {} from index", segment_name);

    auto seq_number = region_manager.getSeqNumber();
    auto handle = index.lookup(file, segment_id);
    std::shared_ptr<InsertCxt> insert_cxt = nullptr;
    if (!handle)
    {
        UInt64 source_segment_id = segment_id / num_segments_per_source_buffer;
        String source_segment_name = getSourceSegmentName(file, source_segment_id);
        LOG_TRACE(log, "{} not found, check InFlightInserts by {}", segment_name, source_segment_name);
        bool is_newly_created;
        std::tie(insert_cxt, is_newly_created) = in_flight_inserts.getOrCreateContext(source_segment_name);
        if (is_newly_created)
        {
            // double check index
            handle = index.lookup(file, segment_id);
            if (handle)
            {
                in_flight_inserts.removeContext(source_segment_name);
                insert_cxt.reset();
                LOG_TRACE(log, "{} already inserted to index, remove {} from in_flight_inserts", segment_name, source_segment_name);
                ProfileEvents::increment(ProfileEvents::NexusFSHit);
            }
            else
            {
                LOG_TRACE(log, "create InsertCxt for {}, read from source and insert to cache", segment_name);
                ProfileEvents::increment(ProfileEvents::NexusFSMiss);
                num_inflight_inserts++;

                {
                    std::lock_guard<std::mutex> lock(insert_cxt->mutex);
                    ProfileEventTimeIncrement<Microseconds> source_watch(ProfileEvents::NexusFSReadFromSourceMicroseconds);

                    insert_cxt->buffer = Buffer(source_buffer_size);
                    off_t offset_in_source = getOffsetInSourceFile(source_segment_id * num_segments_per_source_buffer);
                    size_t bytes_read
                        = source->readBigAt(reinterpret_cast<char *>(insert_cxt->buffer.data()), source_buffer_size, offset_in_source);
                    ProfileEvents::increment(ProfileEvents::NexusFSReadFromSourceBytesRead, bytes_read);
                    LOG_TRACE(log, "read {} bytes from source, key={}, offset={}", bytes_read, source_segment_name, offset_in_source);

                    insert_cxt->buffer.shrink(bytes_read);
                    insert_cxt->ready = true;
                    insert_cxt->cv.notify_all();
                }

                auto source_file_size = source->getFileSize();
                getInsertWorker().addTaskRemote([insert_cxt, source_segment_id, file, source_segment_name, source_file_size, this]()
                {
                    auto get_file_and_segment_size = [source_file_size, this]() { return std::make_pair(source_file_size, segment_size); };
                    for (UInt32 i = 0; i < num_segments_per_source_buffer; i++)
                    {
                        if (insert_cxt->buffer.size() > segment_size * i)
                        {
                            UInt64 current_segment_id = source_segment_id * num_segments_per_source_buffer + i;
                            size_t data_size = std::min(insert_cxt->buffer.size() - segment_size * i, static_cast<size_t>(segment_size));
                            UInt8 * data_ptr = insert_cxt->buffer.data() + segment_size * i;
                            BufferView view(data_size, data_ptr);
                            insert(file, current_segment_id, view, get_file_and_segment_size);
                        }
                    }
                    in_flight_inserts.removeContext(source_segment_name);
                    num_inflight_inserts--;
                });
            }
        }
        else
        {
            LOG_TRACE(log, "found InsertCxt for {}, wait and read from InsertCxt", segment_name);
            ProfileEvents::increment(ProfileEvents::NexusFSHitInflightInsert);
            ProfileEvents::increment(ProfileEvents::NexusFSHit);
        }
    }
    else
    {
        LOG_TRACE(log, "{} found, {}", segment_name, handle->toString());
        ProfileEvents::increment(ProfileEvents::NexusFSHit);
    }
    return std::make_tuple(handle, insert_cxt, seq_number);
}

std::pair<OpResult, size_t>
NexusFS::readFromInsertCxtInternal(std::shared_ptr<InsertCxt> & cxt, const off_t offset_in_segment, const size_t max_size, char * to) const
{
    {
        std::unique_lock<std::mutex> lock(cxt->mutex);
        auto timeout = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms / 3);
        if (!cxt->cv.wait_until(lock, timeout, [&] { return cxt->ready; }))
        {
            // timeout, deep retry
            return {OpResult::DEEP_RETRY, 0};
        }
    }

    size_t size = getReadSizeInSegment(offset_in_segment, cxt->buffer.size(), max_size);
    if (size == 0)
        return {OpResult::SUCCESS, 0};

    memcpy(to, cxt->buffer.data() + offset_in_segment, size);

    return {OpResult::SUCCESS, size};
}

std::pair<OpResult, NexusFSBufferWithHandle>
NexusFS::readFromInsertCxtInternal(std::shared_ptr<InsertCxt> & cxt, const off_t offset_in_segment, const size_t max_size) const
{
    {
        std::unique_lock<std::mutex> lock(cxt->mutex);
        auto timeout = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms / 3);
        if (!cxt->cv.wait_until(lock, timeout, [&] { return cxt->ready; }))
        {
            // timeout, deep retry
            return {OpResult::DEEP_RETRY, NexusFSBufferWithHandle()};
        }
    }

    size_t size = getReadSizeInSegment(offset_in_segment, cxt->buffer.size(), max_size);
    if (size == 0)
        return {OpResult::SUCCESS, NexusFSBufferWithHandle()};

    NexusFSBufferWithHandle bwh;
    bwh.buffer
        = std::make_unique<BufferWithOwnMemory<ReadBuffer>>(size, reinterpret_cast<char *>(cxt->buffer.data() + offset_in_segment), 0);
    bwh.buffer->buffer().resize(size);
    bwh.insert_cxt = cxt;

    return {OpResult::SUCCESS, std::move(bwh)};
}

std::pair<OpResult, size_t> NexusFS::readFromBufferInternal(
    std::shared_ptr<BlockHandle> & handle, const UInt64 seq_number, const off_t offset_in_segment, const size_t size, char * to)
{
    chassert(buffer_manager);
    auto [op_result, buffer] = buffer_manager->pin(handle, seq_number);
    if (op_result == OpResult::SUCCESS)
    {
        LOG_TRACE(
            log,
            "{} pinned, going to copy {} bytes from buffer({})",
            handle->toString(),
            size,
            reinterpret_cast<void *>(buffer + offset_in_segment));
        chassert(buffer);
        memcpy(to, reinterpret_cast<char *>(buffer + offset_in_segment), size);
        handle->unpin();
        return {OpResult::SUCCESS, size};
    }
    return {op_result, 0};
}

std::pair<OpResult, NexusFSBufferWithHandle> NexusFS::readFromBufferInternal(
    std::shared_ptr<BlockHandle> & handle, const UInt64 seq_number, const off_t offset_in_segment, const size_t size)
{
    chassert(buffer_manager);
    auto [op_result, buffer] = buffer_manager->pin(handle, seq_number);
    if (op_result == OpResult::SUCCESS)
    {
        LOG_TRACE(
            log,
            "{} pinned, return a buffer({}) with {} bytes",
            handle->toString(),
            reinterpret_cast<void *>(buffer + offset_in_segment),
            size);
        chassert(buffer);
        NexusFSBufferWithHandle bwh;
        bwh.handle = handle;
        bwh.buffer = std::make_unique<BufferWithOwnMemory<ReadBuffer>>(size, reinterpret_cast<char *>(buffer + offset_in_segment), 0);
        bwh.buffer->buffer().resize(size);
        return {OpResult::SUCCESS, std::move(bwh)};
    }
    return {op_result, NexusFSBufferWithHandle()};
}

std::pair<OpResult, size_t> NexusFS::readFromDiskInternal(
    std::shared_ptr<BlockHandle> & handle, const UInt64 seq_number, const off_t offset_in_segment, const size_t size, char * to)
{
    if (!handle->isRelAddressValid())
        return {OpResult::DEEP_RETRY, 0};

    auto addr = handle->getRelAddress();
    chassert(addr.rid().valid());
    RegionDescriptor desc = region_manager.openForRead(addr.rid(), seq_number);
    switch (desc.getStatus())
    {
        case OpenStatus::Retry:
            if (region_manager.getSeqNumber() != seq_number)
                return {OpResult::DEEP_RETRY, 0};
            else
                return {OpResult::RETRY, 0};
        case OpenStatus::Error:
            return {OpResult::ERROR, 0};
        case OpenStatus::Ready:
            addr = addr.add(offset_in_segment);
            size_t bytes_read = readEntry(desc, addr, size, to);
            LOG_TRACE(log, "read {} bytes from disk, addr=<{},{}>", bytes_read, addr.rid().index(), addr.offset());
            if (bytes_read > 0)
            {
                region_manager.touch(addr.rid());
            }
            region_manager.close(std::move(desc));
            return {OpResult::SUCCESS, bytes_read};
    }
    return {OpResult::ERROR, 0}; // this line should not be reached
}

std::pair<bool, size_t> NexusFS::readFromInsertCxt(
    Stopwatch & watch, const String & segment_name, std::shared_ptr<InsertCxt> & cxt, off_t offset_in_segment, size_t max_size, char * to)
    const
{
    auto [op_result, bytes_read] = readFromInsertCxtInternal(cxt, offset_in_segment, max_size, to);
    while (op_result == OpResult::RETRY && watch.elapsedMilliseconds() < timeout_ms)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtRetry);
        std::tie(op_result, bytes_read) = readFromInsertCxtInternal(cxt, offset_in_segment, max_size, to);
    }
    switch (op_result)
    {
        case OpResult::SUCCESS:
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxt);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtBytesRead, bytes_read);
            return {false, bytes_read};
        case OpResult::RETRY:
            ProfileEvents::increment(ProfileEvents::NexusFSTimeout);
            return {true, 0};
        case OpResult::ERROR:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "readFromInsertCxt failed when reading {}, cxt={}",
                segment_name,
                reinterpret_cast<void *>(cxt.get()));
        default:
            chassert(op_result == OpResult::DEEP_RETRY);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtDeepRetry);
            return {true, 0};
    }
}

std::pair<bool, NexusFSBufferWithHandle> NexusFS::readFromInsertCxt(
    Stopwatch & watch, const String & segment_name, std::shared_ptr<InsertCxt> & cxt, off_t offset_in_segment, size_t max_size) const
{
    auto [op_result, bwh] = readFromInsertCxtInternal(cxt, offset_in_segment, max_size);
    while (op_result == OpResult::RETRY && watch.elapsedMilliseconds() < timeout_ms)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtRetry);
        std::tie(op_result, bwh) = readFromInsertCxtInternal(cxt, offset_in_segment, max_size);
    }
    switch (op_result)
    {
        case OpResult::SUCCESS:
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtNonCopy);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtNonCopyBytesRead, bwh.getSize());
            return {false, std::move(bwh)};
        case OpResult::RETRY:
            ProfileEvents::increment(ProfileEvents::NexusFSTimeout);
            return {true, NexusFSBufferWithHandle()};
        case OpResult::ERROR:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "readFromInsertCxt(non-copy) failed when reading {}, cxt={}",
                segment_name,
                reinterpret_cast<void *>(cxt.get()));
        default:
            chassert(op_result == OpResult::DEEP_RETRY);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromInsertCxtDeepRetry);
            return {true, NexusFSBufferWithHandle()};
    }
}

std::pair<bool, size_t> NexusFS::readFromBuffer(
    Stopwatch & watch,
    const String & segment_name,
    std::shared_ptr<NexusFSComponents::BlockHandle> & handle,
    UInt64 seq_number,
    off_t offset_in_segment,
    size_t size,
    char * to)
{
    auto [op_result, bytes_read] = readFromBufferInternal(handle, seq_number, offset_in_segment, size, to);
    while (op_result == OpResult::RETRY && watch.elapsedMilliseconds() < timeout_ms)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferRetry);
        std::tie(op_result, bytes_read) = readFromBufferInternal(handle, seq_number, offset_in_segment, size, to);
    }
    switch (op_result)
    {
        case OpResult::SUCCESS:
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromBuffer);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferBytesRead, bytes_read);
            return {false, bytes_read};
        case OpResult::RETRY:
            ProfileEvents::increment(ProfileEvents::NexusFSTimeout);
            return {true, 0};
        case OpResult::ERROR:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "readFromBuffer failed when reading {}, handle={}",
                segment_name,
                reinterpret_cast<void *>(handle.get()));
        default:
            chassert(op_result == OpResult::DEEP_RETRY);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferDeepRetry);
            return {true, 0};
    }
}

std::pair<bool, NexusFSBufferWithHandle> NexusFS::readFromBuffer(
    Stopwatch & watch,
    const String & segment_name,
    std::shared_ptr<NexusFSComponents::BlockHandle> & handle,
    UInt64 seq_number,
    off_t offset_in_segment,
    size_t size)
{
    auto [op_result, bwh] = readFromBufferInternal(handle, seq_number, offset_in_segment, size);
    while (op_result == OpResult::RETRY && watch.elapsedMilliseconds() < timeout_ms)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferRetry);
        std::tie(op_result, bwh) = readFromBufferInternal(handle, seq_number, offset_in_segment, size);
    }
    switch (op_result)
    {
        case OpResult::SUCCESS:
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferNonCopy);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferNonCopyBytesRead, bwh.getSize());
            return {false, std::move(bwh)};
        case OpResult::RETRY:
            ProfileEvents::increment(ProfileEvents::NexusFSTimeout);
            return {true, NexusFSBufferWithHandle()};
        case OpResult::ERROR:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "readFromBuffer(non-copy) failed when reading {}, handle={}",
                segment_name,
                reinterpret_cast<void *>(handle.get()));
        default:
            chassert(op_result == OpResult::DEEP_RETRY);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromBufferDeepRetry);
            return {true, NexusFSBufferWithHandle()};
    }
}

std::pair<bool, size_t> NexusFS::readFromDisk(
    Stopwatch & watch,
    const String & segment_name,
    std::shared_ptr<NexusFSComponents::BlockHandle> & handle,
    UInt64 seq_number,
    off_t offset_in_segment,
    size_t size,
    char * to)
{
    auto [op_result, bytes_read] = readFromDiskInternal(handle, seq_number, offset_in_segment, size, to);
    while (op_result == OpResult::RETRY && watch.elapsedMilliseconds() < timeout_ms)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSReadFromDiskRetry);
        std::tie(op_result, bytes_read) = readFromDiskInternal(handle, seq_number, offset_in_segment, size, to);
    }
    switch (op_result)
    {
        case OpResult::SUCCESS:
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromDisk);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromDiskBytesRead, bytes_read);
            return {false, bytes_read};
        case OpResult::RETRY:
            ProfileEvents::increment(ProfileEvents::NexusFSTimeout);
            return {true, 0};
        case OpResult::ERROR:
            throw Exception(
                ErrorCodes::CANNOT_OPEN_FILE,
                "readFromDisk failed when reading {}, handle={}",
                segment_name,
                reinterpret_cast<void *>(handle.get()));
        default:
            chassert(op_result == OpResult::DEEP_RETRY);
            ProfileEvents::increment(ProfileEvents::NexusFSReadFromDiskDeepRetry);
            return {true, 0};
    }
}

size_t
NexusFS::read(const String & file, const off_t offset, const size_t max_size, std::unique_ptr<ReadBufferFromFileBase> & source, char * to)
{
    UInt64 segment_id = getSegmentId(offset);
    String segment_name = getSegmentName(file, segment_id);
    off_t offset_in_segment = getOffsetInSegment(offset);

    Stopwatch watch;
    UInt32 num_tries = 0;
    while (watch.elapsedMilliseconds() < timeout_ms)
    {
        num_tries++;
        if (num_tries > 1)
            ProfileEvents::increment(ProfileEvents::NexusFSDeepRetry);

        auto [handle, cxt, seq_number] = open(segment_name, file, segment_id, source);

        if (cxt)
        {
            // read from InsertCxt
            auto offset_in_source_buffer = getOffsetInSourceBuffer(offset);
            auto [should_retry, bytes_read] = readFromInsertCxt(watch, segment_name, cxt, offset_in_source_buffer, max_size, to);
            if (should_retry)
                continue;
            else
                return bytes_read;
        }

        chassert(handle);
        size_t size = getReadSizeInSegment(offset_in_segment, handle->getSize(), max_size);
        if (size == 0)
            return 0;

        if (enable_buffer)
        {
            // read from memroy buffer
            auto [should_retry, bytes_read] = readFromBuffer(watch, segment_name, handle, seq_number, offset_in_segment, size, to);
            if (should_retry)
                continue;
            else
                return bytes_read;
        }
        else
        {
            // read from disk
            auto [should_retry, bytes_read] = readFromDisk(watch, segment_name, handle, seq_number, offset_in_segment, size, to);
            if (should_retry)
                continue;
            else
                return bytes_read;
        }
    }

    ProfileEventTimeIncrement<Microseconds> source_watch(ProfileEvents::NexusFSReadFromSourceMicroseconds);
    LOG_WARNING(log, "read tries for {} times and timeout ({}ms), read directly from source", num_tries, watch.elapsedMilliseconds());
    size_t bytes_read = source->readBigAt(to, max_size, offset);
    ProfileEvents::increment(ProfileEvents::NexusFSReadFromSourceBytesRead, bytes_read);
    LOG_TRACE(log, "read {} bytes from source, key={}, offset={}", bytes_read, segment_name, offset);
    return bytes_read;
}

NexusFSBufferWithHandle
NexusFS::read(const String & file, const off_t offset, const size_t max_size, std::unique_ptr<ReadBufferFromFileBase> & source)
{
    UInt64 segment_id = getSegmentId(offset);
    String segment_name = getSegmentName(file, segment_id);
    off_t offset_in_segment = getOffsetInSegment(offset);

    Stopwatch watch;
    UInt32 num_tries = 0;
    while (watch.elapsedMilliseconds() < timeout_ms)
    {
        num_tries++;
        if (num_tries > 1)
            ProfileEvents::increment(ProfileEvents::NexusFSDeepRetry);

        auto [handle, cxt, seq_number] = open(segment_name, file, segment_id, source);

        if (cxt)
        {
            // read from InsertCxt
            auto offset_in_source_buffer = getOffsetInSourceBuffer(offset);
            auto [should_retry, bwh] = readFromInsertCxt(watch, segment_name, cxt, offset_in_source_buffer, max_size);
            if (should_retry)
                continue;
            else
                return std::move(bwh);
        }

        chassert(handle);
        size_t size = getReadSizeInSegment(offset_in_segment, handle->getSize(), max_size);
        if (size == 0)
            return NexusFSBufferWithHandle();

        // read from memroy buffer
        chassert(enable_buffer);
        auto [should_retry, bwh] = readFromBuffer(watch, segment_name, handle, seq_number, offset_in_segment, size);
        if (should_retry)
            continue;
        else
            return std::move(bwh);
    }

    LOG_WARNING(log, "read tries for {} times and timeout ({}ms), read directly from source", num_tries, watch.elapsedMilliseconds());
    NexusFSBufferWithHandle bwh;
    bwh.buffer = std::make_unique<BufferWithOwnMemory<ReadBuffer>>(max_size, nullptr, 0);

    ProfileEventTimeIncrement<Microseconds> source_watch(ProfileEvents::NexusFSReadFromSourceMicroseconds);
    size_t bytes_read = source->readBigAt(bwh.buffer->position(), max_size, offset);
    bwh.buffer->buffer().resize(bytes_read);
    ProfileEvents::increment(ProfileEvents::NexusFSReadFromSourceBytesRead, bytes_read);
    LOG_TRACE(log, "read {} bytes from source, key={}, offset={}", bytes_read, segment_name, offset);
    return bwh;
}

std::future<NexusFSBufferWithHandle>
NexusFS::prefetchToBuffer(const String & file, off_t offset, size_t max_size, std::unique_ptr<ReadBufferFromFileBase> & source)
{
    auto promise = std::make_shared<std::promise<NexusFSBufferWithHandle>>();

    if (support_prefetch)
    {
        getReadWorker().addTaskRemote([&file, &source, this, offset, max_size, promise]() {
            try
            {
                auto bwh = read(file, offset, max_size, source);
                ProfileEvents::increment(ProfileEvents::NexusFSPrefetchToBuffer);
                ProfileEvents::increment(ProfileEvents::NexusFSPrefetchToBufferBytesRead, bwh.getSize());
                promise->set_value(std::move(bwh));
            }
            catch (Exception & e)
            {
                promise->set_exception(std::make_exception_ptr(e));
            }
        });
    }
    else
        promise->set_exception(
            std::make_exception_ptr(Exception(ErrorCodes::NOT_IMPLEMENTED, "support_prefetch = false, prefetchToBuffer is not supported")));

    return promise->get_future();
}

std::shared_ptr<BlockHandle> NexusFS::insert(
    const String & file, UInt64 segment_id, BufferView buf_view, std::function<std::pair<size_t, UInt32>()> get_file_and_segment_size)
{
    size_t size = buf_view.size();
    UInt32 aligned_size = alignedSize(size);

    if (size == 0)
    {
        auto handle = std::make_shared<BlockHandle>(RelAddress(), 0);
        index.insert(file, segment_id, handle, get_file_and_segment_size);
        return handle;
    }

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
        LOG_ERROR(log, "failed to insert {}#{}, size={}", file, segment_id, slot_size);
        return nullptr;
    }

    chassert(addr.offset() + slot_size <= region_manager.regionSize());
    chassert(slot_size % alloc_align_size == 0ULL);

    auto handle = std::make_shared<BlockHandle>(addr, size);
    writeEntry(handle, buf_view);
    index.insert(file, segment_id, handle, get_file_and_segment_size);

    LOG_TRACE(
        log,
        "create {} for {}#{}, write to disk addr=<{},{}>, size={}, slot_size={}",
        handle->toString(),
        file,
        segment_id,
        addr.rid().index(),
        addr.offset(),
        buf_view.size(),
        slot_size);

    allocator.close(std::move(desc));

    return handle;
}

void NexusFS::writeEntry(std::shared_ptr<NexusFSComponents::BlockHandle> & handle, HybridCache::BufferView value)
{
    auto addr = handle->getRelAddress();
    chassert(addr.offset() + value.size() <= region_manager.regionSize());

    auto rid = addr.rid();
    auto & region = region_manager.getRegion(rid);
    region.writeToBuffer(addr.offset(), value);
    region.addHandle(handle);
    num_segments++;

    ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheBytesWrite, value.size());
}

size_t NexusFS::readEntry(const RegionDescriptor & rdesc, RelAddress addr, UInt32 size, char * to)
{
    chassert(addr.offset() + size <= region_manager.regionSize());

    LOG_TRACE(log, "read from disk addr=<{},{}>, size={} ", addr.rid().index(), addr.offset(), size);
    ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheBytesRead, size);

    return region_manager.read(rdesc, addr, size, to);
}

UInt32 NexusFS::onRegionReclaim(RegionId rid, BufferView /*buffer*/)
{
    UInt32 eviction_count = 0;
    auto & region = region_manager.getRegion(rid);
    std::vector<std::shared_ptr<NexusFSComponents::BlockHandle>> handles;
    region.getHandles(handles);
    chassert(region.getNumItems() == handles.size());

    for (auto & handle : handles)
    {
        if (!handle)
        {
            LOG_ERROR(log, "reclaim a handle in from region {}, but it is null", rid.index());
            continue;
        }

        // auto addr = handle->getRelAddress();
        // chassert(addr.rid().valid());
        // auto size = handle->getSize();
        // BufferView value{size, buffer.data() + addr.offset()};

        const auto reinsertion_res = reinsertOrRemoveItem(handle);
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

    region.resetHandles();
    chassert(region.getNumItems() >= eviction_count);
    return eviction_count;
}

void NexusFS::onRegionCleanup(RegionId rid, BufferView /*buffer*/)
{
    UInt32 eviction_count = 0;
    auto & region = region_manager.getRegion(rid);
    std::vector<std::shared_ptr<NexusFSComponents::BlockHandle>> handles;
    region.getHandles(handles);
    chassert(region.getNumItems() == handles.size());

    for (auto & handle : handles)
    {
        if (!handle)
        {
            LOG_ERROR(log, "cleanup a handle in from region {}, but it is null", rid.index());
            continue;
        }
        // auto addr = handle->getRelAddress();
        // chassert(addr.rid().valid());

        removeItem(handle);
        eviction_count++;

        // if (destructor_callback && remove_res)
        //     destructor_callback(key, value, DestructorEvent::Recycled);
    }

    region.resetHandles();
    chassert(region.getNumItems() >= eviction_count);
}

NexusFS::ReinsertionRes NexusFS::reinsertOrRemoveItem(std::shared_ptr<NexusFSComponents::BlockHandle> & handle)
{
    removeItem(handle);
    return ReinsertionRes::kRemoved;
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
    serialized_config.set_file_prefix(config.file_prefix);

    return serialized_config;
}

void NexusFS::removeItem(std::shared_ptr<NexusFSComponents::BlockHandle> & handle)
{
    num_segments--;
    handle->invalidRelAddress();
    LOG_TRACE(log, "invalid {} because of removeItem", handle->toString());
}

void NexusFS::persist()
{
    LOG_INFO(log, "Starting NexusFS persist");
    auto stream = createMetadataOutputStream(*device, metadata_size);
    Protos::NexusFSConfig config = serialized_config;
    google::protobuf::io::CodedOutputStream ostream(stream.get());
    google::protobuf::util::SerializeDelimitedToCodedStream(config, &ostream);
    region_manager.persist(&ostream);
    index.persist(&ostream);

    LOG_INFO(log, "Finished NexusFS persist");
}

bool NexusFS::recover()
{
    LOG_INFO(log, "Starting NexusFS recovery");
    reset();
    bool recovered = false;
    try
    {
        auto stream = createMetadataInputStream(*device, metadata_size);
        if (!stream)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to createMetadataInputStream");
        Protos::NexusFSConfig config;
        google::protobuf::io::CodedInputStream istream(stream.get());
        google::protobuf::util::ParseDelimitedFromCodedStream(&config, &istream, nullptr);

        if (config.cache_size() != serialized_config.cache_size() || config.metadata_size() != serialized_config.metadata_size()
            || config.region_size() != serialized_config.region_size() || config.segment_size() != serialized_config.segment_size()
            || config.version() != serialized_config.version() || config.alloc_align_size() != serialized_config.alloc_align_size()
            || config.file_prefix() != serialized_config.file_prefix())
        {
            LOG_ERROR(log, "Recovery config: {}", config.DebugString());
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Recovery config does not match cache config");
        }

        region_manager.recover(&istream);
        index.recover(&istream, region_manager, num_segments);

        // successful recovery, invalid current metadata
        auto output_stream = createMetadataOutputStream(*device, metadata_size);
        if (!output_stream)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to createMetadataOutputStream");
        recovered = output_stream->invalidate();
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Exception: {}", e.what());
        LOG_ERROR(log, "Failed to recover NexusFS. Resetting cache.");
        reset();
        return false;
    }
    if (recovered)
        LOG_INFO(
            log,
            "Finished NexusFS recovery. Recover {} inodes, {} files, {} segments",
            index.getNumInodes(),
            index.getNumFileMetas(),
            num_segments);
    return recovered;
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

}
