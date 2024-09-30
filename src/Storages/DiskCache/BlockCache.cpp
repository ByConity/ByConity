#include <algorithm>
#include <cmath>
#include <exception>
#include <memory>
#include <random>
#include <utility>
#include <math.h>

#include <fmt/core.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/BlockCache.h>
#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/HitsReinsertionPolicy.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/PercentageReinsertionPolicy.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/Types.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/InjectPause.h>
#include <Common/ProfileEvents.h>
#include <Common/thread_local_rng.h>
#include <common/bit_cast.h>
#include <common/defines.h>
#include <common/function_traits.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <common/types.h>

namespace ProfileEvents
{
extern const Event BlockCacheInsertCount;
extern const Event BlockCacheInsertHashCollisionCount;
extern const Event BlockCacheSuccInsertCount;
extern const Event BlockCacheLookupFalsePositiveCount;
extern const Event BlockCacheLookupEntryHeaderChecksumErrorCount;
extern const Event BlockCacheLookupValueChecksumErrorCount;
extern const Event BlockCacheRemoveCount;
extern const Event BlockCacheSuccRemoveCount;
extern const Event BlockCacheEvictionLookupMissCount;
extern const Event BlockCacheEvictionExpiredCount;
extern const Event BlockCacheAllocErrorCount;
extern const Event BlockCacheLogicWrittenCount;
extern const Event BlockCacheLookupCount;
extern const Event BlockCacheSuccLookupCount;
extern const Event BlockCacheReinsertionErrorCount;
extern const Event BlockCacheReinsertionCount;
extern const Event BlockCacheReinsertionBytes;
extern const Event BlockCacheLookupForItemDestructorErrorCount;
extern const Event BlockCacheRemoveAttemptionCollisions;
extern const Event BlockCacheReclaimEntryHeaderChecksumErrorCount;
extern const Event BlockCacheReclaimValueChecksumErrorCount;
extern const Event BlockCacheCleanupEntryHeaderChecksumErrorCount;
extern const Event BlockCacheCleanupValueChecksumErrorCount;
}

namespace CurrentMetrics
{
extern const Metric BlockCacheUsedSizeBytes;
extern const Metric BlockCacheHoleCount;
extern const Metric BlockCacheHoleBytesTotal;
}

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int NOT_IMPLEMENTED;
}

namespace DB::HybridCache
{
BlockCache::Config & BlockCache::Config::validate()
{
    chassert(scheduler != nullptr);
    if (!device || !eviction_policy)
        throw Exception("missing required param", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (region_size > 256u << 20)
        throw Exception("region is too large", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (cache_size <= 0)
        throw Exception("invalid size", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (cache_size % region_size != 0)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Cache size is not aligned to region size! cache size: {} region size: {]}",
            cache_size,
            region_size);
    if (getNumberRegions() < clean_regions_pool)
        throw Exception("not enough space on device", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (num_in_mem_buffers == 0)
        throw Exception("there must be at least one in-mem buffers", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (num_priorities == 0)
        throw Exception("allocator must have at least one priority", ErrorCodes::INVALID_CONFIG_PARAMETER);

    reinsertion_config.validate();

    return *this;
}

void BlockCache::validate(BlockCache::Config & config) const
{
    UInt32 align_size = calcAllocAlignSize();
    if (!isPowerOf2(align_size))
        throw Exception("invalid block size", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (config.region_size % align_size != 0)
        throw Exception("invalid region size", ErrorCodes::INVALID_CONFIG_PARAMETER);
    auto shift_width = NumBits<decltype(RelAddress().offset())>::value;
    if (config.cache_size > static_cast<UInt64>(align_size) << shift_width)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "can't address cache with {} bits", static_cast<UInt32>(shift_width));
}

UInt32 BlockCache::calcAllocAlignSize() const
{
    auto shift_width = NumBits<decltype(RelAddress().offset())>::value;
    UInt32 align_size = static_cast<UInt32>(device.getSize() >> shift_width);
    if (align_size <= kMinAllocAlignSize)
        return kMinAllocAlignSize;
    if (isPowerOf2(align_size))
        return align_size;
    return roundUpToPowerOfTwoOrZero(align_size);
}

BlockCache::BlockCache(Config && config) : BlockCache{std::move(config.validate()), ValidConfigTag{}}
{
}

BlockCache::BlockCache(Config && config, ValidConfigTag)
    : serialized_config{serializeConfig(config)}
    , num_priorities{config.num_priorities}
    , check_expired{std::move(config.check_expired)}
    , destructor_callback{std::move(config.destructor_callback)}
    , checksum_data{config.checksum}
    , device{*config.device}
    , alloc_align_size{calcAllocAlignSize()}
    , read_buffer_size{config.read_buffer_size < kDefReadBufferSize ? kDefReadBufferSize : config.read_buffer_size}
    , region_size{config.region_size}
    , item_destructor_enabled{config.item_destructor_enabled}
    , precise_remove{config.precise_remove}
    , region_manager{
          config.getNumberRegions(),
          config.region_size,
          config.cache_base_offset,
          *config.device,
          config.clean_regions_pool,
          config.clean_region_threads,
          bindThis(&BlockCache::onRegionReclaim, *this),
          bindThis(&BlockCache::onRegionCleanup, *this),
          std::move(config.eviction_policy),
          config.num_in_mem_buffers,
          config.num_priorities,
          config.in_mem_buf_flush_retry_limit}
    , allocator{region_manager, config.num_priorities}
    , reinsertion_policy{makeReinsertionPolicy(config.reinsertion_config)}
{
    validate(config);
    LOG_INFO(log, "Block cache created");
    chassert(read_buffer_size != 0u);
}

std::shared_ptr<BlockCacheReinsertionPolicy> BlockCache::makeReinsertionPolicy(const BlockCacheReinsertionConfig & reinsertion_config)
{
    auto hits_threshold = reinsertion_config.getHitsThreshold();
    if (hits_threshold)
        return std::make_shared<HitsReinsertionPolicy>(hits_threshold, index);

    auto pct_threshold = reinsertion_config.getPctThreshold();
    if (pct_threshold)
        return std::make_shared<PercentageReinsertionPolicy>(pct_threshold);

    return reinsertion_config.getCustomPolicy();
}

UInt32 BlockCache::serializedSize(UInt32 key_size, UInt32 value_size) const
{
    UInt32 size = sizeof(EntryDesc) + key_size + value_size;
    return roundup(size, alloc_align_size);
}

Status BlockCache::insert(HashedKey key, BufferView value)
{
    INJECT_PAUSE(pause_blockcache_insert_entry);

    UInt32 size = serializedSize(key.key().size, value.size());
    if (size > kMaxItemSize)
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheAllocErrorCount);
        ProfileEvents::increment(ProfileEvents::BlockCacheInsertCount);
        return Status::Rejected;
    }

    auto [desc, slot_size, addr] = allocator.allocate(size, kDefaultItemPriority, true);

    switch (desc.getStatus())
    {
        case OpenStatus::Error:
            ProfileEvents::increment(ProfileEvents::BlockCacheAllocErrorCount);
            ProfileEvents::increment(ProfileEvents::BlockCacheInsertCount);
            return Status::Rejected;
        case OpenStatus::Ready:
            ProfileEvents::increment(ProfileEvents::BlockCacheInsertCount);
            break;
        case OpenStatus::Retry:
            return Status::Retry;
    }

    const auto status = writeEntry(addr, slot_size, key, value);
    auto new_obj_size_hint = encodeSizeHint(slot_size);
    if (status == Status::Ok)
    {
        const auto lr = index.insert(key.keyHash(), encodeRelAddress(addr.add(slot_size)), new_obj_size_hint);
        UInt64 new_obj_size = decodeSizehint(new_obj_size_hint);
        UInt64 old_obj_size = 0;
        if (lr.isFound())
        {
            old_obj_size = decodeSizehint(lr.getSizeHint());
            ProfileEvents::increment(ProfileEvents::BlockCacheInsertHashCollisionCount);
            CurrentMetrics::add(CurrentMetrics::BlockCacheHoleCount);
            CurrentMetrics::add(CurrentMetrics::BlockCacheHoleBytesTotal, old_obj_size);
        }
        ProfileEvents::increment(ProfileEvents::BlockCacheSuccInsertCount);
        if (new_obj_size < old_obj_size)
            CurrentMetrics::add(CurrentMetrics::BlockCacheUsedSizeBytes, old_obj_size - new_obj_size);
        else
            CurrentMetrics::add(CurrentMetrics::BlockCacheUsedSizeBytes, new_obj_size - old_obj_size);
    }
    allocator.close(std::move(desc));
    INJECT_PAUSE(pause_blockcache_insert_done);
    return status;
}

bool BlockCache::couldExist(HashedKey key)
{
    const auto lr = index.lookup(key.keyHash());
    if (!lr.isFound())
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheLookupCount);
        return false;
    }
    return true;
}

UInt64 BlockCache::estimateWriteSize(HashedKey key, BufferView value) const
{
    return serializedSize(key.key().size, value.size());
}

Status BlockCache::lookup(HashedKey key, Buffer & value)
{
    const auto seq_number = region_manager.getSeqNumber();
    const auto lr = index.lookup(key.keyHash());
    if (!lr.isFound())
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheLookupCount);
        return Status::NotFound;
    }
    auto addr_end = decodeRelAddress(lr.getAddress());
    RegionDescriptor desc = region_manager.openForRead(addr_end.rid(), seq_number);
    switch (desc.getStatus())
    {
        case OpenStatus::Ready: {
            auto status = readEntry(desc, addr_end, decodeSizehint(lr.getSizeHint()), key, value);
            if (status == Status::Ok)
            {
                region_manager.touch(addr_end.rid());
                ProfileEvents::increment(ProfileEvents::BlockCacheSuccLookupCount);
            }
            region_manager.close(std::move(desc));
            ProfileEvents::increment(ProfileEvents::BlockCacheLookupCount);
            return status;
        }
        case OpenStatus::Retry:
            return Status::Retry;
        default:
            chassert(false);
            return Status::DeviceError;
    }
}

std::pair<Status, std::string> BlockCache::getRandomAlloc(Buffer & value)
{
    auto rid = region_manager.getRandomRegion();
    auto & region = region_manager.getRegion(rid);
    auto rand_offset = std::uniform_int_distribution<UInt32>(0, region_size)(thread_local_rng);
    auto offset = region.getLastEntryEndOffset();
    if (rand_offset >= offset)
        return std::make_pair(Status::NotFound, "");

    const auto seq_number = region_manager.getSeqNumber();
    RegionDescriptor rdesc = region_manager.openForRead(rid, seq_number);
    if (rdesc.getStatus() != OpenStatus::Ready)
        return std::make_pair(Status::NotFound, "");

    auto buffer = region_manager.read(rdesc, RelAddress{rid, 0}, offset);
    region_manager.close(std::move(rdesc));
    if (buffer.size() != offset)
        return std::make_pair(Status::NotFound, "");

    while (offset > 0)
    {
        auto * entry_end = buffer.data() + offset;
        auto desc = *reinterpret_cast<const EntryDesc *>(entry_end - sizeof(EntryDesc));
        if (desc.cs_self != desc.computeChecksum())
        {
            LOG_ERROR(
                log,
                fmt::format(
                    "Item header checksum mismatch. Region {} is likely corrupted. Expeccted: {} Actual: {}",
                    rid.index(),
                    desc.cs_self,
                    desc.computeChecksum()));
            break;
        }

        RelAddress addr_end{rid, offset};
        const auto entry_size = serializedSize(desc.key_size, desc.value_size);

        chassert(offset >= entry_size);
        offset -= entry_size;
        if (rand_offset < offset)
            continue;

        BufferView value_view{desc.value_size, entry_end - entry_size};
        if (checksum_data && desc.cs != checksum(value_view))
        {
            LOG_ERROR(
                log,
                fmt::format(
                    "Item value checksum mismatch. Region {} is likely corrupted. Expected: {} Actual: {}",
                    rid.index(),
                    desc.cs,
                    checksum(value_view)));
            break;
        }

        HashedKey key = makeHashKey(entry_end - sizeof(EntryDesc) - desc.key_size, desc.key_size);
        const auto lr = index.lookup(key.keyHash());
        if (!lr.isFound() || addr_end != decodeRelAddress(lr.getAddress()))
            break;

        value = Buffer(value_view);
        return std::make_pair(Status::Ok, key.key().toString());
    }

    return std::make_pair(Status::NotFound, "");
}

Status BlockCache::remove(HashedKey key)
{
    ProfileEvents::increment(ProfileEvents::BlockCacheRemoveCount);

    Buffer value;
    if ((item_destructor_enabled && destructor_callback) || precise_remove)
    {
        Status status = lookup(key, value);

        if (status != Status::Ok)
        {
            value.reset();
            if (status == Status::Retry)
                return status;
            else if (status != Status::NotFound)
            {
                ProfileEvents::increment(ProfileEvents::BlockCacheLookupForItemDestructorErrorCount);
                return Status::BadState;
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::BlockCacheRemoveAttemptionCollisions);
                if (precise_remove)
                    return status;
            }
        }
    }

    auto lr = index.remove(key.keyHash());
    if (lr.isFound())
    {
        UInt64 remove_object_size = decodeSizehint(lr.getSizeHint());
        CurrentMetrics::add(CurrentMetrics::BlockCacheHoleBytesTotal, remove_object_size);
        CurrentMetrics::add(CurrentMetrics::BlockCacheHoleCount);
        CurrentMetrics::sub(CurrentMetrics::BlockCacheUsedSizeBytes, remove_object_size);
        ProfileEvents::increment(ProfileEvents::BlockCacheSuccRemoveCount);
        if (!value.isNull() && destructor_callback)
            destructor_callback(key, value.view(), DestructorEvent::Removed);
        return Status::Ok;
    }
    return Status::NotFound;
}

UInt32 BlockCache::onRegionReclaim(RegionId rid, BufferView buffer)
{
    UInt32 eviction_count = 0;
    auto & region = region_manager.getRegion(rid);
    auto offset = region.getLastEntryEndOffset();
    while (offset > 0)
    {
        const auto * entry_end = buffer.data() + offset;
        auto desc = *reinterpret_cast<const EntryDesc *>(entry_end - sizeof(EntryDesc));
        if (desc.cs_self != desc.computeChecksum())
        {
            ProfileEvents::increment(ProfileEvents::BlockCacheReclaimEntryHeaderChecksumErrorCount);
            LOG_ERROR(
                log,
                fmt::format(
                    "Itrem header checksum mismatch. Region {} is likely corrupted. Expected: {}, Actual: {}. Aborting reclaim.",
                    rid.index(),
                    desc.cs_self,
                    desc.computeChecksum()));
            break;
        }

        const auto entry_size = serializedSize(desc.key_size, desc.value_size);
        HashedKey key = makeHashKey(entry_end - sizeof(EntryDesc) - desc.key_size, desc.key_size);
        BufferView value{desc.value_size, entry_end - entry_size};
        if (checksum_data && desc.cs != checksum(value))
            ProfileEvents::increment(ProfileEvents::BlockCacheReclaimValueChecksumErrorCount);

        const auto reinsertion_res = reinsertOrRemoveItem(key, value, entry_size, RelAddress{rid, offset});
        switch (reinsertion_res)
        {
            case ReinsertionRes::kEvicted:
                eviction_count++;
                CurrentMetrics::sub(CurrentMetrics::BlockCacheUsedSizeBytes, decodeSizehint(encodeSizeHint(entry_size)));
                break;
            case ReinsertionRes::kRemoved:
                CurrentMetrics::sub(CurrentMetrics::BlockCacheHoleCount);
                CurrentMetrics::sub(CurrentMetrics::BlockCacheHoleBytesTotal, decodeSizehint(encodeSizeHint(entry_size)));
                break;
            case ReinsertionRes::kReinserted:
                break;
        }

        if (destructor_callback && reinsertion_res == ReinsertionRes::kEvicted)
            destructor_callback(key, value, DestructorEvent::Recycled);

        chassert(offset >= entry_size);
        offset -= entry_size;
    }
    chassert(region.getNumItems() >= eviction_count);
    return eviction_count;
}

void BlockCache::onRegionCleanup(RegionId rid, BufferView buffer)
{
    UInt32 eviction_count = 0;
    auto & region = region_manager.getRegion(rid);
    auto offset = region.getLastEntryEndOffset();
    while (offset > 0)
    {
        const auto * entry_end = buffer.data() + offset;
        auto desc = *reinterpret_cast<const EntryDesc *>(entry_end - sizeof(EntryDesc));
        if (desc.cs_self != desc.computeChecksum())
        {
            ProfileEvents::increment(ProfileEvents::BlockCacheCleanupEntryHeaderChecksumErrorCount);
            LOG_ERROR(
                log,
                fmt::format(
                    "Item header checksum mismatch. Region {] is likely corrupted. Expected: {}, Actual: {}}",
                    rid.index(),
                    desc.cs_self,
                    desc.computeChecksum()));
            break;
        }

        const auto entry_size = serializedSize(desc.key_size, desc.value_size);
        HashedKey key = makeHashKey(entry_end - sizeof(EntryDesc) - desc.key_size, desc.key_size);
        BufferView value{desc.value_size, entry_end - entry_size};
        if (checksum_data && desc.cs != checksum(value))
            ProfileEvents::increment(ProfileEvents::BlockCacheCleanupValueChecksumErrorCount);

        auto remove_res = removeItem(key, RelAddress{rid, offset});
        if (remove_res)
        {
            eviction_count++;
            CurrentMetrics::sub(CurrentMetrics::BlockCacheUsedSizeBytes, decodeSizehint(encodeSizeHint(entry_size)));
        }
        else
        {
            CurrentMetrics::sub(CurrentMetrics::BlockCacheHoleCount);
            CurrentMetrics::sub(CurrentMetrics::BlockCacheHoleBytesTotal, decodeSizehint(encodeSizeHint(entry_size)));
        }
        if (destructor_callback && remove_res)
            destructor_callback(key, value, DestructorEvent::Recycled);
        chassert(offset >= entry_size);
        offset -= entry_size;
    }
    chassert(region.getNumItems() >= eviction_count);
}

bool BlockCache::removeItem(HashedKey key, RelAddress addr)
{
    if (index.removeIfMatch(key.keyHash(), encodeRelAddress(addr)))
        return true;
    ProfileEvents::increment(ProfileEvents::BlockCacheEvictionLookupMissCount);
    return false;
}

BlockCache::ReinsertionRes BlockCache::reinsertOrRemoveItem(HashedKey key, BufferView value, UInt32 entry_size, RelAddress addr)
{
    auto remove_item = [this, key, addr](bool expired) {
        if (index.removeIfMatch(key.keyHash(), encodeRelAddress(addr)))
        {
            if (expired)
                ProfileEvents::increment(ProfileEvents::BlockCacheEvictionExpiredCount);
            return ReinsertionRes::kEvicted;
        }
        return ReinsertionRes::kRemoved;
    };

    const auto lr = index.peek(key.keyHash());
    if (!lr.isFound() || decodeRelAddress(lr.getAddress()) != addr)
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheEvictionLookupMissCount);
        return ReinsertionRes::kRemoved;
    }

    if (check_expired && check_expired(value))
        return remove_item(true);

    if (!reinsertion_policy || !reinsertion_policy->shouldReinsert(key.key()))
        return remove_item(false);

    UInt16 priority = num_priorities == 0 ? kDefaultItemPriority : std::min<UInt16>(lr.getCurrentHits(), num_priorities - 1);

    UInt32 size = serializedSize(key.key().size, value.size());
    auto [desc, slot_size, new_addr] = allocator.allocate(size, priority, false);

    switch (desc.getStatus())
    {
        case OpenStatus::Ready:
            break;
        case OpenStatus::Error:
            ProfileEvents::increment(ProfileEvents::BlockCacheAllocErrorCount);
            ProfileEvents::increment(ProfileEvents::BlockCacheReinsertionErrorCount);
            break;
        case OpenStatus::Retry:
            ProfileEvents::increment(ProfileEvents::BlockCacheReinsertionErrorCount);
            return remove_item(false);
    }
    auto close_region_guard = make_scope_guard([this, desc = std::move(desc)]() mutable { allocator.close(std::move(desc)); });

    const auto status = writeEntry(new_addr, slot_size, key, value);
    if (status != Status::Ok)
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheReinsertionErrorCount);
        return remove_item(false);
    }

    const auto replaced = index.replaceIfMatch(key.keyHash(), encodeRelAddress(new_addr.add(slot_size)), encodeRelAddress(addr));
    if (!replaced)
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheReinsertionErrorCount);
        return remove_item(false);
    }

    ProfileEvents::increment(ProfileEvents::BlockCacheReinsertionCount);
    ProfileEvents::increment(ProfileEvents::BlockCacheReinsertionBytes, entry_size);
    return ReinsertionRes::kReinserted;
}

Status BlockCache::writeEntry(RelAddress addr, UInt32 slot_size, HashedKey key, BufferView value)
{
    chassert(addr.offset() + slot_size <= region_manager.regionSize());
    chassert(slot_size % alloc_align_size == 0ULL);
    auto buffer = Buffer(slot_size);

    size_t desc_offset = buffer.size() - sizeof(EntryDesc);
    auto * desc = new (buffer.data() + desc_offset) EntryDesc(key.key().size, value.size(), key.keyHash());
    if (checksum_data)
        desc->cs = checksum(value);

    buffer.copyFrom(desc_offset - key.key().size, makeView(key.key()));
    buffer.copyFrom(0, value);

    region_manager.write(addr, std::move(buffer));
    ProfileEvents::increment(ProfileEvents::BlockCacheLogicWrittenCount, key.key().size + value.size());
    return Status::Ok;
}

Status BlockCache::readEntry(const RegionDescriptor & rdesc, RelAddress addr, UInt32 approx_size, HashedKey expected, Buffer & value)
{
    // The item layout is as thus
    // | --- value --- | --- empty --- | --- header --- |

    approx_size = std::min(approx_size, addr.offset());

    chassert(approx_size % alloc_align_size == 0ULL);

    chassert(approx_size >= roundUpToPowerOfTwoOrZero(sizeof(EntryDesc)));

    auto buffer = region_manager.read(rdesc, addr.sub(approx_size), approx_size);
    if (buffer.isNull())
        return Status::DeviceError;

    auto * entry_end = buffer.data() + buffer.size();
    auto desc = *reinterpret_cast<EntryDesc *>(entry_end - sizeof(EntryDesc));
    if (desc.cs_self != desc.computeChecksum())
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheLookupEntryHeaderChecksumErrorCount);
        return Status::DeviceError;
    }

    StringRef key{reinterpret_cast<const char *>(entry_end - sizeof(EntryDesc) - desc.key_size), desc.key_size};
    if (HashedKey::precomputed(key, desc.key_hash) != expected)
    {
        ProfileEvents::increment(ProfileEvents::BlockCacheLookupFalsePositiveCount);
        return Status::NotFound;
    }

    UInt32 size = serializedSize(desc.key_size, desc.value_size);
    if (buffer.size() > size)
        buffer.trimStart(buffer.size() - size);
    else if (buffer.size() < size)
    {
        buffer = region_manager.read(rdesc, addr.sub(size), size);
        if (buffer.isNull())
            return Status::DeviceError;
    }

    value = std::move(buffer);
    value.shrink(desc.value_size);
    if (checksum_data && desc.cs != checksum(value.view()))
    {
        LOG_ERROR(
            log,
            fmt::format(
                "Item value checksum mismatch when looking up key {}. Expected: {}, Actual: {}", key, desc.cs, checksum(value.view())));
        value.reset();
        ProfileEvents::increment(ProfileEvents::BlockCacheLookupValueChecksumErrorCount);
        return Status::DeviceError;
    }
    return Status::Ok;
}

void BlockCache::flush()
{
    LOG_INFO(log, "Flush block cache");
    allocator.flush();
    region_manager.flush();
}

void BlockCache::reset()
{
    LOG_INFO(log, "Reset block cache");
    index.reset();
    allocator.reset();

    CurrentMetrics::set(CurrentMetrics::BlockCacheUsedSizeBytes, 0);
    CurrentMetrics::set(CurrentMetrics::BlockCacheHoleCount, 0);
    CurrentMetrics::set(CurrentMetrics::BlockCacheHoleBytesTotal, 0);
}

void BlockCache::persist(google::protobuf::io::ZeroCopyOutputStream * stream)
{
    LOG_INFO(log, "Starting block cache persist");
    Protos::BlockCacheConfig config = serialized_config;
    config.set_alloc_align_size(alloc_align_size);
    config.set_hole_count(CurrentMetrics::values[CurrentMetrics::BlockCacheHoleCount].load(std::memory_order_relaxed));
    config.set_hole_size_total(CurrentMetrics::values[CurrentMetrics::BlockCacheHoleBytesTotal].load(std::memory_order_relaxed));
    config.set_used_size_bytes(CurrentMetrics::values[CurrentMetrics::BlockCacheUsedSizeBytes].load(std::memory_order_relaxed));
    config.set_reinsertion_policy_enabled(reinsertion_policy != nullptr);
    google::protobuf::io::CodedOutputStream ostream(stream);
    google::protobuf::util::SerializeDelimitedToCodedStream(config, &ostream);
    region_manager.persist(&ostream);
    index.persist(&ostream);

    LOG_INFO(log, "Finished block cache persist");
}

bool BlockCache::recover(google::protobuf::io::ZeroCopyInputStream * stream)
{
    LOG_INFO(log, "Starting block cache recovery");
    reset();
    try
    {
        Protos::BlockCacheConfig config;
        google::protobuf::io::CodedInputStream istream(stream);
        google::protobuf::util::ParseDelimitedFromCodedStream(&config, &istream, nullptr);

        if (config.cache_base_offset() != serialized_config.cache_base_offset() || config.cache_size() != serialized_config.cache_size()
            || config.checksum() != serialized_config.checksum() || config.version() != serialized_config.version()
            || config.alloc_align_size() != alloc_align_size)
        {
            LOG_ERROR(log, "Recovery config: {}", config.DebugString());
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Recovery config does not match cache config");
        }

        CurrentMetrics::set(CurrentMetrics::BlockCacheHoleCount, config.hole_count());
        CurrentMetrics::set(CurrentMetrics::BlockCacheHoleBytesTotal, config.hole_size_total());
        CurrentMetrics::set(CurrentMetrics::BlockCacheUsedSizeBytes, config.used_size_bytes());
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

void BlockCache::drain()
{
    region_manager.drain();
}

Protos::BlockCacheConfig BlockCache::serializeConfig(const Config & config)
{
    Protos::BlockCacheConfig serialized_config;
    serialized_config.set_cache_base_offset(config.cache_base_offset);
    serialized_config.set_cache_size(config.cache_size);
    serialized_config.set_checksum(config.checksum);
    serialized_config.set_version(kFormatVersion);
    return serialized_config;
}
}
