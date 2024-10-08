#pragma once

#include <Common/Logger.h>
#include <cstddef>
#include <limits>
#include <memory>

#include <google/protobuf/io/zero_copy_stream.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Allocator.h>
#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Index.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Common/BitHelpers.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB::HybridCache
{
class BlockCache final : public CacheEngine
{
public:
    struct Config
    {
        Device * device{};
        ExpiredCheck check_expired;
        DestructorCallback destructor_callback;

        bool checksum{};

        UInt64 cache_base_offset{};
        UInt64 cache_size{};

        std::unique_ptr<EvictionPolicy> eviction_policy;
        BlockCacheReinsertionConfig reinsertion_config{};

        // Region size
        UInt64 region_size{16 * 1024 * 1024};
        UInt32 read_buffer_size{};

        // no longer needed, can be removed
        JobScheduler * scheduler{};

        UInt32 clean_regions_pool{1};
        UInt32 clean_region_threads{1};

        // Number of in-memory buffers where writes ae buffered before flushed.
        UInt32 num_in_mem_buffers{1};
        bool item_destructor_enabled{false};
        // Maximum number of retry times for in-mem buffer flushing.
        UInt16 in_mem_buf_flush_retry_limit{10};

        // Number of priorities, items of the same priority will be put into the same region.
        UInt16 num_priorities{1};

        // Whether to remove an item by checking the full key.
        bool precise_remove{false};

        // Calculate the total region number.
        UInt32 getNumberRegions() const
        {
            chassert(0ul == cache_size % region_size);
            return cache_size / region_size;
        }

        // Check invariants.
        Config & validate();
    };

    explicit BlockCache(Config && config);
    BlockCache(const BlockCache &) = delete;
    BlockCache & operator=(const BlockCache &) = delete;
    ~BlockCache() override = default;

    UInt64 getSize() const override { return region_manager.getSize(); }

    bool couldExist(HashedKey key) override;

    UInt64 estimateWriteSize(HashedKey key, BufferView value) const override;

    Status insert(HashedKey key, BufferView value) override;

    Status lookup(HashedKey key, Buffer & value) override;

    Status remove(HashedKey key) override;

    void flush() override;

    void reset() override;

    void persist(google::protobuf::io::ZeroCopyOutputStream * stream) override;

    bool recover(google::protobuf::io::ZeroCopyInputStream * stream) override;

    UInt64 getMaxItemSize() const override { return region_size - sizeof(EntryDesc); }

    UInt32 getAllocAlignSize() const
    {
        chassert(isPowerOf2(alloc_align_size));
        return alloc_align_size;
    }

    std::pair<Status, std::string> getRandomAlloc(Buffer & value) override;

    // Finish all pending jobs in RegionManager
    void drain();

    // the minimum alloc alignment size.
    static constexpr UInt32 kMinAllocAlignSize = 512;

    static constexpr UInt32 kMaxItemSize = kMinAllocAlignSize * static_cast<UInt32>(std::numeric_limits<UInt16>::max());

private:
    LoggerPtr log = getLogger("BlockCache");

    static constexpr UInt32 kFormatVersion = 12;
    // Should be at least the next_two_pow(sizeof(EntryDesc)).
    static constexpr UInt32 kDefReadBufferSize = 4096;
    // Default priority for an item.
    static constexpr UInt16 kDefaultItemPriority = 0;

    struct EntryDesc
    {
        UInt32 key_size{};
        UInt32 value_size{};
        UInt64 key_hash{};
        UInt32 cs_self{};
        UInt32 cs{};

        EntryDesc() = default;
        EntryDesc(UInt32 ks, UInt32 vs, UInt64 kh) : key_size{ks}, value_size{vs}, key_hash{kh} { cs_self = computeChecksum(); }

        UInt32 computeChecksum() const { return checksum(BufferView{offsetof(EntryDesc, cs_self), reinterpret_cast<const UInt8 *>(this)}); }
    };

    static_assert(sizeof(EntryDesc) == 24, "packed struct required");

    struct ValidConfigTag
    {
    };
    BlockCache(Config && config, ValidConfigTag);

    UInt32 serializedSize(UInt32 key_size, UInt32 value_size) const;

    Status writeEntry(RelAddress addr, UInt32 slot_size, HashedKey key, BufferView value);

    Status readEntry(const RegionDescriptor & desc, RelAddress addr, UInt32 approx_size, HashedKey expected, Buffer & value);

    UInt32 onRegionReclaim(RegionId rid, BufferView buffer);

    void onRegionCleanup(RegionId rid, BufferView buffer);

    static Protos::BlockCacheConfig serializeConfig(const Config & config);

    UInt32 calcAllocAlignSize() const;

    static UInt16 encodeSizeHint(UInt32 size_hint)
    {
        auto aligned_size = roundup(size_hint, kMinAllocAlignSize);
        return static_cast<UInt16>(aligned_size / kMinAllocAlignSize);
    }

    static UInt32 decodeSizehint(UInt16 size_hint) { return size_hint * kMinAllocAlignSize; }

    UInt32 encodeRelAddress(RelAddress addr) const
    {
        chassert(addr.offset() != 0u);
        return region_manager.toAbsolute(addr).offset() / alloc_align_size;
    }

    AbsAddress decodeAbsAddress(UInt32 code) const { return AbsAddress{static_cast<UInt64>(code) * alloc_align_size}; }

    RelAddress decodeRelAddress(UInt32 code) const { return region_manager.toRelative(decodeAbsAddress(code).sub(1)).add(1); }

    enum class ReinsertionRes
    {
        kReinserted,
        kRemoved,
        kEvicted,
    };
    ReinsertionRes reinsertOrRemoveItem(HashedKey key, BufferView value, UInt32 entry_size, RelAddress addr);

    bool removeItem(HashedKey key, RelAddress addr);

    void validate(Config & config) const;

    std::shared_ptr<BlockCacheReinsertionPolicy> makeReinsertionPolicy(const BlockCacheReinsertionConfig & reinsertion_config);

    const Protos::BlockCacheConfig serialized_config;

    const UInt16 num_priorities;
    const ExpiredCheck check_expired;
    const DestructorCallback destructor_callback;
    const bool checksum_data{};
    const Device & device;
    const UInt32 alloc_align_size{};
    const UInt32 read_buffer_size{};
    const UInt64 region_size{};
    const bool item_destructor_enabled{false};
    const bool precise_remove{false};

    // Index stores offset of the slot *end*. This enables efficient paradigm
    // "buffer pointer is value pointer", which means value has to be at offset 0
    // of the slot and header (footer) at the end.
    //
    // -------------------------------------------
    // |     Value                    |  Footer  |
    // -------------------------------------------
    // ^                                         ^
    // |                                         |
    // Buffer*                          Index points here
    Index index;
    RegionManager region_manager;
    Allocator allocator;

    std::shared_ptr<BlockCacheReinsertionPolicy> reinsertion_policy;
};
}
