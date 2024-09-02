#pragma once

#include <optional>
#include <future>

#include "Common/HashTable/Hash.h"
#include "Common/LRUCache.h"
#include "Common/ProfileEvents.h"
#include "Common/SipHash.h"
#include "Columns/IColumn.h"

namespace ProfileEvents
{
    extern const Event ColumnCacheHits;
    extern const Event ColumnCacheMisses;
    extern const Event ColumnCacheWeightLost;
}

namespace parquet { class FileMetaData; }

namespace DB
{
struct ArrowColumnCacheCell
{
    ArrowColumnCacheCell(size_t sz, std::shared_future<ColumnPtr> fut) : compressed_size(sz), future(fut) { }
    size_t compressed_size = 0;
    std::shared_future<ColumnPtr> future;
};

struct ArrowColumnWeightFunction
{
    size_t operator() (const ArrowColumnCacheCell & x) const
    {
        return x.compressed_size;
    }
};

/// Cache the result columns returned from ArrowColumnToChColumn
/// Support Parquet and ORC file
class ArrowColumnCache : public LRUCache<UInt128, ArrowColumnCacheCell, UInt128TrivialHash, ArrowColumnWeightFunction>
{
private:
    using Base = LRUCache<UInt128, ArrowColumnCacheCell, UInt128TrivialHash, ArrowColumnWeightFunction>;

    static std::optional<ArrowColumnCache> cache;

public:

    explicit ArrowColumnCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// I know using static instance is bad, but passing down parameters is a pain.
    static void initialize(size_t max_size_in_bytes)
    {
        cache.emplace(max_size_in_bytes);
    }

    static std::optional<ArrowColumnCache> & instance()
    {
        /// ensure we always initialize then should be fine.
        /// This is also bad
        return cache;
    }

    using Key = UInt128;
    using Mapped = ArrowColumnCacheCell;
    using MappedPtr = std::shared_ptr<ArrowColumnCacheCell>;

    static UInt128 calculateKeyHash(const String & path, String column_name, size_t stripe_number)
    {
        UInt128 key;
        SipHash hash;
        hash.update(path.data(), path.size() + 1);
        hash.update(column_name.data(), column_name.size() + 1);
        hash.update(stripe_number);
        hash.get128(key);
        return key;
    }

    template<typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load));
        if (result.second)
            ProfileEvents::increment(ProfileEvents::ColumnCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::ColumnCacheMisses);

        return result;
    }

    // std::mutex mutex;

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::ColumnCacheWeightLost, weight_loss);
    }
};

struct ArrowFooterCacheCell
{
    /// using Content = std::variant<String, std::shared_ptr<FileMetadata>>;
    /// Maybe we should use a uncompressed footer
    /// Current orc lib does not allow use to pass a uncompressed footer object
    String serialized_footer;
    std::shared_ptr<parquet::FileMetaData> parquet_file_metadata;
    size_t weight;
};

struct ArrowFooterWeightFunction
{
    size_t operator() (const ArrowFooterCacheCell & x) const
    {
        return x.weight;
    }
};

/// Cache the file footer for Parquet & ORC file
class ArrowFooterCache : public LRUCache<UInt128, ArrowFooterCacheCell, UInt128TrivialHash, ArrowFooterWeightFunction>
{
private:
    using Base = LRUCache<UInt128, ArrowFooterCacheCell, UInt128TrivialHash, ArrowFooterWeightFunction>;

    static std::optional<ArrowFooterCache> cache;

public:

    explicit ArrowFooterCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// I know using static instance is bad, but passing down parameters is a pain.
    static void initialize(size_t max_size_in_bytes)
    {
        cache.emplace(max_size_in_bytes);
    }

    static std::optional<ArrowFooterCache> & instance()
    {
        /// ensure we always initialize then should be fine.
        /// This is also bad
        return cache;
    }

    using Key = UInt128;
    using Mapped = ArrowFooterCacheCell;
    using MappedPtr = std::shared_ptr<ArrowFooterCacheCell>;

    static UInt128 calculateKeyHash(const String & path)
    {
        UInt128 key;
        SipHash hash;
        hash.update(path.data(), path.size() + 1);
        hash.get128(key);
        return key;
    }

    template<typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load));
        return result.first;
    }

private:
    void onRemoveOverflowWeightLoss([[maybe_unused]] size_t weight_loss) override
    {
    }
};

}
