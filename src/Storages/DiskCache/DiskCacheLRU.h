#pragma once

#include <Core/Types.h>
#include <Storages/DiskCache/BucketLRUCache.h>
#include <Storages/DiskCache/IDiskCache.h>

namespace DB
{
// #define DISK_CACHE_LRU_META_HEADER_SIZE 256
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

enum class DiskCacheState
{
    Caching,
    Cached,
    Deleting,
};

struct DiskCacheMeta
{
    DiskCacheMeta(DiskCacheState state_, size_t size_) : state(state_), size(size_) { }

    DiskCacheState state;
    size_t size;
    mutable std::mutex mutex;

    void setDisk(const DiskPtr & disk) { disk_wp = disk; }
    DiskPtr getDisk() const { return disk_wp.lock(); }

private:
    std::weak_ptr<IDisk> disk_wp;
};

struct DiskCacheWeightFunction
{
    size_t operator()(const DiskCacheMeta & meta) const
    {
        if (meta.state == DiskCacheState::Cached)
            return meta.size;
        return 0;
    }
};

class DiskCacheLRU : public IDiskCache, private BucketLRUCache<String, DiskCacheMeta, std::hash<String>, DiskCacheWeightFunction>
{
private:
    using Base = BucketLRUCache<String, DiskCacheMeta, std::hash<String>, DiskCacheWeightFunction>;

public:
    DiskCacheLRU(Context & context_, VolumePtr storage_volume_, const DiskCacheSettings & settings_);

    virtual void set(const String & key, ReadBuffer & value, size_t weight_hint) override;
    virtual std::pair<DiskPtr, String> get(const String & key) override;
    virtual void load() override;
    size_t getKeyCount() const override { return count(); }
    size_t getCachedSize() const override { return weight(); }

private:
    size_t loadSegmentsFromVolume(const IVolume & volume);
    size_t loadSegmentsFromDisk(const DiskPtr & disk, const String & current_path, String & partial_cache_name);
    bool loadSegmentFromFile(const DiskPtr & disk, const String & segment_rel_path, const String & segment_name);
    bool loadSegment(const DiskPtr & disk, const String & segment_rel_path, const String & segment_name, bool need_check_existence);

    // return cached disk
    DiskPtr writeSegment(const String & key, ReadBuffer & value, size_t weight_hint);
    void removeExternal(const Key & key, const std::shared_ptr<DiskCacheMeta> & value, size_t) override;
    bool isExist(const String & key);

    mutable std::shared_mutex lru_mutex;
    mutable std::shared_mutex evict_mutex;
    String base_path;

    Poco::Logger * logger;
};

}
