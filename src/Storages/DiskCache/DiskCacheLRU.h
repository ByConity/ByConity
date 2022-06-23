#pragma once

#include <Core/Types.h>
#include <Storages/DiskCache/BucketLRUCache.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Poco/File.h>

class DiskCacheTest_LoadV1Meta_Test;
class DiskCacheTest_LoadV2Meta_Test;
class DiskCacheTest_LoadMetaWithNonExistSegment_Test;
class DiskCacheTest_Collect_Test;
class DiskCacheTest_LoadV1MetaAndCollect_Test;
class DiskCacheTest_LoadV2MetaAndCollect_Test;

namespace DB
{
// #define DISK_CACHE_LRU_META_HEADER_SIZE 256
class IDisk;

enum class DiskCacheState
{
    Caching,
    Cached,
    Deleting,
};

struct DiskCacheMeta
{
    DiskCacheMeta(DiskCacheState state_, size_t size_, const String & disk_name_) : state(state_), size(size_), disk_name(disk_name_) { }
    DiskCacheState state;
    size_t size;
    String disk_name;
    mutable std::mutex mutex;
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
    virtual std::optional<String> get(const String & key) override;
    virtual void load() override;
    size_t getKeyCount() const override { return count(); }
    size_t getCachedSize() const override { return weight(); }

private:
    friend class ::DiskCacheTest_LoadV1Meta_Test;
    friend class ::DiskCacheTest_LoadV2Meta_Test;
    friend class ::DiskCacheTest_LoadMetaWithNonExistSegment_Test;
    friend class ::DiskCacheTest_Collect_Test;
    friend class ::DiskCacheTest_LoadV1MetaAndCollect_Test;
    friend class ::DiskCacheTest_LoadV2MetaAndCollect_Test;

    size_t loadSegmentsFromVolume(const IVolume & volume);
    size_t loadSegmentsFromDisk(IDisk & disk, const String & current_path, String & partial_cache_name);
    bool loadSegmentFromFile(IDisk & disk, const String & segment_rel_path, const String & segment_name);
    bool loadSegment(IDisk & disk, const String & segment_rel_path, const String & segment_name, bool need_check_existence);

    // return disk name and file size
    std::pair<String, size_t> writeSegment(const String & key, ReadBuffer & value, size_t weight_hint);
    void removeExternal(const Key & key, const std::shared_ptr<DiskCacheMeta> & value, size_t) override;
    bool isExist(const String & key);

    mutable std::shared_mutex lru_mutex;
    mutable std::shared_mutex evict_mutex;
    String base_path;

    Poco::Logger * logger;
};

}
