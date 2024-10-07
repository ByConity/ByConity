#pragma once

#include <Core/UUID.h>
#include <Common/LRUCache.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>

namespace DB
{

using DataModelPartPtr = std::shared_ptr<Protos::DataModelPart>;
using DataModelPartPtrVector = std::vector<DataModelPartPtr>;

// hold cached manifests for storages
class ManifestEntry
{
public:
    using ManifestData = std::pair<DataModelPartPtrVector, DataModelDeleteBitmapPtrVector>;

    ManifestEntry() = default;
    
    // add new manifest parts into entry
    void addManifest(UInt64 manifest_id, DataModelPartPtrVector && parts, DataModelDeleteBitmapPtrVector && bitmaps);

    // get manifest parts within multiple manifests. return manifest ids that not found
    std::vector<UInt64> getManifestData(const std::vector<UInt64> & manifests, DataModelPartPtrVector & parts, DataModelDeleteBitmapPtrVector & bitmaps);

    inline size_t size() const {return total_manifest_data_size;}

private:
    std::shared_mutex mutex;
    std::atomic_size_t total_manifest_data_size = 0;
    std::unordered_map<UInt64, ManifestData> manifest_map;
};

using ManifestEntryPtr = std::shared_ptr<ManifestEntry>;


struct ManifestEntryWeightFunction
{
    size_t operator()(const ManifestEntry & entry) const { return entry.size(); }
};

class ManifestCache : public LRUCache<UUID, ManifestEntry, UInt128Hash, ManifestEntryWeightFunction>
{

public:
    using Base = LRUCache<UUID, ManifestEntry, UInt128Hash, ManifestEntryWeightFunction>;
    using Delay = std::chrono::seconds;

    explicit ManifestCache(size_t max_parts_to_cache, const Delay & expiration_delay = Delay::zero())
        : Base(max_parts_to_cache, expiration_delay)
    {
    }

    void addManifest(
        const UUID & storage_id,
        UInt64 manifest_id,
        DataModelPartPtrVector && parts,
        DataModelDeleteBitmapPtrVector && bitmaps);

    std::vector<UInt64> getManifestData(
        const UUID & storage_id,
        const std::vector<UInt64> & manifest_id,
        DataModelPartPtrVector & parts,
        DataModelDeleteBitmapPtrVector & bitmaps);
};

}
