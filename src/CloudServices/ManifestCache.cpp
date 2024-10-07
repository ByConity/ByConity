#include <CloudServices/ManifestCache.h>

namespace DB
{


void ManifestEntry::addManifest(UInt64 manifest_id, DataModelPartPtrVector && parts, DataModelDeleteBitmapPtrVector && bitmaps)
{
    std::unique_lock lock(mutex);
    if (!manifest_map.contains(manifest_id))
    {
        manifest_map.emplace(manifest_id, std::make_pair(std::move(parts), std::move(bitmaps)));
        total_manifest_data_size += parts.size() + bitmaps.size();
    }
}

std::vector<UInt64> ManifestEntry::getManifestData(
    const std::vector<UInt64> & manifests,
    DataModelPartPtrVector & parts,
    DataModelDeleteBitmapPtrVector & bitmaps)
{
    std::shared_lock lock(mutex);

    std::vector<UInt64> not_found_manifests;
    for (const auto & manifest : manifests)
    {
        auto iter = manifest_map.find(manifest);
        if (iter != manifest_map.end())
        {
            auto & [manifest_parts, manifest_bitmaps] = iter->second;
            parts.insert(parts.end(), manifest_parts.begin(), manifest_parts.end());
            bitmaps.insert(bitmaps.end(), manifest_bitmaps.begin(), manifest_bitmaps.end());
        }
        else
            not_found_manifests.push_back(manifest);
    }

    return not_found_manifests;
}

void ManifestCache::addManifest(
    const UUID & storage_id,
    UInt64 manifest_id,
    DataModelPartPtrVector && parts,
    DataModelDeleteBitmapPtrVector && bitmaps)
{
    ManifestEntryPtr entry = Base::get(storage_id);
    if (!entry)
    {
        entry = std::make_shared<ManifestEntry>();
        set(storage_id, entry);
    }

    entry->addManifest(manifest_id, std::move(parts), std::move(bitmaps));
}

std::vector<UInt64> ManifestCache::getManifestData(
    const UUID & storage_id,
    const std::vector<UInt64> & manifest_id,
    DataModelPartPtrVector & parts,
    DataModelDeleteBitmapPtrVector & bitmaps)
{
    ManifestEntryPtr entry = Base::get(storage_id);
    if (!entry)
        return manifest_id;

    return entry->getManifestData(manifest_id, parts, bitmaps);
}

}
