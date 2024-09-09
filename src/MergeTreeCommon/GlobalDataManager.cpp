#include <MergeTreeCommon/GlobalDataManager.h>

namespace DB
{

GlobalDataManager::GlobalDataManager(const ContextPtr context_)
    : WithContext(context_)
{
}

void GlobalDataManager::loadDataPartsWithDBM(
    const MergeTreeMetaBase & storage,
    const UUID & storage_uuid,
    const UInt64 table_version,
    const WGWorkerInfoPtr & runtime_worker_info,
    std::unordered_map<String, ServerDataPartsWithDBM> & server_parts,
    std::vector<std::shared_ptr<MergeTreePartition>> & partitions)
{
    auto storage_manager = getStorageDataManager(storage_uuid, runtime_worker_info);
    return storage_manager->loadDataPartsWithDBM(storage, table_version, server_parts, partitions);
}

StorageDataManagerPtr GlobalDataManager::getStorageDataManager(const UUID & storage_uuid)
{
    std::lock_guard<std::mutex> lock(data_mutex);

    auto it = storages_data.find(storage_uuid);

    if (it == storages_data.end())
        return nullptr;
    else
        return it->second;
}

StorageDataManagerPtr GlobalDataManager::getStorageDataManager(const UUID & storage_uuid, const WGWorkerInfoPtr & runtime_worker_info)
{
    std::lock_guard<std::mutex> lock(data_mutex);

    auto it = storages_data.find(storage_uuid);

    if (it == storages_data.end())
        storages_data.emplace(storage_uuid, std::make_shared<StorageDataManager>(getContext(), storage_uuid, runtime_worker_info));
    else
    {
        // reset storage data manager if worker topology change
        if (*(it->second->getWorkerInfo()) != *(runtime_worker_info))
            storages_data[storage_uuid] = std::make_shared<StorageDataManager>(getContext(), storage_uuid, runtime_worker_info);
    }

    return storages_data[storage_uuid];
}

}
