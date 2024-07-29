#include <MergeTreeCommon/StorageDataManager.h>
#include <CloudServices/CnchPartsHelper.h>


namespace DB
{

StorageDataManager::StorageDataManager(const ContextPtr context_,const UUID & uuid_, const WGWorkerInfoPtr & worker_info_ )
    : WithContext(context_),
      storage_uuid(uuid_),
      worker_info(worker_info_)
{
}

void StorageDataManager::loadDataPartsWithDBM(const MergeTreeMetaBase & storage, const UInt64 & version, ServerDataPartsWithDBM & server_parts)
{
    auto table_versions_ptr = getRequiredTableVersions(version);

    LOG_TRACE(&Poco::Logger::get("StorageDataManager"), "Get required table versions from {} to {}",
        table_versions_ptr.back()->getVersion(),
        table_versions_ptr.front()->getVersion());

    for (auto it=table_versions_ptr.begin(); it<table_versions_ptr.end(); it++)
    {
        ServerDataPartsWithDBM parts_with_dbm = (*it)->getAllPartsWithDBM(storage);
        server_parts.first.insert(server_parts.first.end(), parts_with_dbm.first.begin(), parts_with_dbm.first.end());
        server_parts.second.insert(server_parts.second.end(), parts_with_dbm.second.begin(), parts_with_dbm.second.end());
    }
}

UInt64 StorageDataManager::getLatestVersion()
{
    std::shared_lock<std::shared_mutex> read_lock(mutex);
    if (versions.empty())
        return 0;
    return versions.rbegin()->first;
}

std::vector<TableVersionPtr> StorageDataManager::getRequiredTableVersions(const UInt64 required_version)
{
    UInt64 latest_version = getLatestVersion();
    if (latest_version < required_version)
    {
        LOG_TRACE(&Poco::Logger::get("StorageDataManager"), "Latest version {} less than required version {}. Will reload table versions.", 
            latest_version, required_version);
        reloadTableVersions();
    }

    std::vector<TableVersionPtr> res;
    std::shared_lock<std::shared_mutex> read_lock(mutex);

    auto upper_it = versions.upper_bound(required_version);

    for (auto it = std::make_reverse_iterator(upper_it); it!=versions.rend(); it++)
    {
        res.push_back(it->second);
        if (it->second->isCheckpoint())
            break;
    }

    return res;
}

void StorageDataManager::reloadTableVersions()
{
    auto catalog = getContext()->getCnchCatalog();
    auto versions_model = catalog->getAllTableVersions(storage_uuid);

    std::unique_lock<std::shared_mutex> write_lock(mutex);
    bool can_load_from_checkpoint = versions.empty();
    for (auto it = versions_model.rbegin(); it != versions_model.rend(); it++)
    {
        if (versions.count((*it)->version()))
            break;

        auto new_table_version = std::make_shared<TableVersion>(getContext(), storage_uuid, *(*it));
        new_table_version->setWorkerInfo(worker_info);

        versions.emplace((*it)->version(), new_table_version);

        if (can_load_from_checkpoint && (*it)->checkpoint())
            break;
    }
}

}
