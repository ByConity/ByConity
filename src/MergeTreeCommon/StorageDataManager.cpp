#include <MergeTreeCommon/StorageDataManager.h>
#include <CloudServices/CnchPartsHelper.h>


namespace ProfileEvents
{
    extern const Event LoadedServerParts;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageDataManager::StorageDataManager(const ContextPtr context_,const UUID & uuid_, const WGWorkerInfoPtr & worker_info_ )
    : WithContext(context_),
      storage_uuid(uuid_),
      worker_info(worker_info_)
{
    const String & worker_id = worker_info->worker_id;
    // suppose worker id always has the format `{worker_id_prefix}-{index}`
    String worker_id_prefix = worker_id.substr(0, worker_id.find_last_of('-') + 1);
    mock_wg = WorkerGroupHandleImpl::mockWorkerGroupHandle(worker_id_prefix, worker_info->num_workers, getContext());
}

void StorageDataManager::loadDataPartsWithDBM(
    const MergeTreeMetaBase & storage,
    const UInt64 & version,
    std::unordered_map<String, ServerDataPartsWithDBM> & res_server_parts,
    std::vector<std::shared_ptr<MergeTreePartition>> & res_partitions)
{
    auto table_versions_ptr = getRequiredTableVersions(version);

    LOG_TRACE(log, "Get required table versions from {} to {}",
        table_versions_ptr.back()->getVersion(),
        table_versions_ptr.front()->getVersion());

    size_t loaded_parts_count = 0;
    for (auto it=table_versions_ptr.begin(); it<table_versions_ptr.end(); it++)
    {
        ServerDataPartsWithDBM parts_with_dbm = (*it)->getAllPartsWithDBM(storage);

        for (auto & server_part : parts_with_dbm.first)
        {
            const String & partition_id = server_part->info().partition_id;
            auto inner_it = res_server_parts.find(partition_id);
            if (inner_it == res_server_parts.end())
            {
                // add to result partition list
                res_partitions.emplace_back(server_part->part_model_wrapper->partition);
            }
            res_server_parts[partition_id].first.emplace_back(std::move(server_part));
            loaded_parts_count++;
        }

        for (auto & delete_bitmap : parts_with_dbm.second)
        {
            const String & partition_id = delete_bitmap->getModel()->partition_id();
            if (res_server_parts.find(partition_id) == res_server_parts.end())
                throw Exception("Load delete bitmap mismatch server data part. Its a logic error. ", ErrorCodes::LOGICAL_ERROR);

            res_server_parts[partition_id].second.emplace_back(std::move(delete_bitmap));
        }
    }

    ProfileEvents::increment(ProfileEvents::LoadedServerParts, loaded_parts_count);
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
        LOG_TRACE(log, "Latest version {} less than required version {}. Will reload table versions.", 
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
        new_table_version->setWorkerInfo(worker_info, mock_wg);

        versions.emplace((*it)->version(), new_table_version);

        if (can_load_from_checkpoint && (*it)->checkpoint())
            break;
    }
}

/***
 * NOTE: Drop table version only happens when cached versions are invalid. Becareful that 
 * the action may cause currently running query return empty result.
 */
void StorageDataManager::dropTableVersion(ThreadPool & pool, UInt64 version)
{
    std::unique_lock<std::shared_mutex> write_lock(mutex);

    if (version)
    {
        auto it = versions.find(version);
        if (it != versions.end())
        {
            it->second->dropDiskCache(pool);
            versions.erase(it);
        }
    }
    else
    {
        for (const auto & [_, table_version] : versions)
            table_version->dropDiskCache(pool);

        versions.clear();
    }
}

}
