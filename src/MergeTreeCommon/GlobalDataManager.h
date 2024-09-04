#pragma once
#include <MergeTreeCommon/StorageDataManager.h>

namespace DB
{

class GlobalDataManager : public WithContext
{

public:
    GlobalDataManager(const ContextPtr context);

    void loadDataPartsWithDBM(
        const MergeTreeMetaBase & storage,
        const UUID & storage_uuid,
        const UInt64 table_version,
        const WGWorkerInfoPtr & runtime_worker_info,
        std::unordered_map<String, ServerDataPartsWithDBM> & server_parts,
        std::vector<std::shared_ptr<MergeTreePartition>> & partitions);

    StorageDataManagerPtr getStorageDataManager(const UUID & storage_uuid);

private:

    StorageDataManagerPtr getStorageDataManager(const UUID & storage_uuid, const WGWorkerInfoPtr & runtime_worker_info);

    std::mutex data_mutex;
    std::map<UUID, StorageDataManagerPtr> storages_data;
};

using GlobalDataManagerPtr = std::shared_ptr<GlobalDataManager>;

}
