#pragma once

#include <Common/Logger.h>
#include <MergeTreeCommon/TableVersion.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>

namespace DB
{


class StorageDataManager : public std::enable_shared_from_this<StorageDataManager>, public WithContext
{

public:
    StorageDataManager(const ContextPtr context, const UUID & uuid_, const WGWorkerInfoPtr & worker_info_);

    void loadDataPartsWithDBM(
        const MergeTreeMetaBase & storage,
        const UInt64 & version,
        std::unordered_map<String, ServerDataPartsWithDBM> & server_parts,
        std::vector<std::shared_ptr<MergeTreePartition>> & partitions);

    WGWorkerInfoPtr getWorkerInfo() const { return worker_info; }

    WorkerGroupHandle getWorkerGroup() const { return mock_wg; }

    const UUID & getStorageUUID() const { return storage_uuid; }

    // Drop managed table versions and corresponding disk cache. (version == 0) means drop all versions
    void dropTableVersion(ThreadPool & pool, UInt64 version = 0);

private:

    UInt64 getLatestVersion();

    void reloadTableVersions();

    std::vector<TableVersionPtr> getRequiredTableVersions(const UInt64 required_version);

    const UUID storage_uuid;

    ///TODO: make worker info shared in the worker process
    WGWorkerInfoPtr worker_info;
    // Mock a WorkerGroupHandle based on worker_info for parts allocation.
    WorkerGroupHandle mock_wg;

    std::shared_mutex mutex;
    std::map<UInt64, TableVersionPtr> versions;

    LoggerPtr log = getLogger("StorageDataManager");
};

using StorageDataManagerPtr = std::shared_ptr<StorageDataManager>;

}
