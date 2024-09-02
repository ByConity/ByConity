#pragma once

#include <MergeTreeCommon/TableVersion.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>

namespace DB
{


class StorageDataManager : public WithContext
{

public:
    StorageDataManager(const ContextPtr context, const UUID & uuid_, const WGWorkerInfoPtr & worker_info_);

    void loadDataPartsWithDBM(const MergeTreeMetaBase & storage, const UInt64 & version, ServerDataPartsWithDBM & server_parts);

    WGWorkerInfoPtr getWorkerInfo() const { return worker_info; }

    const UUID & getStorageUUID() const { return storage_uuid; }

private:

    UInt64 getLatestVersion();

    void reloadTableVersions();

    std::vector<TableVersionPtr> getRequiredTableVersions(const UInt64 required_version);

    const UUID storage_uuid;

    ///TODO: make worker info shared in the worker process
    WGWorkerInfoPtr worker_info;
    std::shared_mutex mutex;
    std::map<UInt64, TableVersionPtr> versions;
};

using StorageDataManagerPtr = std::shared_ptr<StorageDataManager>;

}
