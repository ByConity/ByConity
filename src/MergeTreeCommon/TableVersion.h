#pragma once

#include <Protos/data_models.pb.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Common/filesystemHelpers.h>
#include <Common/HostWithPorts.h>
#include <shared_mutex>


namespace DB
{

class ManifestDiskCacheSegment;
using ManifestDiskCacheSegmentPtr = std::shared_ptr<ManifestDiskCacheSegment>;

class TableVersion : public std::enable_shared_from_this<TableVersion>, public WithContext
{
public:
    TableVersion(const ContextPtr context_, const UUID & uuid_, const Protos::ManifestListModel & version_model, bool enable_disk_cache = true);

    void setWorkerInfo(const WGWorkerInfoPtr & worker_info);

    ServerDataPartsWithDBM getAllPartsWithDBM(const MergeTreeMetaBase & storage);

    void loadManifestData(const MergeTreeMetaBase & storage);

    UInt64 getVersion() const { return version; }

    bool isCheckpoint() const { return checkpoint_version; }

    void dropDiskCache(ThreadPool & pool);

    friend class ManifestDiskCacheSegment;

private:

    void initialize(const Protos::ManifestListModel & version_model);

    template<typename DataType>
    void fileterDataByWorkerInfo(const MergeTreeMetaBase & storage, std::vector<std::shared_ptr<DataType>> & data_vector);

    ServerDataPartsVector getDataPartsInternal();

    DeleteBitmapMetaPtrVector getDeleteBitmapsInternal();

    UUID storage_uuid;

    UInt64 version;
    bool checkpoint_version {false};
    bool enable_disk_cache {false};
    std::vector<UInt64> txn_list;

    std::atomic<bool> loaded_from_manifest {false};

    WGWorkerInfoPtr worker_info = nullptr;

    std::shared_mutex mutex;
    DataModelPartWrapperVector data_parts;
    DeleteBitmapMetaPtrVector delete_bitmaps;
};

using TableVersionPtr = std::shared_ptr<TableVersion>;

}
