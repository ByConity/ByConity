
#include <MergeTreeCommon/TableVersion.h>
#include <MergeTreeCommon/StorageDataManager.h>
#include <MergeTreeCommon/assignCnchParts.h>
#include <CloudServices/CheckpointHelper.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Catalog/Catalog.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace ProfileEvents
{
    extern const Event LoadManifestPartsCacheHits;
    extern const Event LoadManifestPartsCacheMisses;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ManifestDiskCacheSegment : public IDiskCacheSegment
{
public:
    explicit ManifestDiskCacheSegment(TableVersionPtr version_)
        : IDiskCacheSegment(0, 0, SegmentType::MANIFEST),
          version_ptr(version_)
    {
    }

    String getSegmentName() const override
    {
        String segment_name = UUIDHelpers::UUIDToString(version_ptr->storage_uuid) + "/Manifests/" +
            (version_ptr->worker_info ? version_ptr->worker_info->worker_id + "/" : "") +
            toString(version_ptr->version) + (version_ptr->checkpoint_version ? "/checkpoint" : "/manifest");

        return segment_name;
    }

    void cacheToDisk(IDiskCache & diskcache, bool ) override
    {
        Protos::DataModelManifestData data;

        ServerDataPartsVector server_parts = version_ptr->getDataPartsInternal();
        for (const auto server_part : server_parts)
        {
            auto * part_model = data.mutable_parts()->Add();
            part_model->CopyFrom(*(server_part->part_model_wrapper->part_model));
        }

        DeleteBitmapMetaPtrVector delete_bitmaps = version_ptr->getDeleteBitmapsInternal();
        for (const auto delete_bitmap : delete_bitmaps)
        {
            auto * dbm_model = data.mutable_delete_bitmaps()->Add();
            dbm_model->CopyFrom(*(delete_bitmap->getModel()));
        }

        String serialized = data.SerializeAsString();
        ReadBufferFromString read_buffer(serialized);
        size_t file_size = read_buffer.count();
        diskcache.set(getSegmentName(), read_buffer, file_size, 0);
    }

private:
    TableVersionPtr version_ptr;
};


TableVersion::TableVersion(const ContextPtr context_, const UUID & uuid_, const Protos::ManifestListModel & version_model, bool enable_disk_cache_)
    : WithContext(context_),
      storage_uuid(uuid_)
{
    initialize(version_model);
    if (enable_disk_cache_)
        enable_disk_cache = true;
}

void TableVersion::setWorkerInfo(const WGWorkerInfoPtr & worker_info_)
{
    worker_info = worker_info_;
}

ServerDataPartsWithDBM TableVersion::getAllPartsWithDBM(const MergeTreeMetaBase & storage)
{
    if (!loaded_from_manifest)
        loadManifestData(storage);

    return {getDataPartsInternal(), getDeleteBitmapsInternal()};
}

ServerDataPartsVector TableVersion::getDataPartsInternal()
{
    ServerDataPartsVector res;
    std::shared_lock<std::shared_mutex> lock(mutex);
    for (const auto & part_wrapper : data_parts)
        res.push_back(std::make_shared<ServerDataPart>(part_wrapper));
    return res;
}

DeleteBitmapMetaPtrVector TableVersion::getDeleteBitmapsInternal()
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return delete_bitmaps;
}

template<typename DataType>
void TableVersion::fileterDataByWorkerInfo(const MergeTreeMetaBase & storage, std::vector<std::shared_ptr<DataType>> & data_vector)
{
    if (!worker_info)
        return;

    std::vector<std::shared_ptr<DataType>> worker_hold_data;

    if (storage.isBucketTable())
    {
        for (const auto & data_ptr : data_vector)
        {
            // filter parts by bucket number
            Int64 bucket_number = data_ptr->bucketNumber();
            // Drop range and mark delete has uninitialized bucket number. Need to hold them to make visibility calculation correct.
            if (bucket_number == -1)
                worker_hold_data.emplace_back(data_ptr);
            else if (worker_info->index == (bucket_number % worker_info->num_workers))
                worker_hold_data.emplace_back(data_ptr);
        }
    }
    else
    {
        const String & worker_id = worker_info->worker_id;
        // suppose worker id always has the format `{worker_id_prefix}-{index}`
        String worker_id_prefix = worker_id.substr(0, worker_id.find_last_of('-') + 1);
        WorkerGroupHandle mock_wg = WorkerGroupHandleImpl::mockWorkerGroupHandle(worker_id_prefix, worker_info->num_workers, getContext());

        // Use consistent hash to make sure the parts with the same basic name are always allocated to the same worker
        auto allocate_res = assignCnchParts(mock_wg, data_vector, getContext(), storage.getSettings(), Context::PartAllocator::JUMP_CONSISTENT_HASH);

        // only get the allocated data which belongs to current worker
        worker_hold_data = std::move(allocate_res[worker_id]);
    }

    data_vector.swap(worker_hold_data);
}

void TableVersion::loadManifestData(const MergeTreeMetaBase & storage)
{
    if (loaded_from_manifest)
        return;

    DataModelPartWrapperVector loaded_parts;
    DeleteBitmapMetaPtrVector loaded_dbm;

    const auto load_from_serialized = [&](String & serialized_)
    {
        Protos::DataModelManifestData data_model;
        data_model.ParseFromString(serialized_);

        for (auto & part_model : data_model.parts())
            loaded_parts.push_back(createPartWrapperFromModel(storage, std::move(part_model)));

        for (auto & delete_bitmap_model : data_model.delete_bitmaps())
            loaded_dbm.push_back(createFromModel(storage, delete_bitmap_model));
    };

    auto manifest_seg = std::make_shared<ManifestDiskCacheSegment>(shared_from_this());
    IDiskCachePtr disk_cache;

    if (enable_disk_cache)
    {
        disk_cache = DiskCacheFactory::instance().get(DiskCacheType::Manifest)->getDataCache();
        auto [cache_disk, segment_path] = disk_cache->get(manifest_seg->getSegmentName());
        if (cache_disk && cache_disk->exists(segment_path))
        {
            ReadSettings settings;
            auto read_buffer = cache_disk->readFile(segment_path, settings);
            String serialized_data;
            readStringUntilEOF(serialized_data, *read_buffer);

            if (!serialized_data.empty())
            {
                load_from_serialized(serialized_data);
                std::unique_lock<std::shared_mutex> lock(mutex);
                if (!loaded_from_manifest)
                {
                    data_parts.swap(loaded_parts);
                    delete_bitmaps.swap(loaded_dbm);
                }
            }

            // Disk may be empty if no server parts assigned to this worker. Then, nothin will be loaded.
            LOG_TRACE(log, "Loaded {} data parts and {} delete bitmaps from manifest disk cache {}. Path : {}",
                data_parts.size(),
                delete_bitmaps.size(),
                manifest_seg->getSegmentName(),
                segment_path);

            loaded_from_manifest = true;
            ProfileEvents::increment(ProfileEvents::LoadManifestPartsCacheHits);
            return;
        }
    }

    if (checkpoint_version)
    {
        auto remote_disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
        String checkpoint_file_path = joinPaths({getCheckpointRelativePath(storage), toString(version)});
        if (!remote_disk->exists(checkpoint_file_path))
            throw Exception("Cannot find checkpoint " + toString(version) + " for table " + storage.getStorageID().getFullTableName(), ErrorCodes::LOGICAL_ERROR);

        auto read_buffer = remote_disk->readFile(checkpoint_file_path);
        do
        {
            String serialized_data;
            readStringBinary(serialized_data, *read_buffer);
            load_from_serialized(serialized_data);
        } while(!read_buffer->eof());
    }
    // load from manifest
    else
    {
        auto catalog = getContext()->getCnchCatalog();
        loaded_parts = catalog->getCommittedPartsFromManifest(storage, txn_list);

        if (storage.getInMemoryMetadataPtr()->hasUniqueKey())
            loaded_dbm = catalog->getDeleteBitmapsFromManifest(storage, txn_list);
    }

    ProfileEvents::increment(ProfileEvents::LoadManifestPartsCacheMisses);
    // filter parts by worker info.
    if (worker_info)
    {
        fileterDataByWorkerInfo(storage, loaded_parts);
        fileterDataByWorkerInfo(storage, loaded_dbm);
    }

    std::unique_lock<std::shared_mutex> lock(mutex);
    if (!loaded_from_manifest)
    {
        data_parts.swap(loaded_parts);
        delete_bitmaps.swap(loaded_dbm);
        loaded_from_manifest = true;
    }

    LOG_TRACE(log, "Loaded {} parts and {} delete bitmap in table version {} from {}.",
        data_parts.size(),
        delete_bitmaps.size(),
        version,
        checkpoint_version ? "checkpoint" : "catalog");

    // add loaded data to disk cache
    if (disk_cache)
        disk_cache->cacheSegmentsToLocalDisk({std::move(manifest_seg)});
}

void TableVersion::initialize(const Protos::ManifestListModel & version_model)
{
    version = version_model.version();
    checkpoint_version = version_model.checkpoint();
    const auto & txns = version_model.txn_ids();
    txn_list = std::vector<UInt64>{txns.begin(), txns.end()};
}

void TableVersion::dropDiskCache(ThreadPool & pool)
{
    auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::Manifest)->getDataCache();
    auto manifest_seg = std::make_shared<ManifestDiskCacheSegment>(shared_from_this());

    auto drop_task = [disk_cache, segment_name = manifest_seg->getSegmentName()]()
    {
        try
        {
            disk_cache->drop(segment_name);
        }
        catch(...)
        {
            tryLogCurrentException(&Poco::Logger::get("TableVersion"), "Error occurs when drop manifest disk cache : " + segment_name);
        }
    };

    pool.scheduleOrThrowOnError(drop_task);
}

}
