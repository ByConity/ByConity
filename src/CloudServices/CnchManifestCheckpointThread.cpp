#include <CloudServices/CnchManifestCheckpointThread.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CheckpointHelper.h>
#include <Storages/StorageCnchMergeTree.h>
#include <MergeTreeCommon/TableVersion.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Protos/DataModelHelpers.h>
#include <Catalog/Catalog.h>

namespace DB
{

CnchManifestCheckpointThread::CnchManifestCheckpointThread(ContextPtr context_, const StorageID & id)
    :ICnchBGThread(context_, CnchBGThreadType::ManifestCheckpoint, id)
{
}

void CnchManifestCheckpointThread::runImpl()
{
    UInt64 sleep_ms = 120 * 1000;
    try
    {
        std::lock_guard<std::mutex> task_lock(task_mutex);
        StoragePtr istorage = getStorageFromCatalog();
        checkPointImpl(istorage);

        auto & storage = checkAndGetCnchTable(istorage);
        auto storage_settings = storage.getSettings();

        sleep_ms = storage_settings->checkpoint_delay_period.totalMilliseconds();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to make new checkpoint.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    scheduled_task->scheduleAfter(sleep_ms);
}

UInt64 CnchManifestCheckpointThread::checkPointImpl(StoragePtr & istorage)
{
    UInt64 checkpoint_version = 0;

    auto local_context = getContext();
    auto & storage = checkAndGetCnchTable(istorage);
    auto storage_settings = storage.getSettings();

    if (!storage_settings->enable_publish_version_on_commit)
        return checkpoint_version;

    /// get all table versions
    auto catalog = local_context->getCnchCatalog();
    auto table_versions_model = catalog->getAllTableVersions(getStorageID().uuid);

    // clean up old table versions before make new one.
    UInt64 current_timestamp = local_context->getPhysicalTimestamp();
    UInt64 manifest_list_lifetime = storage_settings->manifest_list_lifetime.totalMilliseconds();
    // current timestamp may be 0 when TSO is unreachable
    TxnTimestamp ttl_ts = TxnTimestamp::fromMilliSeconds(current_timestamp > manifest_list_lifetime ? current_timestamp - manifest_list_lifetime : current_timestamp);

    bool at_least_one_checkpoint = false;
    decltype(table_versions_model) versions_to_clean;
    for (auto it=table_versions_model.rbegin(); it!=table_versions_model.rend(); ++it)
    {
        if (at_least_one_checkpoint)
        {
            if ((*it)->version() < ttl_ts)
            {
                versions_to_clean.push_back(*it);
                if ((*it)->checkpoint())
                {
                    // remove stale checkpoint from DFS
                    auto remote_disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
                    String storage_checkpoint_path = joinPaths({getCheckpointRelativePath(storage), toString((*it)->version())});
                    LOG_DEBUG(log, "Remove checkpoint file of version {} from {}. TTL : {}", (*it)->version(), storage_checkpoint_path, ttl_ts);
                    remote_disk->removeFileIfExists(storage_checkpoint_path);
                }
            }
        }
        else if ((*it)->checkpoint())
        {
            at_least_one_checkpoint = true;
        }
    }

    if (versions_to_clean.size())
    {
        LOG_DEBUG(log, "Clean up {} stale table versions, from {} to {}", versions_to_clean.size(), versions_to_clean.back()->version(),
            versions_to_clean.front()->version());
        catalog->cleanTableVersions(getStorageID().uuid, versions_to_clean);
    }

    Stopwatch watch;
    std::vector<TableVersionPtr> versions_to_checkpoint;
    for (auto it=table_versions_model.rbegin(); it!=table_versions_model.rend(); ++it)
    {
        versions_to_checkpoint.push_back(std::make_shared<TableVersion>(local_context, getStorageID().uuid, *(*it), false));
        // only checkpoint additional manifest list after last checkpoint
        if ((*it)->checkpoint())
            break;
    }

    if (versions_to_checkpoint.size() <= storage_settings->max_active_manifest_list)
        return checkpoint_version;

    LOG_TRACE(log, "Start make new checkpoint from {} versions. Start version: {}, end version : {}",
        versions_to_checkpoint.size(),
        versions_to_checkpoint.back()->getVersion(),
        versions_to_checkpoint.front()->getVersion());

    // calculate visible parts
    ServerDataPartsWithDBM parts_with_dbm = loadPartsWithDBMFromTableVersions(versions_to_checkpoint, storage);
    auto & server_parts = parts_with_dbm.first;
    auto & delete_bitmaps = parts_with_dbm.second;
    server_parts = CnchPartsHelper::calcVisibleParts(server_parts, false, CnchPartsHelper::LoggingOption::DisableLogging);
    DeleteBitmapMetaPtrVector visible_delete_bitmaps;
    CnchPartsHelper::calcVisibleDeleteBitmaps(delete_bitmaps, visible_delete_bitmaps);

    // dump checkpoint parts
    auto checkpoint_version_model = table_versions_model.back();
    checkpoint_version = checkpoint_version_model->version();

    checkPointServerPartsWithDBM(storage, server_parts, visible_delete_bitmaps, checkpoint_version);

    // update table version to make it a checkpoint version.
    checkpoint_version_model->set_checkpoint(true);
    catalog->commitCheckpointVersion(getStorageID().uuid, checkpoint_version_model);

    LOG_TRACE(log, "Made new checkpoint to version {} , elapsed {}ms.", checkpoint_version, watch.elapsedMilliseconds());

    return checkpoint_version;
}

ServerDataPartsWithDBM CnchManifestCheckpointThread::loadPartsWithDBMFromTableVersions(const std::vector<TableVersionPtr> & table_versions, const MergeTreeMetaBase & storage)
{
    ServerDataPartsWithDBM res;
    if (table_versions.empty())
        return res;

    std::mutex data_mutex;

    ThreadPool pool(8);
    Stopwatch watch;
    auto load_task = [&](const TableVersionPtr version)
    {
        ServerDataPartsWithDBM loaded = version->getAllPartsWithDBM(storage);
        std::lock_guard<std::mutex> lock(data_mutex);
        res.first.insert(res.first.end(), loaded.first.begin(), loaded.first.end());
        res.second.insert(res.second.end(), loaded.second.begin(), loaded.second.end());
    };

    for (const auto & table_version : table_versions)
    {
        pool.scheduleOrThrowOnError([&]
            {load_task(table_version);}
        );
    }

    pool.wait();

    LOG_DEBUG(log, "Loaded {} server parts and {} delete bitmaps from {} table versions takes {}ms.",
        res.first.size(),
        res.second.size(),
        table_versions.size(),
        watch.elapsedMilliseconds());
    
    return res;
}

void CnchManifestCheckpointThread::checkPointServerPartsWithDBM(const MergeTreeMetaBase & storage, ServerDataPartsVector & server_parts, DeleteBitmapMetaPtrVector & delete_bitmaps, const UInt64 checkpoint_version)
{
    if (server_parts.empty())
        return;

    ///TODO: consider to make checkpoint by partition later;
    String storage_checkpoint_path = getCheckpointRelativePath(storage);

    auto check_block_size_and_write = [] (WriteBufferFromFileBase & buffer_, std::shared_ptr<Protos::DataModelManifestData> & data_, size_t counter_)
    {
        if ((counter_ % 1000) == 0 && data_->ByteSizeLong() >= CHECKPOINT_MAX_BLOCKS_SIZE_BYTES)
        {
            writeStringBinary(data_->SerializeAsString(), buffer_);
            data_ = std::make_shared<Protos::DataModelManifestData>();
        }
    };

    // firstly ,write local checkpoint file.
    auto local_disk = storage.getStoragePolicy(IStorage::StorageLocation::AUXILITY)->getAnyDisk();
    auto remote_disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
    String local_checkpoint_path = joinPaths({storage_checkpoint_path, toString(checkpoint_version) + ".tmp"});
    
    try
    {
        {
            if (!local_disk->exists(storage_checkpoint_path))
                local_disk->createDirectories(storage_checkpoint_path);

            auto write_buffer = local_disk->writeFile(local_checkpoint_path, WriteSettings());

            size_t counter = 0;
            std::shared_ptr<Protos::DataModelManifestData> data = std::make_shared<Protos::DataModelManifestData>();
            for (auto server_part : server_parts)
            {
                auto * new_part_model = data->mutable_parts()->Add();
                new_part_model->CopyFrom(server_part->part_model());
                check_block_size_and_write(*write_buffer, data, counter++);
            }

            for (auto delete_bitmap : delete_bitmaps)
            {
                auto * new_dbm_model = data->mutable_delete_bitmaps()->Add();
                new_dbm_model->CopyFrom(*(delete_bitmap->getModel()));
                check_block_size_and_write(*write_buffer, data, counter++);
            }

            if (data->parts().size() || data->delete_bitmaps().size())
                writeStringBinary(data->SerializeAsString(), *write_buffer);

            LOG_TRACE(log, "Dumped new checkpoint into temporary local path {}. Checkpoint parts size: {}, delete bitmap size: {}.",
                local_checkpoint_path,
                server_parts.size(),
                delete_bitmaps.size());
        }
        // Then, move the local tmp checkpoint file to DFS
        {
            String remote_checkpoint_path = joinPaths({storage_checkpoint_path, toString(checkpoint_version)});
            if (!remote_disk->exists(storage_checkpoint_path))
                remote_disk->createDirectories(storage_checkpoint_path);

            if (remote_disk->exists(remote_checkpoint_path))
            {
                LOG_WARNING(log, "Checkpoint {} already exists. Will remove it.", remote_checkpoint_path);
                remote_disk->removeRecursive(remote_checkpoint_path);
            }

            auto remote_file = remote_disk->writeFile(remote_checkpoint_path, WriteSettings());
            ReadBufferFromFile local_file(local_disk->getPath() + local_checkpoint_path);
            copyData(local_file, *remote_file);

            LOG_TRACE(log, "Created new checkpoint file {}", remote_checkpoint_path);
        }
    }
    catch (...)
    {
        local_disk->removeFileIfExists(storage_checkpoint_path);
        tryLogCurrentException(log, "Error occurs while make manifest checkpoint.");
    }

    local_disk->removeFileIfExists(local_checkpoint_path);
}

void CnchManifestCheckpointThread::executeManually()
{
    std::lock_guard<std::mutex> task_lock(task_mutex);
    StoragePtr istorage = getStorageFromCatalog();
    checkPointImpl(istorage);
}

}
