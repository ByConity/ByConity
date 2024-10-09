#include <Catalog/Catalog.h>
#include <Storages/MergeTree/AttachContext.h>
#include <Storages/PartitionCommands.h>
#include <Interpreters/executeQuery.h>

namespace DB
{

void AttachContext::determineOperationType(const PartitionCommand & command, bool enable_copy_for_partition_operation)
{
    if (!command.from_table.empty())
    {
        if (command.attach_from_detached)
            operation_type = enable_copy_for_partition_operation ? AttachContext::OperationType::COPY_FROM_OTHER_DETACHED
                                                                 : AttachContext::OperationType::MOVE_FROM_OTHER_DETACHED;
        else
            operation_type = enable_copy_for_partition_operation ? AttachContext::OperationType::COPY_FROM_STORAGE
                                                                 : AttachContext::OperationType::MOVE_FROM_STORAGE;
    }
    else
    {
        if (command.from_zookeeper_path.empty())
            operation_type = enable_copy_for_partition_operation ? AttachContext::OperationType::COPY_FROM_OWN_DETACHED
                                                                 : AttachContext::OperationType::MOVE_FROM_OWN_DETACHED;
        else
            operation_type = enable_copy_for_partition_operation ? AttachContext::OperationType::COPY_FROM_PATH
                                                                 : AttachContext::OperationType::MOVE_FROM_PATH;
    }
}

void AttachContext::writeRenameRecord(UndoRecord & undo_record)
{
    LOG_TRACE(
        logger,
        fmt::format(
            "Write rename record, disk path {}, relative path {} -> {}",
            undo_record.from_disk->getPath(),
            undo_record.from_path,
            undo_record.to_path));

    std::lock_guard<std::mutex> lock(mu);

    auto & res = resources[undo_record.from_disk->getPath()];
    res.rename_records.emplace_back(undo_record);
}

void AttachContext::writeCopyRecord(UndoRecord & undo_record)
{
    LOG_TRACE(
        logger,
        fmt::format(
            "Write copy record, disk path {}, relative path {} -> {}",
            undo_record.from_disk->getPath(),
            undo_record.from_path,
            undo_record.to_path));

    std::lock_guard<std::mutex> lock(mu);

    auto & res = resources[undo_record.from_disk->getPath()];
    res.copy_records.emplace_back(undo_record);
}

void AttachContext::writeDetachedPartRecord(UndoRecord & undo_record)
{
    LOG_TRACE(
        logger, "Write detached part record, disk path {}, relative part path {}", undo_record.from_disk->getPath(), undo_record.from_path);

    std::lock_guard<std::mutex> lock(mu);

    auto & res = detached_parts_to_delete[undo_record.from_disk->getPath()];
    // here we reuse copy_part to record part path in detached directory
    res.copy_records.emplace_back(undo_record);
}

void AttachContext::writeMetaFilesNameRecord(UndoRecord & undo_record)
{
    LOG_TRACE(
        logger,
        fmt::format(
            "Write meta files name to delete record for attaching unique table parts, in disk {}, relative file path {}",
            undo_record.from_disk->getPath(),
            undo_record.name));

    std::lock_guard<std::mutex> lock(mu);

    auto & res = meta_files_to_delete[undo_record.from_disk->getPath()];
    res.rename_records.emplace_back(undo_record);
}

// Used for HDFS
void AttachContext::writeUndoRecord(UndoRecord & undo_record)
{
    switch (operation_type)
    {
        case OperationType::MOVE_FROM_STORAGE:
        case OperationType::MOVE_FROM_OTHER_DETACHED:
        case OperationType::MOVE_FROM_OWN_DETACHED:
        case OperationType::MOVE_FROM_PATH: {
            writeRenameRecord(undo_record);
            break;
        }
        case OperationType::COPY_FROM_STORAGE: {
            writeCopyRecord(undo_record);
            break;
        }
        case OperationType::COPY_FROM_OWN_DETACHED: {
            writeCopyRecord(undo_record);
            // copy partition from its own detached directory, delete parts in detached directory after
            // txn is successfully finished.
            if (undo_record.is_bitmap)
                writeMetaFilesNameRecord(undo_record);
            else
                writeDetachedPartRecord(undo_record);
            break;
        }
        case OperationType::COPY_FROM_PATH:
        case OperationType::COPY_FROM_OTHER_DETACHED:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported operation type now");
        // do nothing
        case OperationType::MOVE_FROM_TASK:
            break;
        default: {
            throw Exception("Unexpected operation type", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void AttachContext::submitUndoRecordToKV(Catalog::Catalog & catalog, const StorageID & storage_id, const TxnTimestamp & txn_id)
{
    UndoResources undo_buffers;
    for (const auto & [disk_name, resource] : resources)
    {
        for (const auto & rename_record : resource.rename_records)
        {
            undo_buffers.emplace_back(txn_id, UndoResourceType::FileSystem, rename_record.from_path, rename_record.to_path);
            undo_buffers.back().setDiskName(disk_name);
        }

        for (const auto & copy_record : resource.copy_records)
        {
            if (copy_record.is_bitmap)
                undo_buffers.emplace_back(
                    txn_id,
                    UndoResourceType::DeleteBitmap,
                    DB::dataModelName(*copy_record.delete_bitmap_model),
                    DeleteBitmapMeta::deleteBitmapFileRelativePath(*copy_record.delete_bitmap_model));
            else
                undo_buffers.emplace_back(txn_id, UndoResourceType::Part, copy_record.name, fs::path(copy_record.name) / "");
            undo_buffers.back().setDiskName(disk_name);
        }
    }

    if (!undo_buffers.empty())
        catalog.writeUndoBuffer(storage_id, txn_id, undo_buffers);
}

void AttachContext::executeOperation()
{
    size_t total_records = 0;
    for (const auto & [_, resource] : resources)
    {
        total_records += resource.rename_records.size();
        total_records += resource.copy_records.size();
    }

    auto & pool = getWorkerPool(total_records);

    for (const auto & [disk_name, resource] : resources)
    {
        for (const auto & rename_record : resource.rename_records)
        {
            if (rename_record.is_bitmap
                && rename_record.delete_bitmap_model->cardinality() <= DeleteBitmapMeta::kInlineBitmapMaxCardinality)
                continue;

            pool.scheduleOrThrowOnError([&disk = rename_record.from_disk,
                                         is_bitmap = rename_record.is_bitmap,
                                         from = rename_record.from_path,
                                         to = rename_record.to_path]() {
                // If it's bitmap and exists physically
                if (is_bitmap)
                {
                    String parent_dir = fs::path(to).parent_path();
                    if (!disk->exists(parent_dir))
                        disk->createDirectories(parent_dir);

                    disk->moveFile(from, to);
                }
                // else if it's a part
                else
                    disk->moveDirectory(from, to);
            });
        }

        for (const auto & copy_record : resource.copy_records)
        {
            String source_data_path = copy_record.from_path;
            String target_data_path = copy_record.to_path;
            // For delete bitmap, we don't need to add data
            if (!copy_record.is_bitmap)
            {
                source_data_path = fs::path(copy_record.from_path) / "data";
                target_data_path = fs::path(copy_record.to_path) / "data";
            }

            pool.scheduleOrThrowOnError(
                [&from_disk = copy_record.from_disk, &to_disk = copy_record.to_disk, from = source_data_path, to = target_data_path]() {
                    String parent_dir = fs::path(to).parent_path();
                    if (!to_disk->exists(parent_dir))
                        to_disk->createDirectories(parent_dir);

                    std::vector<std::pair<std::string, std::string>> files_to_copy;
                    files_to_copy.emplace_back(from, to);
                    from_disk->copyFiles(files_to_copy, to_disk);
                });
        }
    }
    pool.wait();
}

void AttachContext::commit(bool has_exception)
{
    if (new_txn != nullptr)
    {
        query_ctx.getCnchTransactionCoordinator().finishTransaction(new_txn);
    }

    /// If we're not in the interactive transaction session, at this point it's safe
    /// to remove the directory lock
    if (!src_directory.empty() && !isQueryInInteractiveSession(query_ctx.shared_from_this())
        && !query_ctx.getSettingsRef().force_clean_transaction_by_dm)
        query_ctx.getCnchCatalog()->clearFilesysLocks({src_directory}, std::nullopt);

    // Remove parts in detached directory
    // TBD: if having exception for attach partition, we leave parts in detached directory unchanged, in case they will be used in future. As exception does
    // not mean the transaction really failes, this is a false-positive operation which may result in parts remaining in detached directory. We don't think
    // this is a key issue, as attach parts from its own detached directory by using copy is a low-frequent command and it is relatively easier to clear remaining
    // parts under detached directory.
    if (!detached_parts_to_delete.empty() && !has_exception)
    {
        size_t total_records = 0;
        for (const auto & [_, resource] : detached_parts_to_delete)
            total_records += resource.copy_records.size();
        LOG_INFO(logger, "Ready to remove {} parts in detached directory", total_records);

        ThreadPool & pool = getWorkerPool(total_records);
        for (const auto & [_, resource] : detached_parts_to_delete)
        {
            for (const auto & copy_record : resource.copy_records)
            {
                pool.scheduleOrThrowOnError([&disk = copy_record.from_disk, path = copy_record.from_path, logger = logger]() {
                    try
                    {
                        disk->removeRecursive(path);
                    }
                    catch (...)
                    {
                        LOG_WARNING(logger, "Failed to remove {}, exception is {}", path, getCurrentExceptionMessage(false));
                    }
                });
            }
        }
        pool.wait();
    }

    // Remove .meta and .bitmap in detached directory. It is same as removing data parts as described above
    if (!meta_files_to_delete.empty() && !has_exception)
    {
        size_t total_records = 0;
        for (const auto & [_, meta_name_records] : meta_files_to_delete)
            total_records += meta_name_records.rename_records.size();

        ThreadPool & pool = getWorkerPool(total_records);
        for (const auto & [_, meta_name_records] : meta_files_to_delete)
        {
            for (const auto & rename_record : meta_name_records.rename_records)
                pool.scheduleOrThrowOnError(
                    [&disk = rename_record.from_disk, path = rename_record.from_path]() { disk->removeFileIfExists(path); });
        }
        pool.wait();
    }
}

void AttachContext::rollback()
{
    if (new_txn != nullptr)
    {
        query_ctx.getCnchTransactionCoordinator().finishTransaction(new_txn);
    }

    size_t total_records = 0;
    for (const auto & [_, resource] : resources)
    {
        total_records += resource.rename_records.size();
    }

    ThreadPool & pool = getWorkerPool(total_records);
    for (const auto & [_, resource] : resources)
    {
        for (const auto & rename_record : resource.rename_records)
        {
            pool.scheduleOrThrowOnError([&disk = rename_record.from_disk, from = rename_record.from_path, to = rename_record.to_path]() {
                if (disk->exists(to))
                {
                    disk->moveDirectory(to, from);
                }
            });
        }
    }
    pool.wait();
}

ThreadPool & AttachContext::getWorkerPool(int job_nums)
{
    bool need_create_thread_pool = worker_pool == nullptr || worker_pool->finished();
    if (!need_create_thread_pool)
    {
        // Already have a thread pool
        if ((job_nums - static_cast<int>(worker_pool->getMaxThreads())) > expand_thread_pool_threshold)
        {
            worker_pool->wait();
            worker_pool = nullptr;

            need_create_thread_pool = true;
        }
    }

    if (need_create_thread_pool)
    {
        worker_pool = std::make_unique<ThreadPool>(std::max(1, std::min(max_worker_threads, job_nums)));
    }
    return *worker_pool;
}
}
