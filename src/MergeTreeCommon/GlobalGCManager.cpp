/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <MergeTreeCommon/GlobalGCManager.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <Storages/MergeTree/CnchAttachProcessor.h>
#include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Protos/RPCHelpers.h>
#include <Common/Status.h>
#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_TABLE_NAME;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

GlobalGCManager::GlobalGCManager(
    ContextMutablePtr global_context_,
    size_t default_max_threads,
    size_t default_max_free_threads,
    size_t default_max_queue_size)
    : WithContext(global_context_), log(getLogger("GlobalGCManager"))
{
    const auto & config_ref = getContext()->getConfigRef();
    this->max_threads =
        config_ref.getUInt("global_gc.threadpool_max_size", default_max_threads);
    const size_t max_free_threads =
        config_ref.getUInt("global_gc.threadpool_max_free_threads", default_max_free_threads);
    const size_t queue_size =
        config_ref.getUInt("global_gc.threadpool_max_queue_size", default_max_queue_size);

    LOG_DEBUG(log, "init thread pool with max_threads: {} max_free_threads: {} queue_size: {}",
        max_threads, max_free_threads, queue_size);
    if (max_threads > 0)
        threadpool = std::make_unique<ThreadPool>(max_threads, max_free_threads, queue_size);
}

size_t GlobalGCManager::getNumberOfDeletingTables() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return deleting_uuids.size();
}

std::set<UUID> GlobalGCManager::getDeletingUUIDs() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return deleting_uuids;
}

namespace GlobalGCHelpers
{
size_t calculateApproximateWorkLimit(size_t max_threads)
{
    return GlobalGCManager::MAX_BATCH_WORK_SIZE * max_threads * 2;
}

bool canReceiveMoreWork(size_t max_threads, size_t deleting_table_num, size_t num_of_new_tables)
{
    size_t approximate_work_limit = calculateApproximateWorkLimit(max_threads);
    return (deleting_table_num < approximate_work_limit) &&
        ((deleting_table_num + num_of_new_tables) < (approximate_work_limit + GlobalGCManager::MAX_BATCH_WORK_SIZE));
}

size_t amountOfWorkCanReceive(size_t max_threads, size_t deleting_table_num)
{
    size_t approximate_work_limit = calculateApproximateWorkLimit(max_threads);
    if (deleting_table_num < approximate_work_limit)
    {
        size_t batch_num = ((approximate_work_limit + GlobalGCManager::MAX_BATCH_WORK_SIZE) - deleting_table_num - 1) / GlobalGCManager::MAX_BATCH_WORK_SIZE;
        return batch_num * GlobalGCManager::MAX_BATCH_WORK_SIZE;
    }
    return 0;
}

namespace {
    void cleanS3Disks(const StoragePtr & storage, const MergeTreeMetaBase & mergetree_meta, const Context & context, LoggerPtr log)
    {
        auto catalog = context.getCnchCatalog();
        Strings partition_ids = catalog->getPartitionIDs(storage, &context);

        ThreadPool clean_pool(context.getSettingsRef().s3_gc_inter_partition_parallelism);
        for (const String & partition_id : partition_ids)
        {
            clean_pool.scheduleOrThrowOnError([partition_id, &log, &catalog, &storage, &mergetree_meta, &context]() {
                MultiDiskS3PartsLazyCleaner parts_cleaner(std::nullopt, context.getSettingsRef().s3_gc_intra_partition_parallelism);

                LOG_DEBUG(log, "Start GC partition {} for table {}", partition_id, storage->getStorageID().getNameForLogs());

                ServerDataPartsVector parts_in_trash = catalog->getTrashedPartsInPartitions(storage, {partition_id}, 0);
                ServerDataPartsVector parts = catalog->getServerDataPartsInPartitions(storage, {partition_id}, {0}, &context);

                LOG_DEBUG(log, "Will remove {} data parts and {} trashed parts from S3 storage.", parts.size(), parts_in_trash.size());
                std::move(parts_in_trash.begin(), parts_in_trash.end(), std::back_inserter(parts));

                for (auto & part : parts)
                {
                    auto cnch_part = part->toCNCHDataPart(mergetree_meta);

                    auto disks = cnch_part->volume->getDisks();
                    for (const auto & disk : disks)
                    {
                        parts_cleaner.push(disk, cnch_part->getFullRelativePath());
                    }
                }

                parts = catalog->listDetachedParts(mergetree_meta, AttachFilter::createPartitionFilter(partition_id));
                for (auto & part : parts)
                {
                    auto cnch_part = part->toCNCHDataPart(mergetree_meta);

                    auto disks = cnch_part->volume->getDisks();
                    for (const auto & disk : disks)
                    {
                        parts_cleaner.push(disk, cnch_part->getFullRelativePath());
                    }
                }

                parts_cleaner.finalize();

                LOG_DEBUG(log, "Finish GC partition {} for table {}", partition_id, storage->getStorageID().getNameForLogs());

                if (mergetree_meta.getInMemoryMetadataPtr()->hasUniqueKey())
                {
                    auto all_detached_bitmaps
                        = catalog->listDetachedDeleteBitmaps(mergetree_meta, AttachFilter::createPartitionFilter(partition_id));
                    for (auto & detached_bitmap : all_detached_bitmaps)
                        detached_bitmap->removeFile();
                    LOG_DEBUG(
                        log,
                        "Finish GC detached delete bitmap of partition {} for table {}",
                        partition_id,
                        storage->getStorageID().getNameForLogs());
                }
            });
        }
        clean_pool.wait();
    }

void cleanDisks(const Disks & disks, const String & relative_path, LoggerPtr log)
{
    for (const DiskPtr & disk : disks)
    {
        if (disk->exists(relative_path))
        {
            disk->removeRecursive(relative_path);
            LOG_DEBUG(log, "Removed relative path {} of disk type {}, root path {}",
                relative_path, DiskType::toString(disk->getType()), disk->getPath());
        }
        else
            LOG_WARNING(
                log,
                "Relative path {} doesn't exists, disk type is {}, root path is {}",
                relative_path,
                DiskType::toString(disk->getType()),
                disk->getPath());
    }
}

void dropBGStatusesInCatalogForCnchMergeTree(UUID uuid, Catalog::Catalog * catalog)
{
    catalog->dropBGJobStatus(uuid, CnchBGThreadType::Clustering);
    catalog->dropBGJobStatus(uuid, CnchBGThreadType::MergeMutate);
    catalog->dropBGJobStatus(uuid, CnchBGThreadType::PartGC);
    catalog->dropBGJobStatus(uuid, CnchBGThreadType::DedupWorker);
}

void dropBGStatusInCatalogForCnchKafka(UUID uuid, Catalog::Catalog * catalog)
{
    catalog->dropBGJobStatus(uuid, CnchBGThreadType::Consumer);
}

} /// end anonymous namespace

std::optional<Protos::DataModelTable> getCleanableTrashTable(
    ContextPtr context,
    const Protos::TableIdentifier & table_id,
    const TxnTimestamp & ts,
    UInt64 retention_sec,
    String * fail_reason)
{
    std::vector<Protos::DataModelTable> table_versions = context->getCnchCatalog()->getTableHistories(table_id.uuid());

    // sequences of drop/undrop ddl leads to multiple life spans for the table
    std::vector<std::pair<UInt64, UInt64>> lifespans; // [(beg1, end1), (beg2, end2), ..]
    std::pair<UInt64, UInt64> curr_span = {0, 0};
    for (const auto & table : table_versions)
    {
        if (Status::isDeleted(table.status()))
        {
            if (curr_span.second == 0)
            {
                curr_span.second = table.commit_time();
                lifespans.push_back(curr_span);
                curr_span = {0, 0};
            }
        }
        else if (curr_span.first == 0)
        {
            curr_span.first = table.commit_time();
        }
    }

    if (lifespans.empty() || curr_span.first != 0 || curr_span.second != 0)
    {
        if (fail_reason)
            *fail_reason = fmt::format(
                "Can't calculate lifespans for table {}, got {} versions and {} spans",
                table_id.uuid(),
                table_versions.size(),
                lifespans.size());
        return std::nullopt;
    }

    // the above if leads to this assertion
    assert(!table_versions.empty() && Status::isDeleted(table_versions.back().status()));

    // fast path: cannot clean if table is within retention period
    if (lifespans.back().second + TxnTimestamp::fromUnixTimestamp(retention_sec) > ts)
    {
        if (fail_reason)
            *fail_reason = "Under retention period";
        return std::nullopt;
    }

    // otherwise, can clean table iff it's not referenced by any snapshots

    // fast path: no snapshots for database
    if (!table_id.has_db_uuid())
        return table_versions.back();

    UUID table_uuid = RPCHelpers::createUUID(table_versions.back().uuid());
    Snapshots snapshots = context->getCnchCatalog()->getAllSnapshots(RPCHelpers::createUUID(table_id.db_uuid()), &table_uuid);
    // remove expired snapshots
    std::erase_if(snapshots, [&](SnapshotPtr & snapshot) {
        return snapshot->commit_time() + TxnTimestamp::fromUnixTimestamp(snapshot->ttl_in_days() * 3600 * 24) < ts;
    });

    auto log = getLogger("getCleanableTrashTable");
    for (const auto & [beg, end] : lifespans)
    {
        LOG_TRACE(log, "lifespan [{} - {})", beg, end);
        for (const auto & snapshot : snapshots)
        {
            LOG_TRACE(log, "Test snapshot {} with ts {}", snapshot->name(), snapshot->commit_time());
            if (snapshot->commit_time() >= beg && snapshot->commit_time() < end)
            {
                if (fail_reason)
                    *fail_reason = fmt::format("Referenced by active snapshot '{}'", snapshot->name());
                return std::nullopt;
            }
        }
    }

    return table_versions.back();
}

bool executeGlobalGC(const Protos::DataModelTable & table, const Context & context, LoggerPtr log)
{
    auto storage_id = StorageID{table.database(), table.name(), RPCHelpers::createUUID(table.uuid())};

    if (!Status::isDeleted(table.status()))
    {
        LOG_ERROR(log, "Table {} already in trash, but status is not deleted", storage_id.getNameForLogs());
        return false;
    }

    LOG_INFO(log, "Table: {} is deleted, will execute GlobalGC for it", storage_id.getNameForLogs());

    try
    {
        Stopwatch watch;
        auto catalog = context.getCnchCatalog();

        auto storage = catalog->tryGetTableByUUID(context, UUIDHelpers::UUIDToString(storage_id.uuid), TxnTimestamp::maxTS(), true);
        if (!storage)
        {
            LOG_INFO(log, "Fail to get table by UUID, table probably already got deleted");
            return true;
        }

        StorageCnchMergeTree * mergetree = dynamic_cast<StorageCnchMergeTree*>(storage.get());
        if (mergetree)
        {
            LOG_DEBUG(log, "Remove data path for table {}", storage_id.getNameForLogs());
            StoragePolicyPtr remote_storage_policy = mergetree->getStoragePolicy(IStorage::StorageLocation::MAIN);

            DiskType::Type remote_disk_type = remote_storage_policy->getAnyDisk()->getType();
            switch (remote_disk_type)
            {
                /// delete data directory of the table from hdfs
                case DiskType::Type::ByteHDFS: {
                    Disks remote_disks = remote_storage_policy->getDisks();
                    const String & relative_path = mergetree->getRelativeDataPath(IStorage::StorageLocation::MAIN);
                    cleanDisks(remote_disks, relative_path, log);
                    break;
                }
                case DiskType::Type::ByteS3: {
                    cleanS3Disks(storage, *mergetree, context, log);
                    break;
                }
                default:
                    throw Exception(
                        fmt::format("Unexpected disk type {} when global gc", DiskType::toString(remote_disk_type)),
                        ErrorCodes::LOGICAL_ERROR);
            }
            // StoragePolicyPtr local_storage_policy = mergetree->getLocalStoragePolicy();
            // Disks local_disks = local_storage_policy->getDisks();
            // //const String local_store_path = mergetree->getLocalStorePath();
            // cleanDisks(local_disks, relative_path, log);

            LOG_DEBUG(log, "Remove background job statues for table {}", storage_id.getNameForLogs());
            dropBGStatusesInCatalogForCnchMergeTree(storage_id.uuid, catalog.get());
        }

        if (StorageCnchKafka * kafka_storage = dynamic_cast<StorageCnchKafka *>(storage.get()))
            dropBGStatusInCatalogForCnchKafka(storage_id.uuid, catalog.get());

        /// delete metadata of data parts
        LOG_DEBUG(log, "Remove data parts meta for table {}", storage_id.getNameForLogs());
        catalog->clearDataPartsMetaForTable(storage);

        /// delete mutation entries of the table
        LOG_DEBUG(log, "Remove mutation entries for table {}", storage_id.getNameForLogs());
        catalog->clearMutationEntriesForTable(storage);

        /// delete bitmaps
        if (mergetree && mergetree->getInMemoryMetadataPtr()->hasUniqueKey())
        {
            auto all_bitmaps = catalog->getAllDeleteBitmaps(*mergetree);
            LOG_DEBUG(log, "Remove delete bitmap size:  {} for table {}", all_bitmaps.size(), storage_id.getNameForLogs());
            {
                ThreadPool clean_pool(mergetree->getSettings()->gc_remove_bitmap_thread_pool_size);
                for (auto & bitmap : all_bitmaps)
                {
                    clean_pool.scheduleOrThrowOnError([&bitmap]() { bitmap->removeFile(); });
                }
                clean_pool.wait();
            }

            /// delete metadata of delete bitmaps
            LOG_DEBUG(log, "Remove delete bitmaps meta for table {}", storage_id.getNameForLogs());
            catalog->clearDeleteBitmapsMetaForTable(storage);
        }

        /// delete table's metadata
        LOG_DEBUG(log, "Remove table meta for table {}", storage_id.getNameForLogs());
        catalog->clearTableMetaForGC(storage_id.database_name, storage_id.table_name, table.commit_time());

        LOG_INFO(log, "Successfully executed GlobalGC for {} in {} ms", storage_id.getNameForLogs(), watch.elapsedMilliseconds());
    }
    catch (...)
    {
        tryLogCurrentException(log);
        LOG_ERROR(log, "Failed to execute GlobalGC for {}", storage_id.getNameForLogs());
    }

    return true;
}

std::vector<UUID> getUUIDsFromTables(const std::vector<Protos::DataModelTable> & tables)
{
    std::vector<UUID> ret;
    std::transform(tables.begin(), tables.end(),
        std::back_inserter(ret),
        [] (const Protos::DataModelTable & table)
        {
            return RPCHelpers::createUUID(table.uuid());
        }
    );
    return ret;
}

std::vector<Protos::DataModelTable> removeDuplication(
    const std::set<UUID> & deleting_uuids,
    std::vector<Protos::DataModelTable> tables
)
{
    std::set<UUID> added_uuids;
    tables.erase(
        std::remove_if(tables.begin(), tables.end(),
            [& deleting_uuids, & added_uuids] (const Protos::DataModelTable & table)
            {
                UUID uuid = RPCHelpers::createUUID(table.uuid());
                auto ret = added_uuids.insert(uuid);
                return (!ret.second) ||
                        (deleting_uuids.find(uuid) != deleting_uuids.end());
            }
        ), tables.end());
    return tables;
}

}// end namespace GlobalGCHelpers

GlobalGCManager::GlobalGCTask::GlobalGCTask(
    std::vector<Protos::DataModelTable> tables_,
    GlobalGCManager & manager_
) : tables(std::move(tables_)), manager(manager_)
{}

void GlobalGCManager::GlobalGCTask::operator()()
{
    for (const auto & table : tables)
    {
        if (manager.isShutdown())
            return;
        try
        {
            manager.executor(table, *manager.getContext(), manager.log);
        }
        catch(...)
        {
            LOG_WARNING(manager.log, "got exception while remove table {}.{}",
                table.database(), table.name());
            tryLogCurrentException(manager.log);
        }
        manager.removeDeletingUUID(RPCHelpers::createUUID(table.uuid()));
    }
}

bool GlobalGCManager::scheduleImpl(std::vector<Protos::DataModelTable> && tables)
{
    const uint64_t wait_microseconds = 500000; /// 0.5 s
    std::vector<UUID> uuids = GlobalGCHelpers::getUUIDsFromTables(tables);
    bool ret = true;
    GlobalGCTask gc_task{std::move(tables), *this};

    {
        std::lock_guard<std::mutex> lock(mutex);
        deleting_uuids.insert(uuids.begin(), uuids.end());
    }

    try
    {
        threadpool->scheduleOrThrow(std::move(gc_task), 0, wait_microseconds);
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Fail to schedule, got exception: {}", e.what());
        ret = false;
    }

    if (!ret)
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::for_each(uuids.begin(), uuids.end(),
            [this] (const auto & uuid)
            {
                deleting_uuids.erase(uuid);
            }
        );
        return false;
    }

    return true;
}

bool GlobalGCManager::schedule(std::vector<Protos::DataModelTable> tables)
{
    if (!threadpool)
        return false;

    bool is_shutdown_cp = false;
    {
        std::lock_guard<std::mutex> lock(mutex);
        is_shutdown_cp = is_shutdown;
    }

    if (is_shutdown_cp)
    {
        LOG_WARNING(log, "Can't receive work while shutting down");
        return false;
    }

    std::set<UUID> deleting_uuids_clone;
    {
        std::lock_guard<std::mutex> lock(mutex);
        deleting_uuids_clone = deleting_uuids;
    }

    tables = GlobalGCHelpers::removeDuplication(deleting_uuids_clone, std::move(tables));
    if (!GlobalGCHelpers::canReceiveMoreWork(max_threads, deleting_uuids_clone.size(), tables.size()))
    {
        LOG_WARNING(log, "Fail to schedule because too much work to do,"
            " misconfiguration between DM and servers, num of deleting table"
            " {}, number of new add table {}", deleting_uuids_clone.size(), tables.size());
        return false;
    }

    std::vector<Protos::DataModelTable> tables_bucket;
    for (auto && table : tables)
    {
        tables_bucket.push_back(std::move(table));
        if (tables_bucket.size() >= MAX_BATCH_WORK_SIZE)
        {
            if (!scheduleImpl(std::move(tables_bucket)))
            {
                LOG_WARNING(log, "Failed to scheduleImpl, probably because full queue, queue size: {}"
                    , threadpool->active());
                return false;
            }
        }
    }

    // coverity[use_after_move]
    if ((!tables_bucket.empty()) &&
        (!scheduleImpl(std::move(tables_bucket)))
    )
    {
        LOG_WARNING(log, "Failed to scheduleImpl, probably because full queue, number of active job: {}"
            , threadpool->active());
        return false;
    }
    return true;
}

void GlobalGCManager::systemCleanTrash(ContextPtr local_context, StorageID storage_id, LoggerPtr log)
{
    const UInt64 retention_sec = local_context->getSettingsRef().cnch_data_retention_time_in_sec;
    auto catalog = local_context->getCnchCatalog();
    auto table_ids = catalog->getTrashTableVersions(storage_id.database_name, storage_id.table_name);
    if (table_ids.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Trash table {} not found", storage_id.getNameForLogs());

    std::shared_ptr<Protos::TableIdentifier> table_id;
    if (storage_id.hasUUID())
    {
        for (auto & entry : table_ids)
        {
            if (entry.second->uuid() == UUIDHelpers::UUIDToString(storage_id.uuid))
            {
                table_id = entry.second;
                break;
            }
        }
        if (!table_id)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Trash table {} not found", storage_id.getNameForLogs());
    }
    else if (table_ids.size() > 1)
    {
        throw Exception(ErrorCodes::AMBIGUOUS_TABLE_NAME, "Found multiple trash tables, please specify UUID");
    }
    else
    {
        table_id = table_ids.begin()->second;
    }

    TxnTimestamp ts = local_context->getTimestamp();
    String fail_reason;
    auto table_model = GlobalGCHelpers::getCleanableTrashTable(local_context, *table_id, ts, retention_sec, &fail_reason);
    if (table_model.has_value())
    {
        GlobalGCHelpers::executeGlobalGC(*table_model, *local_context, log);
    }
    else
    {
        LOG_INFO(log, "Cannot clean trash table {} because : {}", storage_id.getNameForLogs(), fail_reason);
    }
}

bool GlobalGCManager::isShutdown() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return is_shutdown;
}

void GlobalGCManager::removeDeletingUUID(UUID uuid)
{
    std::lock_guard<std::mutex> lock(mutex);
    deleting_uuids.erase(uuid);
}

void GlobalGCManager::shutdown()
{
    std::lock_guard<std::mutex> lock(mutex);
    is_shutdown = true;
}

GlobalGCManager::~GlobalGCManager()
{
    shutdown();
}

} /// end namespace
