#include <Storages/PartCacheManager.h>

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Storages/CnchDataPartCache.h>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <Common/RWLock.h>
#include <Common/serverLocality.h>
#include <Common/HostWithPorts.h>
#include <Common/Status.h>
#include <Common/ConsistentHashUtils/Hash.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

PartCacheManager::PartCacheManager(Context & context_)
    : context(context_)
{
    partCachePtr = std::make_shared<CnchDataPartCache>(context.getConfigRef().getUInt("size_of_cached_parts", 100000));
    metrics_updater = context.getSchedulePool().createTask("PartMetricsUpdater",[this](){
        updateTablePartitionsMetrics(false);
        /// schedule every 24 hours, maybe could be configurable later
        this->metrics_updater->scheduleAfter(24*60*60*1000);
    });
    metrics_initializer = context.getSchedulePool().createTask("PartMetricsInitializer",[this](){
        updateTablePartitionsMetrics(true);
        /// schedule every 3 seconds
        this->metrics_initializer->scheduleAfter(3*1000);
    });
    meta_lock_cleaner = context.getSchedulePool().createTask("MetaLockCleaner", [this](){
        cleanMetaLock();
        /// schedule every hour.
        this->meta_lock_cleaner->scheduleAfter(3*1000);
    });
    active_table_loader = context.getSchedulePool().createTask("ActiveTablesLoader", [this](){
        // load tables when server start up.
        loadActiveTables();
    });
    if (context.getServerType() == ServerType::cnch_server)
    {
        metrics_updater->activate();
        metrics_updater->scheduleAfter(60*60*1000);
        metrics_initializer->activateAndSchedule();
        meta_lock_cleaner->activateAndSchedule();
        active_table_loader->activateAndSchedule();
    }
}

void PartCacheManager::mayUpdateTableMeta(const IStorage & storage)
{
    /* Fetches partitions from metastore if storage is not present in active_tables*/

    /// Only handle MergeTree tables
    const auto * cnch_table = dynamic_cast<const MergeTreeMetaBase*>(&storage);
    if (cnch_table == nullptr)
        return;

    auto load_nhut = [&](TableMetaEntryPtr & metaPtr) {
        if (context.getSettingsRef().server_write_ha)
        {
            UInt64 pts = context.getPhysicalTimestamp();
            if (pts)
            {
                UInt64 fetched_nhut = context.getCnchCatalog()->getNonHostUpdateTimestampFromByteKV(storage.getStorageUUID());
                if (pts - fetched_nhut > 9)
                    metaPtr->cached_non_host_update_ts = fetched_nhut;
            }
        }
    };

    auto load_table_partitions = [&](TableMetaEntryPtr & metaPtr) {
        auto table_lock = metaPtr->writeLock();
        context.getCnchCatalog()->getPartitionsFromMetastore(*cnch_table, metaPtr->partitions);
        context.getCnchCatalog()->getTableClusterStatus(storage.getStorageUUID(), metaPtr->is_clustered);
        context.getCnchCatalog()->getTablePreallocateVW(storage.getStorageUUID(), metaPtr->preallocate_vw);
    };

    UUID uuid = storage.getStorageUUID();

    TableMetaEntryPtr meta_ptr = nullptr;

    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        auto it = active_tables.find(uuid);

        if (it == active_tables.end())
        {
            /// table is not in active table list, need load partition info;
            auto meta_lock_it = meta_lock_container.find(uuid);
            /// If the meta lock is already exists, reuse it.
            if (meta_lock_it != meta_lock_container.end())
            {
                meta_ptr = std::make_shared<TableMetaEntry>(storage.getDatabaseName(), storage.getTableName(), meta_lock_it->second);
            }
            else
            {
                meta_ptr = std::make_shared<TableMetaEntry>(storage.getDatabaseName(), storage.getTableName());
                /// insert the new meta lock into lock container.
                meta_lock_container.emplace(uuid, meta_ptr->meta_mutex);
            }
            active_tables.emplace(uuid, meta_ptr);
        }
    }

    if (meta_ptr)
    {
        load_nhut(meta_ptr);
        load_table_partitions(meta_ptr);

        /// may reload partition metrics.
        if (context.getServerType() == ServerType::cnch_server)
            meta_ptr->partition_metrics_loaded = false;

    }
}

bool PartCacheManager::trySetCachedNHUTForUpdate(const UUID & uuid, const UInt64 & pts)
{
    std::lock_guard<std::mutex> lock(cached_nhut_mutex);
    auto it = cached_nhut_for_update.find(uuid);
    if (it == cached_nhut_for_update.end())
    {
        cached_nhut_for_update.emplace(uuid, pts);
    }
    else
    {
        if (it->second == pts)
            return false;

        /// renew the cached NHUT;
        it->second = pts;
    }
    return true;
}

bool PartCacheManager::checkIfCacheValidWithNHUT(const UUID & uuid, const UInt64 & nhut)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        UInt64 cached_nhut = table_entry->cached_non_host_update_ts;
        if (cached_nhut == nhut && !table_entry->need_invalid_cache)
            return true;
        else if (cached_nhut < nhut)
        {
            table_entry->cached_non_host_update_ts.compare_exchange_strong(cached_nhut, nhut);
            table_entry->need_invalid_cache = true;
        }

        /// try invalid the part cache if the cached nhut is old enough;
        if (table_entry->need_invalid_cache && context.getPhysicalTimestamp() - table_entry->cached_non_host_update_ts > 9000)
        {
            LOG_DEBUG(&Poco::Logger::get("PartCacheManager::getTableMeta"), "invalid part cache for {}. NHUT is {}", UUIDHelpers::UUIDToString(uuid), table_entry->cached_non_host_update_ts);
            invalidPartCache(uuid);
        }

        return false;
    }
    else
        return true;
}

void PartCacheManager::updateTableNameInMetaEntry(const String & table_uuid, const String & database_name, const String & table_name)
{
    TableMetaEntryPtr table_entry = getTableMeta(UUID(stringToUUID(table_uuid)));
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->database = database_name;
        table_entry->table = table_name;
    }
}

TableMetaEntryPtr PartCacheManager::getTableMeta(const UUID & uuid)
{
    /* If non host server then returns nullptr.
     * If host server, then  returns table metadata.
     * */
    std::unique_lock<std::mutex> lock(cache_mutex);
    if (active_tables.find(uuid) == active_tables.end())
    {
        LOG_TRACE(&Poco::Logger::get("PartCacheManager::getTableMeta"), "Table id {} not found in active_tables", UUIDHelpers::UUIDToString(uuid));
        return nullptr;
    }

    return active_tables[uuid];
}

std::vector<TableMetaEntryPtr> PartCacheManager::getAllActiveTables()
{
    std::vector<TableMetaEntryPtr> res;
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto & [uuid, entry] : active_tables)
        res.push_back(entry);
    return res;
}

UInt64 PartCacheManager::getTableLastUpdateTime(const UUID & uuid)
{
    UInt64 last_update_time {0};
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        {
            auto lock = table_entry->readLock();
            last_update_time = table_entry->last_update_time;
        }
        if (last_update_time == 0)
        {
            UInt64 ts = context.tryGetTimestamp();
            auto lock = table_entry->writeLock();
            if (table_entry->last_update_time == 0)
            {
                if (ts!=TxnTimestamp::maxTS())
                    table_entry->last_update_time = ts;
            }
            last_update_time = table_entry->last_update_time;
        }
    }
    return last_update_time;
}

void PartCacheManager::setTableClusterStatus(const UUID & uuid, const bool clustered)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->is_clustered = clustered;
    }
}

bool PartCacheManager::getTableClusterStatus(const UUID & uuid)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);

    bool clustered;
    if (table_entry)
    {
        auto lock = table_entry->readLock();
        clustered =  table_entry->is_clustered;
    }
    else
        context.getCnchCatalog()->getTableClusterStatus(uuid, clustered);

    return clustered;
}

void PartCacheManager::setTablePreallocateVW(const UUID & uuid, const String vw)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);
    if (table_entry)
    {
        auto lock = table_entry->writeLock();
        table_entry->preallocate_vw = vw;
    }
}

String PartCacheManager::getTablePreallocateVW(const UUID & uuid)
{
    TableMetaEntryPtr table_entry = getTableMeta(uuid);

    String vw;
    if (table_entry)
    {
        auto lock = table_entry->readLock();
        vw =  table_entry->preallocate_vw;
    }
    else
        context.getCnchCatalog()->getTablePreallocateVW(uuid, vw);

    return vw;
}

bool PartCacheManager::getTablePartitionMetrics(const IStorage & table, std::unordered_map<String, PartitionFullPtr> & partitions, bool require_partition_info)
{
    TableMetaEntryPtr table_entry = getTableMeta(table.getStorageUUID());
    if (table_entry)
    {
        auto lock = table_entry->readLock();
        if (table_entry->partition_metrics_loaded)
        {
            auto * storage = dynamic_cast<const MergeTreeMetaBase*>(&table);
            if (!storage)
                return true;
            FormatSettings format_settings {};
            for (auto it = table_entry->partitions.begin(); it != table_entry->partitions.end(); it++)
            {
                PartitionFullPtr partition_ptr = std::make_shared<CnchPartitionInfoFull>(it->second);
                const auto & partition_key_sample = storage->getInMemoryMetadataPtr()->getPartitionKey().sample_block;
                if (partition_key_sample.columns() > 0 && require_partition_info)
                {
                    WriteBufferFromOwnString out;
                    partition_ptr->partition_info_ptr->partition_ptr->serializeText(*storage, out, format_settings);
                    partition_ptr->partition = out.str();
                    if (partition_key_sample.columns() == 1)
                    {
                        partition_ptr->first_partition = partition_ptr->partition;
                    }
                    else
                    {
                        WriteBufferFromOwnString out;
                        const DataTypePtr & type = partition_key_sample.getByPosition(0).type;
                        auto column = type->createColumn();
                        column->insert(partition_ptr->partition_info_ptr->partition_ptr->value[0]);
                        type->getDefaultSerialization()->serializeTextQuoted(*column, 0, out, format_settings);
                        partition_ptr->first_partition = out.str();
                    }
                }
                partitions.emplace(it->first, partition_ptr);
            }
            return true;
        }
    }
    return false;
}

bool PartCacheManager::getTablePartitions(const IStorage & storage, Catalog::PartitionMap & partitions)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(storage.getStorageUUID());
    if (meta_ptr)
    {
        auto table_lock = meta_ptr->readLock();
        partitions = meta_ptr->partitions;
        return true;
    }
    return false;
}

Strings PartCacheManager::getPartitionIDList(const IStorage & table)
{
    Catalog::PartitionMap partitions;
    getTablePartitions(table, partitions);
    Strings parrition_ids;

    for (auto it=partitions.begin(); it!=partitions.end(); it++)
        parrition_ids.push_back(it->first);

    return parrition_ids;
}

bool PartCacheManager::getPartitionList(const IStorage & table, std::vector<std::shared_ptr<MergeTreePartition>> & partition_list)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());

    if (meta_ptr)
    {
        auto table_lock = meta_ptr->readLock();
        for (auto it=meta_ptr->partitions.begin(); it!=meta_ptr->partitions.end(); it++)
            partition_list.push_back(it->second->partition_ptr);
        return true;
    }

    return false;
}

void PartCacheManager::invalidPartCache(const UUID & uuid)
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    invalidPartCacheWithoutLock(uuid, lock);
}

void PartCacheManager::invalidCacheWithNewTopology(const HostWithPortsVec & servers)
{
    // do nothing if servers is empty
    if (servers.empty())
        return;
    String rpc_port = std::to_string(context.getRPCPort());
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto it=active_tables.begin(); it!= active_tables.end();)
    {
        auto hashed_index = consistentHashForString(UUIDHelpers::UUIDToString(it->first), servers.size());
        if (!isLocalServer(servers[hashed_index].getRPCAddress(), rpc_port))
        {
            LOG_DEBUG(&Poco::Logger::get("PartCacheManager::invalidCacheWithNewTopology"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(it->first));
            partCachePtr->dropCache(it->first);
            it = active_tables.erase(it);
        }
        else
            it++;
    }
    /// reload active tables when topology change.
    active_table_loader->schedule();
}

void PartCacheManager::invalidPartCacheWithoutLock(const UUID & uuid, std::unique_lock<std::mutex> &)
{
    active_tables.erase(uuid);
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::invalidPartCacheWithoutLock"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(uuid));
    partCachePtr->dropCache(uuid);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const DataPartsVector & parts)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    /// TODO: optimized the lock here.
    std::unordered_map<String, DataPartsVector> partition_to_parts;
    for (auto & part : parts)
    {
        const String & partition_id = part->info.partition_id;
        auto it = partition_to_parts.find(partition_id);
        if (it != partition_to_parts.end())
        {
            it->second.emplace_back(part);
        }
        else
        {
            DataPartsVector part_list{part};
            partition_to_parts.emplace(partition_id, part_list);
        }
    }

    auto lock = meta_ptr->writeLock();

    UInt64 ts = context.tryGetTimestamp();

    for (auto it = partition_to_parts.begin(); it != partition_to_parts.end(); it++)
    {
        auto cached = partCachePtr->get({uuid, it->first});

        PartitionMetricsPtr partition_metrics{nullptr};
        auto found = meta_ptr->partitions.find(it->first);
        if (found != meta_ptr->partitions.end())
            partition_metrics = found->second->metrics_ptr;

        for (auto & part : it->second)
        {
            if (cached)
            {
                auto got = cached->find(part->name);
                if (got != cached->end())
                    cached->erase(got);
            }

            if (partition_metrics && !part->deleted && !part->isPartial())
            {
                meta_ptr->metrics_last_update_time = ts;
                if (partition_metrics->total_parts_number < 1
                    || partition_metrics->total_parts_size < part->bytes_on_disk
                    || partition_metrics->total_rows_count < part->rows_count)
                {
                    partition_metrics->total_parts_number = 0;
                    partition_metrics->total_parts_size = 0;
                    partition_metrics->total_rows_count = 0;
                    meta_ptr->partition_metrics_loaded =false;
                    break;
                }
                else
                {
                    partition_metrics->total_parts_number -= 1;
                    partition_metrics->total_parts_size -= part->bytes_on_disk;
                    partition_metrics->total_rows_count -= part->rows_count;
                }
            }
        }
    }
}

void PartCacheManager::insertDataPartsIntoCache(const IStorage & table, const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model, const bool is_merged_parts, const bool should_update_metrics)
{
    /// Only cache MergeTree tables
    if (dynamic_cast<const MergeTreeMetaBase*>(&table) == nullptr)
        return;
    mayUpdateTableMeta(table);
    UUID uuid = table.getStorageUUID();
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);
    if (meta_ptr)
    {
        UInt64 ts = context.tryGetTimestamp();
        auto table_lock = meta_ptr->writeLock();
        for (auto & part_model : parts_model)
        {
            auto & storage = dynamic_cast<const MergeTreeMetaBase&>(table);
            auto partition_ptr = createParitionFromMetaString(storage, part_model.partition_minmax());
            String partition_id = partition_ptr->getID(storage);
            Catalog::PartitionMap::iterator it = meta_ptr->partitions.emplace(partition_id, std::make_shared<CnchPartitionInfo>(partition_ptr)).first;
            if (should_update_metrics && !part_model.part_info().hint_mutation() && (!part_model.has_deleted() || !part_model.deleted()))
            {
                meta_ptr->metrics_last_update_time = ts;
                it->second->metrics_ptr->total_parts_number += 1;
                it->second->metrics_ptr->total_parts_size += part_model.size();
                it->second->metrics_ptr->total_rows_count += part_model.rows_count();
            }
            /// insert into cache directly if cache status of current partition is not UINIT;
            if (it->second->cache_status != CacheStatus::UINIT)
            {
                auto part_wrapper_ptr = createPartWrapperFromModel(storage, part_model);
                partCachePtr->insert({uuid, partition_id}, part_wrapper_ptr->name, part_wrapper_ptr);
            }
        }
        if (!is_merged_parts)
            meta_ptr->last_update_time = (ts==TxnTimestamp::maxTS()) ? 0: ts;
    }
    else
    {
        throw Exception("Table is not initialized before save parts into cache.", ErrorCodes::UNKNOWN_STORAGE);
    }
}

void PartCacheManager::reloadPartitionMetrics(const UUID & uuid, const TableMetaEntryPtr & table_meta)
{
    table_meta->loading_metrics = true;
    try
    {
        auto cnch_catalog = context.getCnchCatalog();
        auto partitions_ = cnch_catalog->getTablePartitionMetricsFromMetastore(UUIDHelpers::UUIDToString(uuid));

        {
            size_t total_parts_number {0};
            auto lock = table_meta->writeLock();
            for (auto it = table_meta->partitions.begin(); it != table_meta->partitions.end(); it++)
            {
                const String & partition_id = it->first;
                auto found = partitions_.find(partition_id);
                /// update metrics if we have recalculated it for current partition
                if (found != partitions_.end())
                    it->second->metrics_ptr = found->second;

                total_parts_number += it->second->metrics_ptr->total_parts_number;
            }
            table_meta->metrics_last_update_time = context.tryGetTimestamp(__PRETTY_FUNCTION__);
            table_meta->partition_metrics_loaded = true;
            /// reset load_parts_by_partition if parts number of current table is less than 5 million;
            if (table_meta->load_parts_by_partition && total_parts_number<5000000)
                table_meta->partition_metrics_loaded = false;
        }
    }
    catch (...)
    {
        tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        LOG_ERROR(&Poco::Logger::get(__func__), "Reload partition metric failed.");
    }
    table_meta->loading_metrics = false;
}

void PartCacheManager::cleanMetaLock()
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto it = meta_lock_container.begin(); it!=meta_lock_container.end();)
    {
        /// remove the meta_lock if it is not used elsewhere.
        if (it->second.unique())
        {
            it = meta_lock_container.erase(it);
        }
        else
        {
            it++;
        }
    }
}

void PartCacheManager::loadActiveTables()
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Reloading active tables.");
    auto cnch_catalog = context.getCnchCatalog();
    auto tables_meta = cnch_catalog->getAllTables();

    auto rpc_port = context.getRPCPort();
    for (auto & table_meta : tables_meta)
    {
        if (table_meta.database() == "cnch_system" || table_meta.database() == "system" || Status::isDeleted(table_meta.status()))
            continue;
        auto entry = getTableMeta(RPCHelpers::createUUID(table_meta.uuid()));
        if (!entry)
        {
            StoragePtr table = Catalog::CatalogFactory::getTableByDataModel(context.shared_from_this(), &table_meta);

            auto host_port = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(table->getStorageUUID()), true);
            if (host_port.empty())
                continue;

            if (isLocalServer(host_port.getRPCAddress(), toString(rpc_port)))
                mayUpdateTableMeta(*table);
        }
    }
}

void PartCacheManager::updateTablePartitionsMetrics(bool skip_if_already_loaded)
{
    auto cnch_catalog = context.getCnchCatalog();

    std::unordered_map<UUID, TableMetaEntryPtr> tables_snapshot;
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        tables_snapshot = active_tables;
    }

    for (auto it = tables_snapshot.begin(); it != tables_snapshot.end(); it++)
    {
        if (it->second->loading_metrics || (skip_if_already_loaded && it->second->partition_metrics_loaded))
            continue;
        UUID uuid = it->first;
        TableMetaEntryPtr meta_ptr = it->second;
        context.getPartCacheManagerThreadPool().trySchedule([this, uuid, meta_ptr]() {reloadPartitionMetrics(uuid, meta_ptr);});
    }
}

inline static bool isVisible(const DB::DataModelPartWrapperPtr & part_wrapper_ptr, const UInt64 & ts)
{
    return ts == 0
        || (UInt64(part_wrapper_ptr->part_model->part_info().mutation()) <= ts
            && part_wrapper_ptr->part_model->commit_time() <= ts);
}

DB::ServerDataPartsVector PartCacheManager::getOrSetServerDataPartsInPartitions(
    const IStorage & table, const Strings & partitions,
    PartCacheManager::LoadPartsFunc && load_func, const UInt64 & ts)
{
    ServerDataPartsVector res;
    mayUpdateTableMeta(table);
    auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());

    if (!meta_ptr)
        return res;

    Strings all_existing_partitions = getPartitionIDList(table);

    /// On cnch worker, we disable part cache to avoid cache synchronization with server.
    if (context.getServerType() != ServerType::cnch_server)
    {
        DataModelPartPtrVector fetched = load_func(partitions, all_existing_partitions);
        for (auto & part_model_ptr : fetched)
        {
            auto part_wrapper_ptr = createPartWrapperFromModel(storage, *part_model_ptr);
            if (isVisible(part_wrapper_ptr, ts))
                res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
        }
        return res;
    }

    if (meta_ptr->load_parts_by_partition)
        res = getServerPartsByPartition(storage, meta_ptr, partitions, all_existing_partitions, load_func, ts);
    else
        res = getServerPartsInternal(storage, meta_ptr, partitions, all_existing_partitions, load_func, ts);

    return res;
}

static const size_t LOG_PARTS_SIZE = 100000;

static void logPartsVector(const MergeTreeMetaBase & storage, const ServerDataPartsVector & res)
{
    if (unlikely(res.size() % LOG_PARTS_SIZE == 0))
        LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "{} getting parts and now loaded {} parts in memory", storage.getStorageID().getNameForLogs(), res.size());
}

DB::ServerDataPartsVector PartCacheManager::getServerPartsInternal(
    const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr, const Strings & partitions,
    const Strings & all_existing_partitions, PartCacheManager::LoadPartsFunc & load_func, const UInt64 & ts)
{
    ServerDataPartsVector res;
    UUID uuid = storage.getStorageUUID();

    DataModelPartWrapperVector data_from_cache;
    Strings partitions_not_cached;
    {
        auto lock = meta_ptr->writeLock();
        for (auto & partition_id : partitions)
        {
            /// required partition may not exist. skip it.
            if (!meta_ptr->partitions.count(partition_id))
                continue;

            PartitionInfoPtr partition_info_ptr = meta_ptr->partitions[partition_id];
            auto cached = partCachePtr->get({uuid, partition_id});

            if (partition_info_ptr->cache_status == CacheStatus::LOADED && cached)
            {
                for (auto it = cached->begin(); it != cached->end(); it++)
                    data_from_cache.push_back(it->second);
            }
            else
            {
                /// its okay if other task is loading the same partition.
                partition_info_ptr->cache_status = CacheStatus::LOADING;
                partitions_not_cached.push_back(partition_id);
            }
        }
    }

    for (auto & data_wrapper_ptr : data_from_cache)
    {
        if (isVisible(data_wrapper_ptr, ts))
        {
            res.push_back(std::make_shared<ServerDataPart>(data_wrapper_ptr));
            logPartsVector(storage, res);
        }
    }

    if (partitions_not_cached.empty())
        return res;

    try
    {
        /// Save data part model as well as data part to avoid build them with metaentry lock.
        std::unordered_map<String, DataModelPartWrapperVector> partition_to_parts;
        DataModelPartPtrVector fetched = load_func(partitions_not_cached, all_existing_partitions);

        /// The load_func may include partitions that not in the required `partitions_not_cache`
        /// Need to have an extra filter
        std::unordered_set<String> partitions_set(partitions_not_cached.begin(), partitions_not_cached.end());

        for (auto & part_model_ptr : fetched)
        {
            auto part_wrapper_ptr = createPartWrapperFromModel(storage, *part_model_ptr);
            const auto & partition_id = part_wrapper_ptr->info->partition_id;
            if (!partitions_set.contains(partition_id))
                continue;
            auto it = partition_to_parts.find(partition_id);
            if (it != partition_to_parts.end())
                it->second.emplace_back(part_wrapper_ptr);
            else
                partition_to_parts[partition_id] = DataModelPartWrapperVector{part_wrapper_ptr};
        }

        /// merge fetched parts with that in cache;
        {
            auto lock = meta_ptr->writeLock();
            for (auto & [partition_id, parts_wrapper_vector] : partition_to_parts)
            {
                auto cached = partCachePtr->get({uuid, partition_id});
                for (const auto & part_wrapper_ptr : parts_wrapper_vector)
                {
                    if (cached)
                    {
                        auto it = cached->find(part_wrapper_ptr->name);
                        // do not update cache if the cached data is newer than bytekv.
                        if (it == cached->end() || it->second->part_model->commit_time() < part_wrapper_ptr->part_model->commit_time())
                        {
                            partCachePtr->insert({uuid, partition_id}, part_wrapper_ptr->name, part_wrapper_ptr);
                        }
                    }
                    else
                    {
                        partCachePtr->insert({uuid, partition_id}, part_wrapper_ptr->name, part_wrapper_ptr);
                    }

                    /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                    if (isVisible(part_wrapper_ptr, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                        logPartsVector(storage, res);
                    }
                }

                // change CacheStatus to LOADED only if it is LOADING. Other task may fail to fetch and change CacheStatus to UINIT before.
                if (meta_ptr->partitions[partition_id]->cache_status == CacheStatus::LOADING)
                    meta_ptr->partitions[partition_id]->cache_status = CacheStatus::LOADED;
            }
        }
    }
    catch (Exception & e)
    {
        /// fetch parts timeout, we change to fetch by partition;
        if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
            meta_ptr->load_parts_by_partition = true;

        // change the partitino cache status to UINIT if loading failed.
        for (auto & partition_id : partitions_not_cached)
        {
            if (meta_ptr->partitions[partition_id]->cache_status == CacheStatus::LOADING)
                meta_ptr->partitions[partition_id]->cache_status = CacheStatus::UINIT;
        }
        throw e;
    }

    return res;
}

ServerDataPartsVector PartCacheManager::getServerPartsByPartition(const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr,
    const Strings & partitions, const Strings & all_existing_partitions,
    PartCacheManager::LoadPartsFunc & load_func, const UInt64 & ts)
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Get parts by partitions for table : {}", storage.getLogName());
    Stopwatch watch;
    ServerDataPartsVector res;
    UUID uuid = storage.getStorageUUID();

    for (size_t i = 0; i<partitions.size();)
    {
        String partition_id = partitions[i];
        bool need_load_parts = false;
        bool goto_next_partition = false;
        PartitionInfoPtr partition_info_ptr;
        DataModelPartWrapperVector data_from_cache;

        {
            auto lock = meta_ptr->writeLock();
             /// required partition may not exist. skip it.
            if (!meta_ptr->partitions.count(partition_id))
            {
                i++;
                continue;
            }

            partition_info_ptr = meta_ptr->partitions[partition_id];
            auto cached = partCachePtr->get({uuid, partition_id});

            if (partition_info_ptr->cache_status == CacheStatus::LOADED && cached)
            {
                for (auto it = cached->begin(); it != cached->end(); it++)
                    data_from_cache.push_back(it->second);

                /// already get parts from cache, continue to next partition
                i++;
                goto_next_partition = true;
            }
            else if (partition_info_ptr->cache_status == CacheStatus::LOADED || partition_info_ptr->cache_status == CacheStatus::UINIT)
            {
                partition_info_ptr->cache_status = CacheStatus::LOADING;
                need_load_parts = true;
            }
        }

        if (goto_next_partition)
        {
            for (auto & data_wrapper_ptr : data_from_cache)
            {
                if (isVisible(data_wrapper_ptr, ts))
                {
                    res.push_back(std::make_shared<ServerDataPart>(data_wrapper_ptr));
                    logPartsVector(storage, res);
                }
            }
            /// Data part in current partition have been loaded from cache, continue loading next partition.
            continue;
        }

        /// Now cache status must be LOADING;
        /// need to load parts from metastore
        if (need_load_parts)
        {
            DataModelPartPtrVector fetched;
            try
            {
                std::unordered_map<String, DataModelPartWrapperVector> partition_to_parts;
                fetched = load_func({partition_id}, all_existing_partitions);
                DataModelPartWrapperVector fetched_data;
                for (auto & dataModelPartPtr : fetched)
                {
                    fetched_data.push_back(createPartWrapperFromModel(storage, *dataModelPartPtr));
                }

                auto lock = meta_ptr->writeLock();
                /// It happens that new parts have been inserted into cache during loading parts from bytekv, we need merge them to make
                /// sure the cache contains all parts of the partition.
                auto cached = partCachePtr->get({uuid, partition_id});
                if (!cached)
                {
                    /// Insert a Map to the cache to ensure the partition id exists in LRU even if there is no part
                    cached = std::make_shared<DataPartModelsMap>();
                    partCachePtr->insert({uuid, partition_id}, cached);
                }

                for (auto & data_wrapper_ptr : fetched_data)
                {
                    auto final_wrapper_ptr = data_wrapper_ptr;
                    String partition_id = data_wrapper_ptr->info->partition_id;

                    auto it = cached->find(data_wrapper_ptr->name);
                    // do not update cache if the cached data is newer than bytekv.
                    if (it != cached->end() && it->second->part_model->commit_time() > data_wrapper_ptr->part_model->commit_time())
                    {
                        final_wrapper_ptr = createPartWrapperFromModel(storage, *(it->second->part_model));
                    }
                    else
                    {
                        partCachePtr->insert({uuid, partition_id}, data_wrapper_ptr->name, data_wrapper_ptr);
                    }

                    /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                    if (isVisible(final_wrapper_ptr, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(final_wrapper_ptr));
                        logPartsVector(storage, res);
                    }
                }

                partition_info_ptr->cache_status = CacheStatus::LOADED;

                /// go to next partition;
                i++;
            }
            catch(Exception & e)
            {
                /// change cache status to UINIT if exception occurs during fetch.
                auto lock = meta_ptr->writeLock();
                partition_info_ptr->cache_status = CacheStatus::UINIT;
                throw e;
            }

            /// Finish fetching parts, notify other waiting tasks if any.
            {
                std::unique_lock<std::mutex> lock(meta_ptr->fetch_mutex);
                meta_ptr->fetch_cv.notify_all();
            }
        }
        else /// other task is fetching parts now, just wait for the result
        {
            {
                std::unique_lock<std::mutex> lock(meta_ptr->fetch_mutex);
                meta_ptr->fetch_cv.wait_for(lock, std::chrono::milliseconds(5000), [&partition_info_ptr]() {return partition_info_ptr->cache_status == CacheStatus::LOADED;});
            }

            auto lock = meta_ptr->readLock();
            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = partCachePtr->get({uuid, partition_id});
                if (!cached)
                {
                    throw Exception("Cannot get already loaded parts from cache. Its a logic error.", ErrorCodes::LOGICAL_ERROR);
                }
                for (auto it = cached->begin(); it != cached->end(); it++)
                {
                    DataPartPtr datapart_ptr = createPartFromModel(storage, *(it->second->part_model));
                    /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                    if (isVisible(it->second, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(it->second));
                        logPartsVector(storage, res);
                    }
                }
                i++;
            }
            // if cache status does not change to loaded, get parts of current partition again.
        }

        /// stop if fetch part time exceeds the query max execution time.
        checkTimeLimit(watch);
    }

    return res;
}

void PartCacheManager::checkTimeLimit(Stopwatch & watch)
{
    if (CurrentThread::getQueryId().toString().empty())
        return;

    auto thread_group = CurrentThread::getGroup();

    if (!thread_group)
        return;

    if (auto query_context = thread_group->query_context.lock())
    {
        if (query_context->getSettingsRef().max_execution_time.totalSeconds() < watch.elapsedSeconds())
        {
            throw Exception("Get parts timeout over query max execution time.", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }
}

std::pair<UInt64, UInt64> PartCacheManager::dumpPartCache()
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    return {partCachePtr->count(), partCachePtr->weight()};
}

std::unordered_map<String, std::pair<size_t, size_t>> PartCacheManager::getTableCacheInfo()
{
    CnchDataPartCachePtr cachePtr = nullptr;
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        cachePtr = partCachePtr;
    }
    if (!cachePtr)
        return {};

    return cachePtr->getTableCacheInfo();
}

void PartCacheManager::reset()
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::reset"), "Resetting part cache manager.");
    std::unique_lock<std::mutex> lock(cache_mutex);
    active_tables.clear();
    partCachePtr->reset();
    /// reload active tables when topology change.
    active_table_loader->schedule();
}

void PartCacheManager::shutDown()
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::shutdown"), "Shutdown method of part cache manager called.");
    metrics_updater->deactivate();
    metrics_initializer->deactivate();
    active_table_loader->deactivate();
    meta_lock_cleaner->deactivate();
}

}
