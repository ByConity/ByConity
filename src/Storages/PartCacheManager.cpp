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

#include <Storages/PartCacheManager.h>

#include <Core/Types.h>
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
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

Catalog::PartitionMap TableMetaEntry::getPartitions(const Strings & wanted_partition_ids)
{
    Catalog::PartitionMap res;
    if (wanted_partition_ids.size() < 100)
    {
        /// When wanted_partition_ids is small, then go with find
        for (const auto & partition_id : wanted_partition_ids)
        {
            if (auto it = partitions.find(partition_id); it != partitions.end())
                res.emplace(partition_id, *it);
        }
    }
    else
    {
        /// Otherwise, leverage wait free scan
        std::unordered_set<String> wanted_partition_set;
        for (const auto & partition_id : wanted_partition_ids)
            wanted_partition_set.insert(partition_id);
        
        for (auto it = partitions.begin(); it != partitions.end(); it++)
        {
            auto & partition_info_ptr = *it;
            if (wanted_partition_set.contains(partition_info_ptr->partition_id))
                res.emplace(partition_info_ptr->partition_id, partition_info_ptr);
        }
    }
    return res;
}

Strings TableMetaEntry::getPartitionIDs()
{
    Strings partition_ids;
    partition_ids.reserve(partitions.size());
    for (auto it = partitions.begin(); it != partitions.end(); it++)
        partition_ids.push_back((*it)->partition_id);
    return partition_ids;
}

std::vector<std::shared_ptr<MergeTreePartition>> TableMetaEntry::getPartitionList()
{
    std::vector<std::shared_ptr<MergeTreePartition>> partition_list;
    partition_list.reserve(partitions.size());
    for (auto it = partitions.begin(); it != partitions.end(); it++)
        partition_list.push_back((*it)->partition_ptr);
    return partition_list;
}

PartCacheManager::PartCacheManager(ContextMutablePtr context_)
    : WithMutableContext(context_)
{
    part_cache_ptr = std::make_shared<CnchDataPartCache>(getContext()->getConfigRef().getUInt("size_of_cached_parts", 100000));
    metrics_updater = getContext()->getSchedulePool().createTask("PartMetricsUpdater",[this](){
        try
        {
            updateTablePartitionsMetrics(false);
        }
        catch(...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        /// schedule every 24 hours, maybe could be configurable later
        this->metrics_updater->scheduleAfter(24 * 60 * 60 * 1000);
    });
    metrics_initializer = getContext()->getSchedulePool().createTask("PartMetricsInitializer",[this](){
        try
        {
            updateTablePartitionsMetrics(true);
        }
        catch(...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        /// schedule every 3 seconds
        this->metrics_initializer->scheduleAfter(3 * 1000);
    });
    meta_lock_cleaner = getContext()->getSchedulePool().createTask("MetaLockCleaner", [this](){
        try
        {
            cleanMetaLock();
        }
        catch(...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        /// schedule every hour.
        this->meta_lock_cleaner->scheduleAfter(3 * 1000);
    });
    active_table_loader = getContext()->getSchedulePool().createTask("ActiveTablesLoader", [this](){
        // load tables when server start up.
        try
        {
            loadActiveTables();
        }
        catch(...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
    });
    if (getContext()->getServerType() == ServerType::cnch_server)
    {
        metrics_updater->activate();
        metrics_updater->scheduleAfter(60 * 60 * 1000);
        metrics_initializer->activateAndSchedule();
        meta_lock_cleaner->activateAndSchedule();
        active_table_loader->activateAndSchedule();
    }
}

PartCacheManager::~PartCacheManager()
{
    try
    {
        shutDown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void PartCacheManager::mayUpdateTableMeta(const IStorage & storage)
{
    /* Fetches partitions from metastore if storage is not present in active_tables*/

    /// Only handle MergeTree tables
    const auto * cnch_table = dynamic_cast<const MergeTreeMetaBase*>(&storage);
    if (!cnch_table)
        return;

    auto load_nhut = [&](TableMetaEntryPtr & meta_ptr)
    {
        if (getContext()->getSettingsRef().server_write_ha)
        {
            UInt64 pts = getContext()->getPhysicalTimestamp();
            if (pts)
            {
                UInt64 fetched_nhut = getContext()->getCnchCatalog()->getNonHostUpdateTimestampFromByteKV(storage.getStorageUUID());
                if (pts - fetched_nhut > 9)
                    meta_ptr->cached_non_host_update_ts = fetched_nhut;
            }
        }
    };

    auto load_table_partitions = [&](TableMetaEntryPtr & meta_ptr)
    {
        auto table_lock = meta_ptr->writeLock();
        getContext()->getCnchCatalog()->getPartitionsFromMetastore(*cnch_table, meta_ptr->partitions);
        getContext()->getCnchCatalog()->getTableClusterStatus(storage.getStorageUUID(), meta_ptr->is_clustered);
        getContext()->getCnchCatalog()->getTablePreallocateVW(storage.getStorageUUID(), meta_ptr->preallocate_vw);
        meta_ptr->table_definition_hash = storage.getTableHashForClusterBy();
    };

    UUID uuid = storage.getStorageUUID();
    String server_vw_name = cnch_table->getSettings()->cnch_server_vw;

    TableMetaEntryPtr meta_ptr = nullptr;

    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        auto it = active_tables.find(uuid);

        if (it == active_tables.end() || it->second->server_vw_name != server_vw_name)
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
            meta_ptr->server_vw_name = server_vw_name;
            active_tables.emplace(uuid, meta_ptr);
        }
        else
        {
            // update table_definition_hash for active tables
            it->second->table_definition_hash = storage.getTableHashForClusterBy();
        }
    }

    if (meta_ptr)
    {
        load_nhut(meta_ptr);
        load_table_partitions(meta_ptr);

        /// may reload partition metrics.
        if (getContext()->getServerType() == ServerType::cnch_server)
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
        if (table_entry->need_invalid_cache && getContext()->getPhysicalTimestamp() - table_entry->cached_non_host_update_ts > 9000)
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
            UInt64 ts = getContext()->tryGetTimestamp();
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
        auto table = getContext()->getCnchCatalog()->getTableByUUID(*getContext(), UUIDHelpers::UUIDToString(uuid), TxnTimestamp::maxTS());
        if (table)
            table_entry->table_definition_hash = table->getTableHashForClusterBy();
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
        getContext()->getCnchCatalog()->getTableClusterStatus(uuid, clustered);

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
        getContext()->getCnchCatalog()->getTablePreallocateVW(uuid, vw);

    return vw;
}

bool PartCacheManager::getTablePartitionMetrics(const IStorage & i_storage, std::unordered_map<String, PartitionFullPtr> & partitions, bool require_partition_info)
{
    TableMetaEntryPtr table_entry = getTableMeta(i_storage.getStorageUUID());
    if (table_entry)
    {
        if (table_entry->partition_metrics_loaded)
        {
            const auto * storage = dynamic_cast<const MergeTreeMetaBase*>(&i_storage);
            if (!storage)
                return true;
            FormatSettings format_settings {};
            auto & table_partitions = table_entry->partitions;
            for (auto it = table_partitions.begin(); it != table_partitions.end(); it++)
            {
                PartitionFullPtr partition_ptr = std::make_shared<CnchPartitionInfoFull>(*it);
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
                partitions.emplace(partition_ptr->partition_info_ptr->partition_id, partition_ptr);
            }
            return true;
        }
    }
    return false;
}

bool PartCacheManager::getPartitionList(const IStorage & table, std::vector<std::shared_ptr<MergeTreePartition>> & partition_list)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());
    if (meta_ptr)
    {
        partition_list = meta_ptr->getPartitionList();
        return true;
    }
    return false;
}

bool PartCacheManager::getPartitionIDs(const IStorage & storage, std::vector<String> & partition_ids)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(storage.getStorageUUID());

    if (meta_ptr)
    {
        partition_ids = meta_ptr->getPartitionIDs();
        return true;
    }

    return false;
}

void PartCacheManager::invalidPartCache(const UUID & uuid)
{
    std::unique_lock<std::mutex> lock(cache_mutex);
    invalidPartCacheWithoutLock(uuid, lock);
}

void PartCacheManager::invalidCacheWithNewTopology(const CnchServerTopology & topology)
{
    // do nothing if servers is empty
    if (topology.empty())
        return;
    String rpc_port = std::to_string(getContext()->getRPCPort());
    std::unique_lock<std::mutex> lock(cache_mutex);
    for (auto it=active_tables.begin(); it!= active_tables.end();)
    {
        auto server = topology.getTargetServer(UUIDHelpers::UUIDToString(it->first), it->second->server_vw_name);
        if (!isLocalServer(server.getRPCAddress(), rpc_port))
        {
            LOG_DEBUG(&Poco::Logger::get("PartCacheManager::invalidCacheWithNewTopology"), "Dropping part cache of {}", UUIDHelpers::UUIDToString(it->first));
            part_cache_ptr->dropCache(it->first);
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
    part_cache_ptr->dropCache(uuid);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const TableMetaEntryPtr & meta_ptr, const std::unordered_map<String, Strings> & partition_to_parts)
{
    auto lock = meta_ptr->writeLock();

    for (const auto & partition_to_part : partition_to_parts)
    {
        auto cached = part_cache_ptr->get({uuid, partition_to_part.first});

        for (const auto & part_name : partition_to_part.second)
        {
            if (cached)
            {
                auto got = cached->find(part_name);
                if (got != cached->end())
                    cached->erase(part_name);
            }
        }
    }
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const DataPartsVector & parts)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    std::unordered_map<String, Strings> partition_to_parts;
    Strings partition_ids;
    for (auto & part : parts)
    {
        const String & partition_id = part->info.partition_id;
        auto it = partition_to_parts.find(partition_id);
        if (it != partition_to_parts.end())
        {
            it->second.emplace_back(part->name);
        }
        else
        {
            Strings part_list{part->name};
            partition_to_parts.emplace(partition_id, part_list);
            partition_ids.push_back(partition_id);
        }
    }
    invalidPartCache(uuid, meta_ptr, partition_to_parts);
}

void PartCacheManager::invalidPartCache(const UUID & uuid, const Strings & part_names, MergeTreeDataFormatVersion version)
{
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);

    if (!meta_ptr)
        return;

    std::unordered_map<String, Strings> partition_to_parts;
    for (const auto & part_name : part_names)
    {
        MergeTreePartInfo part_info = MergeTreePartInfo::fromPartName(part_name, version);
        String partition_id = part_info.partition_id;
        auto it = partition_to_parts.find(partition_id);
        if (it != partition_to_parts.end())
        {
            it->second.emplace_back(part_name);
        }
        else
        {
            Strings part_list{part_name};
            partition_to_parts.emplace(partition_id, part_list);
        }
    }
    invalidPartCache(uuid, meta_ptr, partition_to_parts);
}

void PartCacheManager::insertDataPartsIntoCache(const IStorage & table, const pb::RepeatedPtrField<Protos::DataModelPart> & parts_model, const bool is_merged_parts, const bool should_update_metrics)
{
    /// Only cache MergeTree tables
    if (!dynamic_cast<const MergeTreeMetaBase*>(&table))
        return;

    mayUpdateTableMeta(table);
    UUID uuid = table.getStorageUUID();
    TableMetaEntryPtr meta_ptr = getTableMeta(uuid);
    if (!meta_ptr)
    {
        throw Exception("Table is not initialized before save parts into cache.", ErrorCodes::UNKNOWN_STORAGE);
    }

    /// Do all parse jobs outside lock
    std::unordered_map<String, DataModelPartWrapperVector> partitionid_to_parts;
    std::unordered_map<String, std::shared_ptr<MergeTreePartition>> partitionid_to_partition;
    auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    for (auto & part_model : parts_model)
    {
        auto part_wrapper_ptr = createPartWrapperFromModel(storage, part_model);
        const auto & partition_id = part_wrapper_ptr->info->partition_id;
        if (!partitionid_to_partition.contains(partition_id))
            partitionid_to_partition[partition_id] = createParitionFromMetaString(storage, part_model.partition_minmax());
        auto it = partitionid_to_parts.find(partition_id);
        if (it != partitionid_to_parts.end())
            it->second.emplace_back(part_wrapper_ptr);
        else
            partitionid_to_parts[partition_id] = DataModelPartWrapperVector{part_wrapper_ptr};
    }

    UInt64 ts = getContext()->tryGetTimestamp();
    auto update_metrics = [&](const std::shared_ptr<Protos::DataModelPart> & model, std::shared_ptr<PartitionMetrics> metrics_ptr) {
        if (!meta_ptr->partition_metrics_loaded)
            return;
        meta_ptr->metrics_last_update_time = ts;
        metrics_ptr->update(*model);
        if (!metrics_ptr->validateMetrics())
            meta_ptr->partition_metrics_loaded = false;
    };

    /// Get or create partitions from meta_ptr
    Strings partition_ids;
    partition_ids.reserve(partitionid_to_parts.size());
    for (const auto & pair : partitionid_to_parts)
        partition_ids.push_back(pair.first);
    auto meta_partitions = meta_ptr->getPartitions(partition_ids);
    /// Create when there are new partitions
    if (meta_partitions.size() < partitionid_to_parts.size())
    {
        auto & partitions = meta_ptr->partitions;
        for (auto & p : partitionid_to_parts)
        {
            const auto & partition_id = p.first;
            if (meta_partitions.contains(partition_id))
                continue;
            auto it = partitions.emplace(partition_id
                , std::make_shared<CnchPartitionInfo>(partitionid_to_partition[partition_id], partition_id)).first;
            meta_partitions.emplace(partition_id, *it);
        }
    }

    for (auto & [partition_id, parts_wrapper_vector] : partitionid_to_parts)
    {
        auto & partition_info_ptr = meta_partitions[partition_id];
        auto partition_write_lock = partition_info_ptr->writeLock();
        bool need_insert_into_cache = false;
        /// Check if new parts should be inserted into cache; Skip if cache status is UINIT
        if (partition_info_ptr->cache_status != CacheStatus::UINIT)
        {
            need_insert_into_cache = true;
            /// Check whether the partition cache has been invalidate by LRU.
            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    partition_info_ptr->cache_status = CacheStatus::UINIT;
                    need_insert_into_cache = false;
                }
            }
        }

        if (need_insert_into_cache)
        {
            auto cached = part_cache_ptr->get({uuid, partition_id});
            if (!cached)
            {
                /// directly insert all new parts into cache.
                cached = std::make_shared<DataPartModelsMap>();
                cached->insert(parts_wrapper_vector, [](const DataModelPartWrapperPtr & part_wrapper_ptr) { return part_wrapper_ptr->name; });
                part_cache_ptr->insert({uuid, partition_id}, cached);
            }
            else
            {
                cached->insert(parts_wrapper_vector, [](const DataModelPartWrapperPtr & part_wrapper_ptr) { return part_wrapper_ptr->name; });
                /// Force LRU cache update status (weight/evict).
                part_cache_ptr->set({uuid, partition_id}, cached);
            }
        }
        if (should_update_metrics)
        {
            for (const auto & part_wrapper_ptr : parts_wrapper_vector)
            {
                update_metrics(part_wrapper_ptr->part_model, partition_info_ptr->metrics_ptr);
            }
        }
    }

    if (!is_merged_parts)
        meta_ptr->last_update_time = (ts==TxnTimestamp::maxTS()) ? 0: ts;
}

void PartCacheManager::reloadPartitionMetrics(const UUID & uuid, const TableMetaEntryPtr & table_meta)
{
    table_meta->loading_metrics = true;
    auto current_table_definition_hash = table_meta->table_definition_hash.load(); // store table TDH used for comparison with part TDH as it table TDH may change during comparison
    try
    {
        auto cnch_catalog = getContext()->getCnchCatalog();
        auto partitions = cnch_catalog->getTablePartitionMetricsFromMetastore(UUIDHelpers::UUIDToString(uuid));
        auto is_fully_clustered = true;
        {
            size_t total_parts_number {0};
            auto & meta_partitions = table_meta->partitions;
            for (auto it = meta_partitions.begin(); it != meta_partitions.end(); it++)
            {
                auto & partition_info_ptr = *it;
                const String & partition_id = partition_info_ptr->partition_id;
                auto found = partitions.find(partition_id);
                /// update metrics if we have recalculated it for current partition
                if (found != partitions.end())
                    partition_info_ptr->metrics_ptr = found->second;

                total_parts_number += partition_info_ptr->metrics_ptr->total_parts_number;
                if (!partition_info_ptr->metrics_ptr->is_deleted)
                {
                    auto is_partition_clustered = (partition_info_ptr->metrics_ptr->is_single_table_definition_hash && partition_info_ptr->metrics_ptr->table_definition_hash == current_table_definition_hash) && !partition_info_ptr->metrics_ptr->has_bucket_number_neg_one;
                    if(!is_partition_clustered)
                        is_fully_clustered = false;
                }
            }
            table_meta->metrics_last_update_time = getContext()->tryGetTimestamp(__PRETTY_FUNCTION__);
            table_meta->partition_metrics_loaded = true;
            /// reset load_parts_by_partition if parts number of current table is less than 5 million;
            if (table_meta->load_parts_by_partition && total_parts_number<5000000)
                table_meta->load_parts_by_partition = false;
            if (is_fully_clustered && table_meta->is_clustered == false)
                cnch_catalog->setTableClusterStatus(uuid, is_fully_clustered, current_table_definition_hash);
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
    auto tables_meta = getContext()->getCnchCatalog()->getAllTables();
    if (tables_meta.empty())
        return;
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Reloading {} active tables.", tables_meta.size());

    auto rpc_port = getContext()->getRPCPort();
    for (auto & table_meta : tables_meta)
    {
        if (table_meta.database() == "cnch_system" || table_meta.database() == "system" || Status::isDeleted(table_meta.status()))
            continue;

        auto entry = getTableMeta(RPCHelpers::createUUID(table_meta.uuid()));
        if (!entry)
        {
            StoragePtr table = Catalog::CatalogFactory::getTableByDataModel(getContext(), &table_meta);

            auto host_port = getContext()->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), true);
            if (host_port.empty())
                continue;

            if (isLocalServer(host_port.getRPCAddress(), toString(rpc_port)))
                mayUpdateTableMeta(*table);
        }
    }
}

void PartCacheManager::updateTablePartitionsMetrics(bool skip_if_already_loaded)
{
    auto cnch_catalog = getContext()->getCnchCatalog();

    std::unordered_map<UUID, TableMetaEntryPtr> tables_snapshot;
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        tables_snapshot = active_tables;
    }

    for (auto & table_snapshot : tables_snapshot)
    {
        if (table_snapshot.second->loading_metrics || (skip_if_already_loaded && table_snapshot.second->partition_metrics_loaded))
            continue;
        UUID uuid = table_snapshot.first;
        TableMetaEntryPtr meta_ptr = table_snapshot.second;
        getContext()->getPartCacheManagerThreadPool().trySchedule([this, uuid, meta_ptr]() {reloadPartitionMetrics(uuid, meta_ptr);});
    }
}

inline static bool isVisible(const DB::DataModelPartWrapperPtr & part_wrapper_ptr, const UInt64 & ts)
{
    return ts == 0
        || (UInt64(part_wrapper_ptr->part_model->part_info().mutation()) <= ts
            && part_wrapper_ptr->part_model->commit_time() <= ts);
}

DB::ServerDataPartsVector PartCacheManager::getOrSetServerDataPartsInPartitions(
    const IStorage & table,
    const Strings & partitions,
    PartCacheManager::LoadPartsFunc && load_func,
    const UInt64 & ts)
{
    ServerDataPartsVector res;
    mayUpdateTableMeta(table);
    const auto & storage = dynamic_cast<const MergeTreeMetaBase &>(table);
    TableMetaEntryPtr meta_ptr = getTableMeta(table.getStorageUUID());

    if (!meta_ptr)
        return res;

    /// On cnch worker, we disable part cache to avoid cache synchronization with server.
    if (getContext()->getServerType() != ServerType::cnch_server)
    {
        DataModelPartPtrVector fetched = load_func(partitions,  meta_ptr->getPartitionIDs());
        for (auto & part_model_ptr : fetched)
        {
            auto part_wrapper_ptr = createPartWrapperFromModel(storage, *part_model_ptr);
            if (isVisible(part_wrapper_ptr, ts))
                res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
        }
        return res;
    }

    if (meta_ptr->load_parts_by_partition)
        res = getServerPartsByPartition(storage, meta_ptr, partitions, load_func, ts);
    else
        res = getServerPartsInternal(storage, meta_ptr, partitions, meta_ptr->getPartitionIDs(), load_func, ts);

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

    auto meta_partitions = meta_ptr->getPartitions(partitions);

    Strings partitions_not_cached;
    for (auto & [partition_id, partition_info_ptr] : meta_partitions)
    {
        bool hit_cache = false;
        {
            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (cached)
                {
                    hit_cache = true;
                    for (auto it = cached->begin(); it != cached->end(); ++it)
                    {
                        const auto & part_wrapper_ptr = *it;
                        if (isVisible(part_wrapper_ptr, ts))
                        {
                            res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                            logPartsVector(storage, res);
                        }
                    }
                }
            }
        }
        if (!hit_cache)
        {
            partitions_not_cached.push_back(partition_id);
            auto partition_write_lock = partition_info_ptr->writeLock();
            partition_info_ptr->cache_status = CacheStatus::LOADING;
        }
    }

    if (partitions_not_cached.empty())
        return res;

    auto fallback_cache_status = [&partitions_not_cached, &meta_partitions]()
    {
        // change the partitino cache status to UINIT if loading failed.
        for (auto & partition_id : partitions_not_cached)
        {
            auto & partition_info = meta_partitions[partition_id];
            auto partition_write_lock = partition_info->writeLock();
            if (partition_info->cache_status == CacheStatus::LOADING)
                partition_info->cache_status = CacheStatus::UINIT;
        } 
    };

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

        {
            /// merge fetched parts with that in cache;
            for (const auto & partition_id : partitions_not_cached)
            {
                auto & partition_info_ptr = meta_partitions[partition_id];
                auto partition_write_lock = partition_info_ptr->writeLock();

                /// Other other task may fail to fetch and change CacheStatus to UINIT before.
                /// If so, do not touch the cache since insert parts may be missed.
                /// Also, other task may success to fetch and change CacheStatus to LOADED before.
                /// If so, no need to modify cache again.
                if (partition_info_ptr->cache_status != CacheStatus::LOADING)
                    continue;

                DataModelPartWrapperVector * parts_wrapper_vector;
                std::optional<DataModelPartWrapperVector> empty_part_vector;
                auto it = partition_to_parts.find(partition_id);
                /// For empty partition, still need to insert an empty vector to cache
                if (it == partition_to_parts.end())
                {
                    empty_part_vector = std::make_optional<DataModelPartWrapperVector>();
                    parts_wrapper_vector =  &empty_part_vector.value();
                }
                else
                {
                    parts_wrapper_vector = &it->second;
                }

                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    /// directly insert all fetched parts into cache.
                    cached = std::make_shared<DataPartModelsMap>();
                    for (const auto & part_wrapper_ptr : *parts_wrapper_vector)
                    {
                        cached->update(part_wrapper_ptr->name, part_wrapper_ptr);
                    }
                    part_cache_ptr->insert({uuid, partition_id}, cached);
                }
                else
                {
                    /// Its THREAD SAFE to update cache inplace
                    for (const auto & part_wrapper_ptr : *parts_wrapper_vector)
                    {
                        auto it = cached->find(part_wrapper_ptr->name);
                        /// do not update cache if the cached data is newer than kv.
                        if (it == cached->end() || (*it)->part_model->commit_time() < part_wrapper_ptr->part_model->commit_time())
                        {
                            cached->update(part_wrapper_ptr->name, part_wrapper_ptr);
                        }
                    }
                    /// Force LRU cache update status(weight/evict).
                    part_cache_ptr->set({uuid, partition_id}, cached);
                }

                partition_info_ptr->cache_status = CacheStatus::LOADED;
            }

            /// Add fetched parts to result outside to reduce write lock time
            for (auto & [partition_id, parts_wrapper_vector] : partition_to_parts)
            {
                for (const auto & part_wrapper_ptr : parts_wrapper_vector)
                {
                    if (isVisible(part_wrapper_ptr, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                        logPartsVector(storage, res);
                    }                    
                }
            }
        }
    }
    catch (Exception & e)
    {
        /// fetch parts timeout, we change to fetch by partition;
        if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
            meta_ptr->load_parts_by_partition = true;

        fallback_cache_status();
        throw e;
    }
    catch (...)
    {
        fallback_cache_status();
        throw;
    }

    return res;
}

ServerDataPartsVector PartCacheManager::getServerPartsByPartition(const MergeTreeMetaBase & storage, const TableMetaEntryPtr & meta_ptr,
    const Strings & partitions, PartCacheManager::LoadPartsFunc & load_func, const UInt64 & ts)
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager"), "Get parts by partitions for table : {}", storage.getLogName());
    Stopwatch watch;
    ServerDataPartsVector res;
    UUID uuid = storage.getStorageUUID();

    auto meta_partitions = meta_ptr->getPartitions(partitions);

    for (auto partition_it = meta_partitions.begin(); partition_it != meta_partitions.end();)
    {
        const auto & partition_id = partition_it->first;
        PartitionInfoPtr & partition_info_ptr = partition_it->second;
        bool need_load_parts = false;

        {
            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (cached)
                {
                    for (auto it = cached->begin(); it != cached->end(); ++it)
                    {
                        const auto & part_wrapper_ptr = *it;
                        if (isVisible(part_wrapper_ptr, ts))
                        {
                            res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                            logPartsVector(storage, res);
                        }
                    }

                    /// already get parts from cache, continue to next partition
                    partition_it++;
                    continue;
                }
            }

            auto partition_write_lock = partition_info_ptr->writeLock();
            /// Double check
            if (partition_info_ptr->cache_status != CacheStatus::LOADING)
            {
                partition_info_ptr->cache_status = CacheStatus::LOADING;
                need_load_parts = true;
            }
        }

        /// Now cache status must be LOADING;
        /// need to load parts from metastore
        if (need_load_parts)
        {
            DataModelPartPtrVector fetched;
            try
            {
                std::unordered_map<String, DataModelPartWrapperVector> partition_to_parts;
                fetched = load_func({partition_id}, {partition_id});
                DataModelPartWrapperVector fetched_data;
                for (auto & dataModelPartPtr : fetched)
                {
                    fetched_data.push_back(createPartWrapperFromModel(storage, *dataModelPartPtr));
                }

                /// It happens that new parts have been inserted into cache during loading parts from bytekv, we need merge them to make 
                /// sure the cache contains all parts of the partition.
                auto partition_write_lock = partition_info_ptr->writeLock();
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    /// directly insert all fetched parts into cache
                    cached = std::make_shared<DataPartModelsMap>();
                    for (auto & data_wrapper_ptr : fetched_data)
                    {
                        cached->update(data_wrapper_ptr->name, data_wrapper_ptr);
                    }
                    part_cache_ptr->insert({uuid, partition_id}, cached);
                }
                else
                {
                    for (auto & data_wrapper_ptr : fetched_data)
                    {
                        auto it = cached->find(data_wrapper_ptr->name);
                        /// do not update cache if the cached data is newer than bytekv.
                        if (it == cached->end() || (*it)->part_model->commit_time() < data_wrapper_ptr->part_model->commit_time())
                        {
                            cached->update(data_wrapper_ptr->name, data_wrapper_ptr);
                        }
                    }
                    /// Force LRU cache update status(weight/evict).
                    part_cache_ptr->set({uuid, partition_id}, cached);
                }

                partition_info_ptr->cache_status = CacheStatus::LOADED;

                /// Release partition lock before construct ServerDataPart
                partition_write_lock.reset();

                /// Finish fetching parts, notify other waiting tasks if any.
                meta_ptr->fetch_cv.notify_all();

                for (auto & data_wrapper_ptr : fetched_data)
                {
                    /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                    if (isVisible(data_wrapper_ptr, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(data_wrapper_ptr));
                        logPartsVector(storage, res);
                    }                    
                }

                /// go to next partition;
                partition_it++;
            }
            catch (...)
            {
                /// change cache status to UINIT if exception occurs during fetch.
                auto partition_write_lock = partition_info_ptr->writeLock();
                partition_info_ptr->cache_status = CacheStatus::UINIT;
                throw;
            }
        }
        else /// other task is fetching parts now, just wait for the result
        {
            {
                std::unique_lock<std::mutex> lock(meta_ptr->fetch_mutex);
                if (!meta_ptr->fetch_cv.wait_for(lock, std::chrono::milliseconds(5000)
                    , [&partition_info_ptr]() {return partition_info_ptr->cache_status == CacheStatus::LOADED;}))
                {
                    LOG_TRACE(&Poco::Logger::get("PartCacheManager"), "Wait timeout 5000ms for other thread loading table: {}, partition: {}", storage.getStorageID().getNameForLogs(), partition_id);
                    checkTimeLimit(watch);
                    continue;
                }
            }

            if (partition_info_ptr->cache_status == CacheStatus::LOADED)
            {
                auto cached = part_cache_ptr->get({uuid, partition_id});
                if (!cached)
                {
                    throw Exception("Cannot get already loaded parts from cache. Its a logic error.", ErrorCodes::LOGICAL_ERROR);
                }
                for (auto it = cached->begin(); it != cached->end(); ++it)
                {
                    const auto & part_wrapper_ptr = *it;
                    /// Only filter the parts when both commit_time and txnid are smaller or equal to ts (txnid is helpful for intermediate parts).
                    if (isVisible(part_wrapper_ptr, ts))
                    {
                        res.push_back(std::make_shared<ServerDataPart>(part_wrapper_ptr));
                        logPartsVector(storage, res);
                    }
                }
                partition_it++;
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
    return {part_cache_ptr->count(), part_cache_ptr->weight()};
}

std::unordered_map<String, std::pair<size_t, size_t>> PartCacheManager::getTableCacheInfo()
{
    CnchDataPartCachePtr cache_ptr;
    {
        std::unique_lock<std::mutex> lock(cache_mutex);
        cache_ptr = part_cache_ptr;
    }
    if (!cache_ptr)
        return {};

    return cache_ptr->getTableCacheInfo();
}

void PartCacheManager::reset()
{
    LOG_DEBUG(&Poco::Logger::get("PartCacheManager::reset"), "Resetting part cache manager.");
    std::unique_lock<std::mutex> lock(cache_mutex);
    active_tables.clear();
    part_cache_ptr->reset();
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
