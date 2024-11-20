#pragma once
#include <CloudServices/CnchServerResource.h>
#include <CloudServices/CnchWorkerResource.h>
#include <Interpreters/WorkerStatusManager.h>
#include <Protos/cnch_worker_rpc.pb.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DataLakes/StorageCnchLakeBase.h>
#include <Storages/Hive/StorageCloudHive.h>
#include <Storages/MergeTree/CloudTableDefinitionCache.h>
#include <Storages/RemoteFile/IStorageCloudFile.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageDictCloudMergeTree.h>
#include <Common/CurrentThread.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
extern const Event QueryCreateTablesMicroseconds;
extern const Event QueryLoadResourcesMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int WORKER_TABLE_NOT_FOUND;
}

template <typename T>
static void loadQueryResource(const T & query_resource, const ContextPtr & context)
{
    static LoggerPtr log = getLogger("WorkerResource");
    LOG_TRACE(log, "Receiving resources for Session: {}", query_resource.txn_id());
    Stopwatch watch;
    auto session = context->acquireNamedCnchSession(query_resource.txn_id(), query_resource.timeout(), false);
    auto session_context = session->context;
    session_context->setTemporaryTransaction(query_resource.txn_id(), query_resource.primary_txn_id());
    if (query_resource.has_session_timezone())
        session_context->setSetting("session_timezone", query_resource.session_timezone());

    CurrentThread::QueryScope query_scope(session_context);
    auto worker_resource = session_context->getCnchWorkerResource();
    /// store cloud tables in cnch_session_resource.
    {
        Stopwatch create_timer;
        /// create a copy of session_context to avoid modify settings in SessionResource
        auto context_for_create = Context::createCopy(session_context);
        for (int i = 0; i < query_resource.create_queries_size(); i++)
        {
            auto create_query = query_resource.create_queries().at(i);
            auto object_columns = query_resource.dynamic_object_column_schema().at(i);

            worker_resource->executeCreateQuery(context_for_create, create_query, false, ColumnsDescription::parse(object_columns));
        }
        for (int i = 0; i < query_resource.cacheable_create_queries_size(); i++)
        {
            const auto & item = query_resource.cacheable_create_queries().at(i);
            ColumnsDescription object_columns;
            if (item.has_dynamic_object_column_schema())
                object_columns = ColumnsDescription::parse(item.dynamic_object_column_schema());

           worker_resource->executeCacheableCreateQuery(
                context_for_create,
                RPCHelpers::createStorageID(item.storage_id()),
                item.definition(),
                item.local_table_name(),
                static_cast<WorkerEngineType>(item.local_engine_type()),
                "",
                object_columns);
        }
        create_timer.stop();
        LOG_INFO(
            log,
            "Prepared {} tables for session {} in {} us",
            query_resource.create_queries_size() + query_resource.cacheable_create_queries_size(),
            query_resource.txn_id(),
            create_timer.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::QueryCreateTablesMicroseconds, create_timer.elapsedMicroseconds());
    }

    bool lazy_load_parts = query_resource.has_lazy_load_data_parts() && query_resource.lazy_load_data_parts();
    for (const auto & data : query_resource.data_parts())
    {
        /// By default, calling getTable (from WorkerResource) will trigger loading data parts.
        /// Here is the first time and happens before parts are ready. So don't trigger load data parts here.
        StorageID storage_id = {data.database(), data.table()};
        auto storage = worker_resource->tryGetTable(storage_id, /*load_data_parts*/ false);
        if (!storage)
            throw Exception(
                ErrorCodes::WORKER_TABLE_NOT_FOUND, "Table {} not found in worker resource, it's a bug.", storage_id.getNameForLogs());

        bool is_dict_table = false;
        if (lazy_load_parts)
            is_dict_table = !!dynamic_cast<StorageDictCloudMergeTree *>(storage.get());

        if (auto * cloud_merge_tree = dynamic_cast<StorageCloudMergeTree *>(storage.get()))
        {
            if (data.has_table_version())
            {
                WGWorkerInfoPtr worker_info = RPCHelpers::createWorkerInfo(query_resource.worker_info());
                UInt64 version = data.table_version();
                cloud_merge_tree->setDataDescription(std::move(worker_info), version);
                LOG_DEBUG(log, "Received table {} with data version {}", cloud_merge_tree->getStorageID().getNameForLogs(), version);
            }
            else if (!data.server_parts().empty())
            {
                MergeTreeMutableDataPartsVector server_parts;
                if (cloud_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
                    server_parts = createBasePartAndDeleteBitmapFromModelsForSend<IMergeTreeMutableDataPartPtr>(
                        *cloud_merge_tree, data.server_parts(), data.server_part_bitmaps());
                else
                    server_parts = createPartVectorFromModelsForSend<IMergeTreeMutableDataPartPtr>(*cloud_merge_tree, data.server_parts());

                auto server_parts_size = server_parts.size();

                if (query_resource.has_disk_cache_mode())
                {
                    auto disk_cache_mode = SettingFieldDiskCacheModeTraits::fromString(query_resource.disk_cache_mode());
                    if (disk_cache_mode != DiskCacheMode::AUTO)
                    {
                        for (auto & part : server_parts)
                            part->disk_cache_mode = disk_cache_mode;
                    }
                }

                cloud_merge_tree->receiveDataParts(std::move(server_parts));

                LOG_DEBUG(
                    log,
                    "Received {} parts for table {}(txn_id: {}), disk_cache_mode {}, is_dict: {}, lazy_load_parts: {}",
                    server_parts_size,
                    cloud_merge_tree->getStorageID().getNameForLogs(),
                    query_resource.txn_id(),
                    query_resource.disk_cache_mode(),
                    is_dict_table,
                    lazy_load_parts);
            }

            if (!data.virtual_parts().empty())
            {
                MergeTreeMutableDataPartsVector virtual_parts;
                if (cloud_merge_tree->getInMemoryMetadataPtr()->hasUniqueKey())
                    virtual_parts = createBasePartAndDeleteBitmapFromModelsForSend<IMergeTreeMutableDataPartPtr>(
                        *cloud_merge_tree, data.virtual_parts(), data.virtual_part_bitmaps());
                else
                    virtual_parts
                        = createPartVectorFromModelsForSend<IMergeTreeMutableDataPartPtr>(*cloud_merge_tree, data.virtual_parts());

                auto virtual_parts_size = virtual_parts.size();

                if (query_resource.has_disk_cache_mode())
                {
                    auto disk_cache_mode = SettingFieldDiskCacheModeTraits::fromString(query_resource.disk_cache_mode());
                    if (disk_cache_mode != DiskCacheMode::AUTO)
                    {
                        for (auto & part : virtual_parts)
                            part->disk_cache_mode = disk_cache_mode;
                    }
                }

                    cloud_merge_tree->receiveVirtualDataParts(std::move(virtual_parts));

                    LOG_DEBUG(
                        log,
                        "Received {} virtual parts for table {}(txn_id: {}), disk_cache_mode {}, is_dict: {}, lazy_load_parts: {}",
                        virtual_parts_size,
                        cloud_merge_tree->getStorageID().getNameForLogs(),
                        query_resource.txn_id(),
                        query_resource.disk_cache_mode(),
                        is_dict_table,
                        lazy_load_parts);
            }

            std::set<Int64> required_bucket_numbers;
            for (const auto & bucket_number : data.bucket_numbers())
                required_bucket_numbers.insert(bucket_number);

            cloud_merge_tree->setRequiredBucketNumbers(required_bucket_numbers);

            for (const auto & mutation_str : data.cnch_mutation_entries())
            {
                auto mutation_entry = CnchMergeTreeMutationEntry::parse(mutation_str);
                cloud_merge_tree->addMutationEntry(mutation_entry);
            }


            /// prepareDataPartsForRead/loadDataParts is an expensive action as it may involve remote read.
            /// The worker rpc thread pool may be blocked when there are many `sendResources` requests.
            /// lazy_load_parts means the storage just receives server_parts in rpc. And it will call `prepareDataPartsForRead` later (before reading).
            /// One exception is StorageDictCloudMergeTree as it use a different read logic rather than StorageCloudMergeTree::read.
            if (!lazy_load_parts || is_dict_table)
            {
                cloud_merge_tree->prepareDataPartsForRead();
            }
        }
        else if (auto * hive_table = dynamic_cast<StorageCloudHive *>(storage.get()))
        {
            auto settings = hive_table->getSettings();
            auto lake_scan_infos = ILakeScanInfo::deserialize(data.lake_scan_info_parts(), context, storage->getInMemoryMetadataPtr(), *settings);
            hive_table->loadLakeScanInfos(lake_scan_infos);
        }
        else if (auto * cloud_file_table = dynamic_cast<IStorageCloudFile *>(storage.get()))
        {
            auto data_parts = createCnchFileDataParts(session_context, data.file_parts());
            cloud_file_table->loadDataParts(data_parts);

            LOG_DEBUG(
                log,
                "Received and loaded {}  cloud file parts for table {}",
                data_parts.size(),
                cloud_file_table->getStorageID().getNameForLogs());
        }
        else
            throw Exception("Unknown table engine: " + storage->getName(), ErrorCodes::UNKNOWN_TABLE);
    }

    std::unordered_map<String, UInt64> udf_infos;
    for (const auto & udf_info : query_resource.udf_infos())
    {
        udf_infos.emplace(udf_info.function_name(), udf_info.version());
        LOG_DEBUG(log, "Received UDF meta data from server, name: {}, version: {}", udf_info.function_name(), udf_info.version());
    }

    watch.stop();
    LOG_INFO(log, "Load all resources for session {} in {} us.", query_resource.txn_id(), watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::QueryLoadResourcesMicroseconds, watch.elapsedMicroseconds());
}


template <typename T>
static void prepareQueryResource(
    T & query_resource,
    const WorkerId & worker_id,
    const std::vector<AssignedResource> & worker_resources,
    const ContextPtr & context,
    bool send_mutations,
    LoggerPtr & log)
{
    query_resource.set_txn_id(context->getCurrentTransactionID());
    query_resource.set_primary_txn_id(context->getCurrentTransaction()->getPrimaryTransactionID());

    const auto & settings = context->getSettingsRef();
    auto max_execution_time = settings.max_execution_time.value.totalSeconds();
    /// recycle_timeout refers to the time when the session is recycled under abnormal case,
    /// so it should be larger than max_execution_time to make sure the session is not to be destroyed in advance.
    UInt64 recycle_timeout = max_execution_time > 0 ? max_execution_time + 60UL : 3600;
    query_resource.set_timeout(recycle_timeout);
    if (!settings.session_timezone.value.empty())
        query_resource.set_session_timezone(settings.session_timezone.value);
    if (settings.enable_lazy_load_data_parts.value)
        query_resource.set_lazy_load_data_parts(true);

    bool require_worker_info = false;
    for (const auto & resource : worker_resources)
    {
        if (!resource.sent_create_query)
        {
            const auto & def = resource.table_definition;
            if (resource.table_definition.cacheable)
            {
                auto * cacheable = query_resource.add_cacheable_create_queries();
                RPCHelpers::fillStorageID(resource.storage->getStorageID(), *cacheable->mutable_storage_id());
                cacheable->set_definition(def.definition);
                if (!resource.object_columns.empty())
                    cacheable->set_dynamic_object_column_schema(resource.object_columns.toString());
                cacheable->set_local_engine_type(static_cast<UInt32>(def.engine_type));
                cacheable->set_local_table_name(def.local_table_name);

            }
            else
            {
                query_resource.add_create_queries(def.definition);
                query_resource.add_dynamic_object_column_schema(resource.object_columns.toString());
            }
        }

        /// parts
        auto & table_data_parts = *query_resource.mutable_data_parts()->Add();
        /// Send storage's mutations to worker if needed.
        if (send_mutations)
        {
            auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(resource.storage.get());
            if (cnch_merge_tree)
            {
                for (auto const & mutation_str : cnch_merge_tree->getPlainMutationEntries())
                {
                    LOG_TRACE(log, "Send mutations to worker: {}", mutation_str);
                    table_data_parts.add_cnch_mutation_entries(mutation_str);
                }
            }
        }

        table_data_parts.set_database(resource.storage->getDatabaseName());
        table_data_parts.set_table(resource.table_definition.local_table_name);
        if (resource.table_version)
        {
            require_worker_info = true;
            table_data_parts.set_table_version(resource.table_version);
        }

        if (settings.query_dry_run_mode != QueryDryRunMode::SKIP_SEND_PARTS)
        {
            if (!resource.server_parts.empty())
            {
                // todo(jiashuo): bitmap need handler?
                fillBasePartAndDeleteBitmapModels(
                    *resource.storage,
                    resource.server_parts,
                    *table_data_parts.mutable_server_parts(),
                    *table_data_parts.mutable_server_part_bitmaps());
            }

            if (!resource.virtual_parts.empty())
            {
                fillPartsModelForSend(*resource.storage, resource.virtual_parts, *table_data_parts.mutable_virtual_parts());
                auto * bitmaps_model = table_data_parts.mutable_virtual_part_bitmaps();
                for (const auto & virtual_part : resource.virtual_parts)
                {
                    for (auto & bitmap_meta : virtual_part->part->delete_bitmap_metas)
                    {
                        bitmaps_model->Add()->CopyFrom(*bitmap_meta);
                    }
                }
            }
        }

        if (!resource.lake_scan_info_parts.empty())
        {
            auto * mutable_lake_scan_infos = table_data_parts.mutable_lake_scan_info_parts();
            auto & cnch_lake = dynamic_cast<StorageCnchLakeBase &>(*resource.storage);
            cnch_lake.serializeLakeScanInfos(*mutable_lake_scan_infos, resource.lake_scan_info_parts);
        }

        if (!resource.file_parts.empty())
        {
            fillCnchFilePartsModel(resource.file_parts, *table_data_parts.mutable_file_parts());
        }

        /// bucket numbers
        for (const auto & bucket_num : resource.bucket_numbers)
            *table_data_parts.mutable_bucket_numbers()->Add() = bucket_num;
    }

    // need add worker info if query by table version
    if (require_worker_info)
    {
        auto current_wg = context->getCurrentWorkerGroup();
        auto * worker_info = query_resource.mutable_worker_info();
        worker_info->set_worker_id(worker_id.id);
        worker_info->set_index(current_wg->getWorkerIndex(worker_id.id));
        worker_info->set_num_workers(current_wg->workerNum());

        if (worker_info->num_workers() <= worker_info->index())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid worker index {} for worker group {}, which contains {} workers.",
                toString(worker_info->index()),
                current_wg->getVWName(),
                toString(current_wg->workerNum()));
    }

    query_resource.set_disk_cache_mode(context->getSettingsRef().disk_cache_mode.toString());
}

}
