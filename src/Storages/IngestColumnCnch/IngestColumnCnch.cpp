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

#include <memory>
#include <Storages/IngestColumnCnch/IngestColumnBlockInputStream.h>
#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/PartitionCommands.h>
#include <DaemonManager/DaemonManagerClient.h>
#include <DaemonManager/BackgroundJob.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchCreateQueryHelper.h>
#include <Catalog/Catalog.h>
#include <DataTypes/MapHelpers.h>
#include <CloudServices/CnchServerClient.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/TransactionWrapperBlockInputStream.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <ResourceManagement/VWScheduleAlgo.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Parsers/queryToString.h>
#include <CloudServices/CnchWorkerResource.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <common/sleep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int DUPLICATE_COLUMN;
    extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
}

namespace
{

String createCloudMergeTreeCreateQuery(
    const StorageCnchMergeTree & table,
    const String & suffix
)
{
    String table_name = table.getTableName() + '_' + suffix;
    String database_name = table.getDatabaseName() + '_' + suffix;
    return table.getCreateQueryForCloudTable(
        table.getCreateTableSql(),
        table_name, nullptr, false, std::nullopt, Strings{}, database_name);
}

String reconstructAlterQuery(
    const StorageID & target_storage_id,
    const StorageID & source_storage_id,
    const struct PartitionCommand & command,
    const String & suffix)
{
    auto get_column_name = [] (const String & input)
    {
        if (!isMapImplicitKey(input))
            return backQuoteIfNeed(input);
        String map_name = parseMapNameFromImplicitColName(input);
        String key_name = parseKeyNameFromImplicitColName(input, map_name);
        return map_name + '{' + key_name + '}';
    };

    auto get_comma_separated = [& get_column_name] (const Strings & input)
    {
        String res;
        String separator{" "};

        for (const String & s : input)
        {
            res += separator + get_column_name(s);
            separator = " ,";
        }
        return res;
    };

    const String prepared_suffix = "_" + suffix;
    String query_first_part = fmt::format("ALTER TABLE {}.{} INGEST PARTITION {} COLUMNS {}",
        backQuoteIfNeed(target_storage_id.getDatabaseName() + prepared_suffix),
        backQuoteIfNeed(target_storage_id.getTableName() + prepared_suffix),
        queryToString(command.partition),
        get_comma_separated(command.column_names));


    String query_key_part = (command.key_names.empty()) ? "" :
        fmt::format("KEY {}", get_comma_separated(command.key_names));

    String query_end_part = fmt::format(" FROM {}.{}",
        backQuoteIfNeed(source_storage_id.getDatabaseName() + prepared_suffix),
        backQuoteIfNeed(source_storage_id.getTableName() + prepared_suffix));

    return fmt::format("{} {} {}", query_first_part, query_key_part, query_end_part);
}

std::vector<std::pair<size_t, uint32_t>>
getIngestWorkersIndexAndJobsWithOrder(const std::vector<ResourceManagement::WorkerMetrics> & worker_metrics, UInt64 max_ingest_task)
{
    if(worker_metrics.empty())
    {
        throw Exception("Can not get WorkerMetrics from ResourceManagement before Ingest with bucket.", ErrorCodes::SYSTEM_ERROR);
    }

    std::vector<std::pair<size_t, uint32_t>> workers_index_with_task;

    for (size_t i = 0; i < worker_metrics.size(); i++)
    {
        if (worker_metrics[i].num_queries < max_ingest_task || max_ingest_task == 0)
        {
            workers_index_with_task.push_back({i, worker_metrics[i].num_queries});
        }
    }

    std::sort(
        workers_index_with_task.begin(), workers_index_with_task.end(), [](std::pair<size_t, uint32_t> x, std::pair<size_t, uint32_t> y) {
            return x.second < y.second;
        });

    return workers_index_with_task;
}

BlockInputStreamPtr forwardIngestPartitionToWorkerWithBucketTableImpl(
    StorageCnchMergeTree & target_table,
    const String & create_source_cloud_merge_tree_query,
    const String & create_target_cloud_merge_tree_query,
    const String & query_for_send_to_worker,
    const PartitionCommand & command,
    const WorkerGroupHandle & worker_group,
    const ContextPtr & context,
    LoggerPtr log)
{
    auto workers_index_with_task = getIngestWorkersIndexAndJobsWithOrder(
        worker_group->getMetrics().worker_metrics_vec, context->getSettingsRef().max_ingest_task_on_workers);

    size_t worker_nums = workers_index_with_task.size();

    std::vector<Int64> buckets_for_ingest = command.bucket_nums;

    if (buckets_for_ingest.empty())
    {
        assert(target_table.getInMemoryMetadataPtr()->getBucketNumberFromClusterByKey() > 0);
        buckets_for_ingest.resize(target_table.getInMemoryMetadataPtr()->getBucketNumberFromClusterByKey());
        std::iota(buckets_for_ingest.begin(), buckets_for_ingest.end(), 0);
    }

    auto buckets_lists = splitBucketsForWorker(worker_nums, buckets_for_ingest);

    auto tmp_ingest_context = Context::createCopy(context);
    std::vector<BlockInputStreamPtr> remote_streams;

    for (size_t index = 0; index < worker_nums; index++)
    {
        if (buckets_lists[index].empty())
        {
            continue;
        }

        LOG_TRACE(log, fmt::format("{} with {} buckets", query_for_send_to_worker, buckets_lists[index].size()));

        auto worker_index = workers_index_with_task[index].first;
        const auto * write_shard_ptr = &(worker_group->getShardsInfo().at(worker_index));
        auto worker_client = worker_group->getWorkerClients().at(worker_index);

        auto query_for_send = fmt::format("{} BUCKETS {}", query_for_send_to_worker, fmt::join(buckets_lists[index], ", "));

        if (query_for_send.size() > tmp_ingest_context->getSettingsRef().max_query_size)
        {
            tmp_ingest_context->setSetting("max_query_size", query_for_send.size() + 1);
        }

        worker_client->sendCreateQueries(tmp_ingest_context, {create_target_cloud_merge_tree_query, create_source_cloud_merge_tree_query});
        remote_streams.emplace_back(
            CnchStorageCommonHelper::sendQueryPerShard(tmp_ingest_context, query_for_send, *write_shard_ptr, true));
    }

    return std::make_shared<UnionBlockInputStream>(remote_streams, nullptr, worker_nums);
}

} /// end namespace anonymous

BlockInputStreamPtr forwardIngestPartitionToWorker(
    StorageCnchMergeTree & target_table,
    StorageCnchMergeTree & source_table,
    const struct PartitionCommand & command,
    ContextPtr context
    )
{
    if (auto sleep_ms = context->getSettingsRef().sleep_in_send_ingest_to_worker_ms.totalMilliseconds())
        sleepForMilliseconds(sleep_ms);

    LoggerPtr log = target_table.getLogger();
    TxnTimestamp txn_id = context->getCurrentTransactionID();
    std::hash<String> hasher;
    const String transaction_string = toString(txn_id.toUInt64());
    String source_database = source_table.getDatabaseName();
    const String task_id = transaction_string + "_" +
        std::to_string(hasher(queryToString(command.partition) + source_database + source_table.getTableName()));
    const String & create_target_cloud_merge_tree_query = createCloudMergeTreeCreateQuery(target_table, task_id);
    LOG_TRACE(log, "create target cloud merge tree query: {}", create_target_cloud_merge_tree_query);

    const String & create_source_cloud_merge_tree_query = createCloudMergeTreeCreateQuery(source_table, task_id);
    LOG_TRACE(log, "create source cloud merge tree query: {}", create_source_cloud_merge_tree_query);

    const String query_for_send_to_worker = reconstructAlterQuery(target_table.getStorageID(), source_table.getStorageID(), command, task_id);
    LOG_TRACE(log, "reconstruct query to send to worker {}", query_for_send_to_worker);
    WorkerGroupHandle worker_group = context->getCurrentWorkerGroup();
    auto num_of_workers = worker_group->getShardsInfo().size();
    if (!num_of_workers)
        throw Exception("No heathy worker available", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

    auto ordered_key_names = getOrderedKeys(command.key_names, *target_table.getInMemoryMetadataPtr());
    auto ingest_column_names = command.column_names;

    if (context->getSettingsRef().optimize_ingest_with_bucket
        && checkIngestWithBucketTable(source_table, target_table, ordered_key_names, ingest_column_names))
    {
        return forwardIngestPartitionToWorkerWithBucketTableImpl(
            target_table,
            create_source_cloud_merge_tree_query,
            create_target_cloud_merge_tree_query,
            query_for_send_to_worker,
            command,
            worker_group,
            context,
            log);
    }
    else
    {
        std::size_t index = std::hash<String>{}(task_id) % num_of_workers;
        auto * write_shard_ptr = &(worker_group->getShardsInfo().at(index));
        auto worker_client = worker_group->getWorkerClient(index, /*skip_busy_worker*/false).second;
        worker_client->sendCreateQueries(context,
            {create_target_cloud_merge_tree_query, create_source_cloud_merge_tree_query});

        return CnchStorageCommonHelper::sendQueryPerShard(context, query_for_send_to_worker, *write_shard_ptr, true);
    }
}

Pipe StorageCloudMergeTree::ingestPartition(const StorageMetadataPtr & /*metadata_snapshot*/, const PartitionCommand & command, ContextPtr local_context)
{
    LOG_TRACE(log, "execute ingest partition in worker");

    BlockInputStreamPtr in = std::make_shared<IngestColumnBlockInputStream>(shared_from_this(), command, std::move(local_context));
    return Pipe{std::make_shared<SourceFromInputStream>(std::move(in))};
}

Pipe ingestPartitionInServer(
    StorageCnchMergeTree & storage,
    const struct PartitionCommand & command,
    ContextPtr local_context)
{
    LoggerPtr log = storage.getLogger();
    LOG_DEBUG(log, "execute ingest partition in server");
    StorageMetadataPtr target_meta_data_ptr = storage.getInMemoryMetadataPtr();
    const Names & column_names = command.column_names;
    /// Order is important when execute the outer join.
    const Names ordered_key_names = getOrderedKeys(command.key_names, *target_meta_data_ptr);

    /// Perform checking in server side
    bool has_map_implicite_key = false;
    checkIngestColumns(column_names, *target_meta_data_ptr, has_map_implicite_key);
    /// Now not support ingesting to storage with compact map type
    if (has_map_implicite_key && storage.getSettings()->enable_compact_map_data)
        throw Exception("INGEST PARTITON not support compact map type now", ErrorCodes::NOT_IMPLEMENTED);

    String from_database = command.from_database.empty() ? local_context->getCurrentDatabase() : command.from_database;
    StoragePtr source_storage = local_context->getCnchCatalog()->tryGetTable(*local_context, from_database, command.from_table, local_context->getCurrentCnchStartTime());
    if (!source_storage)
        throw Exception("Failed to get StoragePtr for source table", ErrorCodes::SYSTEM_ERROR);

    auto * source_merge_tree = dynamic_cast<StorageCnchMergeTree *>(source_storage.get());
    if (!source_merge_tree)
        throw Exception("INGEST PARTITON source table only support cnch merge tree table", ErrorCodes::NOT_IMPLEMENTED);

    /// check whether the columns have equal structure between target table and source table
    StorageMetadataPtr source_meta_data_ptr = source_merge_tree->getInMemoryMetadataPtr();
    checkColumnStructure(*target_meta_data_ptr, *source_meta_data_ptr, column_names);
    checkColumnStructure(*target_meta_data_ptr, *source_meta_data_ptr, ordered_key_names);
    checkColumnStructure(*target_meta_data_ptr, *source_meta_data_ptr,
        target_meta_data_ptr->getColumnsRequiredForPartitionKey());

    TransactionCnchPtr cur_txn = local_context->getCurrentTransaction();
    String partition_id = storage.getPartitionIDFromQuery(command.partition, local_context);
    std::shared_ptr<Catalog::Catalog> catalog = local_context->getCnchCatalog();

    ServerDataPartsVector source_parts = catalog->getServerDataPartsInPartitions(source_storage, {partition_id}, local_context->getCurrentCnchStartTime(), local_context.get());
    LOG_DEBUG(log, "number of server source parts: {}", source_parts.size());
    ServerDataPartsVector visible_source_parts =
        CnchPartsHelper::calcVisibleParts(source_parts, false, CnchPartsHelper::LoggingOption::EnableLogging);
    LOG_DEBUG(log, "number of visible_server source parts: {}", source_parts.size());

    const StorageID target_storage_id = storage.getStorageID();
    /// lock must be acquire before fetching the source parts
    TxnTimestamp txn_id = cur_txn->getTransactionID();
    LockInfoPtr partition_lock = std::make_shared<LockInfo>(txn_id);
    partition_lock->setMode(LockMode::X);
    partition_lock->setTimeout(local_context->getSettingsRef().ingest_column_memory_lock_timeout.value.totalMilliseconds()); // default 5s
    partition_lock->setUUIDAndPrefix(target_storage_id.uuid, LockInfo::task_domain);
    partition_lock->setPartition(partition_id);

    Stopwatch lock_watch;

    auto cnch_lock = std::make_shared<CnchLockHolder>(local_context, std::move(partition_lock));
    cnch_lock->lock();
    LOG_DEBUG(log, "Acquired lock in {} ms", lock_watch.elapsedMilliseconds());

    /// remove the merge mutate tasks that could cause WW conflict before get server part
    auto daemon_manager_client_ptr = local_context->getDaemonManagerClient();
    if (!daemon_manager_client_ptr)
        throw Exception("Failed to get daemon manager client", ErrorCodes::SYSTEM_ERROR);

    std::optional<DaemonManager::BGJobInfo> merge_job_info = daemon_manager_client_ptr->getDMBGJobInfo(target_storage_id.uuid, CnchBGThreadType::MergeMutate, local_context->getCurrentQueryId());
    if (!merge_job_info)
    {
        throw Exception("Failed to get merge job info for " + target_storage_id.getNameForLogs() , ErrorCodes::SYSTEM_ERROR);
    }

    if (merge_job_info->host_port.empty())
        LOG_DEBUG(log, "Host port of merge job is empty, the merge thread is stopped");
    else
    {
        auto server_client_ptr = local_context->getCnchServerClient(merge_job_info->host_port);
        if (!server_client_ptr)
            throw Exception("Failed to get server client with host port " + merge_job_info->host_port, ErrorCodes::SYSTEM_ERROR);
        if (!server_client_ptr->removeMergeMutateTasksOnPartitions(target_storage_id, {partition_id}))
            throw Exception("Failed to get remove MergeMutateTasks on partition_id " + partition_id + " for table " + target_storage_id.getNameForLogs(), ErrorCodes::SYSTEM_ERROR);
    }

    /// The server part must be fetch using the new timestamp, not the ts of transaction
    ServerDataPartsVector target_parts = catalog->getServerDataPartsInPartitions(storage.shared_from_this(), {partition_id}, local_context->getTimestamp(), local_context.get());
    LOG_DEBUG(log, "number of server target parts: {}", target_parts.size());
    ServerDataPartsVector visible_target_parts = CnchPartsHelper::calcVisibleParts(target_parts, false, CnchPartsHelper::LoggingOption::EnableLogging);
    LOG_DEBUG(log, "number of visible server target parts: {}", target_parts.size());

    if (visible_source_parts.empty())
    {
        LOG_INFO(log, "There is no part to ingest, do nothing");
        return {};
    }

    BlockInputStreamPtr in = forwardIngestPartitionToWorker(storage, *source_merge_tree, command, local_context);
    cur_txn->setMainTableUUID(storage.getStorageUUID());
    in = std::make_shared<TransactionWrapperBlockInputStream>(in, std::move(cur_txn), std::move(cnch_lock));
    return Pipe{std::make_shared<SourceFromInputStream>(std::move(in))};
}

/// TODO return pipe
Pipe StorageCnchMergeTree::ingestPartition(const struct PartitionCommand & command, const ContextPtr local_context)
{
    return ingestPartitionInServer(*this, command, local_context);
}

} /// end namespace

