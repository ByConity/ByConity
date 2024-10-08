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

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/PartCacheManager.h>
#include <Storages/System/StorageSystemCnchPartsInfoLocal.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_TABLE_LOCK;
}

StorageSystemCnchPartsInfoLocal::StorageSystemCnchPartsInfoLocal(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    auto ready_state = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Unloaded", static_cast<Int8>(ReadyState::Unloaded)},
        {"Loading", static_cast<Int8>(ReadyState::Loading)},
        {"Loaded", static_cast<Int8>(ReadyState::Loaded)},
        {"Recalculated", static_cast<Int8>(ReadyState::Recalculated)},
    });

    storage_metadata.setColumns(ColumnsDescription({
        {"uuid", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"partition", std::make_shared<DataTypeString>()},
        {"first_partition", std::make_shared<DataTypeString>()},
        {"metrics_available", std::make_shared<DataTypeUInt8>()},
        {"total_parts_number", std::make_shared<DataTypeUInt64>()},
        {"total_parts_size", std::make_shared<DataTypeUInt64>()},
        {"total_rows_count", std::make_shared<DataTypeUInt64>()},
        {"ready_state", std::move(ready_state)},
        /// Boolean
        {"recalculating", std::make_shared<DataTypeUInt8>()},
        {"last_update_time", std::make_shared<DataTypeUInt64>()},
        {"last_snapshot_time", std::make_shared<DataTypeUInt64>()},
        {"last_modification_time", std::make_shared<DataTypeDateTime>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemCnchPartsInfoLocal::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*max_block_size*/)
{
    auto cache_manager = context->getPartCacheManager();

    if (context->getServerType() != ServerType::cnch_server || !cache_manager)
        return {};

    std::vector<TableMetaEntryPtr> active_tables = cache_manager->getAllActiveTables();

    if (active_tables.empty())
        return {};

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    Block res_block;

    NameSet names_set(column_names.begin(), column_names.end());
    std::vector<UInt8> columns_mask(sample_block.columns());

    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            res_block.insert(sample_block.getByPosition(i));
        }
    }

    // if require_partition_info is false, we can skip read partition when collecting metrics.
    bool require_partition_info = false;
    if (names_set.count("partition") || names_set.count("first_partition"))
        require_partition_info = true;

    Block block_to_filter;

    /// Add `database` column.
    MutableColumnPtr database_column_mut = ColumnString::create();
    /// Add `table` column.
    MutableColumnPtr table_column_mut = ColumnString::create();
    /// add `last_update_time` column
    MutableColumnPtr last_update_mut = ColumnUInt64::create();

    /// ADD 'index' column
    MutableColumnPtr index_column_mut = ColumnUInt64::create();

    // get current timestamp for later use.
    UInt64 current_ts = context->getTimestamp();

    String tenant_id = context->getTenantId();

    for (size_t i = 0; i < active_tables.size(); i++)
    {
        const auto &db = active_tables[i]->database;
        if (!tenant_id.empty())
        {
            if (db.size() <= tenant_id.size() + 1)
                continue;
            if (db[tenant_id.size()] != '.')
                continue;
            if (!startsWith(db, tenant_id))
                continue;
            database_column_mut->insert(db.substr(tenant_id.size() + 1));
        }
        else
        {
            database_column_mut->insert(db);
        }
        table_column_mut->insert(active_tables[i]->table);
        UInt64 last_update_time = active_tables[i]->metrics_last_update_time;
        /// if the last update time is not set. we just use current timestamp to make sure the table's parts info are always returned in incremental mode.
        if (last_update_time == TxnTimestamp::maxTS())
            last_update_time = current_ts;
        last_update_mut->insert((last_update_time >> 18) / 1000);
        index_column_mut->insert(i);
    }

    block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(last_update_mut), std::make_shared<DataTypeUInt64>(), "last_update_time"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(index_column_mut), std::make_shared<DataTypeUInt64>(), "index"));

    /// Filter block with `database` and `table` column.
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return {};

    using PartitionData = std::unordered_map<String, PartitionFullPtr>;

    size_t max_threads = 10;
    ThreadPool collect_metrics_pool(max_threads);

    ColumnPtr filtered_index_column = block_to_filter.getByName("index").column;
    std::vector<PartitionData> metrics_collection{filtered_index_column->size(), PartitionData{}};
    std::atomic_size_t task_index{0};
    std::size_t total_task_size = filtered_index_column->size();
    std::atomic_bool need_abort = false;
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    try
    {
        for (size_t i = 0; i < max_threads; i++)
        {
            collect_metrics_pool.scheduleOrThrowOnError([&, thread_group = CurrentThread::getGroup()]() {
                DB::ThreadStatus thread_status;

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                size_t current_task;
                while ((current_task = task_index++) < total_task_size && !need_abort)
                {
                    StoragePtr storage = nullptr;
                    try
                    {
                        auto entry = active_tables[(*filtered_index_column)[current_task].get<UInt64>()];
                        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, entry->database);
                        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, entry->database, entry->table))
                            continue;

                        PartitionData & metrics_data = metrics_collection[current_task];
                        storage = DatabaseCatalog::instance().getTable({entry->database, entry->table}, context);
                        /// Extra check to avoid double counting of the same table.
                        if (storage && UUIDHelpers::UUIDToString(storage->getStorageUUID()) == entry->table_uuid)
                            cache_manager->getPartsInfoMetrics(*storage, metrics_data, require_partition_info);
                    }
                    catch (Exception & e)
                    {
                        if (e.code() == ErrorCodes::CANNOT_GET_TABLE_LOCK && storage)
                            LOG_WARNING(
                                getLogger("PartsInfoLocal"),
                                "Failed to get parts info for table {} because cannot get table lock, skip it.",
                                storage->getStorageID().getFullTableName());
                    }
                    catch (...)
                    {
                    }
                }
            });
        }
    }
    catch (...)
    {
        need_abort = true;
        collect_metrics_pool.wait();
        throw;
    }

    collect_metrics_pool.wait();

    MutableColumns res_columns = res_block.cloneEmptyColumns();
    for (size_t i = 0; i < filtered_index_column->size(); i++)
    {
        auto entry = active_tables[(*filtered_index_column)[i].get<UInt64>()];
        PartitionData & metrics = metrics_collection[i];
        for (auto & [partition_id, metric] : metrics)
        {
            size_t src_index = 0;
            size_t dest_index = 0;
            const auto & metrics_data = metric->partition_info_ptr->metrics_ptr->read();
            bool is_valid_metrics = metrics_data.validateMetrics();
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(entry->table_uuid);
            if (columns_mask[src_index++])
            {
                if (!tenant_id.empty())
                    res_columns[dest_index++]->insert(getOriginalDatabaseName(entry->database, tenant_id));
                else
                    res_columns[dest_index++]->insert(entry->database);
            }
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(entry->table);
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(partition_id);
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(metric->partition);
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(metric->first_partition);
            if (is_valid_metrics)
            {
                // valid metrics
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(true);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_parts_number);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_parts_size);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_rows_count);
            }
            else
            {
                // invalid metrics
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(false);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
            }
            if (columns_mask[src_index++])
            {
                ReadyState state = ReadyState::Unloaded;
                if (metric->partition_info_ptr->metrics_ptr->finishFirstRecalculation())
                    state = ReadyState::Recalculated;
                else if (entry->partition_metrics_loaded)
                    state = ReadyState::Loaded;
                else if (entry->loading_metrics)
                    state = ReadyState::Loading;
                res_columns[dest_index++]->insert(state);
            }
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(metric->partition_info_ptr->metrics_ptr->isRecalculating());
            if (columns_mask[src_index++])
            {
                UInt64 last_update_time = metrics_data.last_update_time;
                if (last_update_time == TxnTimestamp::maxTS())
                    last_update_time = current_ts;
                res_columns[dest_index++]->insert(TxnTimestamp(last_update_time).toSecond());
            }
            if (columns_mask[src_index++])
            {
                UInt64 last_snapshot_time = metrics_data.last_snapshot_time;
                if (last_snapshot_time == TxnTimestamp::maxTS())
                    last_snapshot_time = current_ts;
                res_columns[dest_index++]->insert(TxnTimestamp(last_snapshot_time).toSecond());
            }
            if (columns_mask[src_index++])
            {
                res_columns[dest_index++]->insert(TxnTimestamp(metrics_data.last_modification_time).toSecond());
            }
        }
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(res_block), std::move(chunk)));
}

}
