#include <Storages/System/StorageSystemCnchTrashItemsInfoLocal.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/PartCacheManager.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_TABLE_LOCK;
}

StorageSystemCnchTrashItemsInfoLocal::StorageSystemCnchTrashItemsInfoLocal(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {{"uuid", std::make_shared<DataTypeString>()},
         {"database", std::make_shared<DataTypeString>()},
         {"table", std::make_shared<DataTypeString>()},
         {"metrics_available", std::make_shared<DataTypeUInt8>()},
         {"total_parts_number", std::make_shared<DataTypeInt64>()},
         {"total_parts_size", std::make_shared<DataTypeInt64>()},
         {"total_bitmap_number", std::make_shared<DataTypeInt64>()},
         {"total_bitmap_size", std::make_shared<DataTypeInt64>()},
         {"last_update_time", std::make_shared<DataTypeUInt64>()},
         {"last_snapshot_time", std::make_shared<DataTypeUInt64>()}}));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemCnchTrashItemsInfoLocal::read(
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

    for (size_t i = 0; i < active_tables.size(); i++)
    {
        database_column_mut->insert(active_tables[i]->database);
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

    size_t max_threads = 10;
    ThreadPool collect_metrics_pool(max_threads);
    ColumnPtr filtered_index_column = block_to_filter.getByName("index").column;
    std::atomic_size_t task_index{0};
    std::size_t total_task_size = filtered_index_column->size();
    std::vector<std::shared_ptr<TableMetrics>> metrics_collection{total_task_size};
    std::atomic_bool need_abort = false;

    try {
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
                        storage = DatabaseCatalog::instance().getTable({entry->database, entry->table}, context);
                        if (storage && UUIDHelpers::UUIDToString(storage->getStorageUUID()) == entry->table_uuid)
                            metrics_collection[current_task] = cache_manager->getTrashItemsInfoMetrics(*storage);
                    }
                    catch (Exception & e)
                    {
                        if (e.code() == ErrorCodes::CANNOT_GET_TABLE_LOCK && storage)
                            LOG_WARNING(
                                getLogger("TrashItemsInfoLocal"),
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
        auto & metrics = metrics_collection[i];
        {
            size_t src_index = 0;
            size_t dest_index = 0;
            bool is_valid_metrics = metrics? metrics->getDataRef().validateMetrics(): false;
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(entry->table_uuid);
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(entry->database);
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(entry->table);
            if (columns_mask[src_index++])
                res_columns[dest_index++]->insert(is_valid_metrics);
            if (is_valid_metrics)
            {
                const auto & metrics_data = metrics->getDataRef();
                // valid metrics
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_parts_number.load());
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_parts_size.load());
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_bitmap_number.load());
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(metrics_data.total_bitmap_size.load());
                if (columns_mask[src_index++])
                {
                    UInt64 last_update_time = metrics_data.last_update_time.load();
                    if (last_update_time == TxnTimestamp::maxTS())
                        last_update_time = current_ts;
                    res_columns[dest_index++]->insert(TxnTimestamp(last_update_time).toSecond());
                }
                if (columns_mask[src_index++])
                {
                    UInt64 last_snapshot_time = metrics_data.last_snapshot_time.load();
                    if (last_snapshot_time == TxnTimestamp::maxTS())
                        last_snapshot_time = current_ts;
                    res_columns[dest_index++]->insert(TxnTimestamp(last_snapshot_time).toSecond());
                }
            }
            else
            {
                // invalid metrics
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
                if (columns_mask[src_index++])
                    res_columns[dest_index++]->insert(0);
            }
        }
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);


    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(res_block), std::move(chunk)));
}
}


