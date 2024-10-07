#include <Catalog/Catalog.h>
#include <Storages/CnchPartitionInfo.h>
#include <Storages/CnchTablePartitionMetricsHelper.h>
#include <Storages/PartCacheManager.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
extern const Metric SystemCnchPartsInfoRecalculationTasksSize;
extern const Metric SystemCnchTrashItemsInfoRecalculationTasksSize;
}

namespace DB
{


CnchTablePartitionMetricsHelper::CnchTablePartitionMetricsHelper(ContextPtr context_)
    : WithContext(context_), log(getLogger("CnchTablePartitionMetricsHelper"))
{
    metrics_updater = getContext()->getMetricsRecalculationSchedulePool().createTask("PartMetricsUpdater", [this]() {
        try
        {
            LOG_TRACE(log, "Recalculation scheduled.");
            recalculateOrSnapshotPartitionsMetrics();
        }
        catch (...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        /// Schedule every 30 mins, maybe could be configurable later.
        this->metrics_updater->scheduleAfter(30 * 60 * 1000);
    });
    metrics_initializer = getContext()->getMetricsRecalculationSchedulePool().createTask("PartMetricsInitializer", [this]() {
        try
        {
            initTablePartitionsMetrics();
        }
        catch (...)
        {
            tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }
        /// schedule every 10 seconds
        this->metrics_initializer->scheduleAfter(10 * 1000);
    });
    if (getContext()->getServerType() == ServerType::cnch_server)
    {
        metrics_updater->activate();
        /// First schedule ETA in 5 mins.
        /// We don't need to strictly follow the interval above,
        /// since we will validate inside the recalculate function.
        metrics_updater->scheduleAfter(5 * 30 * 1000);
        metrics_initializer->activateAndSchedule();
    }
}
bool CnchTablePartitionMetricsHelper::getPartsInfoMetrics(
    const IStorage & i_storage, std::unordered_map<String, PartitionFullPtr> & partitions, bool require_partition_info)
{
    auto mgr = getContext()->getPartCacheManager();
    if (mgr == nullptr)
        return false;

    auto table_entry = mgr->getTableMeta(i_storage.getStorageUUID());

    if (table_entry)
    {
        const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(&i_storage);
        if (!storage)
            return true;
        FormatSettings format_settings{};
        auto & table_partitions = table_entry->partitions;
        for (auto it = table_partitions.begin(); it != table_partitions.end(); it++)
        {
            PartitionFullPtr partition_ptr = std::make_shared<CnchPartitionInfoFull>(*it);
            const auto & partition_key_sample = storage->getInMemoryMetadataPtr()->getPartitionKey().sample_block;
            if (partition_key_sample.columns() > 0 && require_partition_info)
            {
                {
                    WriteBufferFromOwnString out;
                    partition_ptr->partition_info_ptr->partition_ptr->serializeText(*storage, out, format_settings);
                    partition_ptr->partition = out.str();
                }
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
    return false;
}

void CnchTablePartitionMetricsHelper::updateMetrics(
    const DataModelPartWrapperVector & parts_wrapper_vector,
    std::shared_ptr<PartitionMetrics> partition_info_metrics_ptr,
    const UUID & uuid,
    UInt64 ts)
{
    if (!partition_info_metrics_ptr)
        return;
    auto update_metrics
        = [uuid, ts, this](const std::shared_ptr<Protos::DataModelPart> & model, std::shared_ptr<PartitionMetrics> metrics_ptr) {
              auto mgr = getContext()->getPartCacheManager();
              if (mgr == nullptr)
                  return;

              auto meta_ptr = mgr->getTableMeta(uuid);
              if (meta_ptr == nullptr)
                  return;
              meta_ptr->metrics_last_update_time = ts;
              metrics_ptr->update(*model);
          };
    for (const auto & part_wrapper_ptr : parts_wrapper_vector)
    {
        update_metrics(part_wrapper_ptr->part_model, partition_info_metrics_ptr);
    }
}

void CnchTablePartitionMetricsHelper::shutDown(PartCacheManager * manager)
{
    auto tables_snapshot = manager->getTablesSnapshot();

    for (auto & [table_uuid, table_meta_ptr] : tables_snapshot)
    {
        if (table_meta_ptr == nullptr)
            continue;
        table_meta_ptr->trash_item_metrics->shutDown();
        for (auto & partition : table_meta_ptr->partitions)
        {
            if (partition && partition->metrics_ptr)
            {
                partition->metrics_ptr->shutDown();
            }
        }
    }

    metrics_updater->deactivate();
    metrics_initializer->deactivate();
    getTablePartitionThreadPool().finalize();
}

void CnchTablePartitionMetricsHelper::initTablePartitionsMetrics()
{
    auto mgr = getContext()->getPartCacheManager();
    if (mgr == nullptr)
        return;

    auto tables_snapshot = mgr->getTablesSnapshot();

    auto cnch_catalog = getContext()->getCnchCatalog();

    for (auto & table_snapshot : tables_snapshot)
    {
        if (table_snapshot.second == nullptr || table_snapshot.second->loading_metrics || table_snapshot.second->partition_metrics_loaded)
            continue;
        UUID uuid = table_snapshot.first;
        TableMetaEntryPtr meta_ptr = table_snapshot.second;
        initializeMetricsFromMetastore(uuid, meta_ptr);
    }
}

void CnchTablePartitionMetricsHelper::recalculateOrSnapshotPartitionsMetrics(bool force)
{
    auto mgr = getContext()->getPartCacheManager();
    if (mgr == nullptr)
        return;

    auto tables_snapshot = mgr->getTablesSnapshot();
    auto cnch_catalog = getContext()->getCnchCatalog();
    UInt64 current_time{0};
    try
    {
        current_time = getContext()->getTimestamp();
    }
    catch (...)
    {
        tryLogDebugCurrentException(log);

        return;
    }


    for (auto & [table_uuid, meta_ptr] : tables_snapshot)
    {
        LOG_TRACE(log, "recalculateOrSnapshotPartitionsMetrics {}, force: {}", UUIDHelpers::UUIDToString(table_uuid), force);

        if (meta_ptr->loading_metrics || !meta_ptr->partition_metrics_loaded)
            continue;

        /// To avoid starvation of tasks, we need to wait indefinitely.
        recalculateOrSnapshotPartitionsMetrics(meta_ptr, current_time, force, std::nullopt);
    }
}


void CnchTablePartitionMetricsHelper::recalculateOrSnapshotPartitionsMetrics(
    TableMetaEntryPtr & table_meta_ptr, size_t current_time, bool force, std::optional<size_t> schedule_timeout)
{
    if (table_meta_ptr == nullptr)
    {
        LOG_WARNING(log, "Get empty table_meta_ptr");
        return;
    }

    auto mgr = getContext()->getPartCacheManager();
    if (mgr == nullptr)
        return;
    auto cnch_catalog = getContext()->getCnchCatalog();

    /// Recalculate table level `load_parts_by_partition`.
    {
        {
            size_t total_parts_number{0};
            auto & meta_partitions = table_meta_ptr->partitions;
            for (auto it = meta_partitions.begin(); it != meta_partitions.end(); it++)
            {
                auto & partition_info_ptr = *it;
                if (partition_info_ptr == nullptr || partition_info_ptr->metrics_ptr == nullptr)
                    continue;
                auto metrics_data = partition_info_ptr->metrics_ptr->read();
                total_parts_number += metrics_data.total_parts_number;
            }
            {
                auto lock = table_meta_ptr->writeLock();
                table_meta_ptr->partition_metrics_loaded = true;
                /// reset load_parts_by_partition if parts number of current table is less than 5 million;
                if (table_meta_ptr->load_parts_by_partition && total_parts_number < 5000000)
                    table_meta_ptr->load_parts_by_partition = false;
            }
        }
    }

    for (auto & partition : table_meta_ptr->partitions)
    {
        if (partition == nullptr || partition->metrics_ptr == nullptr)
            continue;

        LOG_TRACE(log, "recalculateOrSnapshotPartitionsMetrics {} {}", table_meta_ptr->table, partition->partition_id);

        auto thread_group = CurrentThread::getGroup();
        auto task = [this, partition, current_time, table_meta_ptr, force, thread_group]() {
            CurrentMetrics::Increment metric_increment(CurrentMetrics::SystemCnchPartsInfoRecalculationTasksSize);
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
            /// After actually recalculated, update `metrics_last_update_time`.
            if (partition->metrics_ptr->recalculate(current_time, getContext(), force))
            {
                auto lock = table_meta_ptr->writeLock();
                table_meta_ptr->metrics_last_update_time = current_time;
            }
        };

        if (schedule_timeout)
        {
            getTablePartitionThreadPool().scheduleOrThrow(task, 0, *schedule_timeout);
        }
        else
        {
            getTablePartitionThreadPool().scheduleOrThrowOnError(task);
        }
    }

    /// Schedule a table level trash items recalculation.
    auto task = [this, table_meta_ptr, current_time, force]() {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::SystemCnchTrashItemsInfoRecalculationTasksSize};
        table_meta_ptr->trash_item_metrics->recalculate(current_time, getContext(), force);
    };

    if (schedule_timeout)
    {
        getTablePartitionThreadPool().scheduleOrThrow(task, 0, *schedule_timeout);
    }
    else
    {
        getTablePartitionThreadPool().scheduleOrThrowOnError(task);
    }
}

void CnchTablePartitionMetricsHelper::initializeMetricsFromMetastore(const UUID uuid, const TableMetaEntryPtr & table_meta)
{
    if (table_meta == nullptr)
    {
        LOG_WARNING(log, "Get empty table_meta_ptr");
        return;
    }

    auto table_uuid = UUIDHelpers::UUIDToString(uuid);
    LOG_TRACE(log, "initializeMetricsFromMetastore {}", table_uuid);

    /// Before loading partition metrics, we must make sure all partition info is cached.
    if (table_meta->cache_status != CacheStatus::LOADED)
    {
        return;
    }

    table_meta->loading_metrics = true;
    UInt64 current_time{0};
    try
    {
        current_time = getContext()->getTimestamp();
    }
    catch (...)
    {
        tryLogDebugCurrentException(log);

        return;
    }

    auto metrics_map = getContext()->getCnchCatalog()->loadPartitionMetricsSnapshotFromMetastore(table_uuid);

    auto & meta_partitions = table_meta->partitions;
    for (auto it = meta_partitions.begin(); it != meta_partitions.end(); it++)
    {
        auto & partition_info_ptr = *it;
        if (partition_info_ptr == nullptr)
            continue;
        const String & partition_id = partition_info_ptr->partition_id;
        auto found = metrics_map.find(partition_id);

        /// Partition exists, but no metrics snapshot found.
        /// In this case, we need to (try) recalculate the history.
        if (found != metrics_map.end())
        {
            /// Initialize metrics from snapshot.
            LOG_TRACE(log, "Partition {} metrics loaded from metastore: {}", found->first, found->second->read().toString());
            partition_info_ptr->metrics_ptr->restoreFromSnapshot(*found->second);
        }

        /// Recalculate no matter if metrics snapshot exists or not.
        /// This will benifit the accuracy with the price of IO consumption.
        getTablePartitionThreadPool().scheduleOrThrowOnError([this, partition_info_ptr, current_time, table_meta]() {
            CurrentMetrics::Increment metric_increment(CurrentMetrics::SystemCnchPartsInfoRecalculationTasksSize);
            /// After actually recalculated, update `metrics_last_update_time`.
            if (partition_info_ptr->metrics_ptr->recalculate(current_time, getContext()))
            {
                auto lock = table_meta->writeLock();
                table_meta->metrics_last_update_time = current_time;
            }
        });
    }

    auto && table_trash_items_snapshot = getContext()->getCnchCatalog()->loadTableTrashItemsMetricsSnapshotFromMetastore(table_uuid);
    if (table_trash_items_snapshot.last_snapshot_time() != 0)
    {
        table_meta->trash_item_metrics->restoreFromSnapshot(table_trash_items_snapshot);
    }

    /// Schedule a trash items metrics recalculation.
    getTablePartitionThreadPool().scheduleOrThrowOnError([this, table_meta, current_time]() {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::SystemCnchTrashItemsInfoRecalculationTasksSize};

        table_meta->trash_item_metrics->recalculate(current_time, getContext());
    });

    table_meta->partition_metrics_loaded = true;
    table_meta->loading_metrics = false;
}

void CnchTablePartitionMetricsHelper::updateMetrics(
    const Catalog::TrashItems & items, std::shared_ptr<TableMetrics> table_metrics_ptr, bool positive)
{
    if (table_metrics_ptr == nullptr)
        return;

    auto current_time = getContext()->getTimestamp();
    for (const auto & part : items.data_parts)
        table_metrics_ptr->getDataRef().update(part, current_time, positive);
    /// not handling staged parts because we never move them to trash
    for (const auto & bitmap : items.delete_bitmaps)
        table_metrics_ptr->getDataRef().update(bitmap, current_time, positive);
}
std::shared_ptr<TableMetrics> CnchTablePartitionMetricsHelper::getTrashItemsInfoMetrics(const IStorage & i_storage)
{
    if (auto mgr = getContext()->getPartCacheManager())
    {
        if (auto table_entry = mgr->getTableMeta(i_storage.getStorageUUID()))
        {
            return table_entry->trash_item_metrics;
        }
    }
    return nullptr;
}
}
