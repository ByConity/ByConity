#pragma once
#include <Catalog/Catalog.h>
#include <Core/Types.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/IStorage.h>
#include <Storages/TableMetaEntry.h>
#include <common/logger_useful.h>

namespace DB
{

/**
 * @class CnchTablePartitionMetricsHelper
 * @brief It's a server level object owned by `PartCacheManager`.
 * It contains some helper logic that help with:
 * 1. Triggering initializing metrics
 * 2. Updating metrics (in real time)
 * 3. Recalculating metrics
 */
class CnchTablePartitionMetricsHelper: WithContext
{
public:
    explicit CnchTablePartitionMetricsHelper(ContextPtr context_);
    /**
     * @brief Get CNCH parts metrics (partition level). This function does not
     * have extra IO costs.
     */
    bool getPartsInfoMetrics(
        const IStorage & i_storage, std::unordered_map<String, PartitionFullPtr> & partitions, bool require_partition_info);

    /**
     * @brief Get CNCH trash items metrics (table level). This function does not
     * have extra IO costs.
     */
    std::shared_ptr<TableMetrics> getTrashItemsInfoMetrics(const IStorage & i_storage);

    /**
     * @brief Update CNCH parts metrics for a single partition.
     */
    void updateMetrics(
        const DataModelPartWrapperVector & parts_wrapper_vector,
        std::shared_ptr<PartitionMetrics> partition_info_metrics_ptr,
        const UUID & uuid,
        UInt64 ts);

    /**
     * @brief Update CNCH trash items metrics for a table.
     */
    void updateMetrics(const Catalog::TrashItems & items, std::shared_ptr<TableMetrics> table_metrics_ptr, bool positive = true);

    /**
     * @brief A hook function gets triggered when shutdown.
     */
    void shutDown();

    /**
     * @brief Trigger a recalculation of both CNCH parts and trash items
     * metrics.
     */
    void recalculateOrSnapshotPartitionsMetrics(TableMetaEntryPtr & table_meta_ptr, size_t current_time, bool force = false);

private:
    // Used to correct the metrics periodically.
    BackgroundSchedulePool::TaskHolder metrics_updater;
    // Used to collect metrics if it is not ready.
    BackgroundSchedulePool::TaskHolder metrics_initializer;

    // Schedule a recalculation task.
    // This thread pool used to be called `PartCacheManagerThreadPool` so reused its setting.
    ThreadPool table_partition_thread_pool{getContext()->getSettingsRef().part_cache_manager_thread_pool_size};
    ThreadPool & getTablePartitionThreadPool() { return table_partition_thread_pool; }

    Poco::Logger * log;

    /**
     * @brief Trigger a recalculation of both CNCH parts and trash items
     * metrics.
     */
    void recalculateOrSnapshotPartitionsMetrics(bool force = false);
    /**
     * @brief Init both CNCH parts and trash items metrics by loading snapshots
     * from metastore.
     */
    void initTablePartitionsMetrics();

    /**
     * @brief A inner function that initialize a table's metrics. (both CNCH parts and trash items)
     *
     * @param uuid Table UUID.
     * @param table_meta TableMetaEntryPtr of that table.
     */
    void initializeMetricsFromMetastore(UUID uuid, const TableMetaEntryPtr & table_meta);
};
}
