#pragma once
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/IStorage.h>
#include <Storages/TableMetaEntry.h>
#include <common/logger_useful.h>

namespace DB
{

class PartCacheManager;

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
    void shutDown(PartCacheManager * manager);

    /**
     * @brief Trigger a  recalculation of both CNCH parts and trash items
     * metrics. When using this function, special attention needs to be
     * paid to whether the caller can accept long waits.
     * If `timeout` is set, be careful about potential task starvation.
     *
     * @param table_meta_ptr Represent the table we need to do the recalculation.
     * @param current_time A (current) timestamp that marks the bound.
     * @param force Whether to force the actual recalculation.
     * @param schedule_timeout Will wait forever with `std::nullopt`. Abort immediately by default.
     */
    void recalculateOrSnapshotPartitionsMetrics(
        TableMetaEntryPtr & table_meta_ptr, size_t current_time, bool force = false, std::optional<size_t> schedule_timeout = 0);

private:
    // Used to correct the metrics periodically.
    BackgroundSchedulePool::TaskHolder metrics_updater;
    // Used to collect metrics if it is not ready.
    BackgroundSchedulePool::TaskHolder metrics_initializer;

    // Schedule a recalculation task.
    // This thread pool used to be called `PartCacheManagerThreadPool` so reused its setting.
    ThreadPool table_partition_thread_pool{
        getContext()->getSettingsRef().part_cache_manager_thread_pool_size,
        getContext()->getSettingsRef().part_cache_manager_thread_pool_size,
        1000 * getContext()->getSettingsRef().part_cache_manager_thread_pool_size,
        // As a background schedule pool, we don't want the pool gets shutdown
        // when queries throw exceptions.
        false};
    ThreadPool & getTablePartitionThreadPool() { return table_partition_thread_pool; }

    LoggerPtr log;

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
