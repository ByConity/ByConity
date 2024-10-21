/**
 * @brief Metrics class for both parts and trashed items.
 */

#pragma once

#include <atomic>
#include <optional>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <Protos/data_models.pb.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Common/RWLock.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace Catalog
{
    struct TrashItems;
}

class StorageCnchMergeTree;

/**
 * @class TableMetrics
 * @brief Table level metrics for trashed items.
 */
class TableMetrics
{
public:
    /**
     * @class TableMetricsData
     * @brief Table level aggregated metrics data for trashed items.
     * It's held by `TableMetrics`.
     * There is no need to get lock to access these data concurrently.
     *
     */
    struct TableMetricsData
    {
        std::atomic<int64_t> total_parts_number{0};
        std::atomic<int64_t> total_parts_size{0};
        std::atomic<int64_t> total_bitmap_number{0};
        std::atomic<int64_t> total_bitmap_size{0};
        std::atomic<uint64_t> last_update_time{0};
        std::atomic<uint64_t> last_snapshot_time{0};

        TableMetricsData() = default;
        explicit TableMetricsData(Protos::TableTrashItemsMetricsSnapshot & snapshot);
        TableMetricsData(const TableMetricsData & rhs);
        TableMetricsData & operator=(const TableMetricsData & rhs);
        TableMetricsData operator+(const TableMetricsData & rhs) const;
        TableMetricsData & operator+=(const TableMetricsData & rhs);

        /**
         * @brief Generate a snapshot.
         */
        Protos::TableTrashItemsMetricsSnapshot toSnapshot() const;
        String toString() const { return toSnapshot().DebugString(); }

        bool validateMetrics() const;
        /// We have two phases of GC. For dropped_parts,
        /// phase-one GC will add some value to statistics,
        /// meanwhile phase-two GC will substract it from statistics.
        /// So `positive` means whether the value is added or substracted.
        void update(const ServerDataPartPtr & part, size_t ts, bool positive = true);
        void update(const DeleteBitmapMetaPtr & bitmap, size_t ts, bool positive = true);
        void update(const Protos::DataModelPart & part, size_t ts, bool positive = true);
        void update(const Protos::DataModelDeleteBitmap & bitmap, size_t ts, bool positive = true);
        bool matches(const TableMetricsData & rhs) const;
    };

    void recalculate(size_t current_time, ContextPtr context, bool force = false);
    bool isRecalculating() { return recalculating.load(); }
    void restoreFromSnapshot(Protos::TableTrashItemsMetricsSnapshot & snapshot);

    TableMetrics() = delete;
    explicit TableMetrics(const String & table_uuid_) : table_uuid(table_uuid_) { }

    TableMetricsData & getDataRef() { return data; }
    String & getTraceID() { return table_uuid; }

    void shutDown();

private:
    /// Metrics data.
    TableMetricsData data;
    String table_uuid;
    /// If there is a on-going recalculation.
    std::atomic<bool> recalculating{false};
    /// Protected by `recalculating`.
    std::optional<Stopwatch> stopwatch;
    std::atomic<bool> shutdown = false;
    /// When loading for the first time, we consider this value is false to trigger a more agressive recalculation.
    /// Protected by `recalculating`.
    bool recalculateion_miss = false;
};

/**
 * @class PartitionMetrics
 * @brief Partition level metrics for parts.
 *
 */
class PartitionMetrics
{
public:
    /**
     * @class PartitionMetricsStore
     * @brief Partition level aggregated metrics data for parts.
     * Must guards with a lock before concurrently access.

     */
    struct PartitionMetricsStore
    {
        /// Parts that visible to users.
        int64_t total_parts_size{0};
        int64_t total_parts_number{0};
        int64_t total_rows_count{0};
        /// Parts that is in phase-one GC.
        int64_t dropped_parts_size{0};
        int64_t dropped_parts_number{0};
        // Will be true if there is one part that has bucket_number == -1
        bool has_bucket_number_neg_one{false};
        // False if there are multiple table_definition_hash in this partition
        bool is_single_table_definition_hash{true};
        // Will represent the single table_definition_hash if true, else any other table_definition_hash
        uint64_t table_definition_hash{0};
        // Do not consider deleted parts
        bool is_deleted{true};
        uint64_t last_update_time{0};
        uint64_t last_snapshot_time{0};
        uint64_t last_modification_time{0};

        PartitionMetricsStore() = default;
        explicit PartitionMetricsStore(const Protos::PartitionPartsMetricsSnapshot & snapshot);
        explicit PartitionMetricsStore(ServerDataPartsWithDBM & parts_with_dbm, const StorageCnchMergeTree & storage);

        PartitionMetricsStore operator+(const PartitionMetricsStore & rhs) const;
        PartitionMetricsStore & operator+=(const PartitionMetricsStore & rhs);

        Protos::PartitionPartsMetricsSnapshot toSnapshot() const;
        String toString() const { return toSnapshot().ShortDebugString(); }

        void updateLastModificationTime(const Protos::DataModelPart & part_model);
        void update(const Protos::DataModelPart & part_model);
        /// Called by phase-one GC.
        void removeDroppedPart(const Protos::DataModelPart & part_model);
        bool validateMetrics() const;
        bool matches(const PartitionMetricsStore & rhs) const;
    };

    bool validateMetrics();
    /// Called by phase-one GC.
    void removeDroppedPart(const Protos::DataModelPart & part_model);
    bool isRecalculating() { return recalculating.load(); }

    void update(const Protos::DataModelPart & part_model);

    PartitionMetricsStore read();


    /**
     * @brief Try recalculate.
     *
     * @return If the recalculation accually happened.
     */
    bool recalculate(size_t current_time, ContextPtr context, bool force = false);

    void restoreFromSnapshot(const PartitionMetrics & other);

    void recalculateBottomHalf(ContextPtr context);
    bool finishFirstRecalculation() {return finished_first_recalculation.load();}

    PartitionMetrics() = delete;
    PartitionMetrics(const String & table_uuid_, const String & partition_id_, bool newly_inserted = false)
        : table_uuid(table_uuid_), partition_id(partition_id_), finished_first_recalculation(newly_inserted)
    {
    }

    PartitionMetrics(Protos::PartitionPartsMetricsSnapshot & snapshot, const String & table_uuid_, const String & partition_id_)
        : new_store(snapshot), table_uuid(table_uuid_), partition_id(partition_id_)
    {
    }
    PartitionMetrics(Protos::PartitionPartsMetricsSnapshot && snapshot, const String & table_uuid_, const String & partition_id_)
        : new_store(std::move(snapshot)), table_uuid(table_uuid_), partition_id(partition_id_)
    {
    }

    String getTraceID() { return table_uuid + " " + partition_id; }
    void shutDown();
    /// This is called before `shutDown` to quickly notify the recalculation task to abort itself.
    void notifyShutDown() { shutdown = true; }

    ~PartitionMetrics() { shutDown(); }

private:
    /// TIMEOUT in seconds.
    static constexpr int BIAS = 6;
    /// Partial (or full) data store of metrics.
    PartitionMetricsStore new_store;
    /// If current `PartitionMetrics` is recalculating.
    std::atomic<bool> recalculating{false};
    /// Protected by `recalculating`.
    std::optional<Stopwatch> stopwatch;
    /// Store the data that before recalculation time point.
    /// Will be `std::nullopt` when not recalculating.
    std::optional<std::pair<size_t, PartitionMetricsStore>> old_store;
    String table_uuid;
    String partition_id;
    std::shared_mutex mutex;
    /// Recalculation miss is ture when the current recalculation result matches the data in memory.
    /// When loading for the first time, we consider this value is false to trigger a more agressive recalculation.
    /// Protected by `recalculating`.
    bool recalculateion_miss = false;
    /// We can consider the statistics is accurate after the first recalculation.
    /// Such as caller `MaterializedView` needs to know this.
    std::atomic<bool> finished_first_recalculation = false;

    /// Hold a task to recalculate after timeout.
    BackgroundSchedulePool::TaskHolder recalculate_task;
    std::atomic<bool> recalculate_task_initialized = false;
    std::atomic<bool> shutdown = false;
    std::atomic<size_t> recalculate_current_time = 0;
};

using PartitionMetricsStorePtr = std::shared_ptr<PartitionMetrics::PartitionMetricsStore>;
}
