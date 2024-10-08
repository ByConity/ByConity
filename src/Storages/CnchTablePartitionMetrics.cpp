#include <algorithm>
#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper.h>
#include <Storages/CnchTablePartitionMetrics.h>

namespace DB
{

static constexpr auto seconds_to_timestamp = [](uint64_t seconds) { return (seconds * 1000) << 18; };
static constexpr auto hours_to_timestamp = [](uint64_t hours) { return seconds_to_timestamp(hours * 60 * 60); };
static constexpr auto days_to_timestamp = [](uint64_t days) { return hours_to_timestamp(days * 24); };


/**
 * @brief Whether or not to trigger a recalculation.
 *
 * A recalculation gets activated when:
 * 1. Recalculation never happens before.
 * 2. Metrics get updated recently and we haven't recalculated in 6 hours.
 * 3. Last update time is longer than 3 days.
 */
static bool
need_recalculate(uint64_t last_update_time, uint64_t last_snapshot_time, uint64_t current_time, bool miss, bool force, const ContextPtr & context)
{
    UInt64 recently_used_threshold = context->getSettingsRef().table_partition_metrics_recalculate_recently_used_threshold;
    UInt64 not_recently_used_threshold = context->getSettingsRef().table_partition_metrics_recalculate_not_recently_used_threshold;
    if (force)
        return true;
    if (last_update_time == 0)
    {
        // Case 1.
        return true;
    }
    else if (!miss && last_update_time == last_snapshot_time)
    {
        return true;
    }
    /// Use `last_snapshot_time` in case of `last_update_time` always be updated to `current_time`.
    else if (last_update_time > last_snapshot_time && (current_time - last_snapshot_time > hours_to_timestamp(recently_used_threshold)))
    {
        // Case 2.
        return true;
    }
    else if ((current_time - last_update_time) > days_to_timestamp(not_recently_used_threshold))
    {
        // Case 3.
        return true;
    }

    return false;
}

/**
 * @brief Whether or not to trigger a snapshot.
 *
 * A Snapshot gets activated when:
 * 1. Haven't took a snapshot.
 * 2. There are some new metrics that need to save that older than 1 hours.
 */
static bool need_snapshot(uint64_t last_update_time, uint64_t last_snapshot_time, uint64_t current_time, const ContextPtr & context)
{
    UInt64 snapshot_threshold = context->getSettingsRef().table_partition_metrics_snapshot_threshold;
    if (last_update_time == 0)
    {
        // Case 1.
        return true;
    }
    else if (last_update_time > last_snapshot_time && (current_time - last_snapshot_time) > hours_to_timestamp(snapshot_threshold))
    {
        // Case 2.
        return true;
    }

    return false;
}

void PartitionMetrics::shutDown()
{
    shutdown = true;
    if (recalculate_task_initialized && recalculate_task)
        recalculate_task->deactivate();
}

void PartitionMetrics::restoreFromSnapshot(const PartitionMetrics & other)
{
    if (shutdown)
        return;

    auto log = getLogger("PartitionMetrics");

    auto expected_value = false;
    if (!recalculating.compare_exchange_strong(expected_value, true))
    {
        LOG_WARNING(log, "{} Some other threads is calculating.", getTraceID());
        return;
    }

    std::unique_lock write_lock(mutex);
    /// Some incremental data might be inserted before the snapshot retrieved.
    new_store += other.new_store;

    recalculating = false;
}

bool PartitionMetrics::recalculate(size_t current_time, ContextPtr context, bool force)
{
    if (shutdown)
        return false;
    auto log = getLogger("PartitionMetrics");

    auto expected_value = false;
    if (!recalculating.compare_exchange_strong(expected_value, true))
    {
        LOG_WARNING(log, "{} Some other threads is calculating.", getTraceID());
        return false;
    }

    try
    {
        LOG_TRACE(log, "{} current metrics: {}", getTraceID(), read().toString());
        /// Try to trigger a recalculation.
        if (auto info = read(); need_recalculate(
                info.last_update_time,
                info.last_snapshot_time,
                current_time,
                recalculateion_miss,
                force || !info.validateMetrics(),
                context))
        {
            stopwatch = std::make_optional<Stopwatch>();
            LOG_TRACE(
                log, "{} Start recalculate {} {}, recalculation triggered at {}", getTraceID(), partition_id, getTraceID(), current_time);

            /// 1. Set pivot and old store.
            {
                std::unique_lock write_lock(mutex);

                /// TSO + BIAS(in seconds)
                old_store = std::make_optional<std::pair<size_t, PartitionMetricsStore>>(
                    {current_time + seconds_to_timestamp(BIAS), PartitionMetricsStore{}});
                std::swap(old_store.value().second, new_store);
            }

            /// 2. Wait for at least a timeout.
            LOG_TRACE(log, "{} Schedule a bottom half recalculation.", getTraceID());
            /// Only need to initialize recalculate_task for once.
            if (!recalculate_task_initialized)
            {
                std::unique_lock write_lock(mutex);
                if (!recalculate_task_initialized)
                {
                    recalculate_task
                        = context->getMetricsRecalculationSchedulePool().createTask("Recalculate-" + getTraceID(), [this, context]() {
                              SCOPE_EXIT({ recalculating = false; });
                              recalculateBottomHalf(context);
                          });
                    recalculate_task_initialized = true;
                }
            }
            if (shutdown)
            {
                recalculating = false;
                return false;
            }
            /// Only update current_time when schedule success
            /// There is no need to reset `recalculating` since the task will
            /// be executed asynchronously.
            if (recalculate_task->scheduleAfter(2 * BIAS * 1000))
                recalculate_current_time = current_time;
            else
            {
                LOG_WARNING(log, "{} schedule recalculate task failed", getTraceID());
                recalculating = false;
            }

            return true;
        }

        /// Try to trigger a snapshot even without a recalculation.
        if (auto info = read(); need_snapshot(info.last_update_time, info.last_snapshot_time, current_time, context))
        {
            LOG_TRACE(log, "{} recalculate {} {}, snapshot triggered at {}", getTraceID(), table_uuid, partition_id, current_time);

            Protos::PartitionPartsMetricsSnapshot snapshot;
            {
                std::unique_lock write_lock(mutex);
                /// 1. Update snapshot time
                new_store.last_snapshot_time = current_time;
                /// 2. Save the current state to metastore.
                ///
                /// At current time, it's guarantee that there is no `old_store`.
                snapshot = new_store.toSnapshot();
            }
            context->getCnchCatalog()->savePartitionMetricsSnapshotToMetastore(table_uuid, partition_id, snapshot);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        /// Try merge `old_store` back into `new_store`.
        if (old_store.has_value())
        {
            std::unique_lock write_lock(mutex);
            new_store += old_store.value().second;
            old_store = std::nullopt;
        }
    }

    recalculating = false;

    return false;
}

void PartitionMetrics::recalculateBottomHalf(ContextPtr context)
{
    if (shutdown)
        return;
    size_t current_time = recalculate_current_time;
    auto log = getLogger("PartitionMetrics");
    LOG_TRACE(log, "{} Recalculate bottom half.", getTraceID());

    if (!old_store.has_value())
    {
        LOG_WARNING(log, "{} recalculateBottomHalf seems not executed in right order.", getTraceID());
        return;
    }

    try
    {
        {
            /// 3. Get from KV.
            LOG_TRACE(log, "{} Get from KV", getTraceID());
            auto metrics = context->getCnchCatalog()->getPartitionMetricsStoreFromMetastore(
                table_uuid, partition_id, old_store.value().first, [this]() { return shutdown.load(); });
            LOG_TRACE(log, "{} After recalculate: {}", getTraceID(), metrics.toString());

            recalculateion_miss = metrics.matches(read());

            /// 4. Replace the old value.
            {
                std::unique_lock write_lock(mutex);
                /// Inherit `last_update_time` and `last_snapshot_time`.
                metrics.last_snapshot_time = old_store.value().second.last_snapshot_time;
                metrics.last_update_time = current_time;

                LOG_TRACE(log, "{} Update old_store", getTraceID());

                new_store += metrics;
                old_store = std::nullopt;
            }
            finished_first_recalculation = true;
            auto elapsed_seconds = -1;
            if (stopwatch.has_value())
            {
                stopwatch.value().stop();
                elapsed_seconds = stopwatch.value().elapsedSeconds();
            }

            LOG_DEBUG(
                log, "{} recalculate finished in {} seconds with value: {}", getTraceID(), toString(elapsed_seconds), read().toString());
            stopwatch = std::nullopt;
        }

        /// Try to trigger a snapshot.
        /// Abortion may happens during recalculation, which will leave a damaged metrics.
        /// We must test `shutdown` again to explicitly avoid this.
        if (auto info = read(); need_snapshot(info.last_update_time, info.last_snapshot_time, current_time, context) && !shutdown)
        {
            LOG_TRACE(log, "{} recalculate {} {}, snapshot triggered at {}", getTraceID(), table_uuid, partition_id, current_time);

            Protos::PartitionPartsMetricsSnapshot snapshot;
            {
                std::unique_lock write_lock(mutex);
                /// 1. Update snapshot time
                new_store.last_snapshot_time = current_time;
                /// 2. Save the current state to metastore.
                ///
                /// At current time, it's guarantee that there is no `old_store`.
                snapshot = new_store.toSnapshot();
            }
            context->getCnchCatalog()->savePartitionMetricsSnapshotToMetastore(table_uuid, partition_id, snapshot);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        /// Try merge `old_store` back into `new_store`.
        if (old_store.has_value())
        {
            std::unique_lock write_lock(mutex);
            new_store += old_store.value().second;
            old_store = std::nullopt;
        }
    }
}

void PartitionMetrics::update(const Protos::DataModelPart & part_model)
{
    std::unique_lock write_lock(mutex);
    if (old_store.has_value())
    {
        if (part_model.commit_time() > old_store.value().first)
        {
            new_store.update(part_model);
        }
        else
        {
            old_store.value().second.update(part_model);
        }
    }
    else
    {
        new_store.update(part_model);
    }
}
PartitionMetrics::PartitionMetricsStore PartitionMetrics::read()
{
    std::shared_lock read_lock(mutex);

    if (old_store.has_value())
    {
        return old_store->second + new_store;
    }
    else
    {
        return new_store;
    }
}
bool PartitionMetrics::validateMetrics()
{
    std::shared_lock read_lock(mutex);

    if (old_store.has_value())
    {
        return old_store.value().second.validateMetrics() || new_store.validateMetrics();
    }
    else
    {
        return new_store.validateMetrics();
    }
}

void TableMetrics::shutDown()
{
    shutdown = true;
}

void TableMetrics::restoreFromSnapshot(Protos::TableTrashItemsMetricsSnapshot & snapshot)
{
    if (shutdown)
        return;

    auto log = getLogger("TableMetrics");

    auto expected_value = false;
    if (!recalculating.compare_exchange_strong(expected_value, true))
    {
        LOG_TRACE(log, "{} Some other threads is calculating.", getTraceID());
        return;
    }

    /// Some incremental data might be inserted before the snapshot retrieved.
    data += TableMetricsData(snapshot);

    recalculating = false;
}

void TableMetrics::recalculate(size_t current_time, ContextPtr context, bool force)
{
    if (shutdown)
        return;
    auto log = getLogger("TableMetrics");

    auto expected_value = false;
    if (!recalculating.compare_exchange_strong(expected_value, true))
    {
        LOG_TRACE(log, "{} Some other threads is calculating.", getTraceID());
        return;
    }

    try
    {
        /// Try to trigger a recalculation.
        if (auto info = getDataRef(); need_recalculate(
                info.last_update_time,
                info.last_snapshot_time,
                current_time,
                recalculateion_miss,
                force || !info.validateMetrics(),
                context))
        {
            stopwatch = std::make_optional<Stopwatch>();
            LOG_TRACE(log, "{} Start recalculate {}, recalculation triggered at {}", getTraceID(), table_uuid, current_time);

            auto metrics = context->getCnchCatalog()->getTableTrashItemsMetricsDataFromMetastore(
                table_uuid, current_time, [this]() { return shutdown.load(); });

            /// Inherit `last_update_time` and `last_snapshot_time`.
            metrics.last_update_time = current_time;
            metrics.last_snapshot_time.store(data.last_snapshot_time);

            recalculateion_miss = metrics.matches(data);

            data = metrics;

            auto elapsed_seconds = -1;
            if (stopwatch.has_value())
            {
                stopwatch.value().stop();
                elapsed_seconds = stopwatch.value().elapsedSeconds();
            }
            LOG_DEBUG(
                log,
                "{} Recalculate finished in {} seconds with value: {}",
                getTraceID(),
                toString(elapsed_seconds),
                getDataRef().toString());
            stopwatch = std::nullopt;
        }

        /// Try to trigger a snapshot.
        if (auto info = getDataRef(); need_snapshot(info.last_update_time, info.last_snapshot_time, current_time, context))
        {
            LOG_TRACE(log, "{} recalculate {}, snapshot triggered at {}", getTraceID(), table_uuid, current_time);

            /// 1. Update `last_snapshot_time`.
            data.last_snapshot_time = current_time;
            /// 2. Save current state to metastore.
            context->getCnchCatalog()->saveTableTrashItemsMetricsToMetastore(table_uuid, getDataRef().toSnapshot());
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
    }
    recalculating = false;
}
Protos::TableTrashItemsMetricsSnapshot TableMetrics::TableMetricsData::toSnapshot() const
{
    Protos::TableTrashItemsMetricsSnapshot res;
    res.set_total_parts_size(total_parts_size);
    res.set_total_parts_number(total_parts_number);
    res.set_total_bitmap_size(total_bitmap_size);
    res.set_total_bitmap_number(total_bitmap_number);
    res.set_last_update_time(last_update_time);
    res.set_last_snapshot_time(last_snapshot_time);
    return res;
}
TableMetrics::TableMetricsData::TableMetricsData(const TableMetricsData & rhs)
{
    this->total_bitmap_size.store(rhs.total_bitmap_size);
    this->total_bitmap_number.store(rhs.total_bitmap_number);
    this->total_parts_size.store(rhs.total_parts_size);
    this->total_parts_number.store(rhs.total_parts_number);
    this->last_update_time.store(rhs.last_snapshot_time);
    this->last_snapshot_time.store(rhs.last_snapshot_time);
}
TableMetrics::TableMetricsData::TableMetricsData(Protos::TableTrashItemsMetricsSnapshot & snapshot)
{
    total_parts_size = snapshot.total_parts_size();
    total_parts_number = snapshot.total_parts_number();
    total_bitmap_size = snapshot.total_bitmap_size();
    total_bitmap_number = snapshot.total_bitmap_number();
    last_update_time = snapshot.last_update_time();
    last_snapshot_time = snapshot.last_snapshot_time();
}
bool TableMetrics::TableMetricsData::validateMetrics() const
{
    return total_parts_size >= 0 && total_parts_number >= 0 && total_bitmap_number >= 0 && total_bitmap_size >= 0;
}
void TableMetrics::TableMetricsData::update(const ServerDataPartPtr & part, size_t ts, bool positive)
{
    if (positive)
    {
        total_parts_number++;
        total_parts_size += part->size();
    }
    else
    {
        total_parts_number--;
        total_parts_size -= part->size();
    }
    last_update_time.store(std::max(last_update_time.load(), ts));
}
void TableMetrics::TableMetricsData::update(const DeleteBitmapMetaPtr & bitmap, size_t ts, bool positive)
{
    if (positive)
    {
        total_bitmap_number++;
        total_bitmap_size += bitmap->getModel()->file_size();
    }
    else
    {
        total_bitmap_number--;
        total_bitmap_size -= bitmap->getModel()->file_size();
    }
    last_update_time.store(std::max(last_update_time.load(), ts));
}
void TableMetrics::TableMetricsData::update(const Protos::DataModelPart & part, size_t ts, bool positive)
{
    if (positive)
    {
        total_parts_number++;
        total_parts_size += part.size();
    }
    else
    {
        total_parts_number--;
        total_parts_size -= part.size();
    }
    last_update_time.store(std::max(last_update_time.load(), ts));
}
void TableMetrics::TableMetricsData::update(const Protos::DataModelDeleteBitmap & bitmap, size_t ts, bool positive)
{
    if (positive)
    {
        total_bitmap_number++;
        total_bitmap_size += bitmap.file_size();
    }
    else
    {
        total_bitmap_number--;
        total_bitmap_size -= bitmap.file_size();
    }
    last_update_time.store(std::max(last_update_time.load(), ts));
}
PartitionMetrics::PartitionMetricsStore::PartitionMetricsStore(const Protos::PartitionPartsMetricsSnapshot & snapshot)
{
    total_parts_size = snapshot.total_parts_size();
    total_rows_count = snapshot.total_rows_count();
    total_parts_number = snapshot.total_parts_number();
    has_bucket_number_neg_one = snapshot.hash_bucket_number_neg_one();
    is_single_table_definition_hash = snapshot.is_single_table_definition_hash();
    table_definition_hash = snapshot.table_definition_hash();
    is_deleted = snapshot.is_deleted();
    last_update_time = snapshot.last_update_time();
    last_snapshot_time = snapshot.last_snapshot_time();
    last_modification_time = snapshot.last_modification_time();
}
Protos::PartitionPartsMetricsSnapshot PartitionMetrics::PartitionMetricsStore::toSnapshot() const
{
    Protos::PartitionPartsMetricsSnapshot res;
    res.set_total_parts_size(total_parts_size);
    res.set_total_rows_count(total_rows_count);
    res.set_total_parts_number(total_parts_number);
    res.set_hash_bucket_number_neg_one(has_bucket_number_neg_one);
    res.set_is_single_table_definition_hash(is_single_table_definition_hash);
    res.set_table_definition_hash(table_definition_hash);
    res.set_is_deleted(is_deleted);
    res.set_last_update_time(last_update_time);
    res.set_last_snapshot_time(last_snapshot_time);
    res.set_last_modification_time(last_modification_time);
    return res;
}
PartitionMetrics::PartitionMetricsStore PartitionMetrics::PartitionMetricsStore::operator+(const PartitionMetricsStore & rhs) const
{
    PartitionMetricsStore res;
    res.total_parts_size = this->total_parts_size + rhs.total_parts_size;
    res.total_rows_count = this->total_rows_count + rhs.total_rows_count;
    res.total_parts_number = this->total_parts_number + rhs.total_parts_number;
    res.has_bucket_number_neg_one = this->has_bucket_number_neg_one || rhs.has_bucket_number_neg_one;
    res.is_single_table_definition_hash = this->is_single_table_definition_hash && rhs.is_single_table_definition_hash;
    // TODO: verify with GuanZhe.
    res.table_definition_hash = this->table_definition_hash;
    res.is_deleted = this->is_deleted;

    res.last_update_time = std::max(this->last_update_time, rhs.last_update_time);
    res.last_snapshot_time = std::max(this->last_snapshot_time, rhs.last_snapshot_time);
    res.last_modification_time = std::max(this->last_modification_time, rhs.last_modification_time);

    return res;
}

static auto isMergePart(const Protos::DataModelPart & part) -> bool {

    return !part.deleted() && part.part_info().hint_mutation() == 0 && part.part_info().level() != 0;
}

void PartitionMetrics::PartitionMetricsStore::updateLastModificationTime(const Protos::DataModelPart & part_model)
{
    if (isMergePart(part_model))
    {
        last_modification_time = std::max(last_modification_time, part_model.last_modification_time());
    }
    else if (part_model.deleted())
    {
        last_modification_time = std::max(
            last_modification_time,
            part_model.has_last_modification_time() ? part_model.last_modification_time() : part_model.commit_time());
    }
    else
    {
        last_modification_time = std::max(last_modification_time, part_model.commit_time());
    }
}


void PartitionMetrics::PartitionMetricsStore::update(const Protos::DataModelPart & part_model)
{
    /// We ignore rows_count for partial parts.
    auto is_partial_part = part_model.part_info().hint_mutation();

    auto is_deleted_part = part_model.has_deleted() && part_model.deleted();

    updateLastModificationTime(part_model);

    if (is_deleted_part && is_partial_part)
        return;

    /// To minimize costs, we don't calculate part visibility when updating PartitionMetrics. For those parts marked as delete,
    /// just subtract them from statistics. And the non-deleted parts added into statistics. The non-deleted parts are added into
    /// statistics. For drop range, we have save all coreved parts info into it, it can be processed as deleted part directly.

    if (is_deleted_part)
    {
        total_rows_count -= (part_model.has_covered_parts_rows() ? part_model.covered_parts_rows() : part_model.rows_count());
        total_parts_size -= (part_model.has_covered_parts_size() ? part_model.covered_parts_size() : part_model.size());
        total_parts_number -= (part_model.has_covered_parts_count() ? part_model.covered_parts_count() : 1);
    }
    else
    {
        if (!is_partial_part)
            total_rows_count += part_model.rows_count();
        total_parts_size += part_model.size();
        total_parts_number += 1;
        if (part_model.bucket_number() == -1)
            has_bucket_number_neg_one = true;
        if (table_definition_hash != 0 && table_definition_hash != part_model.table_definition_hash())
            is_single_table_definition_hash = false;
        table_definition_hash = part_model.table_definition_hash();
        is_deleted = false;
    }

    last_update_time = std::max(last_update_time, part_model.commit_time());
}
bool PartitionMetrics::PartitionMetricsStore::validateMetrics() const
{
    return total_rows_count >= 0 && total_parts_size >= 0 && total_parts_number >= 0;
}
TableMetrics::TableMetricsData & TableMetrics::TableMetricsData::operator=(const TableMetricsData & rhs)
{
    this->total_parts_number.store(rhs.total_parts_number);
    this->total_parts_size.store(rhs.total_parts_size);
    this->total_bitmap_number.store(rhs.total_bitmap_number);
    this->total_bitmap_size.store(rhs.total_bitmap_size);
    this->last_update_time.store(rhs.last_update_time);
    this->last_snapshot_time.store(rhs.last_snapshot_time);
    return *this;
}
PartitionMetrics::PartitionMetricsStore & PartitionMetrics::PartitionMetricsStore::operator+=(const PartitionMetricsStore & rhs)
{
    *this = *this + rhs;
    return *this;
}
TableMetrics::TableMetricsData TableMetrics::TableMetricsData::operator+(const TableMetricsData & rhs) const
{
    TableMetricsData res;
    res.total_parts_number = this->total_parts_number + rhs.total_parts_number;
    res.total_parts_size = this->total_parts_size + rhs.total_parts_size;
    res.total_bitmap_number = this->total_bitmap_number + rhs.total_bitmap_number;
    res.total_bitmap_size = this->total_bitmap_size + rhs.total_bitmap_size;
    res.last_update_time = std::max(this->last_update_time.load(), rhs.last_update_time.load());
    res.last_snapshot_time = std::max(this->last_snapshot_time.load(), rhs.last_snapshot_time.load());
    return res;
}
TableMetrics::TableMetricsData & TableMetrics::TableMetricsData::operator+=(const TableMetricsData & rhs)
{
    *this = *this + rhs;
    return *this;
}
bool TableMetrics::TableMetricsData::matches(const TableMetricsData & rhs) const
{
    return total_parts_size == rhs.total_parts_size && total_parts_number == rhs.total_parts_number
        && total_bitmap_size == rhs.total_bitmap_size && total_bitmap_number == rhs.total_bitmap_number;
}
bool PartitionMetrics::PartitionMetricsStore::matches(const PartitionMetricsStore & rhs) const
{
    return total_parts_size == rhs.total_parts_size && total_parts_number == rhs.total_parts_number
        && total_rows_count == rhs.total_rows_count && last_modification_time == rhs.last_modification_time;
}
}
