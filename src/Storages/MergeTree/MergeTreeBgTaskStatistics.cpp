#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>

#include <Catalog/Catalog.h>
#include <Common/ThreadPool.h>
#include <Interpreters/executeQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
}

static void addInsertedPartsImpl(MergeTreeBgTaskStatsInfo & partition_stats, UInt64 num_parts, UInt64 bytes, time_t ts)
{
    if (num_parts == 0)
        return;

    /// stats.first is inserted_bytes
    auto update_duration_stats = [bytes](MergeTreeBgTaskStatsInfo::InsertedAndMergedBytes & stats) { stats.first += bytes; };
    partition_stats.last_hour_stats.update(ts, update_duration_stats);
    partition_stats.last_6hour_stats.update(ts, update_duration_stats);
    partition_stats.inserted_parts += num_parts;
    partition_stats.last_insert_time = std::max(partition_stats.last_insert_time, ts);
}

static void addMergedPartsImpl(MergeTreeBgTaskStatsInfo & partition_stats, UInt64 num_parts, UInt64 bytes, time_t ts)
{
    if (num_parts == 0)
        return;

    /// stats.second is merged_bytes
    auto update_duration_stats = [bytes](MergeTreeBgTaskStatsInfo::InsertedAndMergedBytes & stats) { stats.second += bytes; };
    partition_stats.last_hour_stats.update(ts, update_duration_stats);
    partition_stats.last_6hour_stats.update(ts, update_duration_stats);
    partition_stats.merged_parts += num_parts;
}

static void addRemovedPartsImpl(MergeTreeBgTaskStatsInfo & partition_stats, UInt64 num_parts)
{
    if (num_parts == 0)
        return;

    partition_stats.removed_parts += num_parts;
}

MergeTreeBgTaskStatistics::MergeTreeBgTaskStatistics(StorageID storage_id_)
    : storage_id(storage_id_)
{

}

void MergeTreeBgTaskStatistics::addInsertedParts(const String & partition_id, UInt64 num_parts, UInt64 bytes, time_t ts)
{
    /// Initializing by server_part_log may be delayed, we may get duplicated records if addInsertedParts before initialization.
    if (getInitializeState() == InitializeState::Uninitialized)
        return;
    std::unique_lock<std::shared_mutex> lock(mutex);
    addInsertedPartsImpl(partition_stats_map[partition_id], num_parts, bytes, ts);
}

void MergeTreeBgTaskStatistics::addMergedParts(const String & partition_id, UInt64 num_parts, UInt64 bytes, time_t ts)
{
    if (getInitializeState() == InitializeState::Uninitialized)
        return;
    std::unique_lock<std::shared_mutex> lock(mutex);
    addMergedPartsImpl(partition_stats_map[partition_id], num_parts, bytes, ts);
}

void MergeTreeBgTaskStatistics::addRemovedParts(const String & partition_id, UInt64 num_parts)
{
    if (getInitializeState() == InitializeState::Uninitialized)
        return;
    std::unique_lock<std::shared_mutex> lock(mutex);
    addRemovedPartsImpl(partition_stats_map[partition_id], num_parts);
}

void MergeTreeBgTaskStatistics::dropPartition(const String & partition_id)
{
    if (getInitializeState() == InitializeState::Uninitialized)
        return;
    std::unique_lock<std::shared_mutex> lock(mutex);
    partition_stats_map.erase(partition_id);
}

void MergeTreeBgTaskStatistics::fillMissingPartitions(const Strings & partition_ids)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (auto partition_id: partition_ids)
        partition_stats_map.try_emplace(partition_id);
}

void MergeTreeBgTaskStatistics::executeWithReadLock(ReadCallback callback) const
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    callback(partition_stats_map);
}

void MergeTreeBgTaskStatistics::executeWithWriteLock(WriteCallback callback)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    callback(partition_stats_map);
}



/// Attached parts will be stored as InsertPart in server part log if using hdfs, so inserted_parts in statistics
/// may be more than really inserted parts when parts is detached and attached. For merge adaptive controller,
/// this will cause a weaker write amplification control. For partition selector, this will enable select more
/// merge tasks in these partitions. Both is ok. 
static const char * SQL_LOAD_PARTITION_DIGEST_INFO_TEMPLATE = R"""(
SELECT uuid, partition_id,
    toStartOfHour(event_time) AS start_of_hour,
    countIf(event_type='InsertPart') AS inserted_parts,
    sumIf(bytes, event_type='InsertPart') AS inserted_bytes,
    maxIf(event_time, event_type='InsertPart') AS last_insert_time,
    sumIf(num_source_parts, event_type='MergeParts') AS merged_parts,
    sumIf(bytes, event_type='MergeParts') AS merged_bytes,
    countIf(event_type='RemovePart' and rows>0) AS removed_parts
FROM system.server_part_log
WHERE table NOT LIKE '\%CHTMP' AND event_date >= today() - 7 AND {}
GROUP BY uuid, partition_id, start_of_hour
HAVING inserted_parts > 0 OR merged_parts > 0 OR removed_parts > 0;
)""";

MergeTreeBgTaskStatisticsInitializer & MergeTreeBgTaskStatisticsInitializer::instance()
{
    static MergeTreeBgTaskStatisticsInitializer ret;
    return ret;
}

void MergeTreeBgTaskStatisticsInitializer::initStatsByPartLog()
{
    Stopwatch watch;

    std::unordered_map<UUID, std::shared_ptr<MergeTreeBgTaskStatistics> > initialize_tasks_map_;
    {
        std::lock_guard<std::mutex> lock(mutex);
        initialize_tasks_map_.swap(initialize_tasks_map);
        scheduled = false;
    }

    MergeTreeBgTaskStatistics::InitializeState final_state = MergeTreeBgTaskStatistics::InitializeState::InitializeSucceed;
    try
    {
        LoggerPtr log = getLogger("MergeTreeBgTaskStatisticsInitializer");

        /// Maybe it is not efficient to filter too much storage uuid during reading server_part_log
        bool filter_uuids_in_query = initialize_tasks_map_.size() <= 128;
        Names uuid_strings;
        for (const auto & [uuid, t]: initialize_tasks_map_)
        {
            if (filter_uuids_in_query)
                uuid_strings.push_back(fmt::format("\'{}\'", UUIDHelpers::UUIDToString(uuid)));
        }

        String load_sql = fmt::format(SQL_LOAD_PARTITION_DIGEST_INFO_TEMPLATE, 
            filter_uuids_in_query ? (fmt::format(" uuid IN ({})", fmt::join(uuid_strings, ","))) : "1");
        LOG_DEBUG(log, "Initializing bg task statistics of {} tables with sql:\n{}", initialize_tasks_map_.size(), load_sql);

        ContextPtr global_context = Context::getGlobalContextInstance();
        if (!global_context)
        {
            LOG_WARNING(log, "Failed to get global context when initializing bg task statistics info.");
            return;
        }

        auto query_context = Context::createCopy(global_context);
        query_context->makeQueryContext();

        auto system_db = DatabaseCatalog::instance().getDatabase("system", query_context);
        if (!system_db->isTableExist("server_part_log", query_context))
        {
            LOG_WARNING(log, "Failed to load server_part_log digest information, table system.server_part_log does not exist!");
            return;
        }

        auto block_io = executeQuery(load_sql, query_context, true);
        auto input_block = block_io.getInputStream();
        
        size_t total_rows = 0;
        while (true)
        {
            auto res = input_block->read();
            if (!res)
                break;

#define MERGE_TREE_BG_TASK_STATS_GET_COLUMN(col_name, type) \
            auto * col_ ## col_name = checkAndGetColumn<Column ## type>(*res.getByName(#col_name).column); \
            if (unlikely(!col_ ## col_name || col_ ## col_name->size() == 0)) \
            { \
                LOG_WARNING(log, "Failed to load server_part_log digest information of column {} type {} from system.server_part_log.", #col_name, #type); \
                return; \
            }

            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(uuid, UUID)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(partition_id, String)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(start_of_hour, UInt32)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(inserted_parts, UInt64)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(inserted_bytes, UInt64)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(last_insert_time, UInt32)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(merged_parts, UInt64)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(merged_bytes, UInt64)
            MERGE_TREE_BG_TASK_STATS_GET_COLUMN(removed_parts, UInt64)

#undef MERGE_TREE_BG_TASK_STATS_GET_COLUMN

            auto size = col_uuid->size();
            for (size_t i = 0; i < size; ++i)
            {
                UUID uuid = UUID(col_uuid->getElement(i));

                auto it = initialize_tasks_map_.find(uuid);
                if (it == initialize_tasks_map_.end())
                    continue;

                String partition_id = col_partition_id->getDataAt(i).toString();
                UInt32 start_of_hour = static_cast<UInt32>(col_start_of_hour->get64(i));
                UInt64 inserted_parts = col_inserted_parts->get64(i);
                UInt64 inserted_bytes = col_inserted_bytes->get64(i);
                UInt32 last_insert_time = static_cast<UInt32>(col_last_insert_time->get64(i));
                UInt64 merged_parts = col_merged_parts->get64(i);
                UInt64 merged_bytes = col_merged_bytes->get64(i);
                UInt64 removed_parts = col_removed_parts->get64(i);

                it->second->executeWithWriteLock(
                    [&] (MergeTreeBgTaskStatistics::PartitionStatsMap & partition_stats_map)
                    {
                        MergeTreeBgTaskStatsInfo & partition_stats = partition_stats_map[partition_id];
                        addInsertedPartsImpl(partition_stats, inserted_parts, inserted_bytes, last_insert_time);
                        addMergedPartsImpl(partition_stats, merged_parts, merged_bytes, start_of_hour);
                        addRemovedPartsImpl(partition_stats, removed_parts);
                    }
                );
            }
            total_rows += size;
        }

        LOG_INFO(log, "Successfully loaded {} server_part_log digest entries in {} ms.", total_rows, watch.elapsedMilliseconds());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to load server_part_log digest info.");
        final_state = MergeTreeBgTaskStatistics::InitializeState::InitializeFailed;
    }

    for (auto & [_, t]: initialize_tasks_map_)
        t->setInitializeState(final_state);
}

MergeTreeBgTaskStatisticsPtr MergeTreeBgTaskStatisticsInitializer::getOrCreateTableStats(StorageID storage_id)
{
    MergeTreeBgTaskStatisticsPtr res = nullptr;
    table_stats_map.if_contains(storage_id.uuid, [&res](auto & kv) { res = kv.second; });

    if (res == nullptr)
    {
        std::lock_guard<std::mutex> lock(mutex);

        /// Maybe table stats of the same storage_id is created by another thread.
        table_stats_map.if_contains(storage_id.uuid, [&res](auto & kv) { res = kv.second; });
        if (res)
            return res;

        res = std::make_shared<MergeTreeBgTaskStatistics>(storage_id);
        res->setInitializeState(MergeTreeBgTaskStatistics::InitializeState::Initializing);
        table_stats_map.try_emplace_l(storage_id.uuid, [&res](auto & kv) { res = kv.second; }, res);

        initialize_tasks_map.emplace(storage_id.uuid, res);
        if (!scheduled)
        {
            scheduled = true;
            scheduleAfter(lock, 10000);
        }
    }

    return res;
}

std::vector<MergeTreeBgTaskStatisticsPtr> MergeTreeBgTaskStatisticsInitializer::getAllTableStats() const
{
    std::vector<MergeTreeBgTaskStatisticsPtr> res;
    table_stats_map.for_each([&res](auto & kv) { res.push_back(kv.second); });
    return res;
}

void MergeTreeBgTaskStatisticsInitializer::scheduleAfter([[maybe_unused]] std::lock_guard<std::mutex> & lock, UInt64 delay_ms)
{
    if (!scheduled_initialize_task)
        scheduled_initialize_task = Context::getGlobalContextInstance()->getSchedulePool().createTask("BgTaskStatsInit", [this] { initStatsByPartLog(); });
    
    scheduled_initialize_task->scheduleAfter(delay_ms);
}

}
