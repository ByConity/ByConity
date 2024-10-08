#pragma once

#include <common/types.h>
#include <Common/DateLUT.h>
#include <Core/Names.h>
#include <Core/UUID.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/StorageID.h>

#include <memory>
#include <mutex>

#include <parallel-hashmap/parallel_hashmap/phmap.h>

namespace DB
{

// TODO(shiyuze): reduce size or using disk cache
struct MergeTreeBgTaskStatsInfo
{
    /// Stores stats info in both last interval and current interval. For example, if using interval of 1 hour and 
    /// now is 08:20, will store stats of both inverval of 07:00 ~ 08:00 (last_interval_stats) and 08:00 ~ 09:00
    /// (curr_interval_stats). Once got 09:00, stats in 08:00 ~ 09:00 will be rolled up to last_interval_stats,
    /// and curr_interval_stats will be 09:00 ~ 10:00.
    template <typename T, time_t interval>
    struct IntervalStatsInfo
    {
        /// Stores stats of current interval and last interval.
        T last_interval_stats{};
        T curr_interval_stats{};
        time_t curr_interval_start{0};

        /// If got next interval, rollup curr_interval_stats to last_interval_stats.
        bool rollupIfNeeded(time_t ts_interval_start)
        {
            if (ts_interval_start > curr_interval_start)
            {
                time_t diff = ts_interval_start - curr_interval_start;
                if (diff == interval)
                    last_interval_stats = curr_interval_stats;
                else
                    last_interval_stats = {};

                curr_interval_stats = {};
                curr_interval_start = ts_interval_start;
            }
            else if (unlikely(ts_interval_start < curr_interval_start))
            {
                /// Should not update to historical stats
                return false;
            }
            
            return true;
        }

        bool update(time_t ts, std::function<void (T &)> update_callback)
        {
            const DateLUTImpl & lut = DateLUT::serverTimezoneInstance();
            time_t ts_interval_start = lut.toStartOfSecondInterval(ts, interval);

            bool res = rollupIfNeeded(ts_interval_start);
            if (res)
                update_callback(curr_interval_stats);
            return res;
        }

        std::optional<T> lastIntervalStats(time_t ts)
        {
            const DateLUTImpl & lut = DateLUT::serverTimezoneInstance();
            time_t ts_interval_start = lut.toStartOfSecondInterval(ts, interval);

            bool res = rollupIfNeeded(ts_interval_start);
            if (res && ts_interval_start == curr_interval_start)
                return last_interval_stats;
            return {};
        }
    };

    using InsertedAndMergedBytes = std::pair<UInt64, UInt64>; /// inserted_bytes, merged_bytes
    IntervalStatsInfo<InsertedAndMergedBytes, 60 * 60> last_hour_stats;
    IntervalStatsInfo<InsertedAndMergedBytes, 6 * 60 * 60> last_6hour_stats;
    UInt64 inserted_parts{0};
    UInt64 merged_parts{0};
    UInt64 removed_parts{0};
    time_t last_insert_time{0};
};

/// Just remind how big the MergeTreeBgTaskStatsInfo is.
static_assert(sizeof(MergeTreeBgTaskStatsInfo) == 112);


/// Thread safe
class MergeTreeBgTaskStatistics : public std::enable_shared_from_this<MergeTreeBgTaskStatistics>
{
public:
    using PartitionStatsMap = phmap::flat_hash_map<String, MergeTreeBgTaskStatsInfo>;
    using ReadCallback = std::function<void (const PartitionStatsMap &)>;
    using WriteCallback = std::function<void (PartitionStatsMap &)>;

    enum class InitializeState
    {
        Uninitialized = 0,
        Initializing = 1,
        InitializeFailed = 2,
        InitializeSucceed = 3,
    };

    MergeTreeBgTaskStatistics(StorageID storage_id_);

    void setInitializeState(InitializeState state) { initialize_state.store(state, std::memory_order_release); }
    InitializeState getInitializeState() const { return initialize_state.load(std::memory_order_acquire); }

    void addInsertedParts(const String & partition_id, UInt64 num_parts, UInt64 bytes, time_t ts);
    void addMergedParts(const String & partition_id, UInt64 num_parts, UInt64 bytes, time_t ts);
    void addRemovedParts(const String & partition_id, UInt64 num_parts);
    void dropPartition(const String & partition_id);

    void fillMissingPartitions(const Strings & partition_ids);

    void executeWithReadLock(ReadCallback callback) const;
    void executeWithWriteLock(WriteCallback callback);

    StorageID getStorageID() const { return storage_id; }

protected:
    mutable std::shared_mutex mutex;
    PartitionStatsMap partition_stats_map;
    StorageID storage_id;
    std::atomic<InitializeState> initialize_state{InitializeState::Uninitialized};
};

using MergeTreeBgTaskStatisticsPtr = std::shared_ptr<MergeTreeBgTaskStatistics>;

class MergeTreeBgTaskStatisticsInitializer
{
public:
    static MergeTreeBgTaskStatisticsInitializer & instance();

    void initStatsByPartLog();

    MergeTreeBgTaskStatisticsPtr getOrCreateTableStats(StorageID storage_id);
    std::vector<MergeTreeBgTaskStatisticsPtr> getAllTableStats() const;

private:
    MergeTreeBgTaskStatisticsInitializer() { }

    void scheduleAfter(std::lock_guard<std::mutex> & lock, UInt64 delay_ms = 1000);

protected:
    using TableStatsMap = phmap::parallel_flat_hash_map<UUID,
                                                        MergeTreeBgTaskStatisticsPtr,
                                                        std::hash<UUID>,
                                                        std::equal_to<UUID>,
                                                        std::allocator<std::pair<const UUID, MergeTreeBgTaskStatisticsPtr>>,
                                                        4, /// 2^4 sub maps
                                                        std::mutex>;
    std::mutex mutex;
    TableStatsMap table_stats_map;
    std::unordered_map<UUID, std::shared_ptr<MergeTreeBgTaskStatistics> > initialize_tasks_map;
    bool scheduled{false};
    BackgroundSchedulePoolTaskHolder scheduled_initialize_task;
};

}// DB
