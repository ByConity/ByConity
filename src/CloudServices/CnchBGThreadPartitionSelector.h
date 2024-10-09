#pragma once

#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Poco/Logger.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>

#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <memory>

namespace DB
{

class Context;

constexpr double GC_SPEED_FAST_ENOUGH = 0.95;
constexpr double MERGE_SPEED_FAST_ENOUGH = 1.2;
constexpr size_t POSTPONE_DURATION_SEC = 30;

struct MergeTreeSettings;


/// PartitionSelector is a stateless object may be created and called by MergeMutateThread or GCThread, to
/// select partitions of StorageCnchMergeTree for merge or gc. All historical statistic infos is stored in
/// MergeTreeBgTaskStatistics, partition selector will try to calculate merge/gc speed bese on it.
class CnchBGThreadPartitionSelector
{
public:
    struct EstimatorElement
    {
        String partition_id;
        double merge_speed{1.0};
        double gc_speed{1.0};
        time_t last_insert_time{0};
        time_t merge_postponed_time{0};

        bool eligibleForMerge(time_t now) const
        {
            return merge_speed <= MERGE_SPEED_FAST_ENOUGH && difftime(now, merge_postponed_time) > POSTPONE_DURATION_SEC;
        }

        bool eligibleForGC() const
        {
            return gc_speed <= GC_SPEED_FAST_ENOUGH;
        }
    };

    using EstimatorPtr = std::shared_ptr<EstimatorElement>;
    using Strings = std::vector<String>;
    using StringSet = std::set<String>;

    struct RoundRobinState
    {
        time_t last_time_sec{0};
        UInt32 index{0};
    };

public:
    CnchBGThreadPartitionSelector(
        StorageID storage_id_,
        MergeTreeBgTaskStatisticsPtr & stats,
        const std::unordered_map<String, time_t> & postponed_partitions,
        std::function<Strings ()> get_all_partitions_callback_);
    
    Strings selectForMerge(
        size_t n,
        UInt64 round_robin_interval,
        RoundRobinState & round_robin_state,
        bool only_realtime_partitions = false);

    Strings selectForGC(size_t n, UInt64 round_robin_interval, RoundRobinState & round_robin_state);

private:

    enum Type
    {
        MergeType,
        GCType
    };

    bool needRoundRobinPick(time_t last_round_robin_time, UInt64 interval, String & out_reason);
    Strings doRoundRobinPick(Type type, size_t n, RoundRobinState & round_robin_state);

    StorageID storage_id;
    std::function<Strings ()> get_all_partitions_callback;

    std::unordered_map<String, EstimatorPtr> estimators;
    bool should_only_round_robin{false};

    LoggerPtr log;
};

}
