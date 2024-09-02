#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <Storages/IStorage_fwd.h>

#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <memory>

namespace DB
{

class Context;
struct StorageID;

constexpr double GC_SPEED_FAST_ENOUGH = 0.95;
constexpr double MERGE_SPEED_FAST_ENOUGH = 1.2;
constexpr size_t POSTPONE_DURATION_SEC = 30;

struct MergeGCSpeedEstimator
{
    String partition;
    size_t insert_parts{0};
    size_t merge_parts{0};
    size_t remove_parts{0};

    time_t last_insert_time{0}; // each partition's newest insertion timestamp

    double merge_speed{0}; // merge_parts / insert_parts
    double gc_speed{0};    // gc_parts / merge_parts

    /// We need to stop selecting the partition for some reasons:
    /// - the partition is selected at last schedule. Need to wait tasks finish to correct the speed.
    /// - feedback from the merge selector that currently it could not select any part range from this partition.
    time_t ts_postpone_merge{0};

    MergeGCSpeedEstimator(String partition_, size_t insert_, size_t merge_, size_t remove_, UInt64 insert_time)
    : partition(partition_), insert_parts(insert_), merge_parts(merge_), remove_parts(remove_), last_insert_time(insert_time)
    {
        update_merge_speed();
        update_gc_speed();
    }

    inline void insert(size_t num, time_t ts)
    {
        insert_parts += num;
        last_insert_time = ts;
        update_merge_speed();
    }

    inline void merge(size_t num)
    {
        merge_parts += num;
        update_merge_speed();
        update_gc_speed();
    }

    inline void remove(size_t num)
    {
        remove_parts += num;
        update_gc_speed();
    }

    inline void markPostponeForMerge()
    {
        ts_postpone_merge = time(nullptr);
    }

    bool eligibleForMerge() const
    {
        return merge_speed <= MERGE_SPEED_FAST_ENOUGH && difftime(time(nullptr), ts_postpone_merge) > POSTPONE_DURATION_SEC;
    }

    bool eligibleForGC() const
    {
        return gc_speed <= GC_SPEED_FAST_ENOUGH;
    }

private:
    inline void update_merge_speed()
    {
        if (insert_parts == 0)
            merge_speed = 1;
        merge_speed = static_cast<double>(merge_parts) / insert_parts;
    }

    inline void update_gc_speed()
    {
        if (merge_parts == 0)
            gc_speed = 1;
        gc_speed = static_cast<double>(remove_parts) / merge_parts;
    }
};

class CnchBGThreadPartitionSelector : public WithMutableContext
{
    using Estimator = MergeGCSpeedEstimator;
    using EstimatorPtr = std::shared_ptr<Estimator>;
    using EstimatorSet = std::unordered_set<EstimatorPtr>;
    using Strings = std::vector<String>;
    using StringSet = std::set<String>;

    enum Type
    {
        MergeType,
        GCType
    };

    struct RoundRobinState
    {
        size_t last_time_sec{0};
        size_t index{0};
    };

public:
    explicit CnchBGThreadPartitionSelector(ContextMutablePtr global_context);

    Strings selectForMerge(const StoragePtr & storage, size_t n = 1, bool only_realtime_partitions = false);
    Strings selectForGC(const StoragePtr & storage);

    void addInsertParts(UUID uuid, const String & partition, size_t num, time_t ts);
    void addMergeParts(UUID uuid, const String & partition, size_t num, time_t ts);
    void addRemoveParts(UUID uuid, const String & partition, size_t num, time_t ts);
    void postponeMerge(UUID uuid, const String & partition);

private:
    bool needRoundRobinPick(const StoragePtr & storage, Type type, size_t & out_n_suggestion);
    Strings doRoundRobinPick(const StoragePtr & storage, Type type, size_t n);

    Poco::Logger * log;
    
    /// Whether loading digest information from system.server_part_log successful.
    bool load_success = true;

    /// TODO: (zuochuang.zema) make the container per-table to reduce contention?
    mutable std::mutex mutex;
    std::unordered_map<UUID, std::map<String, EstimatorPtr>> container;

    mutable std::mutex round_robin_states_mutex;
    /// states[merge_states, gc_states]
    std::unordered_map<UUID, RoundRobinState> round_robin_states[2];
};

}
