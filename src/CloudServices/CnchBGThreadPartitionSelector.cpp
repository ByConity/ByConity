#include <CloudServices/CnchBGThreadPartitionSelector.h>

#include <Catalog/Catalog.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/StorageID.h>
#include <Storages/StorageCnchMergeTree.h>

#include <fmt/format.h>

namespace DB
{

CnchBGThreadPartitionSelector::CnchBGThreadPartitionSelector(
    StorageID storage_id_,
    MergeTreeBgTaskStatisticsPtr & stats,
    const std::unordered_map<String, time_t> & postponed_partitions,
    std::function<Strings ()> get_all_partitions_callback_)
    : storage_id(storage_id_)
    , get_all_partitions_callback(get_all_partitions_callback_)
    , log(getLogger(storage_id_.getNameForLogs() + " [PartitionSelector]"))
{
    if (stats)
    {
        stats->executeWithReadLock([&, this] (const MergeTreeBgTaskStatistics::PartitionStatsMap & partition_stats_map)
        {
            for (const auto & [partition_id, p_stats]: partition_stats_map)
            {
                double merge_speed = (p_stats.inserted_parts == 0 ? 1.0 : static_cast<double>(p_stats.merged_parts) / p_stats.inserted_parts);
                double gc_speed = (p_stats.merged_parts == 0 ? 1.0 : static_cast<double>(p_stats.removed_parts) / p_stats.merged_parts);

                estimators.emplace(partition_id, std::make_shared<EstimatorElement>(EstimatorElement{
                    .partition_id = partition_id,
                    .merge_speed = merge_speed,
                    .gc_speed = gc_speed,
                    .last_insert_time = p_stats.last_insert_time,
                }));
            }
        });
    }

    for (const auto & [partition_id, postponed_time]: postponed_partitions)
    {
        auto it = estimators.find(partition_id);
        if (it != estimators.end())
            it->second->merge_postponed_time = postponed_time;
    }

    /// TODO(shiyuze): Maybe allow initailizing failed
    should_only_round_robin = (stats->getInitializeState() != MergeTreeBgTaskStatistics::InitializeState::InitializeSucceed);
}

bool CnchBGThreadPartitionSelector::needRoundRobinPick(time_t last_round_robin_time, UInt64 interval, String & out_reason)
{
    if (should_only_round_robin)
    {
        out_reason = "bg_task_stats is not be initialized.";
        return true;
    }

    if (estimators.empty())
    {
        out_reason = "estimators is empty.";
        return true;
    }
    
    if (static_cast<UInt64>(time(nullptr) - last_round_robin_time) > interval)
    {
        out_reason = "reached round robin interval.";
        return true;
    }

    return false;
}

Strings CnchBGThreadPartitionSelector::doRoundRobinPick(Type type, UInt64 n, RoundRobinState & round_robin_state)
{
    StringSet res;
    if (!get_all_partitions_callback)
        return {};

    const String & type_name = (type == CnchBGThreadPartitionSelector::Type::MergeType ? "Merge" : "GC");
    
    Strings all_partitions = get_all_partitions_callback();
    
    size_t num_all_partitions = all_partitions.size();
    round_robin_state.last_time_sec = time(nullptr);
    if (num_all_partitions <= n)
    {
        LOG_TRACE(log, "Return all partitions for {}: {}", type_name, fmt::join(all_partitions, ","));
        return all_partitions;
    }
    
    /// Try adding n non-duplicated partitions
    size_t size_before = res.size();
    UInt32 end_index = round_robin_state.index + num_all_partitions;
    while (res.size() < size_before + n && round_robin_state.index < end_index)
    {
        res.insert(all_partitions[round_robin_state.index++ % num_all_partitions]);
    }
    round_robin_state.index %= num_all_partitions;

    LOG_TRACE(log, "Round-robin partitions for {}: {}", type_name, fmt::join(res, ","));

    return {res.begin(), res.end()};
}

Strings CnchBGThreadPartitionSelector::selectForMerge(
    size_t n,
    UInt64 round_robin_interval,
    RoundRobinState & round_robin_state,
    bool only_realtime_partitions)
{
    String reason;
    if (n == 0 || needRoundRobinPick(round_robin_state.last_time_sec, round_robin_interval, reason))
    {
        LOG_TRACE(log, "MergeMutateThread: use round-robin as {}", (n == 0 ? "required partition size is 0" : reason));
        return doRoundRobinPick(MergeType, std::max(1UL, n), round_robin_state);
    }

    if (n >= estimators.size())
    {
        LOG_TRACE(log, "MergeMutateThread: select all known partitions as known_partitions.size {} < required_size {}",
            estimators.size(), n);

        Names res;
        for (const auto & [_, estimator] : estimators)
            res.push_back(estimator->partition_id);
        return res;
    }

    time_t now = time(nullptr);
    std::vector<EstimatorPtr> candidates{};
    for (const auto & [_, estimator] : estimators)
    {
        if (estimator->eligibleForMerge(now))
            candidates.push_back(estimator);
    }

    if (n > candidates.size())
    {
        LOG_TRACE(log, "MergeMutateThread: add all known partitions as candidates.size {} < required_size {}",
            estimators.size(), n);

        candidates.clear();
        for (const auto & [_, estimator] : estimators)
            candidates.push_back(estimator);
    }

    LOG_TRACE(log, "MergeMutateThread: candidates size - {}", candidates.size());

    Strings res{};
    if (!only_realtime_partitions && candidates.size() == n)
    {
        for (const auto & candidate : candidates)
            res.push_back(candidate->partition_id);
        return res;
    }

    /// candidates.size() > n, we will select (n+1)/2 partitions by insert time, and n-(n+1)/2 by merge_speed.
    size_t i = 0;
    size_t k = only_realtime_partitions ? n : (n + 1) / 2;
    /// Maybe some estimators have same merge speed (unknown partitions), these partitions can be selected randomly.
    static UInt32 seed = 0;
    std::mt19937 rng(seed++);
    std::shuffle(candidates.begin(), candidates.end(), rng);

    /// Sort k partitions in insert_time order.
    std::nth_element(
        candidates.begin(),
        candidates.begin() + k,
        candidates.end(),
        [](auto & lhs, auto & rhs) { return lhs->last_insert_time > rhs->last_insert_time; }
    );

    /// Sort following k partitions in merge_speed order.
    if (!only_realtime_partitions && k < n)
    {
        std::nth_element(
            candidates.begin() + k,
            candidates.begin() + n,
            candidates.end(),
            [](auto & lhs, auto & rhs) { return lhs->merge_speed < rhs->merge_speed; }
        );
    }

    for (const auto & candidate : candidates)
    {
        auto type = i < k ? "realtime" : "slow";
        LOG_TRACE(log, "MergeMutateThread: select {} partition {}", type, candidate->partition_id);
        res.push_back(candidate->partition_id);
        if (++i >= n)
            break;
    }

    return res;
}

Strings CnchBGThreadPartitionSelector::selectForGC(size_t n, UInt64 round_robin_interval, RoundRobinState & round_robin_state)
{
    String reason;
    if (n == 0 || needRoundRobinPick(round_robin_state.last_time_sec, round_robin_interval, reason))
    {
        LOG_TRACE(log, "GCThread: use round-robin as {}", (n == 0 ? "required partition size is 0" : reason));
        return doRoundRobinPick(GCType, std::max(1UL, n), round_robin_state);
    }

    LOG_TRACE(log, "GCThread: estimators size - {}", estimators.size());
    std::vector<EstimatorPtr> candidates{};
    for (const auto & [_, estimator] : estimators)
    {
        if (estimator->eligibleForGC())
            candidates.push_back(estimator);
    }

    LOG_TRACE(log, "GCThread: candidates size - {}", candidates.size());

    if (n > candidates.size())
    {
        LOG_TRACE(log, "GCThread: add all known partitions as candidates.size {} < required_size {}", estimators.size(), n);

        candidates.clear();
        for (const auto & [_, estimator] : estimators)
            candidates.push_back(estimator);
    }

    Strings res{};
    res.reserve(n);

    if (candidates.size() == n)
    {
        for (const auto & candidate : candidates)
            res.push_back(candidate->partition_id);
        return res;
    }

    size_t i = 0;
    /// Maybe some estimators have same merge speed (unknown partitions), these partitions can be selected randomly.
    static UInt32 seed = 0;
    std::mt19937 rng(seed++);
    std::shuffle(candidates.begin(), candidates.end(), rng);

    std::nth_element(
        candidates.begin(),
        candidates.begin() + n,
        candidates.end(),
        [](auto & lhs, auto & rhs) { return lhs->gc_speed < rhs->gc_speed; }
    );

    for (const auto & candidate : candidates)
    {
        LOG_TRACE(log, "GCThread: select partition {}", candidate->partition_id);
        res.push_back(candidate->partition_id);
        if (++i >= n)
            break;
    }

    return res;
}

}
