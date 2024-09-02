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
const char * SQL_LOAD_PARTITION_DIGEST_INFO = R"(
SELECT uuid, partition_id,
    countIf(event_type='InsertPart') AS insert_parts,
    maxIf(event_time, event_type='InsertPart') as last_insert_time,
    sumIf(num_source_parts, event_type='MergeParts') AS merge_parts,
    countIf(event_type='RemovePart' and rows>0) AS remove_real_parts
FROM system.server_part_log
WHERE table NOT LIKE '%CHTMP' AND event_date >= today() - 7
GROUP BY uuid, partition_id
HAVING insert_parts > 0;
)";

CnchBGThreadPartitionSelector::CnchBGThreadPartitionSelector(ContextMutablePtr global_context_)
: WithMutableContext(global_context_), log(&Poco::Logger::get("PartitionSelector"))
{
    try
    {
        auto system_db = DatabaseCatalog::instance().getDatabase("system", getContext());
        if (!system_db->isTableExist("server_part_log", getContext()))
        {
            LOG_WARNING(log, "Table system.server_part_log does not exist!");
            load_success = false;
            return;
        }

        auto tmp_context = Context::createCopy(getContext());
        auto block_io = executeQuery(SQL_LOAD_PARTITION_DIGEST_INFO, tmp_context, true);
        auto input_block = block_io.getInputStream();
        
        while (true)
        {
            auto res = input_block->read();
            if (!res)
                break;

            auto * col_uuid = checkAndGetColumn<ColumnUUID>(*res.getByName("uuid").column);
            auto * col_partition = checkAndGetColumn<ColumnString>(*res.getByName("partition_id").column);
            auto * col_insert = checkAndGetColumn<ColumnUInt64>(*res.getByName("insert_parts").column);
            auto * col_insert_time = checkAndGetColumn<ColumnUInt32>(*res.getByName("last_insert_time").column);
            auto * col_merge = checkAndGetColumn<ColumnUInt64>(*res.getByName("merge_parts").column);
            auto * col_remove = checkAndGetColumn<ColumnUInt64>(*res.getByName("remove_real_parts").column);

            if (!col_uuid || col_uuid->size() == 0)
            {
                LOG_WARNING(log, "Failed to load server_part_log digest information from system.server_part_log.");
                return;
            }

            auto size = col_uuid->size();
            for (size_t i = 0; i < size; ++i)
            {
                UUID uuid = UUID(col_uuid->getElement(i));
                String partition = col_partition->getDataAt(i).toString();
                UInt64 insert = col_insert->get64(i);
                UInt64 insert_time = col_insert_time->get64(i);
                UInt64 merge = col_merge->get64(i);
                UInt64 remove = col_remove->get64(i);
                container[uuid][partition] = std::make_shared<Estimator>(partition, insert, merge, remove, insert_time);
            }
            LOG_INFO(log, "Successfully loaded {} server_part_log digest entries.", size);
        }
        
        return;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to load server_part_log digest info.");
        load_success = false;
    }
}

bool CnchBGThreadPartitionSelector::needRoundRobinPick(const StoragePtr & storage, Type type, size_t & out_n_suggestion)
{
    if (auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
    {
        auto storage_settings = (*cnch_table).getSettings();
        size_t interval{0};
        if (type == MergeType)
        {
            interval = storage_settings->cnch_merge_round_robin_partitions_interval.value;
        }
        else if (type == GCType)
        {
            interval = storage_settings->cnch_gc_round_robin_partitions_interval.value;
            out_n_suggestion = storage_settings->cnch_gc_round_robin_partitions_number;
        }
        
        if (!load_success)
        {
            LOG_TRACE(log, "Will use round-robin as load_success is false");
            return true;
        }

        size_t last_time_sec{0};
        {
            std::lock_guard lock(round_robin_states_mutex);
            last_time_sec = round_robin_states[type][storage->getStorageUUID()].last_time_sec;
        }

        return static_cast<size_t>(time(nullptr)) - last_time_sec > interval;
    }

    return false;
}


/// Try to select at most $n partitions from $storage for GC/Merge($type).
Strings CnchBGThreadPartitionSelector::doRoundRobinPick(const StoragePtr & storage, Type type, size_t n)
{
    auto all_partitions = getContext()->getCnchCatalog()->getPartitionIDs(storage, nullptr);
    auto size = all_partitions.size();
    if (size == 0)
        return {};

    StringSet res;
    auto storage_id = storage->getStorageID();

    size_t start_index{0};
    {
        std::lock_guard lock(round_robin_states_mutex);
        start_index = round_robin_states[type][storage_id.uuid].index;
        round_robin_states[type][storage_id.uuid].index += (size <= n) ? 0 : n;
        round_robin_states[type][storage_id.uuid].last_time_sec = static_cast<size_t>(time(nullptr));
    }

    if (size <= n)
    {
        LOG_TRACE(log, "{} Return all partitions for {}: {}", 
            storage_id.getNameForLogs(), 
            type == MergeType ? "Merge" : "GC", 
            fmt::format("{}", fmt::join(all_partitions, ",")));
        return all_partitions;
    }
    
    for (size_t i = 0; i < n; ++i)
    {
        res.insert(all_partitions[start_index++ % size]);
    }
    LOG_TRACE(log, "{} Round-robin partitions for {}: {}",
        storage_id.getNameForLogs(),
        type == MergeType ? "Merge" : "GC",
        fmt::format("{}", fmt::join(res, ",")));
    return {res.begin(), res.end()};
}

Strings CnchBGThreadPartitionSelector::selectForMerge(const StoragePtr & storage, size_t n, bool only_realtime_partitions)
{
    auto storage_id = storage->getStorageID();
    if (n == 0)
    {
        LOG_TRACE(log, "{} Use round-robin as required partition size is 0", storage_id.getNameForLogs());
        return doRoundRobinPick(storage, MergeType, 1);
    }

    if (needRoundRobinPick(storage, MergeType, n))
    {
        LOG_TRACE(log, "{} Use round-robin as needRoundRobinPick() returns true", storage_id.getNameForLogs());
        return doRoundRobinPick(storage, MergeType, n);
    }

    auto uuid = storage_id.uuid;
    std::vector<EstimatorPtr> candidates{};

    std::unique_lock lock(mutex);
    auto estimators = container[uuid];
    lock.unlock();
    
    /// Sometimes there might not be enough data in system.server_part_log, eg: server start without binding K8s PVC.
    /// Then we need to select more partitions from Catalog (by round-robin strategy).
    if (n > estimators.size())
    {
        LOG_TRACE(log, "{} Select all known partitions and do a round-robin selection as known_partitions.size {} < required_size {}",
            storage_id.getNameForLogs(),
            estimators.size(),
            n);

        StringSet res;
        for (const auto & [_, estimator] : estimators)
            res.insert(estimator->partition);

        auto round_robin_res = doRoundRobinPick(storage, MergeType, n);
        for (const auto & p : round_robin_res)
            res.insert(p);

        return {res.begin(), res.end()};
    }

    for (const auto & [_, estimator] : estimators)
    {
        if (estimator->eligibleForMerge())
            candidates.push_back(estimator);
    }

    /// candidates.size() == 0
    if (candidates.empty())
    {
        LOG_TRACE(log, "{} Use round-robin as no available candidate partitions", storage_id.getNameForLogs());
        return doRoundRobinPick(storage, MergeType, n);
    }

    /// candidates.size() <= n
    if (auto size = candidates.size(); size <= n)
    {
        Strings all;
        all.reserve(size);
        for (const auto & candidate : candidates)
            all.push_back(candidate->partition);
        LOG_TRACE(log, "{} MergeMutateThread: select all candidate partitions: {}", storage_id.getNameForLogs(), fmt::format("{}", fmt::join(all, ",")));

        return all;
    }

    /// candidates.size() > n
    Strings res{};
    size_t i = 0;
    size_t k = only_realtime_partitions ? n : (n + 1) / 2;

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
        LOG_TRACE(log, "{} MergeMutateThread: select {} partition {}", storage_id.getNameForLogs(), type, candidate->partition);
        res.push_back(candidate->partition);
        if (++i >= n)
            break;
    }

    return res;
}

Strings CnchBGThreadPartitionSelector::selectForGC(const StoragePtr & storage)
{
    size_t n = 10;
    if (needRoundRobinPick(storage, GCType, n))
        return doRoundRobinPick(storage, GCType, n);

    auto storage_id = storage->getStorageID();
    auto uuid = storage_id.uuid;
    std::vector<EstimatorPtr> candidates{};

    std::unique_lock lock(mutex);
    auto estimators = container[uuid];
    lock.unlock();
    
    if (estimators.empty())
    {
        LOG_TRACE(log, "{} GCThread: do round-robin as estimators is empty.", storage_id.getNameForLogs());
        return doRoundRobinPick(storage, GCType, n);
    }
    LOG_TRACE(log, "{} GCThread: estimators size - {}", storage_id.getNameForLogs(), estimators.size());

    for (const auto & [_, estimator] : estimators)
    {
        if (estimator->eligibleForGC())
            candidates.push_back(estimator);
    }

    if (candidates.empty())
    {
        LOG_TRACE(log, "{} GCThread: do round-robin as candidates is empty.", storage_id.getNameForLogs());
        return doRoundRobinPick(storage, GCType, n);
    }
    LOG_TRACE(log, "{} GCThread: candidates size - {}", storage_id.getNameForLogs(), candidates.size());

    Strings res{};
    size_t i = 0;
    res.reserve(n);

    std::nth_element(
        candidates.begin(),
        candidates.begin() + n,
        candidates.end(),
        [](auto & lhs, auto & rhs) { return lhs->gc_speed < rhs->gc_speed; }
    );

    for (const auto & candidate : candidates)
    {
        LOG_TRACE(log, "{} GCThread: select partition {}", storage_id.getNameForLogs(), candidate->partition);
        res.push_back(candidate->partition);
        if (++i >= n)
            break;
    }

    return res;
}

void CnchBGThreadPartitionSelector::addInsertParts(UUID uuid, const String & partition, size_t num, time_t ts)
{
    std::lock_guard lock(mutex);
    if (auto it = container[uuid].find(partition); it != container[uuid].end())
        it->second->insert(num, ts);
    else
        container[uuid][partition] = std::make_shared<Estimator>(partition, num, 0, 0, ts);
}

void CnchBGThreadPartitionSelector::addMergeParts(UUID uuid, const String & partition, size_t num, time_t ts)
{
    std::lock_guard lock(mutex);
    if (auto it = container[uuid].find(partition); it != container[uuid].end())
        it->second->merge(num);
    else
        container[uuid][partition] = std::make_shared<Estimator>(partition, 0, num, 0, ts);
}

void CnchBGThreadPartitionSelector::addRemoveParts(UUID uuid, const String & partition, size_t num, time_t ts)
{
    std::lock_guard lock(mutex);
    if (auto it = container[uuid].find(partition); it != container[uuid].end())
        it->second->remove(num);
    else
        container[uuid][partition] = std::make_shared<Estimator>(partition, 0, 0, num, ts);
}

void CnchBGThreadPartitionSelector::postponeMerge(UUID uuid, const String & partition)
{
    std::lock_guard lock(mutex);
    if (auto it = container[uuid].find(partition); it != container[uuid].end())
        it->second->markPostponeForMerge();

}

}
