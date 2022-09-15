#pragma once

#include <mutex>
#include <Core/NamesAndTypes.h>
#include <Storages/StorageCloudHive.h>
#include <Storages/Hive/HiveDataPart.h>
#include <Storages/MergeTree/RowGroupsInDataPart.h>

namespace DB
{

class CnchHiveReadPool;
struct CnchHiveReadTask;
class HiveDataPart;

using CnchHiveReadPoolPtr = std::shared_ptr<CnchHiveReadPool>;
using CnchHiveReadTaskPtr = std::unique_ptr<CnchHiveReadTask>;

struct CnchHiveReadTask
{
    HiveDataPartCNCHPtr data_part;
    const size_t current_row_group;
    const size_t sum_row_group_in_parts;
    const size_t part_idx;

    CnchHiveReadTask(HiveDataPartCNCHPtr data_part_, const size_t current_row_group_, const size_t sum_row_group_in_parts_, const size_t part_idx_)
        : data_part(data_part_)
        , current_row_group(current_row_group_)
        , sum_row_group_in_parts(sum_row_group_in_parts_)
        , part_idx(part_idx_)
    {}

    bool isFinished()
    {
        return sum_row_group_in_parts == 0;
    }

};

class CnchHiveReadPool : private boost::noncopyable
{
public:
    CnchHiveReadPool(
        const size_t threads,
        const size_t sum_row_groups,
        RowGroupsInDataParts && parts_,
        const StorageCloudHive & data,
        const StorageMetadataPtr & metadata_snapshot_,
        Names column_names);

    Block getHeader() const;
    CnchHiveReadTaskPtr getTask(const size_t thread);

private:

    struct BackoffState
    {
        size_t current_threads;
        Stopwatch time_since_prev_event {CLOCK_MONOTONIC_COARSE};
        size_t num_events = 0;

        BackoffState(size_t threads) : current_threads(threads) {}
    };

    BackoffState backoff_state;

private:
    void fillPerThreadInfo(
        const size_t threads,
        const size_t sum_row_groups);

    struct ThreadTask
    {
        struct PartIndexAndPath
        {
            size_t part_idx;
            size_t need_read_row_group_index;
        };
        std::vector<PartIndexAndPath> parts_and_groups;
    };

    RowGroupsInDataParts parts;
    const StorageCloudHive & data;
    StorageMetadataPtr metadata_snapshot;
    Names column_names;
    std::vector<ThreadTask> threads_tasks;
    std::vector<size_t> threads_row_groups_sum;
    mutable std::mutex mutex;

    // Poco::Logger * log = Poco::Logger::get("CnchHiveReadPool");
};

}
