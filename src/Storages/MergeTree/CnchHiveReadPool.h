/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <mutex>
#include <Core/NamesAndTypes.h>
#include <Storages/Hive/HiveDataPart.h>
#include <Storages/MergeTree/RowGroupsInDataPart.h>
#include <Storages/StorageCloudHive.h>
#include <Poco/Logger.h>
#include "Storages/Hive/HiveDataSelectExecutor.h"

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

    CnchHiveReadTask(
        HiveDataPartCNCHPtr data_part_, const size_t current_row_group_, const size_t sum_row_group_in_parts_, const size_t part_idx_)
        : data_part(data_part_), current_row_group(current_row_group_), sum_row_group_in_parts(sum_row_group_in_parts_), part_idx(part_idx_)
    {
    }

    bool isFinished() const { return sum_row_group_in_parts == 0; }
};

class ICnchHiveReadPool : private boost::noncopyable
{
public:
    virtual ~ICnchHiveReadPool() = default;
    virtual Block getHeader() const = 0;
    virtual CnchHiveReadTaskPtr getTask(const size_t & thread) = 0;
};

using HiveReadPoolPtr = std::shared_ptr<ICnchHiveReadPool>;

class CnchHiveReadPool : public ICnchHiveReadPool
{
public:
    CnchHiveReadPool(
        const size_t & threads,
        const size_t & sum_row_groups,
        RowGroupsInDataParts && parts_,
        const StorageCloudHive & data,
        const StorageMetadataPtr & metadata_snapshot_,
        Names column_names);

    Block getHeader() const override;
    CnchHiveReadTaskPtr getTask(const size_t & thread) override;

private:
    struct BackoffState
    {
        size_t current_threads;
        Stopwatch time_since_prev_event{CLOCK_MONOTONIC_COARSE};
        size_t num_events = 0;

        BackoffState(size_t threads) : current_threads(threads) { }
    };

    BackoffState backoff_state;

    void fillPerThreadInfo(const size_t & threads, const size_t & sum_row_groups, RowGroupsInDataParts data_parts);

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

class HiveDistributedReadPool : public ICnchHiveReadPool
{
public:
    HiveDistributedReadPool(
        const StorageMetadataPtr & metadata_snapshot_,
        const StorageCloudHive & data_,
        const size_t & num_threads_,
        DistributedReadingExtension extension_,
        Names column_names_)
        : metadata_snapshot(metadata_snapshot_)
        , data(data_)
        , num_threads(num_threads_)
        , extension(extension_)
        , column_names(column_names_)
    {
    }

    Block getHeader() const override;
    CnchHiveReadTaskPtr getTask(const size_t & thread) override;

private:
    StorageMetadataPtr metadata_snapshot;
    const StorageCloudHive & data;
    size_t num_threads;
    DistributedReadingExtension extension;
    Names column_names;

    std::mutex mutex;
    Poco::Logger * log {&Poco::Logger::get("HiveDistributedReadPool")};
    bool no_more_tasks_available{false};
    std::deque<RowGroupsInDataPart> buffered_ranges;
};

}
