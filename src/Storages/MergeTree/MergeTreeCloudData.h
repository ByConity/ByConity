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
#include <MergeTreeCommon/MergeTreeMetaBase.h>

#include <boost/multi_index/global_fun.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/range/iterator_range_core.hpp>

namespace DB
{

class MergeTreeCloudData : public MergeTreeMetaBase
{
public:
    std::string getName() const override { return "MergeTreeCloudData"; }

    /// Add prepared parts
    void addDataParts(MutableDataPartsVector & parts, UInt64 worker_topology_hash = 0);

    /// XXX:
    void removeDataParts(const DataPartsVector & parts, DataPartsVector * parts_not_found = nullptr);
    void removeDataParts(const Names & names, Names * names_not_found = nullptr);

    /// Remove Outdated parts of which timestamp is less than expired ts from container.
    /// DO NOT check reference count of parts.
    void unloadOldPartsByTimestamp(Int64 expired_ts);

    ////////// Receive and load data parts before reading data //////////

    /// Load parts, deactivate outdated parts and construct coverage link
    /// [Preallocate Mode] if worker_topology_hash is not empty, need to check whether the given topology is matched with worker's topology
    void loadDataParts(MutableDataPartsVector & parts, UInt64 worker_topology_hash = 0);

    /// Receive parts from server.
    void receiveDataParts(MutableDataPartsVector && parts, UInt64 worker_topology_hash = 0);
    /// Receive virtual parts from server.
    void receiveVirtualDataParts(MutableDataPartsVector && parts, UInt64 worker_topology_hash = 0);
    /// Load all (virtual) data parts (build part index and load part checksums from vfs).
    void prepareDataPartsForRead();

    /// set data description in sendResource stage if query with table version
    void setDataDescription(WGWorkerInfoPtr && worker_info_, UInt64 data_version_);
    void prepareVersionedPartsForRead(ContextPtr local_context, SelectQueryInfo & query_info, const Names & column_names);

protected:
    void addPreparedPart(MutableDataPartPtr & part, DataPartsLock &);

    void tryRemovePartContributionToColumnSizes(const DataPartPtr & part);

    DataPartPtr getActivePartToReplace(
        const MergeTreePartInfo & new_part_info,
        const String & new_part_name,
        DataPartPtr & out_covering_part,
        DataPartsLock & /* data_parts_lock */);

    void deactivateOutdatedParts();

    size_t loadFromServerPartsInPartition(const Strings & required_partitions, std::unordered_map<String, ServerDataPartsWithDBM> & server_parts_by_partition);

    void loadDataPartsInParallel(MutableDataPartsVector & parts);

    static void runOverPartsInParallel(MutableDataPartsVector & parts, size_t threads, const std::function<void(MutableDataPartPtr &)> & op);

    /// TODO: calculateColumnSizesImpl

    MergeTreeCloudData(
        const StorageID & table_id_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeMetaBase::MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

    ~MergeTreeCloudData() override = default;

    /// guard for loading received data_parts and virtual_data_parts.
    std::mutex load_data_parts_mutex;
    bool data_parts_loaded{false};
    bool versioned_data_parts_loaded{false};
    MutableDataPartsVector received_data_parts;
    MutableDataPartsVector received_virtual_data_parts;

    // data description for query with table version;
    WGWorkerInfoPtr worker_info;
    UInt64 data_version {0};
};

}
