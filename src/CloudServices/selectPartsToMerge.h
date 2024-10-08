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

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Common/Logger.h>
#include <common/logger_useful.h>

namespace DB
{
class MergeTreeMetaBase;

enum class ServerSelectPartsDecision
{
    SELECTED = 0,
    CANNOT_SELECT = 1,
    NOTHING_TO_MERGE = 2,
};

struct SelectPartsToMergeSettings
{
    size_t max_total_size_to_merge{150ULL * 1024 * 1024 * 1024}; /// same with cnch_merge_max_total_bytes_to_merge
    size_t num_default_workers{0};
    bool aggressive{0};
    bool enable_batch_select{0};
    bool final{0};
    // bool merge_with_ttl_allowed{0};
};

using ServerCanMergeCallback = std::function<bool(const ServerDataPartPtr &, const ServerDataPartPtr &)>;

ServerSelectPartsDecision selectPartsToMerge(
    const MergeTreeMetaBase & data,
    std::vector<ServerDataPartsVector> & res,
    const ServerDataPartsVector & data_parts,
    const std::multimap<String, UInt64> & unselectable_part_rows,
    ServerCanMergeCallback can_merge_callback,
    const SelectPartsToMergeSettings & settings,
    LoggerPtr log);

/**
* Group data parts by bucket number
*/
void groupPartsByBucketNumber(const MergeTreeMetaBase & data, std::unordered_map<Int64, ServerDataPartsVector> & grouped_buckets, const ServerDataPartsVector & parts);

}
