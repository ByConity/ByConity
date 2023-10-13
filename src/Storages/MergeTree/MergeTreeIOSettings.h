/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once
#include <cstddef>
#include <Core/Settings.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

class MMappedFileCache;
using MMappedFileCachePtr = std::shared_ptr<MMappedFileCache>;

struct MergeTreeReaderSettings
{
    /// Common settings
    ReadSettings read_settings;
    /// If save_marks_in_cache is false, then, if marks are not in cache,
    ///  we will load them but won't save in the cache, to avoid evicting other data.
    bool save_marks_in_cache = false;
    /// Convert old-style nested (single arrays with same prefix, `n.a`, `n.b`...) to subcolumns of data type Nested.
    bool convert_nested_to_subcolumns = false;
    /// Validate checksums on reading (should be always enabled in production).
    bool checksum_on_read = true;

    /// whether read the original bitmap columns in BitEngine mode
    bool read_source_bitmap = true;
};

struct MergeTreeWriterSettings
{
    MergeTreeWriterSettings() = default;

    MergeTreeWriterSettings(
        const Settings & global_settings,
        const MergeTreeSettingsPtr & storage_settings,
        bool can_use_adaptive_granularity_,
        bool rewrite_primary_key_,
        bool blocks_are_granules_size_ = false,
        bool optimize_map_column_serialization_ = false,
        bool enable_disk_based_key_index_ = false)
        : min_compress_block_size(
            storage_settings->min_compress_block_size ? storage_settings->min_compress_block_size : global_settings.min_compress_block_size)
        , max_compress_block_size(
              storage_settings->max_compress_block_size ? storage_settings->max_compress_block_size
                                                        : global_settings.max_compress_block_size)
        , can_use_adaptive_granularity(can_use_adaptive_granularity_)
        , rewrite_primary_key(rewrite_primary_key_)
        , blocks_are_granules_size(blocks_are_granules_size_)
        , optimize_map_column_serialization(optimize_map_column_serialization_)
        , enable_disk_based_key_index(enable_disk_based_key_index_)
    {
    }

    size_t min_compress_block_size;
    size_t max_compress_block_size;
    bool can_use_adaptive_granularity;
    bool rewrite_primary_key;
    bool blocks_are_granules_size;

    bool optimize_map_column_serialization = false;
    bool enable_disk_based_key_index = false;
};

struct BitengineWriteSettings
{
    bool only_recode = false;
    bool bitengine_encode_without_lock = false;
    bool bitengine_encode_in_fast_mode = false;
    bool skip_bitengine_encode = false;

    Float64 bitengine_encode_loss_rate = 0.1;
};

}
