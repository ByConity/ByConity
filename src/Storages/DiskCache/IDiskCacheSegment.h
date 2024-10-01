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

#include <Core/Types.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Core/Settings.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <common/types.h>

#include <memory>
#include <vector>

namespace DB
{
class IDiskCache;

enum SegmentType
{
    PART_DATA = 0,
    FILE_DATA = 1,
    SENCONDARY_INDEX = 2,
    GIN_INDEX = 3,
    BITMAP_INDEX = 4,
    PRIMARY_INDEX = 5,
    CHECKSUMS_DATA = 6,
    META_INFO = 7,
    GEO_INDEX = 8,
    MANIFEST = 9,
};

extern std::unordered_map<SegmentType, String> SegmentTypeToString;

class IDiskCacheSegment
{
public:
    explicit IDiskCacheSegment(size_t start_segment_number, size_t size, SegmentType seg_type) : segment_number(start_segment_number), segment_size(size), segment_type(seg_type) { }
    virtual ~IDiskCacheSegment() = default;

    virtual String getSegmentName() const = 0;
    virtual String getMarkName() const {return {};}
    virtual SegmentType getSegmentType() const { return segment_type; }
    virtual void cacheToDisk(IDiskCache & diskcache, bool throw_exception = false) = 0;

    static String formatSegmentName(
        const String & uuid, const String & part_name, const String & column_name, UInt32 segment_number, const String & extension);

protected:
    size_t segment_number;
    size_t segment_size;
    SegmentType segment_type;
};

using IDiskCacheSegmentPtr = std::shared_ptr<IDiskCacheSegment>;
using IDiskCacheSegmentsVector = std::vector<IDiskCacheSegmentPtr>;

}
