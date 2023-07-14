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

#include "IDiskCacheStrategy.h"

#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Common/escapeForFileName.h>

namespace DB
{

std::set<size_t> IDiskCacheStrategy::transferRangesToSegmentNumbers(const MarkRanges & ranges) const
{
    std::set<size_t> segment_numbers;
    for (const auto & range : ranges)
    {
        size_t segment_begin = range.begin / segment_size;
        size_t segment_end = range.end / segment_size;
        if (range.end % segment_size == 0)
            segment_end -= 1;
        for (size_t i = segment_begin; i <= segment_end; ++i)
            segment_numbers.insert(i);
    }
    return segment_numbers;
}

}
