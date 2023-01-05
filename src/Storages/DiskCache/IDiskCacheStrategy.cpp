#include "IDiskCacheStrategy.h"

#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/DiskCacheSegment.h>
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
