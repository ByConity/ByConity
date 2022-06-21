#pragma once

#include <memory>

namespace DB
{
class IDiskCache;
using IDiskCachePtr = std::shared_ptr<IDiskCache>;

class IDiskCacheStrategy;
using IDiskCacheStrategyPtr = std::shared_ptr<IDiskCacheStrategy>;

class IDiskCacheSegment;
using IDiskCacheSegmentPtr = std::shared_ptr<IDiskCacheSegment>;
using IDiskCacheSegmentsVector = std::vector<IDiskCacheSegmentPtr>;
}
