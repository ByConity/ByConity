#pragma once
#include <memory>
#include <vector>

namespace DB
{
class MergeTreeDataPartCNCH;

using MergeTreeDataPartCNCHPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;
using MergeTreeDataPartsCNCHVector = std::vector<MergeTreeDataPartCNCHPtr>;
using MutableMergeTreeDataPartCNCHPtr = std::shared_ptr<MergeTreeDataPartCNCH>;
using MutableMergeTreeDataPartsCNCHVector = std::vector<MutableMergeTreeDataPartCNCHPtr>;
}
