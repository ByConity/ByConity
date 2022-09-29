#pragma once

#include <memory>
#include <vector>
#include <roaring.hh>

namespace DB
{

class IMergeTreeDataPart;

/// Deprecated
using IMergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using IMergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using IMergeTreeDataPartsVector = std::vector<IMergeTreeDataPartPtr>;
using IMutableMergeTreeDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using IMutableMergeTreeDataPartsVector = std::vector<IMutableMergeTreeDataPartPtr>;

using Roaring = roaring::Roaring;
using DeleteBitmapPtr = std::shared_ptr<Roaring>;
using DeleteBitmapVector = std::vector<DeleteBitmapPtr>;
using ImmutableDeleteBitmapPtr = std::shared_ptr<const Roaring>;
using ImmutableDeleteBitmapVector = std::vector<ImmutableDeleteBitmapPtr>;

using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using MergeTreeDataPartsVector = std::vector<MergeTreeDataPartPtr>;
using MergeTreeMutableDataPartsVector = std::vector<MergeTreeMutableDataPartPtr>;

}
