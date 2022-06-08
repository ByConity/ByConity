#pragma once

#include <memory>
#include <vector>
#include <roaring.hh>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

class IMergeTreeDataPart;
using IMergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using IMergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using IMergeTreeDataPartsVector = std::vector<IMergeTreeDataPartPtr>;
using IMutableMergeTreeDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
using IMutableMergeTreeDataPartsVector = std::vector<IMutableMergeTreeDataPartPtr>;

using DeleteBitmapVector = std::vector<DeleteBitmapPtr>;
using ImmutableDeleteBitmapPtr = std::shared_ptr<const Roaring>;
using ImmutableDeleteBitmapVector = std::vector<ImmutableDeleteBitmapPtr>;

}
