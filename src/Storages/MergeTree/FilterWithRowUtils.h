#pragma once

#include <memory>
#include <roaring.hh>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

using MutableFilterBitmapPtr = std::shared_ptr<roaring::Roaring>;
using FilterBitmapPtr = std::shared_ptr<const roaring::Roaring>;

DeleteBitmapPtr combineFilterBitmap(const RangesInDataPart & part, MergeTreeData::DeleteBitmapGetter delete_bitmap_getter);


}
