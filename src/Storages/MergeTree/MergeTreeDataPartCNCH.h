#pragma once

#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

/// Mock cnch part for Catalog usage.
class MergeTreeDataPartCNCH : public MergeTreeDataPartWide
{
public:
    MergeTreeDataPartCNCH(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr& volume_,
        const std::optional<String> & relative_path_)
        : MergeTreeDataPartWide(storage_, name_, info_, volume_, relative_path_)
        {}
};

using MergeTreeDataPartCNCHPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;
using MergeTreeDataPartsCNCHVector = std::vector<MergeTreeDataPartCNCHPtr>;
using MutableMergeTreeDataPartCNCHPtr = std::shared_ptr<MergeTreeDataPartCNCH>;
using MutableMergeTreeDataPartsCNCHVector = std::vector<MutableMergeTreeDataPartCNCHPtr>;

}
