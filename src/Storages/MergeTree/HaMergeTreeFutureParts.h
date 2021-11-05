#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <Core/Types.h>
#include <map>
#include <limits>
#include <set>

namespace DB
{

/// Stores future parts of HaMergeTreeQueue.
/// Used by MergePredicate to determine whether a part is covered by future parts in logs.
///
/// It differs from ActiveDataPartSet in the following aspects
/// 1. it allows conflict parts (with intersecting block range) which could occur in ha logs
/// 2. adding a covering part will not replace the covered parts
class HaMergeTreeFutureParts
{
public:
    explicit HaMergeTreeFutureParts(MergeTreeDataFormatVersion format_version_) : format_version(format_version_) {}

    void add(UInt64 lsn, const String & part_name);

    void remove(UInt64 lsn, const String & part_name);

    void clear();

    bool hasContainingPart(const MergeTreePartInfo & info, String * out_containing_part = nullptr) const;

private:
    MergeTreeDataFormatVersion format_version;
    /// new part info -> lsn.
    /// if two or more logs have the same new part, lsn is set to MAX_VALUE
    /// and all the LSNs for that part is recorded in "dup_future_parts" below
    std::map<MergeTreePartInfo, UInt64> future_parts;
    std::map<MergeTreePartInfo, std::set<UInt64>> dup_future_parts;

    static constexpr UInt64 kInvalidLsn = std::numeric_limits<UInt64>::max();
};

}

