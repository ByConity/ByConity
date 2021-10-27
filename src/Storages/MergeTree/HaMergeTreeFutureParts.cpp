#include <Storages/MergeTree/HaMergeTreeFutureParts.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void HaMergeTreeFutureParts::add(UInt64 lsn, const String & part_name)
{
    auto info = MergeTreePartInfo::fromPartName(part_name, format_version);
    auto res = future_parts.emplace(info, lsn);
    /// if more than one logs produce the same part, records all the LSNs for that part
    if (!res.second)
    {
        auto prev_lsn = res.first->second;
        if (prev_lsn == kInvalidLsn)
        {
            dup_future_parts[info].insert(lsn);
        }
        else
        {
            dup_future_parts[info] = { prev_lsn, lsn };
            res.first->second = kInvalidLsn;
        }
    }
}

void HaMergeTreeFutureParts::remove(UInt64 lsn, const String & part_name)
{
    auto info = MergeTreePartInfo::fromPartName(part_name, format_version);
    if (auto it = future_parts.find(info); it != future_parts.end())
    {
        bool should_remove_part = true;
        if (it->second == kInvalidLsn)
        {
            auto dup_it = dup_future_parts.find(info);
            if (dup_it == dup_future_parts.end())
                throw Exception("Cannot find part " + part_name + " of lsn " + std::to_string(lsn)  + " in future parts",
                                ErrorCodes::LOGICAL_ERROR);
            dup_it->second.erase(lsn);
            if (dup_it->second.empty())
                dup_future_parts.erase(dup_it);
            else
                should_remove_part = false;
        }
        if (should_remove_part)
            future_parts.erase(it);
    }
}

void HaMergeTreeFutureParts::clear()
{
    future_parts.clear();
    dup_future_parts.clear();
}

bool HaMergeTreeFutureParts::hasContainingPart(const MergeTreePartInfo & info, String * out_containing_part) const
{
    MergeTreePartInfo min_part { info.partition_id, 0, 0, 0 };
    for (auto it = future_parts.lower_bound(min_part); it != future_parts.end(); ++it)
    {
        auto & lhs = it->first;
        if (lhs.partition_id != info.partition_id || lhs.min_block > info.min_block)
            break;
        if (lhs.contains(info))
        {
            if (out_containing_part)
                *out_containing_part = lhs.getPartName();
            return true;
        }
    }
    return false;
}

}
