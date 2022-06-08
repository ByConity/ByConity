#pragma once

#include <Storages/CnchPartitionInfo.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>

namespace DB::Catalog
{

using DataPartPtr = std::shared_ptr<const MergeTreeDataPartCNCH>;
using DataPartsVector = std::vector<DataPartPtr>;

struct CommitItems
{
    DataPartsVector data_parts;
    DeleteBitmapMetaPtrVector delete_bitmaps;
    DataPartsVector staged_parts;

    bool empty() const
    {
        return data_parts.empty() && delete_bitmaps.empty() && staged_parts.empty();
    }
};

/// keep partitions sorted as bytekv manner;
struct partition_comparator
{
    bool operator() (const String & a, const String & b) const
    {
        String a_ = a + "_";
        String b_ = b + "_";
        return a_ < b_;
    }
};

using PartitionMap = std::map<String, PartitionInfoPtr, partition_comparator>;

inline String normalizePath(const String & path)
{
    if (path.empty()) return "";
    /// normalize directory format
    String normalized_path;
    /// change all ////// to /
    std::for_each(path.begin(), path.end(), [&normalized_path](char c)
    {
        if (c == '/' && !normalized_path.empty() && normalized_path.back() == '/')
            return;
        normalized_path.push_back(c);
    });
    /// remove trailing /
    if (normalized_path.size() > 1 && normalized_path.back() == '/')
        normalized_path.pop_back();
    return normalized_path;
}

struct BatchedCommitIndex
{
    size_t parts_begin;
    size_t parts_end;
    size_t bitmap_begin;
    size_t bitmap_end;
    size_t staged_begin;
    size_t staged_end;
    size_t expected_parts_begin;
    size_t expected_parts_end;
    size_t expected_bitmap_begin;
    size_t expected_bitmap_end;
    size_t expected_staged_begin;
    size_t expected_staged_end;
};

}
