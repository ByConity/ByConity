#pragma once

#include <common/types.h>

#include <unordered_map>

namespace DB
{
struct ColumnStatistics
{
    // number of distinct values, including null.
    UInt64 ndv;

    // minimum value
    double min;

    // maximum value
    double max;

    // number of nulls
    UInt64 null_counts;

    // average length of the values. 
    double avg_len;

    // maximun length of the values.
    UInt64 max_len;

    // data distribution
    // Statistics::Histogram histogram;
};

struct TableStatistics
{
    UInt64 row_count;

    std::unordered_map<String, ColumnStatistics> column_statistics;

    // std::unordered_map<PartitionKey, PartitionStatisticsPtr> partition_statistics;
};

}
