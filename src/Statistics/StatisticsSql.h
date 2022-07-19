#pragma once
#include <Statistics/BucketBounds.h>
#include <Statistics/StatsNdvBuckets.h>

namespace DB::Statistics
{
String constructGroupBySqlForStatsNdvBucket(const BucketBounds & bounds, const String & table_name, const String & column_name);

String constructSelectSql(const String & table_name, const std::vector<String> & items);

} // namespace DB
