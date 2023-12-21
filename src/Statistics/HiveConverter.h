#include <Statistics/StatisticsCollectorObjects.h>

#include <Storages/Hive/Metastore/IMetaClient.h>

namespace DB::Statistics::StatisticsImpl
{

std::pair<TableStats, ColumnStatsMap>
convertHiveToStats(UInt64 row_count, const NameToType & naem_to_type, const ApacheHive::TableStatsResult & hive_stats);
}
