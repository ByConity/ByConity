#include <DataTypes/IDataType.h>
#include <Statistics/BucketBounds.h>
#include <Statistics/StatisticsSql.h>
#include <boost/algorithm/string/join.hpp>

namespace DB::Statistics
{
String constructGroupBySqlForStatsNdvBucket(const BucketBounds & bounds, const String & table_name, const String & column_name)
{
    String sql;
    sql += "select ";
    sql += bounds.getSqlForBucketId(column_name) + " as system__bucket_id, ";
    sql += "count(" + column_name + "), ";
    sql += "cpc(" + column_name + ") ";
    sql += "from " + table_name + " ";
    sql += "group by system__bucket_id";
    return sql;
}

String constructSelectSql(const String & table_name, const std::vector<String> & items)
{
    auto all_items = boost::algorithm::join(items, ", ");
    String sql = "select " + all_items + " from " + table_name;
    return sql;
}
} // namespace DB
