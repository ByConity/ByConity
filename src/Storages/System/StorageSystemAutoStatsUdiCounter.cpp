#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
#include <Storages/System/StorageSystemAutoStatsUdiCounter.h>

namespace DB
{
using namespace Statistics;

NamesAndTypesList StorageSystemAutoStatsUdiCounter::getNamesAndTypes()
{
    return {
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"udi_count", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemAutoStatsUdiCounter::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto & instance = AutoStats::AutoStatisticsMemoryRecord::instance();
    auto records = instance.getAll();
    auto col_id = 0;
    auto & uuid_column = res_columns[col_id++];
    auto & count_column = res_columns[col_id++];

    for (const auto & [key, count] : records)
    {
        uuid_column->insert(key);
        count_column->insert(count);
    }
}

}
