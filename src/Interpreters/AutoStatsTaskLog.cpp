#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/now64.h>
#include <Interpreters/AutoStatsTaskLog.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/StatsTableIdentifier.h>

namespace DB
{

using namespace Statistics;
using namespace Statistics::AutoStats;

NamesAndTypesList AutoStatsTaskLogElement::getNamesAndTypes()
{
    /// TODO (gouguilin): use protobuf to do reflection
    auto task_type_enum_values = DataTypeEnum8::Values({{"Manual", 1}, {"Auto", 2}});
    auto task_type_enum_type = std::make_shared<DataTypeEnum8>(task_type_enum_values);

    auto status_enum_values = DataTypeEnum8::Values(
        {{"NotExists", 1}, {"Created", 2}, {"Pending", 3}, {"Running", 4}, {"Error", 5}, {"Failed", 6}, {"Success", 7}, {"Cancelled", 8}});
    auto status_enum_type = std::make_shared<DataTypeEnum8>(status_enum_values);

    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale)},
        {"task_uuid", std::make_shared<DataTypeUUID>()},
        {"task_type", task_type_enum_type},
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"columns", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"udi", std::make_shared<DataTypeUInt64>()},
        {"stats_row_count", std::make_shared<DataTypeUInt64>()},
        {"priority", std::make_shared<DataTypeFloat64>()},
        {"retry", std::make_shared<DataTypeUInt64>()},
        {"status", status_enum_type},
        {"settings_json", std::make_shared<DataTypeString>()},
        {"message", std::make_shared<DataTypeString>()},
    };
}

void AutoStatsTaskLogElement::appendToBlock(MutableColumns & columns) const
{
    Array arr_columns_name;
    for (auto & column_name : columns_name)
    {
        arr_columns_name.push_back(column_name);
    }

    int i = 0;
    columns[i++]->insert(convertToDate(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(task_uuid);
    columns[i++]->insert(task_type);
    columns[i++]->insert(table_uuid);
    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(arr_columns_name);
    columns[i++]->insert(udi);
    columns[i++]->insert(stats_row_count);
    columns[i++]->insert(priority);
    columns[i++]->insert(retry);
    columns[i++]->insert(status);
    columns[i++]->insert(settings_json);
    columns[i++]->insert(message);
}

}
