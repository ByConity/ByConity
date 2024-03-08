#include <Storages/MaterializedView/ViewRefreshTaskLog.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList ViewRefreshTaskLogElement::getNamesAndTypes()
{
    auto status_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"START", static_cast<Int8>(RefreshViewTaskStatus::START)},
        {"FINISH", static_cast<Int8>(RefreshViewTaskStatus::FINISH)},
        {"EXCEPTION", static_cast<Int8>(RefreshViewTaskStatus::EXCEPTION)}});


    auto refresh_type_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"PARTITION_BASED_REFRESH", static_cast<Int8>(RefreshViewTaskType::PARTITION_BASED_REFRESH)},
        {"FULL_REFRESH", static_cast<Int8>(RefreshViewTaskType::FULL_REFRESH)}});

    return {
        {"database", std::make_shared<DataTypeString>()},
        {"view", std::make_shared<DataTypeString>()},
        {"status", status_datatype},
        {"refresh_type", refresh_type_datatype},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"partition_map", std::make_shared<DataTypeString>()},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>()},
        {"drop_query", std::make_shared<DataTypeString>()},
        {"insert_select_query", std::make_shared<DataTypeString>()},
        {"insert_overwrite_query", std::make_shared<DataTypeString>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"drop_query_id", std::make_shared<DataTypeString>()},
        {"insert_select_query_id", std::make_shared<DataTypeString>()},
        {"insert_overwrite_query_id", std::make_shared<DataTypeString>()}};
}

void ViewRefreshTaskLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(database);
    columns[i++]->insert(view);
    columns[i++]->insert(status);
    columns[i++]->insert(refresh_type);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(partition_map);
    columns[i++]->insert(query_duration_ms);
    columns[i++]->insert(drop_query);
    columns[i++]->insert(insert_select_query);
    columns[i++]->insert(insert_overwrite_query);
    columns[i++]->insert(query_id);
    columns[i++]->insert(drop_query_id);
    columns[i++]->insert(insert_select_query_id);
    columns[i++]->insert(insert_overwrite_query_id);
}

}
