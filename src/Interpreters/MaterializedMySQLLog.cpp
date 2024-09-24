#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/MaterializedMySQLLog.h>

namespace DB
{

NamesAndTypesList MaterializedMySQLLogElement::getNamesAndTypes()
{
    auto log_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"EMPTY",               static_cast<Int8>(EMPTY)},
        {"ERROR",               static_cast<Int8>(ERROR)},
        {"START_SYNC",          static_cast<Int8>(START_SYNC)},
        {"MANUAL_STOP_SYNC",    static_cast<Int8>(MANUAL_STOP_SYNC)},
        {"EXCEPTION_STOP_SYNC", static_cast<Int8>(EXCEPTION_STOP_SYNC)},
        {"RESYNC_TABLE",        static_cast<Int8>(RESYNC_TABLE)},
        {"SKIP_DDL",            static_cast<Int8>(SKIP_DDL)}});

    auto source_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"SYNC_MANAGER",    static_cast<Int8>(SYNC_MANAGER)},
        {"WORKER_THREAD",   static_cast<Int8>(WORKER_THREAD)}});

    return
    {
        {"cnch_database",       std::make_shared<DataTypeString>()},
        {"cnch_table",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"database",            std::make_shared<DataTypeString>()},
        {"table",               std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},

        {"event_type",          std::move(log_type)},
        {"event_date",          std::make_shared<DataTypeDate>()},
        {"event_time",          std::make_shared<DataTypeDateTime>()},
        {"resync_table",        std::make_shared<DataTypeString>()},

        {"duration_ms",         std::make_shared<DataTypeUInt64>()},
        {"metric",              std::make_shared<DataTypeUInt64>()},

        {"has_error",           std::make_shared<DataTypeUInt8>()},
        {"event_msg",           std::make_shared<DataTypeString>()},
        {"event_source",        std::move(source_type)}
    };
}

void MaterializedMySQLLogElement::appendToBlock(MutableColumns & columns) const
{
    auto nameset_2_array = [] (const NameSet & elem_list) -> Array {
        Array arr;
        for (const auto & elem : elem_list)
            arr.emplace_back(elem);
        return arr;
    };

    size_t i = 0;

    columns[i++]->insert(cnch_database);
    columns[i++]->insert(nameset_2_array(cnch_tables));
    columns[i++]->insert(database);
    columns[i++]->insert(nameset_2_array(tables));

    columns[i++]->insert(type);
    columns[i++]->insert(DateLUT::serverTimezoneInstance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(resync_table);

    columns[i++]->insert(UInt64(duration_ms));
    columns[i++]->insert(UInt64(metric));

    columns[i++]->insert(UInt64(has_error));
    columns[i++]->insert(event_msg);
    columns[i++]->insert(event_source);
}

}
