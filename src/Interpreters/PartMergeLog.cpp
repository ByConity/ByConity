#include <Interpreters/PartMergeLog.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
NamesAndTypesList PartMergeLogElement::getNamesAndTypes()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"MERGE_SELECT", static_cast<Int8>(MERGE_SELECT)},
        {"COMMIT", static_cast<Int8>(COMMIT)},
    });

    return {
        {"event_type", std::move(event_type_datatype)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},

        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},

        {"new_tasks", std::make_shared<DataTypeUInt32>()},
        {"source_parts_in_new_tasks", std::make_shared<DataTypeUInt32>()},

        {"duration_us", std::make_shared<DataTypeUInt64>()},
        {"get_parts_duration_us", std::make_shared<DataTypeUInt64>()},
        {"select_parts_duration_us", std::make_shared<DataTypeUInt64>()},

        {"exception", std::make_shared<DataTypeString>()},

        {"extended", std::make_shared<DataTypeUInt8>()},
        {"current_parts", std::make_shared<DataTypeUInt32>()},
        {"future_covered_parts", std::make_shared<DataTypeUInt32>()},
        {"future_committed_parts", std::make_shared<DataTypeUInt32>()},
    };
}

void PartMergeLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(UInt64(event_type));
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(UInt64(event_time));

    columns[i++]->insert(database);
    columns[i++]->insert(table);
    columns[i++]->insert(uuid);

    columns[i++]->insert(new_tasks);
    columns[i++]->insert(source_parts_in_new_tasks);

    columns[i++]->insert(duration_us);
    columns[i++]->insert(get_parts_duration_us);
    columns[i++]->insert(select_parts_duration_us);

    columns[i++]->insert(exception);

    columns[i++]->insert(extended);
    columns[i++]->insert(current_parts);
    columns[i++]->insert(future_covered_parts);
    columns[i++]->insert(future_committed_parts);
}

} // end of namespace DB
