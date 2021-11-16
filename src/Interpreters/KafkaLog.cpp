#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/KafkaLog.h>


namespace DB
{


NamesAndTypesList KafkaLogElement::getNamesAndTypes()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(
            DataTypeEnum8::Values {
            {"EMPTY",           static_cast<Int8>(EMPTY)},
            {"POLL",            static_cast<Int8>(POLL)},
            {"PARSE_ERROR",     static_cast<Int8>(PARSE_ERROR)},
            {"WRITE",           static_cast<Int8>(WRITE)},
            {"EXCEPTION",       static_cast<Int8>(EXCEPTION)},
            {"EMPTY_MESSAGE",   static_cast<Int8>(EMPTY_MESSAGE)},
            {"FILTER",     static_cast<Int8>(FILTER)},
            });

    return
    {
        {"event_type",      std::move(event_type_datatype)        },
        {"event_date",      std::make_shared<DataTypeDate>()      },
        {"event_time",      std::make_shared<DataTypeDateTime>()  },
        {"duration_ms",     std::make_shared<DataTypeUInt64>()    },

        {"database",        std::make_shared<DataTypeString>()    },
        {"table",           std::make_shared<DataTypeString>()    },
        {"consumer",        std::make_shared<DataTypeString>()    },

        {"metric",          std::make_shared<DataTypeUInt64>()    },
        {"bytes",           std::make_shared<DataTypeUInt64>()    },

        {"has_error",       std::make_shared<DataTypeUInt8>()     },
        {"exception",       std::make_shared<DataTypeString>()    },
    };
}

void KafkaLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(UInt64(event_type));
    columns[i++]->insert(UInt64(DateLUT::instance().toDayNum(event_time)));
    columns[i++]->insert(UInt64(event_time));
    columns[i++]->insert(UInt64(duration_ms));

    columns[i++]->insert(database);
    columns[i++]->insert(table);
    columns[i++]->insert(consumer);

    columns[i++]->insert(UInt64(metric));
    columns[i++]->insert(UInt64(bytes));

    columns[i++]->insert(UInt64(has_error));
    columns[i++]->insert(last_exception);
}

} // end of namespace DB
