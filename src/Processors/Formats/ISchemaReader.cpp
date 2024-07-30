#include <Processors/Formats/ISchemaReader.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <common/logger_useful.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ONLY_NULLS_WHILE_READING_SCHEMA;
    extern const int INCORRECT_DATA;
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
}

}
