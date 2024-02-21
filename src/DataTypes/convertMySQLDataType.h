#pragma once

#include <string>
#include <type_traits>
#include <Core/MultiEnum.h>
#include <Parsers/IAST.h>
#include "IDataType.h"

namespace DB
{
enum class MySQLDataTypesSupport;

/// Convert data type to query. for example
/// DataTypeUInt8 -> ASTIdentifier(UInt8)
/// DataTypeNullable(DataTypeUInt8) -> ASTFunction(ASTIdentifier(UInt8))
ASTPtr dataTypeConvertToQuery(const DataTypePtr & data_type);

/// Convert MySQL type to ClickHouse data type.
DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support, const std::string & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length, size_t precision, size_t scale);

struct ClickHouseToMySQLDataTypeConversionSettings
{
    bool remap_string_as_text;
    bool remap_fixed_string_as_text;
};
/// Convert ClickHouse datatype string to MySQL type string and other properties.
/// This is purely a string based mapping.
/// The implementation is a port of the SQL logic in
/// https://github.com/ClickHouse/ClickHouse/blob/1c0fa345ac341030d76a687b0900f8e66739d384/src/Interpreters/InterpreterShowColumnsQuery.cpp#L4
/// E.g., whether we consider a column as nullable is also based the logic there.
template <bool full_type>
std::conditional_t<full_type, std::string, std::string_view> convertClickHouseDataTypeToMysqlColumnProperties(
    std::string_view clickhouse_data_type, ClickHouseToMySQLDataTypeConversionSettings settings
);

}
