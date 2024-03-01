#include "convertMySQLDataType.h"
#include <algorithm>
#include <charconv>
#include <Core/Field.h>
#include <common/types.h>
#include <Core/MultiEnum.h>
#include <Core/SettingsEnums.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include <boost/container/container_fwd.hpp>
#include "DataTypeDate.h"
#include "DataTypeDateTime.h"
#include "DataTypeDateTime64.h"
#include "DataTypesDecimal.h"
#include "DataTypeFixedString.h"
#include "DataTypeNullable.h"
#include "DataTypeString.h"
#include "DataTypesNumber.h"
#include "IDataType.h"

namespace DB
{
ASTPtr dataTypeConvertToQuery(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);

    if (!which.isNullable())
        return std::make_shared<ASTIdentifier>(data_type->getName());

    return makeASTFunction("Nullable", dataTypeConvertToQuery(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));
}

DataTypePtr convertMySQLDataType(MultiEnum<MySQLDataTypesSupport> type_support,
        const std::string & mysql_data_type,
        bool is_nullable,
        bool is_unsigned,
        size_t length,
        size_t precision,
        size_t scale)
{
    // Mysql returns mysql_data_type as below:
    // 1. basic_type
    // 2. basic_type options
    // 3. type_with_params(param1, param2, ...)
    // 4. type_with_params(param1, param2, ...) options
    // The options can be unsigned, zerofill, or some other strings.
    auto data_type = std::string_view(mysql_data_type);
    const auto type_end_pos = data_type.find_first_of(R"(( )"); // FIXME: fix style-check script instead
    const auto type_name = data_type.substr(0, type_end_pos);

    DataTypePtr res;

    if (type_name == "tinyint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt8>();
        else
            res = std::make_shared<DataTypeInt8>();
    }
    else if (type_name == "smallint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt16>();
        else
            res = std::make_shared<DataTypeInt16>();
    }
    else if (type_name == "int" || type_name == "mediumint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt32>();
        else
            res = std::make_shared<DataTypeInt32>();
    }
    else if (type_name == "bigint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt64>();
        else
            res = std::make_shared<DataTypeInt64>();
    }
    else if (type_name == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (type_name == "double")
        res = std::make_shared<DataTypeFloat64>();
    else if (type_name == "date")
        res = std::make_shared<DataTypeDate>();
    else if (type_name == "binary")
        res = std::make_shared<DataTypeFixedString>(length);
    else if (type_name == "datetime" || type_name == "timestamp")
    {
        if (!type_support.isSet(MySQLDataTypesSupport::DATETIME64))
        {
            res = std::make_shared<DataTypeDateTime>();
        }
        else if (type_name == "timestamp" && scale == 0)
        {
            res = std::make_shared<DataTypeDateTime>();
        }
        else if (type_name == "datetime" || type_name == "timestamp")
        {
            res = std::make_shared<DataTypeDateTime64>(scale);
        }
    }
    else if (type_support.isSet(MySQLDataTypesSupport::DECIMAL) && (type_name == "numeric" || type_name == "decimal"))
    {
        if (precision <= DecimalUtils::max_precision<Decimal32>)
            res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        else if (precision <= DecimalUtils::max_precision<Decimal64>) //-V547
            res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        else if (precision <= DecimalUtils::max_precision<Decimal128>) //-V547
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        else if (precision <= DecimalUtils::max_precision<Decimal256>)
            res = std::make_shared<DataTypeDecimal<Decimal256>>(precision, scale);
    }

    /// Also String is fallback for all unknown types.
    if (!res)
        res = std::make_shared<DataTypeString>();

    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);

    return res;
}

template <bool full_type>
std::conditional_t<full_type, std::string, std::string_view> convertClickHouseDataTypeToMysqlColumnProperties(
    std::string_view clickhouse_data_type, ClickHouseToMySQLDataTypeConversionSettings settings
)
{
    static const std::unordered_map<std::string_view, std::string_view> mapping{{
        {"Int8",        "TINYINT"},
        {"Int16",       "SMALLINT"},
        {"Int32",       "INTEGER"},
        {"Int64",       "BIGINT"},
        {"UInt8",       "TINYINT"},
        {"UInt16",      "SMALLINT"},
        {"UInt32",      "INTEGER"},
        {"UInt64",      "BIGINT"},
        {"Float32",     "FLOAT"},
        {"Float64",     "DOUBLE"},
        {"UUID",        "CHAR"},
        {"Bool",        "TINYINT"},
        {"Date",        "DATE"},
        {"Date32",      "DATE"},
        {"DateTime",    "DATETIME"},
        {"DateTime64",  "DATETIME"},
        {"Map",         "JSON"},
        {"Tuple",       "JSON"},
        {"Object",      "JSON"},
    }};
    std::conditional_t<full_type, std::string, std::string_view> result;
    std::string_view sv = clickhouse_data_type;
    if (sv.starts_with("LowCardinality"))
    {
        sv.remove_prefix(strlen("LowCardinality("));
        sv.remove_suffix(strlen(")"));
    }

    if (sv.starts_with("Nullable"))
    {
        sv.remove_prefix(strlen("Nullable("));
        sv.remove_suffix(strlen(")"));
    }
    const std::string_view inner_type = sv.substr(0, sv.find('('));
    do 
    {
        if (inner_type == "Decimal")
        {
            sv.remove_prefix(strlen("Decimal("));
            sv.remove_suffix(strlen(")"));
            size_t comma_pos = sv.find(',');
            const std::string_view scale_s = sv.substr(0, comma_pos);
            const std::string_view precision_s = sv.substr(comma_pos + strlen(", "));    
            int scale;
            int precision;
            (void)std::from_chars(scale_s.begin(), scale_s.end(), scale);
            (void)std::from_chars(precision_s.begin(), precision_s.end(), precision);
            if (scale <= 65 && precision <= 30)
            {
                if constexpr (full_type)
                    result = fmt::format("DECIMAL({})", sv);
                else
                    result = "DECIMAL";
                break;
            }
        }
        
        if (const auto it = mapping.find(inner_type); it != mapping.end())
        {
            result = it->second;
            if constexpr (full_type)
            {
                if (inner_type.starts_with("UInt"))
                    result += " UNSIGNED";
            }
            break;
        }

        const std::array<std::pair<std::string_view, std::string_view>, 2> mapping2{{
            {"String", settings.remap_string_as_text ? "TEXT" : "BLOB"},
            {"FixedString", settings.remap_fixed_string_as_text ? "TEXT" : "BLOB"}, 
        }};
        if (const auto it = std::find_if(mapping2.begin(), mapping2.end(), [inner_type](const auto & pair){ return pair.first == inner_type; }); it != mapping2.end())
        {    
            result = it->second;
            break;
        }
        result = "TEXT";
    } while (false);
    return result;
}

template
std::string convertClickHouseDataTypeToMysqlColumnProperties<true>(
    std::string_view clickhouse_data_type, ClickHouseToMySQLDataTypeConversionSettings settings
);

template
std::string_view convertClickHouseDataTypeToMysqlColumnProperties<false>(
    std::string_view clickhouse_data_type, ClickHouseToMySQLDataTypeConversionSettings settings
);

}
