#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include "Columns/ColumnString.h"
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Core/SettingsEnums.h"
#include "DataTypes/IDataType.h"
#include <Interpreters/Context.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/convertMySQLDataType.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

/// This is purely a string (ClickHouse data type string representation) to string (target dialect string representation) conversion function.
template <bool full_type>
class FunctionConvertToDialectDataType : public IFunction
{
public:
    static constexpr auto name =
        full_type ? "convertToDialectColumnType" : "convertToDialectDataType";

    explicit FunctionConvertToDialectDataType(ContextPtr context_) : context(context_), dialect_type(context_->getSettingsRef().dialect_type) { 
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionConvertToDialectDataType<full_type>>(context);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments [[maybe_unused]]) const override
    {
        const ColumnWithTypeAndName & arg = arguments[0];
        if (!isString(*arg.type)) 
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, 
                "Illegal type {} of first argument (pattern) of function {}. Must be String/FixedString.",
                arg.type->getName(), getName()
            );
        }
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type [[maybe_unused]] , size_t input_rows_count) const override
    {
        ColumnPtr arg = arguments[0].column;
        const ColumnString * clickhouse_data_types = checkAndGetColumn<ColumnString>(*arg);

        if (dialect_type == DialectType::MYSQL)
        {
            auto & settings = context->getSettingsRef();
            ClickHouseToMySQLDataTypeConversionSettings mysql_conversion_settings { 
                .remap_string_as_text = settings.mysql_map_string_to_text_in_show_columns,
                .remap_fixed_string_as_text = settings.mysql_map_fixed_string_to_text_in_show_columns,
            };
            auto col = DataTypeString().createColumn();
            col->reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                col->insert(
                    
                    convertClickHouseDataTypeToMysqlColumnProperties<full_type>(
                    clickhouse_data_types->getDataAt(i).toView(), mysql_conversion_settings)
                );
                
            }
            return col;
        }

        return arg;
    }

private:
    ContextPtr context;
    DialectType dialect_type;
};

REGISTER_FUNCTION(ConvertToDialectDataType)
{
    factory.registerFunction<FunctionConvertToDialectDataType<true>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConvertToDialectDataType<false>>(FunctionFactory::CaseInsensitive);
}

}
