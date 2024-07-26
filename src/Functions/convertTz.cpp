#include <Common/Exception.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionMySql.h>

#include <memory>
#include <regex>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionConvertTz : public IFunction
{
public:
    static constexpr auto name = "convert_tz";

    explicit FunctionConvertTz(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionConvertTz>(context));
        return std::make_shared<FunctionConvertTz>(context);
    }

    ArgType getArgumentsType() const override { return ArgType::STRINGS; }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const ColumnWithTypeAndName & to_tz = arguments[2];
        if (to_tz.column == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Argument at index 2 for function {} must be constant", getName());
        }
        return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromMysqlColumn(*to_tz.column));  
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type [[maybe_unused]] , size_t input_rows_count) const override
    {
        auto executeFunction = [&](const std::string & function_name, const ColumnsWithTypeAndName input, const DataTypePtr output_type) {
            auto func = FunctionFactory::instance().get(function_name, context);
            return func->build(input)->execute(input, output_type, input_rows_count);
        };

        const ColumnWithTypeAndName & date_expr = arguments[0];
        const ColumnWithTypeAndName & from_tz = arguments[1];
        const String from_tz_str = extractTimeZoneNameFromMysqlColumn(*from_tz.column);
        const ColumnPtr from_tz_parsed = from_tz.type->createColumnConst(from_tz.column->size(), from_tz_str);
        const auto date_time_type = std::make_shared<DataTypeDateTime>(from_tz_str);
        const ColumnsWithTypeAndName to_date_time_operands = {date_expr, {from_tz_parsed, from_tz.type, from_tz.name}};
        return executeFunction("parseDateTimeBestEffort", to_date_time_operands, date_time_type);
    }

private:
    static const std::regex mysql_utc_offset_pattern;

    ContextPtr context;

    static String extractTimeZoneNameFromMysqlColumn(const IColumn & column)
    {
        String time_zone_name = extractTimeZoneNameFromColumn(column);
        if (time_zone_name.empty() ||
            (time_zone_name[0] != '+' && time_zone_name[0] != '-'))
            return time_zone_name;

        if (!std::regex_match(time_zone_name, mysql_utc_offset_pattern))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "UTC offset should match the regex ((+|-)\\d{1,2}:\\d{2})");

        // Convert +H:MM into +0H:MM (Similarly -H:MM into -0H:MM)
        if (time_zone_name.size() == 5)
            time_zone_name.insert(time_zone_name.begin() + 1, '0');
        // Fixed/UTC+HH:MM:00
        time_zone_name = String("Fixed/UTC") + time_zone_name + ":00";
        return time_zone_name;     
    }

};

// (+/-)HH:MM or (+/-)H:MM
const std::regex FunctionConvertTz::mysql_utc_offset_pattern(R"((\+|-)\d{1,2}:\d{2})", std::regex_constants::ECMAScript);


REGISTER_FUNCTION(ConvertTz)
{
    factory.registerFunction<FunctionConvertTz>(FunctionFactory::CaseInsensitive);
}

}
