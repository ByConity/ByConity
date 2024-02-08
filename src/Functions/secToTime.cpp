#include <iostream>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionMySql.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Common/DateLUT.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

using DecimalX = DataTypeTime::Base::FieldType;
static_assert(std::is_same_v<DecimalX, Decimal64>);

class FunctionSecToTime : public IFunction
{
private:
    ContextPtr context_;

    static constexpr std::array mandatory_argument_names = {"hour", "minute", "second"};

    static Int64 minTime(const DateLUTImpl & lut) { return lut.makeTime(0, 0, 0); }
    static Int64 maxTime(const DateLUTImpl & lut) { return lut.makeTime(23, 59, 59); }

    static constexpr UInt8 scale = DataTypeTime::default_scale;

public:
    static constexpr auto name = "sec_to_time";
    static constexpr auto max_time = 24 * 3600;

    FunctionSecToTime(ContextPtr context) : context_(context) { }

    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionSecToTime>(context));
        return std::make_shared<FunctionSecToTime>(context);
    }

    ArgType getArgumentsType() const override { return ArgType::NUMBERS; }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        if (size != 1)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr arg = arguments[0];

        if (!isNumber(arg) && !isStringOrFixedString(arg))
        {
            throw Exception{
                "Illegal type " + arg->getName() + " of argument " + std::to_string(0) + " of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        return std::make_shared<DataTypeTime>(scale);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const DataTypeDecimal<DecimalX> equivalent_decimal_type{DecimalUtils::max_precision<DecimalX>, scale};
        MutableColumnPtr res_column_p = equivalent_decimal_type.createColumn();
        auto & res_column = assert_cast<ColumnTime &>(*res_column_p);
        res_column.reserve(input_rows_count);
        res_column.insertManyDefaults(input_rows_count);
        auto & result_data = res_column.getData();

        auto to_decimal = FunctionFactory::instance().get(FunctionTo<DataTypeDecimal<DecimalX>>::Type::name, context_);
        auto scale_type = std::make_shared<DataTypeUInt8>();
        ColumnPtr scale_col = scale_type->createColumnConst(input_rows_count, Field(scale));
        ColumnWithTypeAndName scale_arg {std::move(scale_col), std::move(scale_type), "scale"};
        ColumnsWithTypeAndName args {arguments[0], std::move(scale_arg)};
        ColumnPtr col_p = to_decimal->build(args)->execute(args, std::make_shared<DataTypeDecimal<DecimalX>>(DecimalUtils::max_precision<DecimalX>, scale), input_rows_count);
        const auto & col = assert_cast<const ColumnDecimal<DecimalX> &>(*col_p);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto [whole_part, fractional_part] = equivalent_decimal_type.split(col.getElement(i));      
            whole_part %= max_time;
            if (whole_part < 0)
                whole_part += max_time;    
            result_data[i] = DecimalUtils::decimalFromComponents<DecimalX>(
                whole_part, 
                fractional_part, 
                scale
            );
        }
        return res_column_p;
    }
};


class FunctionTimeToSec : public IFunction
{
private:
    ContextPtr context_;

    static constexpr std::array mandatory_argument_names = {"hour", "minute", "second"};

    static Int64 minTime(const DateLUTImpl & lut) { return lut.makeTime(0, 0, 0); }
    static Int64 maxTime(const DateLUTImpl & lut) { return lut.makeTime(23, 59, 59); }

    static constexpr UInt8 scale = DataTypeTime::default_scale;
    
public:
    static constexpr auto name = "time_to_sec";

    FunctionTimeToSec(ContextPtr context) : context_(context) { }

    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionTimeToSec>(context));
        return std::make_shared<FunctionTimeToSec>(context);
    }

    ArgType getArgumentsType() const override { return ArgType::STRINGS; }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        if (size != 1)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(size) + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr arg = arguments[0];

        if (!isDateOrDateTime(arg) && !isTime(arg) && !isStringOrFixedString(arg))
        {
            throw Exception{
                "Illegal type " + arg->getName() + " of argument " + std::to_string(0) + " of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        static_assert(std::is_same_v<DecimalX, Decimal64>);
        return std::make_shared<DataTypeDecimal<DecimalX>>(DecimalUtils::max_precision<DecimalX>, scale);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto to_time = FunctionFactory::instance().get("toTimeType", context_);
        auto scale_type = std::make_shared<DataTypeUInt8>();
        ColumnPtr scale_col = scale_type->createColumnConst(1, Field(scale));
        ColumnWithTypeAndName scale_arg {std::move(scale_col), std::move(scale_type), "scale"};
        ColumnsWithTypeAndName args {arguments[0], std::move(scale_arg)};
        return to_time->build(args)->execute(args, std::make_shared<DataTypeTime>(scale), input_rows_count);
    }
};

REGISTER_FUNCTION(SecToTime)
{
    factory.registerFunction<FunctionSecToTime>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTimeToSec>(FunctionFactory::CaseInsensitive);
}

}
