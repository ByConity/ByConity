#include <iostream>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <common/DateLUT.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


class FunctionSecToTime : public IFunction
{
private:
    ContextPtr context_;

    static constexpr std::array mandatory_argument_names = {"hour", "minute", "second"};

    static Int64 minTime(const DateLUTImpl & lut) { return lut.makeTime(0, 0, 0); }
    static Int64 maxTime(const DateLUTImpl & lut) { return lut.makeTime(23, 59, 59); }

public:
    static constexpr auto name = "sec_to_time";
    static constexpr auto max_time = 24 * 3600;

    FunctionSecToTime(ContextPtr context) : context_(context) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionSecToTime>(context); }

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
        return std::make_shared<DataTypeTime>(0);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto res_column = ColumnTime::create(input_rows_count, 0);
        auto & result_data = res_column->getData();

        ColumnsWithTypeAndName converted;
        if (isStringOrFixedString(arguments[0].type))
        {
            auto to_int = FunctionFactory::instance().get("toInt64", context_);
            auto col = to_int->build(arguments)->execute(arguments, std::make_shared<DataTypeInt64>(), input_rows_count);
            ColumnWithTypeAndName converted_col(col, std::make_shared<DataTypeInt64>(), "unixtime");
            converted.emplace_back(converted_col);
        }
        else
        {
            converted = arguments;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            //auto time = std::max(std::min(converted[0].column->getInt(i), max_time), min_time);
            auto time = converted[0].column->getInt(i) % max_time;
            if (time < 0)
            {
                time += max_time;
            }

            result_data[i] = DecimalUtils::decimalFromComponents<Decimal64>(time, 0, 0);
        }

        return res_column;
    }
};

class FunctionTimeToSec : public IFunction
{
private:
    ContextPtr context_;

    static constexpr std::array mandatory_argument_names = {"hour", "minute", "second"};

    static Int64 minTime(const DateLUTImpl & lut) { return lut.makeTime(0, 0, 0); }
    static Int64 maxTime(const DateLUTImpl & lut) { return lut.makeTime(23, 59, 59); }

public:
    static constexpr auto name = "time_to_sec";

    FunctionTimeToSec(ContextPtr context) : context_(context) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeToSec>(context); }

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
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName converted_time;

        auto to_time = FunctionFactory::instance().get("toTimeType", context_);
        const auto scale_type = std::make_shared<DataTypeUInt8>();
        const auto scale_col = scale_type->createColumnConst(1, Field(0));
        ColumnWithTypeAndName scale_arg {std::move(scale_col), std::move(scale_type), "scale"};
        ColumnsWithTypeAndName args {arguments[0], std::move(scale_arg)};
        auto col = to_time->build(args)->execute(args, std::make_shared<DataTypeTime>(0), input_rows_count);
        ColumnWithTypeAndName converted_time_col(col, std::make_shared<DataTypeTime>(0), "time");
        converted_time.emplace_back(converted_time_col);

        auto to_int = FunctionFactory::instance().get("toInt64", context_);
        return to_int->build(converted_time)->execute(converted_time, std::make_shared<DataTypeInt64>(), input_rows_count);
    }
};

REGISTER_FUNCTION(SecToTime)
{
    factory.registerFunction<FunctionSecToTime>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTimeToSec>(FunctionFactory::CaseInsensitive);
}

}
