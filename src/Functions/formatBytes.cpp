#include <cstddef>
#include <string>
#include <Functions/FunctionFactory.h>
#include <common/types.h>
#include "Columns/IColumn.h"
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Functions/MatchImpl.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionsConversion.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionFormatBytes : public IFunction
{
public:
    static constexpr auto name = "format_bytes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatBytes>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNumber(arguments[0]) && !isString(arguments[0]) && !isFixedString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr src_col = arguments[0].column;
        size_t size = src_col->size();
        auto res_col = ColumnString::create();

        if (isString(arguments[0].type) || isFixedString(arguments[0].type))
        {
            for (size_t i = 0; i < size; ++i)
            {
                res_col->insert(execute(std::stoll(src_col->getDataAt(i).toString())));
            }
        } else {
            for (size_t i = 0; i < size; ++i)
            {
                res_col->insert(execute(src_col->getInt(i)));
            }
        }

        return res_col;
    }
private:
    std::string static stringFraction(const size_t num,
                                const size_t den,
                                const size_t precision,
                                const bool is_negative = false)
    {
        constexpr UInt8 base = 10;

        // prevent division by zero if necessary
        if (den == 0) {
            return "inf";
        }

        // integral part can be computed using regular division
        std::string result;
        if (is_negative)
            result = '-';
        result += std::to_string(num / den);

        // perform first step of long division
        // also cancel early if there is no fractional part
        size_t tmp = num % den;
        if (tmp == 0 || precision == 0) {
            return result;
        }

        // reserve characters to avoid unnecessary re-allocation
        result.reserve(result.size() + precision + 1);

        // fractional part can be computed using long divison
        result += '.';
        for (size_t i = 0; i < precision; ++i) {
            tmp *= base;
            char next_digit = '0' + static_cast<char>(tmp / den);
            result.push_back(next_digit);
            tmp %= den;
        }

        return result;
    }

    std::string static execute(Int64 num, size_t precision = 2)
    {
        constexpr const char size_units[8][4]{
            "B", "KB", "MB", "GB", "TB", "PB", "EB"};

        bool is_negative = false;
        if (num < 0)
        {
            num *= -1;
            is_negative = true;
        }

        size_t unit = floor(log2(num) / log2(1024));

        size_t powers[7];
        for(UInt8 i = 0; i < 7; i++)
            powers[i] = pow(1024, i);

        std::string result;

        result = stringFraction(num, powers[unit], precision, is_negative);

        result.reserve(result.size() + 5);

        result.push_back(' ');
        char first = size_units[unit][0];
        result.push_back(first);

        if (unit != 0) {
            result.push_back('i');
            result.push_back(size_units[unit][1]);
        }

        return result;
    }

};

REGISTER_FUNCTION(FunctionFormatBytes)
{
    factory.registerFunction<FunctionFormatBytes>(FunctionFactory::CaseSensitive);
}

}
