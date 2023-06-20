#include <algorithm>
#include <string_view>
#include <utility>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionSubstringIndex : public IFunction
{
public:
    static constexpr auto name = "substring_index";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSubstringIndex>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 3",
                getName(),
                arguments.size());

        if (!isStringOrFixedString(arguments[0].get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be String or FixedString.",
                arguments[0]->getName(),
                1,
                getName());

        if (!isStringOrFixedString(arguments[1].get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be String or FixedString.",
                arguments[1]->getName(),
                2,
                getName());

        if (!isNumber(arguments[2]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be Integer.",
                arguments[2]->getName(),
                3,
                getName());

        return std::make_shared<DataTypeString>();
    }


    void substring_index(std::string_view str, std::string_view delim, int count, ColumnString * res_col) const
    {
        std::string_view result;
        size_t last_pos = 0;
        size_t pos = std::string_view::npos;
        int current_count = 0;

        if (count > 0)
        {
            while ((pos = str.find(delim, last_pos)) != std::string_view::npos)
            {
                ++current_count;
                if (current_count == count)
                {
                    result = str.substr(0, pos);
                    break;
                }
                last_pos = pos + delim.length();
            }
            if (result.empty())
                result = str;
        }
        else
        {
            count = -count;
            last_pos = str.length();
            while ((pos = str.rfind(delim, last_pos)) != std::string_view::npos)
            {
                ++current_count;
                last_pos = pos - 1;
                if (current_count == count)
                {
                    result = str.substr(pos + delim.length());
                    break;
                }
            }
            if (result.empty() || current_count < count)
                result = str;
        }

        res_col->insertData(result.data(), result.length());
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & icolumn_string = arguments[0].column.get();
        const auto & icolumn_delimiter = arguments[1].column.get();
        const auto & icolumn_index = arguments[2].column.get();

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            Int64 count = icolumn_index->getInt(row);
            std::string_view input = std::string_view(icolumn_string->getDataAt(row));
            std::string_view delim = std::string_view(icolumn_delimiter->getDataAt(row));

            if (count == 0 || delim.empty() || input.empty())
            {
                col_res.get()->insertDefault();
                continue;
            }

            substring_index(input, delim, count, col_res.get());
        }

        return col_res;
    }
};

void registerFunctionSubstringIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubstringIndex>(FunctionFactory::CaseInsensitive);
}

}
