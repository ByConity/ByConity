#include <cstddef>
#include <string>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

class FunctionInterval : public IFunction
{
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();

public:
    static constexpr auto name = "interval";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionInterval>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (arguments.size() > argument_threshold)
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most " + std::to_string(argument_threshold),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            DataTypePtr arg = arguments[arg_idx];
            if (arg->getTypeId() == TypeIndex::Nullable)
            {
                arg = typeid_cast<const DataTypeNullable *>(arg.get())->getNestedType();
            }
            if (!isNumber(arg) && !isNothing(arg))
            {
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
        }

        return std::make_shared<DataTypeInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        // handle null only index
        if (arguments[0].column->onlyNull())
        {
            return result_type->createColumnConst(input_rows_count, -1);
        }

        size_t col_size = arguments[0].column->size();
        size_t arg_size = arguments.size();
        // default value: end index
        auto col_res = ColumnInt32::create(col_size, arg_size - 1);
        auto & vec_res = col_res->getData();
        vec_res.resize(col_size);

        // extract data columns and null maps if exist
        const UInt8 * nullable_args_map[arg_size];
        ColumnPtr raw_column_maps[arg_size];
        extractNullMapAndNestedCol(arguments, raw_column_maps, nullable_args_map);

        for (size_t j = 0; j < col_size; j++)
        {
            // handle null index
            if (nullable_args_map[0] && nullable_args_map[0][j])
            {
                vec_res[j] = -1;
                continue;
            }

            int index = raw_column_maps[0]->getInt(j);
            for (const auto arg_idx : collections::range(1, arg_size))
            {
                // skip null argument
                if (nullable_args_map[arg_idx] && nullable_args_map[arg_idx][j])
                {
                    continue;
                }
                if (index < raw_column_maps[arg_idx]->getInt(j))
                {
                    vec_res[j] = arg_idx - 1;
                    break;
                }
            }
        }

        return col_res;
    }
};

REGISTER_FUNCTION(FunctionInterval)
{
    factory.registerFunction<FunctionInterval>(FunctionFactory::CaseInsensitive);
}

}
