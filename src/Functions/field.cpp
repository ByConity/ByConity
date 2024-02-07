#include <cstddef>
#include <string>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionMySql.h>
#include <Functions/FunctionsConversion.h>
#include <IO/WriteHelpers.h>
#include <common/range.h>
#include "Columns/ColumnNullable.h"
#include "Columns/IColumn.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

class FunctionField : public IFunction
{
    static constexpr size_t argument_threshold = std::numeric_limits<UInt32>::max();

public:
    static constexpr auto name = "field";
    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionField>());
        return std::make_shared<FunctionField>();
    }

    ArgType getArgumentsType() const override { return ArgType::STRINGS; }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > argument_threshold)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most " + std::to_string(argument_threshold),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : collections::range(1, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!isStringOrFixedString(arg) && !isNumber(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function "
                                    + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &/*result_type*/, size_t cnt) const override
    {
        ColumnPtr src_column = arguments[0].column;

        if (!isString(arguments[0].type))
        {
            ColumnsWithTypeAndName args_src {arguments[0]};
            src_column = ConvertImplGenericToString<ColumnString>::execute(args_src, std::make_shared<DataTypeString>(), cnt);
        }

        auto col_res = ColumnUInt32::create();
        ColumnUInt32::Container & vec_res = col_res->getData();
        vec_res.resize_fill(src_column->size());

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            ColumnPtr data_column = arguments[i].column;
            if (!isString(arguments[i].type))
            {
                ColumnsWithTypeAndName args_i {arguments[i]};
                data_column = ConvertImplGenericToString<ColumnString>::execute(args_i, std::make_shared<DataTypeString>(), cnt);
            }

            for (size_t j = 0; j < vec_res.size(); ++j)
            {
                if (!vec_res[j] && src_column->getDataAt(j) == data_column->getDataAt(j))
                {
                    vec_res[j] = i;
                }
            }
        }

        return col_res;
    }
};

REGISTER_FUNCTION(Field)
{
    factory.registerFunction<FunctionField>(FunctionFactory::CaseInsensitive);
}

}
