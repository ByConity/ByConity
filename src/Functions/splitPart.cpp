#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionMySql.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionSplitPart : public IFunction
{
public:
    static constexpr auto name = "split_part";
    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionSplitPart>());
        return std::make_shared<FunctionSplitPart>();
    }

    ArgType getArgumentsType() const override { return ArgType::STR_STR_NUM; }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 3",
                getName(),
                arguments.size());

        for (size_t i = 0; i < arguments.size(); i++)
        {
            DataTypePtr arg = arguments[i];
            if (arg->getTypeId() == TypeIndex::Nullable)
            {
                arg = typeid_cast<const DataTypeNullable *>(arg.get())->getNestedType();
            }

            if (isStringOrFixedString(arg) && i != 2)
            {
                continue;
            }

            if (!isNothing(arg) && !isNumber(arg))
            {
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(i) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
        }
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & icolumn_string = arguments[0].column.get();
        const auto & icolumn_delimiter = arguments[1].column.get();
        ColumnPtr icolumn_index = arguments[2].column;
        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(*arguments[2].column))
            icolumn_index = nullable_col->getNestedColumnPtr();

        MutableColumnPtr res{result_type->createColumn()};
        res->reserve(input_rows_count);

        ColumnNullable & nullable_column = assert_cast<ColumnNullable &>(*res);
        ColumnString * res_nested_col = typeid_cast<ColumnString *>(&nullable_column.getNestedColumn());

        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant)
        {
            return result_type->createColumnConstWithDefaultValue(input_rows_count);
        }


        size_t arg_size = arguments.size();
        // extract data columns and null maps if exist
        const UInt8 * nullable_args_map[arg_size];
        ColumnPtr raw_column_maps[arg_size];
        extractNullMapAndNestedCol(arguments, raw_column_maps, nullable_args_map);


        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (checkNulls(nullable_args_map, row))
            {
                res_nested_col->insertDefault();
                nullable_column.getNullMapData().push_back(1);
                continue;
            }

            splitInto(
                std::string_view(icolumn_string->getDataAt(row)),
                std::string_view(icolumn_delimiter->getDataAt(row)),
                icolumn_index->getInt(row),
                nullable_column,
                res_nested_col);
        }

        return res;
    }


private:
    inline bool checkNulls(const UInt8 ** nullable_args_map, size_t row_num) const
    {
        return (nullable_args_map[0] && nullable_args_map[0][row_num]) || (nullable_args_map[1] && nullable_args_map[1][row_num])
            || (nullable_args_map[2] && nullable_args_map[2][row_num]);
    }

    inline void splitInto(
        const std::string_view & input,
        const std::string_view & delimiter,
        size_t index,
        ColumnNullable & nullable_column,
        ColumnString * res_nested_col) const
    {
        size_t start = 0;
        size_t end = input.find(delimiter);
        size_t count = 0;


        while (end != std::string::npos)
        {
            count++;
            if (count == index)
            {
                res_nested_col->insertData(input.data() + start, end - start);
                nullable_column.getNullMapData().push_back(0);
                return;
            }
            start = end + delimiter.length();
            end = input.find(delimiter, start);
        }

        if (++count == index)
        {
            res_nested_col->insertData(input.data() + start, input.length() - start);
            nullable_column.getNullMapData().push_back(0);
            return;
        }
        res_nested_col->insertDefault();
        nullable_column.getNullMapData().push_back(1);
    }
};

REGISTER_FUNCTION(SplitPart)
{
    factory.registerFunction<FunctionSplitPart>(FunctionFactory::CaseInsensitive);
}

}
