#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Higher-order functions for arrays.
  * These functions optionally apply a map (transform) to array (or multiple arrays of identical size) by lambda function,
  *  and return some result based on that transformation.
  *
  * Examples:
  * arrayMap(x1,...,xn -> expression, array1,...,arrayn) - apply the expression to each element of the array (or set of parallel arrays).
  * arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  *
  * For some functions arrayCount, arrayExists, arrayAll, an overload of the form f(array) is available,
  *  which works in the same way as f(x -> x, array).
  *
  * See the example of Impl template parameter in arrayMap.cpp
  */
template <typename Impl, typename Name>
class FunctionArrayMapped : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayMapped>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Called if at least one function argument is a lambda expression.
    /// For argument-lambda expressions, it defines the types of arguments of these expressions.
    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Function " + getName() + " needs at least one argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
            throw Exception("Function " + getName() + " needs at least one array argument.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_types(arguments.size() - 1);
        for (size_t i = 0; i < nested_types.size(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
            if (!array_type)
                throw Exception("Argument " + toString(i + 2) + " of function " + getName() + " must be array. Found "
                                + arguments[i + 1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
            throw Exception("First argument for this overload of " + getName() + " must be a function with "
                            + toString(nested_types.size()) + " arguments. Found "
                            + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t min_args = Impl::needExpression() ? 2 : 1;
        if (arguments.size() < min_args)
            throw Exception("Function " + getName() + " needs at least "
                            + toString(min_args) + " argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());

            if (!array_type)
                throw Exception("The only argument for function " + getName() + " must be array. Found "
                                + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            DataTypePtr nested_type = array_type->getNestedType();

            if (Impl::needBoolean() && !isUInt8(nested_type))
                throw Exception("The only argument for function " + getName() + " must be array of UInt8. Found "
                                + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return Impl::getReturnType(nested_type, nested_type);
        }
        else
        {
            if (arguments.size() > 2 && Impl::needOneArray())
                throw Exception("Function " + getName() + " needs one array argument.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());

            if (!data_type_function)
                throw Exception("First argument for function " + getName() + " must be a function.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            /// The types of the remaining arguments are already checked in getLambdaArgumentTypes.

            DataTypePtr return_type = removeLowCardinality(data_type_function->getReturnType());
            /// Special cases when we need boolean lambda result:
            ///  - lambda may return Nullable(UInt8) column, in this case after lambda execution we will
            ///    replace all NULLs with 0 and return nested UInt8 column.
            ///  - lambda may return Nothing or Nullable(Nothing) because of default implementation of functions
            ///    for these types. In this case we will just create UInt8 const column full of 0.
            if (Impl::needBoolean() && !isUInt8(removeNullable(return_type)) && !isNothing(removeNullable(return_type)))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Expression for function {} must return UInt8 or Nullable(UInt8), found {}",
                    getName(),
                    return_type->getName());

            const auto * first_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].type.get());

            return Impl::getReturnType(return_type, first_array_type->getNestedType());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (arguments.size() == 1)
        {
            ColumnPtr column_array_ptr = arguments[0].column;
            const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

            if (!column_array)
            {
                const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                if (!column_const_array)
                    throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
                column_array_ptr = column_const_array->convertToFullColumn();
                column_array = assert_cast<const ColumnArray *>(column_array_ptr.get());
            }

            return Impl::execute(*column_array, column_array->getDataPtr());
        }
        else
        {
            const auto & column_with_type_and_name = arguments[0];

            if (!column_with_type_and_name.column)
                throw Exception("First argument for function " + getName() + " must be a function.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * column_function = typeid_cast<const ColumnFunction *>(column_with_type_and_name.column.get());

            if (!column_function)
                throw Exception("First argument for function " + getName() + " must be a function.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            ColumnPtr offsets_column;

            ColumnPtr column_first_array_ptr;
            const ColumnArray * column_first_array = nullptr;

            ColumnsWithTypeAndName arrays;
            arrays.reserve(arguments.size() - 1);

            for (size_t i = 1; i < arguments.size(); ++i)
            {
                const auto & array_with_type_and_name = arguments[i];

                ColumnPtr column_array_ptr = array_with_type_and_name.column;
                const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

                const DataTypePtr & array_type_ptr = array_with_type_and_name.type;
                const auto * array_type = checkAndGetDataType<DataTypeArray>(array_type_ptr.get());

                if (!column_array)
                {
                    const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                    if (!column_const_array)
                        throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
                    column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
                    column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
                }

                if (!array_type)
                    throw Exception("Expected array type, found " + array_type_ptr->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                if (!offsets_column)
                {
                    offsets_column = column_array->getOffsetsPtr();
                }
                else
                {
                    /// The first condition is optimization: do not compare data if the pointers are equal.
                    if (column_array->getOffsetsPtr() != offsets_column
                        && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                        throw Exception("Arrays passed to " + getName() + " must have equal size", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
                }

                if (i == 1)
                {
                    column_first_array_ptr = column_array_ptr;
                    column_first_array = column_array;
                }

                arrays.emplace_back(ColumnWithTypeAndName(column_array->getDataPtr(),
                                                          recursiveRemoveLowCardinality(array_type->getNestedType()),
                                                          array_with_type_and_name.name));
            }

            /// Put all the necessary columns multiplied by the sizes of arrays into the columns.
            auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(column_first_array->getOffsets()));
            auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
            replicated_column_function->appendArguments(arrays);

            auto lambda_result = replicated_column_function->reduce();
            if (lambda_result.column->lowCardinality())
                lambda_result.column = lambda_result.column->convertToFullColumnIfLowCardinality();

            if (Impl::needBoolean())
            {
                /// If result column is Nothing or Nullable(Nothing), just create const UInt8 column with 0 value.
                if (isNothing(removeNullable(lambda_result.type)))
                {
                    auto result_type = std::make_shared<DataTypeUInt8>();
                    lambda_result.column = result_type->createColumnConst(lambda_result.column->size(), 0);
                }
                /// If result column is Nullable(UInt8), then extract nested column and write 0 in all rows
                /// when we have NULL.
                else if (lambda_result.column->isNullable())
                {
                    auto result_column = IColumn::mutate(std::move(lambda_result.column));
                    auto * column_nullable = assert_cast<ColumnNullable *>(result_column.get());
                    auto & null_map = column_nullable->getNullMapData();
                    auto nested_column = IColumn::mutate(std::move(column_nullable->getNestedColumnPtr()));
                    auto & nested_data = assert_cast<ColumnUInt8 *>(nested_column.get())->getData();
                    for (size_t i = 0; i != nested_data.size(); ++i)
                    {
                        if (null_map[i])
                            nested_data[i] = 0;
                    }
                    lambda_result.column = std::move(nested_column);
                }
            }

            return Impl::execute(*column_first_array, lambda_result.column);
        }
    }
};

}
