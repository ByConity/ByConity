#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include "IFunctionMySql.h"

namespace DB
{

template<typename Type>
bool IFunctionMySql::isVaildArguments(const Type & arguments) const
{
    DataTypes expected = getColumnsType(arg_type, arguments);
    if (expected.empty()) return true;

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        DataTypePtr arg;
        if constexpr (std::is_same_v<Type, DataTypes>)
            arg = arguments[i];
        if constexpr (std::is_same_v<Type, ColumnsWithTypeAndName>)
            arg = arguments[i].type;

        if ((isString(expected[i]) && !(isStringOrFixedString(arg) || isIPv6(arg)))
            || (arg_type != ArgType::NOT_DECIMAL && isFloat(expected[i]) && !isNumber(arg))
            || (arg_type == ArgType::NOT_DECIMAL && isFloat(expected[i]) && !(isInteger(arg) || isFloat(arg)))
            || (isUnsignedInteger(expected[i]) && !isUnsignedInteger(arg))
            || (isInteger(expected[i]) && !isInteger(arg))
            || (isDateTime64(expected[i]) && !(isDateTime64(arg) || isDateOrDate32(arg) || isDateTime(arg) || isTime(arg)))
            || (isDateTime(expected[i]) && !isDateTime(arg))
            || (arg_type == ArgType::ARRAY_FIRST && !expected[i]->equals(*arg))
            || (arg_type == ArgType::ARRAY_COMMON && !areCompatibleArrays(arg, expected[i])))
            return false;
    }
    return true;
}

template<typename Type>
bool IFunctionMySql::isVaildIntervalArithmetic(const Type & arguments) const
{
    if (arguments.size() != 2) return false;
    DataTypePtr type0, type1;
    if constexpr (std::is_same_v<Type, DataTypes>)
    {
        type0 = arguments[0];
        type1 = arguments[1];
    }
    if constexpr (std::is_same_v<Type, ColumnsWithTypeAndName>)
    {
        type0 = arguments[0].type;
        type1 = arguments[1].type;
    }

    if (checkAndGetDataType<DataTypeInterval>(type0.get()))
    {
        if (isDateOrDate32(type1) || isDateTime(type1) || isDateTime64(type1) || isTime(type1))
            return true;
        arg_type = ArgType::INTERVAL_DATE;
    }
    else if (checkAndGetDataType<DataTypeInterval>(type1.get()))
    {
        if (isDateOrDate32(type0) || isDateTime(type0) || isDateTime64(type0) || isTime(type0))
            return true;
        arg_type = ArgType::DATE_INTERVAL;
    }
    else if (isDateTime64(type0) && isDateTime64(type1))
        return true;
    return false;
}

ColumnWithTypeAndName IFunctionMySql::recursiveConvertArrayArgument(DataTypePtr toType, const ColumnWithTypeAndName& argument)
{
    /// the actual array may be nested inside const nullable column
    const ColumnArray * arr_col = nullptr;
    const ColumnNullable * null_col = nullptr;
    const auto * const_col = checkAndGetColumn<ColumnConst>(argument.column.get());
    if (const_col)
    {
        null_col = checkAndGetColumn<ColumnNullable>(const_col->getDataColumnPtr().get());
        if (null_col)
            arr_col = assert_cast<const ColumnArray *>(null_col->getNestedColumnPtr().get());
        else
            arr_col = assert_cast<const ColumnArray *>(const_col->getDataColumnPtr().get());
    }
    else if (null_col = checkAndGetColumn<ColumnNullable>(argument.column.get()); null_col)
    {
        arr_col = assert_cast<const ColumnArray *>(null_col->getNestedColumnPtr().get());
    }
    else
    {
        arr_col = assert_cast<const ColumnArray *>(argument.column.get());
    }

    auto expected_nested_type = extractNestedArrayType(*toType);
    auto cur_nested_type = extractNestedArrayType(*argument.type);

    ColumnPtr converted_nested_col = nullptr;
    if (expected_nested_type->equals(*cur_nested_type))
    {
        converted_nested_col = arr_col->getDataPtr();
    }
    else
    {
        auto expected_nested_type_no_null = removeNullable(expected_nested_type);
        auto nested_arg = ColumnWithTypeAndName{arr_col->getDataPtr(), cur_nested_type, argument.name};

        if (isArray(expected_nested_type_no_null))
            converted_nested_col = recursiveConvertArrayArgument(expected_nested_type, nested_arg).column;
        else
        {
            auto input_rows_count = nested_arg.column->size();

            if (isStringOrFixedString(expected_nested_type_no_null) || isIPv6(expected_nested_type_no_null))
                converted_nested_col = convertToTypeStatic<DataTypeString>(nested_arg, expected_nested_type, input_rows_count);
            else if (isFloat(expected_nested_type_no_null))
                converted_nested_col = convertToTypeStatic<DataTypeFloat64>(nested_arg, expected_nested_type, input_rows_count);
            else if (isInteger(expected_nested_type_no_null))
                converted_nested_col = convertToTypeStatic<DataTypeInt64>(nested_arg, expected_nested_type, input_rows_count);
            else if (isUnsignedInteger(expected_nested_type_no_null))
                converted_nested_col = convertToTypeStatic<DataTypeUInt64>(nested_arg, expected_nested_type, input_rows_count);
            else if (isDateTime64(expected_nested_type_no_null))
                converted_nested_col = convertToTypeStatic<DataTypeDateTime64>(nested_arg, expected_nested_type, input_rows_count);
            else if (isDateTime(expected_nested_type_no_null))
                converted_nested_col = convertToTypeStatic<DataTypeDateTime>(nested_arg, expected_nested_type, input_rows_count);
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Convert to Array({}) is not implemented", expected_nested_type->getName());

            if (expected_nested_type->isNullable() && !checkColumn<ColumnNullable>(*converted_nested_col))
                converted_nested_col = makeNullable(converted_nested_col);
        }
    }

    ColumnPtr converted_col = nullptr;
    converted_col = ColumnArray::create(converted_nested_col, arr_col->getOffsetsPtr());

    if (toType->isNullable())
    {
        if (null_col)
            converted_col = ColumnNullable::create(converted_col, null_col->getNullMapColumnPtr());
        else
            converted_col = makeNullable(converted_col);
    }

    if(const_col)
        converted_col = ColumnConst::create(std::move(converted_col), argument.column->size());

    return ColumnWithTypeAndName{converted_col, toType, argument.name};
}

void IFunctionMySql::checkAndConvert(const ColumnsWithTypeAndName & arguments, ColumnsWithTypeAndName & new_arguments) const
{
    DataTypes expected = getColumnsType(arg_type, arguments);

    auto convertAndAddToNewArguments = [&](auto toType, const ColumnWithTypeAndName& argument, bool is_nullable) {
        DataTypePtr new_type = nullptr;
        if constexpr (std::is_same_v<decltype(toType), DataTypeDateTime64>)
            new_type = is_nullable ? makeNullable(std::make_shared<decltype(toType)>(DataTypeDateTime64::default_scale))
                                    : std::make_shared<decltype(toType)>(DataTypeDateTime64::default_scale);
        else
            new_type = is_nullable ? makeNullable(std::make_shared<decltype(toType)>()) : std::make_shared<decltype(toType)>();

        const auto * const_col = checkAndGetColumn<ColumnConst>(argument.column.get());
        auto col = const_col ? const_col->getDataColumnPtr() : argument.column;
        auto converted_col = convertToTypeStatic<decltype(toType)>(ColumnWithTypeAndName{col, argument.type, argument.name}, new_type, col->size());
        /// must convert it back to const column as some funcs expects args to be const
        converted_col = const_col && converted_col->size() ? ColumnConst::create(std::move(converted_col), argument.column->size()) : converted_col;
        new_arguments.emplace_back(converted_col, new_type, argument.name);
    };

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        auto arg = arguments[i].type;
        bool is_nullable = [&]() {
            if (arg->getTypeId() != TypeIndex::Nullable)
                return false;
            arg = typeid_cast<const DataTypeNullable *>(arg.get())->getNestedType();
            return true;
        }();

        /// for funcs like array_remove, array_position,
        /// the first arg should be converted to nullable if the rest has nullable
        if (i == 0 && (arg_type == ArgType::ARRAY_FIRST))
        {
            bool has_nullable = false;
            for (size_t j = 1; j < arguments.size() && !has_nullable; j++)
                has_nullable = arguments[j].type->isNullable();
            if (!is_nullable && has_nullable)
                new_arguments.emplace_back(arguments[0].column ? makeNullable(arguments[0].column) : nullptr,
                                           makeNullable(arguments[0].type),
                                           arguments[0].name);
            else
                new_arguments.emplace_back(arguments[0]);
            continue;
        }

        if (isNothing(arg))
        {
            new_arguments.emplace_back(arguments[i]);
            continue;
        }

        if (!arguments[i].column)
        {
            new_arguments.emplace_back(nullptr, expected[i], arguments[i].name);
            continue;
        }

        if (isString(expected[i]) && !(isStringOrFixedString(arg) || isIPv6(arg)))
            convertAndAddToNewArguments(DataTypeString(), arguments[i], is_nullable);
        else if (arg_type != ArgType::NOT_DECIMAL && isFloat(expected[i]) && !isNumber(arg))
            convertAndAddToNewArguments(DataTypeFloat64(), arguments[i], is_nullable);
        /// for array funcs, if the the non array arg is decimal, conver it to float as the func cannot handle decimal arg
        else if ((arg_type == ArgType::NOT_DECIMAL || arg_type == ArgType::ARRAY_FIRST) && isFloat(expected[i]) && !(isInteger(arg) || isFloat(arg)))
            convertAndAddToNewArguments(DataTypeFloat64(), arguments[i], is_nullable);
        else if (isUnsignedInteger(expected[i]) && !isInteger(arg))
            convertAndAddToNewArguments(DataTypeUInt64(), arguments[i], is_nullable);
        else if (isInteger(expected[i]) && !isInteger(arg))
            convertAndAddToNewArguments(DataTypeInt64(), arguments[i], is_nullable);
        else if (isDateTime64(expected[i]) && !(isDateTime64(arg) || isDateOrDate32(arg) || isDateTime(arg) || isTime(arg)))
            convertAndAddToNewArguments(DataTypeDateTime64(DataTypeDateTime64::default_scale), arguments[i], is_nullable);
        else if (isDateTime(expected[i]) && !isDateTime(arg))
            convertAndAddToNewArguments(DataTypeDateTime(), arguments[i], is_nullable);
        else if (arg_type == ArgType::ARRAY_COMMON && !areCompatibleArrays(arguments[i].type, expected[i]))
            new_arguments.push_back(recursiveConvertArrayArgument(expected[i], arguments[i]));
        else
            new_arguments.emplace_back(arguments[i]);
    }
}

template<typename toType>
ColumnPtr IFunctionMySql::convertToTypeStatic(const ColumnWithTypeAndName & argument, const DataTypePtr & result_type, size_t input_rows_count)
{
    ///                 Conversion Matrix
    /// given\Expect     String      Date           Number
    /// String              NA      trim->toDate     trim->toNumber
    /// Date             toString      NA            toYYYYMMDD/toYYYYMMDDhhmmss
    /// Number           toString  toString->toDate    NA

    auto executeFunction = [&](const std::string & function_name, const ColumnsWithTypeAndName input, DataTypePtr & output_type) {
        auto func = FunctionFactory::instance().get(function_name, nullptr);
        output_type = func->getReturnType(input);
        return func->build(input)->execute(input, output_type, input_rows_count);
    };

    ColumnPtr col = argument.column->convertToFullColumnIfConst();

    DataTypePtr tmp_type;

    auto toFloat64MySql = [&]() {
        if (isDateOrDate32(argument.type))
            return executeFunction("toYYYYMMDDMySql", {{col, argument.type, argument.name}}, tmp_type);
        if (isDateTime(argument.type) || isDateTime64(argument.type) || isDateTime64(argument.type) || isTime(argument.type))
            return executeFunction("toYYYYMMDDhhmmssMySql", {{col, argument.type, argument.name}}, tmp_type);

        tmp_type = argument.type;
        if (isFixedString(tmp_type))
            col = executeFunction("toString", {{col, tmp_type, argument.name}}, tmp_type);

        if (isString(tmp_type))
            col = executeFunction("trimBoth", {{col, tmp_type, argument.name}}, tmp_type);
        else
            col = executeFunction("toString", {{col, tmp_type, argument.name}}, tmp_type);

        return executeFunction("parseFloat64OrZeroMySql", {{col, tmp_type, argument.name}}, tmp_type);
    };

    if constexpr (std::is_same_v<toType, DataTypeString>)
        return executeFunction("toString", {{col, argument.type, argument.name}}, tmp_type);
    else if constexpr (std::is_same_v<toType, DataTypeDateTime64>)
    {
        tmp_type = argument.type;
        if (isFixedString(tmp_type))
            col = executeFunction("toString", {{col, tmp_type, argument.name}}, tmp_type);

        if (isString(tmp_type))
            col = executeFunction("trimBoth", {{col, tmp_type, argument.name}}, tmp_type);
        else
            col = executeFunction("toString", {{col, tmp_type, argument.name}}, tmp_type);

        return executeFunction("parseDateTime64BestEffortOrZero", {{col, tmp_type, argument.name}}, tmp_type);
    }
    else if constexpr (std::is_same_v<toType, DataTypeDateTime>)
    {
        tmp_type = argument.type;
        if (isFixedString(tmp_type))
            col = executeFunction("toString", {{col, tmp_type, argument.name}}, tmp_type);

        if (isString(tmp_type))
            col = executeFunction("trimBoth", {{col, tmp_type, argument.name}}, tmp_type);
        else
            col = executeFunction("toString", {{col, tmp_type, argument.name}}, tmp_type);

        return executeFunction("parseDateTimeBestEffortOrZero", {{col, tmp_type, argument.name}}, tmp_type);
    }
    else if constexpr (std::is_same_v<toType, DataTypeFloat64>)
        return toFloat64MySql();
    else if constexpr (std::is_same_v<toType, DataTypeUInt64>)
    {
        col = toFloat64MySql();
        /// negative decimal cannot be converted to uint64 directly; convert to int64 first.
        if (isDecimal(argument.type))
            col = executeFunction("toInt64", {{col, tmp_type, argument.name}}, tmp_type);
        return executeFunction("toUInt64", {{col, tmp_type, argument.name}}, tmp_type);
    }
    else if constexpr (std::is_same_v<toType, DataTypeInt64>)
    {
        col = toFloat64MySql();
        return executeFunction("toInt64", {{col, tmp_type, argument.name}}, tmp_type);
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "function for converting column from type {} to {} is not implemented", argument.type->getName(), result_type->getName());
}

template<typename toType>
ColumnPtr IFunctionMySql::convertToTypeStatic(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
{
    auto executeFunction = [&](const std::string & function_name, const ColumnsWithTypeAndName input, const DataTypePtr output_type) {
        auto func = FunctionFactory::instance().get(function_name, nullptr);
        return func->build(input)->execute(input, output_type, input_rows_count);
    };

    if constexpr (std::is_same_v<toType, DataTypeDecimal<Decimal64>>)
    {
        return executeFunction("toDecimal64", arguments, result_type);
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "function for converting column from type {} ... to {} is not implemented", arguments[0].type->getName(), result_type->getName());
}

template bool IFunctionMySql::isVaildArguments<DataTypes>(const DataTypes &) const;
template bool IFunctionMySql::isVaildArguments<ColumnsWithTypeAndName>(const ColumnsWithTypeAndName &) const;
template bool IFunctionMySql::isVaildIntervalArithmetic<DataTypes>(const DataTypes &) const;
template bool IFunctionMySql::isVaildIntervalArithmetic<ColumnsWithTypeAndName>(const ColumnsWithTypeAndName &) const;
template ColumnPtr IFunctionMySql::convertToTypeStatic<DataTypeInt64>(const ColumnWithTypeAndName & argument, const DataTypePtr & result_type, size_t input_rows_count);
template ColumnPtr IFunctionMySql::convertToTypeStatic<DataTypeDecimal<Decimal64>>(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count);
}
