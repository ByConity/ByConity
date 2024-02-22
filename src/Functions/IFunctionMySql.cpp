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
            || (isDateTime(expected[i]) && !isDateTime(arg)))
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

void IFunctionMySql::checkAndConvert(const ColumnsWithTypeAndName & arguments, ColumnsWithTypeAndName & new_arguments) const
{
    DataTypes expected = getColumnsType(arg_type, arguments);

    auto convertAndAddToNewArguments = [&](auto toType, const ColumnWithTypeAndName& argument, bool is_nullable) {
        size_t input_rows_count = argument.column->size();
        DataTypePtr new_type = nullptr;
        if constexpr (std::is_same_v<decltype(toType), DataTypeDateTime64>)
            new_type = is_nullable ? makeNullable(std::make_shared<decltype(toType)>(DataTypeDateTime64::default_scale))
                                   : std::make_shared<decltype(toType)>(DataTypeDateTime64::default_scale);
        else
            new_type = is_nullable ? makeNullable(std::make_shared<decltype(toType)>()) : std::make_shared<decltype(toType)>();
        auto converted_col = convertToTypeStatic<decltype(toType)>(argument, new_type, input_rows_count);
        converted_col = isColumnConst(*argument.column) ? ColumnConst::create(std::move(converted_col), input_rows_count) : converted_col;
        new_arguments.emplace_back(converted_col, new_type, argument.name);
    };

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (!arguments[i].column)
        {
            new_arguments.emplace_back(nullptr, expected[i], arguments[i].name);
            continue;
        }

        auto arg = arguments[i].type;
        bool is_nullable = [&]() {
            if (arg->getTypeId() != TypeIndex::Nullable)
                return false;
            arg = typeid_cast<const DataTypeNullable *>(arg.get())->getNestedType();
            return true;
        }();
        if (isNothing(arg))
        {
            new_arguments.emplace_back(arguments[i]);
            continue;
        }

        if (isString(expected[i]) && !(isStringOrFixedString(arg) || isIPv6(arg)))
            convertAndAddToNewArguments(DataTypeString(), arguments[i], is_nullable);
        else if (arg_type != ArgType::NOT_DECIMAL && isFloat(expected[i]) && !isNumber(arg))
            convertAndAddToNewArguments(DataTypeFloat64(), arguments[i], is_nullable);
        else if (arg_type == ArgType::NOT_DECIMAL && isFloat(expected[i]) && !(isInteger(arg) || isFloat(arg)))
            convertAndAddToNewArguments(DataTypeFloat64(), arguments[i], is_nullable);
        else if (isUnsignedInteger(expected[i]) && !isInteger(arg))
            convertAndAddToNewArguments(DataTypeUInt64(), arguments[i], is_nullable);
        else if (isInteger(expected[i]) && !isInteger(arg))
            convertAndAddToNewArguments(DataTypeInt64(), arguments[i], is_nullable);
        else if (isDateTime64(expected[i]) && !(isDateTime64(arg) || isDateOrDate32(arg) || isDateTime(arg) || isTime(arg)))
            convertAndAddToNewArguments(DataTypeDateTime64(DataTypeDateTime64::default_scale), arguments[i], is_nullable);
        else if (isDateTime(expected[i]) && !isDateTime(arg))
            convertAndAddToNewArguments(DataTypeDateTime(), arguments[i], is_nullable);
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

    ColumnPtr col = argument.column;
    col = col->convertToFullColumnIfConst();

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
