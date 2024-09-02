#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}
/// return true if the nested data type are from the same category, e.g.,number, string, date.
/// nullable and lowcardinality check are skipped.
/// used by array functions like array_concat, array_union, array_intersect
inline bool areCompatibleArrays(const DataTypePtr left, const DataTypePtr right)
{
    const auto * left_array_type = typeid_cast<const DataTypeArray *>(removeNullable(left).get());
    const auto * right_array_type = typeid_cast<const DataTypeArray *>(removeNullable(right).get());
    if (!left_array_type || !right_array_type)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected two arrays, but got {} and {}.", left->getName(), right->getName());

    auto left_nested_type = removeLowCardinalityAndNullable(left_array_type->getNestedType());
    auto right_nested_type = removeLowCardinalityAndNullable(right_array_type->getNestedType());

    /// for cases like array(array(T1)) vs array(array(T2))
    if ((isArray(left_nested_type) && !isNothing(right_nested_type))
            || (isArray(right_nested_type) && !isNothing(left_nested_type)))
        return areCompatibleArrays(left_nested_type, right_nested_type);

    if (left_nested_type->equals(*right_nested_type))
        return true;
    else return (isNumber(left_nested_type) && isNumber(right_nested_type))
            || (isString(left_nested_type) && isString(right_nested_type))
            || (isFixedString(left_nested_type) && isFixedString(right_nested_type))
            || (isDateOrDateTime(left_nested_type) && isDateOrDateTime(right_nested_type))
            || isNothing(left_nested_type)
            || isNothing(right_nested_type);
}

/// type could be Nullable(Array) or Array
inline DataTypePtr extractNestedArrayType(const IDataType & type)
{
    const auto * array_type = checkAndGetDataType<DataTypeArray>(&type);
    const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(&type);
    if (nullable_type)
        array_type = checkAndGetDataType<DataTypeArray>(nullable_type->getNestedType().get());

    return removeLowCardinality(array_type->getNestedType());
}

/// convert the innermost data type from small int (float32) to big int (float64),
/// e.g., Array<int8> to Array<Int64>,
/// because we do not have implicit type convert to int8, int16, int32
static DataTypePtr convertToBigIntegerOrFloat(DataTypePtr type)
{
    if (type->isNullable())
    {
        auto inner = assert_cast<const DataTypeNullable *>(type.get())->getNestedType();
        return makeNullable(convertToBigIntegerOrFloat(inner));
    }
    else if (isArray(type))
    {
        auto inner = assert_cast<const DataTypeArray *>(type.get())->getNestedType();
        return std::make_shared<DataTypeArray>(convertToBigIntegerOrFloat(inner));
    }
    else if (type->lowCardinality())
    {
        auto inner = removeLowCardinality(type);
        return convertToBigIntegerOrFloat(inner);
    }
    else if (isSignedInteger(type))
    {
        return std::make_shared<DataTypeInt64>();
    }
    else if (isUnsignedInteger(type))
    {
        return std::make_shared<DataTypeUInt64>();
    }
    else if (isFloat(type))
        return std::make_shared<DataTypeFloat64>();
    else
        return type;
}

template<typename Type>
DataTypes getColumnsType(ArgType argType, const Type & arguments)
{
    size_t column_size = arguments.size();
    DataTypes expectedTypes;
    expectedTypes.reserve(column_size);
    switch (argType)
    {
        case ArgType::STRINGS:
            for (size_t i = 0; i < column_size; ++i)
                expectedTypes.push_back(std::make_shared<DataTypeString>());
            break;
        case ArgType::NOT_DECIMAL: [[fallthrough]];
        case ArgType::NUMBERS:
            for (size_t i = 0; i < column_size; ++i)
                expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
            break;
        case ArgType::UINTS:
            for (size_t i = 0; i < column_size; ++i)
                expectedTypes.push_back(std::make_shared<DataTypeUInt64>());
            break;
        case ArgType::DATES:
            for (size_t i = 0; i < column_size; ++i)
                expectedTypes.push_back(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale));
            break;
        case ArgType::DATE_OR_DATETIME:
            for (size_t i = 0; i < column_size; ++i)
                expectedTypes.push_back(std::make_shared<DataTypeDateTime>());
            break;
        case ArgType::NUM_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::NUM_NUM_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else if (i == 1) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::INT_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0) expectedTypes.push_back(std::make_shared<DataTypeInt64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::INTS:
            for (size_t i = 0; i < column_size; ++i)
                expectedTypes.push_back(std::make_shared<DataTypeInt64>());
            break;
        case ArgType::STR_NUM:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0) expectedTypes.push_back(std::make_shared<DataTypeString>());
                else expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
            }
            break;
        case ArgType::STR_NUM_NUM_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 1 || i == 2) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::STR_NUM_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 1) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::STR_UINT_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 1) expectedTypes.push_back(std::make_shared<DataTypeUInt64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::STR_STR_NUM:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 2) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::STR_STR_UINT:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 2) expectedTypes.push_back(std::make_shared<DataTypeUInt64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::SAME_TYPE:
            {
                if (arguments.size() != 2) break;
                DataTypePtr left;
                DataTypePtr right;
                if constexpr (std::is_same_v<Type, ColumnsWithTypeAndName>)
                {
                    left = arguments[0].type;
                    right = arguments[1].type;
                }
                else
                {
                    left = arguments[0];
                    right = arguments[1];
                }
                if (left->equals(*right)) break;
                else if ((isDateOrDate32(left) || isDateTime(left) || isDateTime64(left) || isTime(left))
                        || (isDateOrDate32(right) || isDateTime(right) || isDateTime64(right) || isTime(right)))
                {
                    expectedTypes = {std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale), std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale)};
                }
                else if (isNumber(left) || isNumber(right))
                {
                    expectedTypes = {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()};
                }
                else if (isEnum(left) || isEnum(right))
                    break;
                else
                {
                    auto common_type = getLeastSupertype(DataTypes{left, right});
                    if (isFloat(common_type))
                        common_type = std::make_shared<DataTypeFloat64>();
                    else if (isSignedInteger(common_type))
                        common_type = std::make_shared<DataTypeInt64>();
                    else if (isUnsignedInteger(common_type))
                        common_type = std::make_shared<DataTypeUInt64>();
                    expectedTypes = { common_type, common_type };
                }
            }
            break;
        case ArgType::DATE_NUM_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0) expectedTypes.push_back(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale));
                else if (i == 1) expectedTypes.push_back(std::make_shared<DataTypeFloat64>());
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::DATE_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0) expectedTypes.push_back(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale));
                else expectedTypes.push_back(std::make_shared<DataTypeString>());
            }
            break;
        case ArgType::STR_DATETIME_STR:
            for (size_t i = 0; i < column_size; ++i)
            {
                if (i == 0 || i == 3) expectedTypes.push_back(std::make_shared<DataTypeString>());
                else expectedTypes.push_back(std::make_shared<DataTypeDateTime>());
            }
            break;
        case ArgType::DATE_INTERVAL:
            {
                expectedTypes.push_back(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale));
                expectedTypes.push_back(std::make_shared<DataTypeInterval>(IntervalKind::Second));
            }
            break;
        case ArgType::INTERVAL_DATE:
            {
                expectedTypes.push_back(std::make_shared<DataTypeInterval>(IntervalKind::Second));
                expectedTypes.push_back(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale));
            }
            break;
        case ArgType::ARRAY_FIRST:
            {
                assert(column_size >= 2);
                DataTypePtr arr_type = nullptr;
                if constexpr (std::is_same_v<Type, ColumnsWithTypeAndName>)
                    arr_type = arguments[0].type;
                else
                    arr_type = arguments[0];
                if (!isArray(removeNullable(arr_type)))
                    break;
                expectedTypes.push_back(arr_type);
                auto array_nested_type = convertToBigIntegerOrFloat(removeNullable(extractNestedArrayType(*arr_type)));

                for (size_t i = 1; i < column_size; ++i)
                    expectedTypes.push_back(isFixedString(array_nested_type) ? std::make_shared<DataTypeString>() : array_nested_type);
            }
            break;
        case ArgType::ARRAY_COMMON:
            {
                assert(column_size == 2);
                DataTypePtr left;
                DataTypePtr right;
                if constexpr (std::is_same_v<Type, ColumnsWithTypeAndName>)
                {
                    left = arguments[0].type;
                    right = arguments[1].type;
                }
                else
                {
                    left = arguments[0];
                    right = arguments[1];
                }
                if (areCompatibleArrays(left,right)) break;

                DataTypePtr common = getLeastSupertype<LeastSupertypeOnError::String>(DataTypes{left,right}, true);
                common = convertToBigIntegerOrFloat(common);
                for (size_t i = 0; i < column_size; ++i)
                    expectedTypes.push_back(common);
            }
            break;
        case ArgType::UNDEFINED:
            break;
    }
    return expectedTypes;
}

class IFunctionMySql : public IFunction
{
public:
    explicit IFunctionMySql(std::unique_ptr<IFunction> function_)
                            : function(std::move(function_))
    {
        arg_type = function->getArgumentsType();
    }

    String getName() const override { return function->getName(); }

    bool isVariadic() const override { return function->isVariadic(); }
    size_t getNumberOfArguments() const override { return function->getNumberOfArguments(); }
    bool isInjective(const ColumnsWithTypeAndName & arguments) const override { return function->isInjective(arguments); }
    bool useDefaultImplementationForNulls() const override { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForNothing() const override { return function->useDefaultImplementationForNothing(); }
    bool useDefaultImplementationForConstants() const override { return function->useDefaultImplementationForConstants(); }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return function->useDefaultImplementationForLowCardinalityColumns(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return function->getArgumentsThatAreAlwaysConstant(); }
    bool canBeExecutedOnDefaultArguments() const override { return function->canBeExecutedOnDefaultArguments(); }
    bool hasInformationAboutMonotonicity() const override { return function->hasInformationAboutMonotonicity(); }
    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override { return function->getMonotonicityForRange(type, left, right);}
    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override { return function->isShortCircuit(settings, number_of_arguments); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override { return function->isSuitableForShortCircuitArgumentsExecution(arguments); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arg_type == ArgType::UNDEFINED)
            return function->getReturnTypeImpl(arguments);

        if (isVaildIntervalArithmetic<ColumnsWithTypeAndName>(arguments))
            return function->getReturnTypeImpl(arguments);

        if (isVaildArguments<ColumnsWithTypeAndName>(arguments))
            return function->getReturnTypeImpl(arguments);

        ColumnsWithTypeAndName new_arguments;
        checkAndConvert(arguments, new_arguments);

        return new_arguments.size() == 0 ? function->getReturnTypeImpl(arguments) : function->getReturnTypeImpl(new_arguments);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arg_type == ArgType::UNDEFINED)
            return function->getReturnTypeImpl(arguments);

        if (isVaildIntervalArithmetic<DataTypes>(arguments))
            return function->getReturnTypeImpl(arguments);

        if (isVaildArguments<DataTypes>(arguments))
            return function->getReturnTypeImpl(arguments);

        return function->getReturnTypeImpl(getColumnsType(arg_type, arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arg_type == ArgType::UNDEFINED)
            return function->executeImpl(arguments, result_type, input_rows_count);

        if (isVaildIntervalArithmetic<ColumnsWithTypeAndName>(arguments))
            return function->executeImpl(arguments, result_type, input_rows_count);

        if (isVaildArguments<ColumnsWithTypeAndName>(arguments))
            return function->executeImpl(arguments, result_type, input_rows_count);

        ColumnsWithTypeAndName new_arguments;
        checkAndConvert(arguments, new_arguments);

        if (new_arguments.size() == 0)
            return function->executeImpl(arguments, result_type, input_rows_count);

        return function->executeImpl(new_arguments, result_type, input_rows_count);
    }

    template<typename Type>
    static ColumnPtr convertToTypeStatic(const ColumnWithTypeAndName & argument, const DataTypePtr & result_type, size_t input_rows_count);

    template<typename Type>
    static ColumnPtr convertToTypeStatic(const ColumnsWithTypeAndName & argument, const DataTypePtr & result_type, size_t input_rows_count);

    /// convert arg like array(array(T)) to the expected toType e.g., nullable(array(array(T')))
    static ColumnWithTypeAndName recursiveConvertArrayArgument(DataTypePtr toType, const ColumnWithTypeAndName& argument);
private:
    std::unique_ptr<IFunction> function;
    mutable ArgType arg_type = ArgType::UNDEFINED;

    template<typename Type>
    bool isVaildArguments(const Type & arguments) const;

    template<typename Type>
    bool isVaildIntervalArithmetic(const Type & arguments) const;

    void checkAndConvert(const ColumnsWithTypeAndName & arguments, ColumnsWithTypeAndName & new_arguments) const;
};
}
