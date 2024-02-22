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
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{
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
                    expectedTypes = {std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale), std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale)};
                else
                    expectedTypes = {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()};
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
        case ArgType::UNDEFINED:
            break;
    }
    return expectedTypes;
}
};

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
