/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once
#include <Functions/IFunctionDateOrDateTime.h>
#include <Functions/TransformTime.h>
#include <DataTypes/DataTypeTime.h>
#include <Functions/IFunctionMySql.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// See DateTimeTransforms.h
template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunctionDateOrDateTime<Transform>
{
public:
    FunctionDateOrDateTimeToSomething(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionDateOrDateTimeToSomething>(context_); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>), context);

        /// For DateTime, if time zone is specified, attach it to type.
        /// If the time zone is specified but empty, throw an exception.
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            std::string time_zone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
            /// only validate the time_zone part if the number of arguments is 2. This is mainly
            /// to accommodate functions like toStartOfDay(today()), toStartOfDay(yesterday()) etc.
            if (arguments.size() == 2 && time_zone.empty())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                    this->getName());
            return std::make_shared<ToDataType>(time_zone);
        }
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            Int64 scale = DataTypeDateTime64::default_scale;
            if (const auto * dt64 =  checkAndGetDataType<DataTypeDateTime64>(arguments[0].type.get()))
                scale = dt64->getScale();

            auto source_scale = scale;
            if constexpr (std::is_same_v<ToStartOfMillisecondImpl, Transform>)
            {
                scale = std::max(source_scale, static_cast<Int64>(3));
            }
            else if constexpr (std::is_same_v<ToStartOfMicrosecondImpl, Transform>)
            {
                scale = std::max(source_scale, static_cast<Int64>(6));
            }
            else if constexpr (std::is_same_v<ToStartOfNanosecondImpl, Transform>)
            {
                scale = std::max(source_scale, static_cast<Int64>(9));
            }

            return std::make_shared<ToDataType>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
        }
        else
            return std::make_shared<ToDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if constexpr (std::is_same_v<Transform, ToHourImpl> || std::is_same_v<Transform, ToMinuteImpl> || std::is_same_v<Transform, ToSecondImpl>)
        {
            if (which.isDateOrDate32())
            {
                ColumnsWithTypeAndName temp_args;
                convertToDateTime(arguments, &temp_args, input_rows_count);
                return DateTimeTransformImpl<DataTypeDateTime, ToDataType, Transform>::execute(temp_args, result_type, input_rows_count);
            }
            if (which.isStringOrFixedString())
            {
                ColumnsWithTypeAndName temp_args;
                auto to_time = FunctionFactory::instance().get("toTimeType", context);
                auto to_time_col = to_time->build(arguments)->execute(arguments, std::make_shared<DataTypeTime>(3), input_rows_count);
                temp_args.emplace_back(std::move(to_time_col), std::make_shared<DataTypeTime>(3), "time");
                const TransformTime<Transform> transformer(3);
                return DateTimeTransformImpl<DataTypeTime, ToDataType, decltype(transformer)>::execute(temp_args, result_type, input_rows_count, transformer);
            }
        }
        else if constexpr (
            std::is_same_v<Transform, ToYearImpl> || std::is_same_v<Transform, ToQuarterImpl> || std::is_same_v<Transform, ToMonthImpl>
            || std::is_same_v<Transform, ToDayOfMonthImpl>)
        {
            if (which.isTime())
            {
                ColumnsWithTypeAndName temp_args;
                convertToDateTime(arguments, &temp_args, input_rows_count);
                return DateTimeTransformImpl<DataTypeDateTime, ToDataType, Transform>::execute(temp_args, result_type, input_rows_count);
            }
        }

        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDate32())
            return DateTimeTransformImpl<DataTypeDate32, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime64() || which.isString())
        {
            const UInt32 scale = which.isString() ? 0 : static_cast<const DataTypeDateTime64 *>(from_type)->getScale();
            const TransformDateTime64<Transform> transformer(scale);
            return DateTimeTransformImpl<DataTypeDateTime64, ToDataType, decltype(transformer)>::execute(arguments, result_type, input_rows_count, transformer);
        }
        else if (which.isTime())
        {
            const auto scale = static_cast<const DataTypeTime *>(from_type)->getScale();
            const TransformTime<Transform> transformer(scale);
            return DateTimeTransformImpl<DataTypeTime, ToDataType, decltype(transformer)>::execute(arguments, result_type, input_rows_count, transformer);
        }
        else if (context->getSettingsRef().enable_implicit_arg_type_convert && which.isNumber())
        {
            const UInt32 scale = 3;
            const TransformDateTime64<Transform> transformer(scale);
            ColumnsWithTypeAndName temp_args;
            auto col = IFunctionMySql::convertToTypeStatic<DataTypeDateTime64>(arguments[0], std::make_shared<DataTypeDateTime64>(scale), input_rows_count);
            temp_args.emplace_back(std::move(col), std::make_shared<DataTypeDateTime64>(scale), arguments[0].name);
            if (arguments.size() == 2)
                temp_args.emplace_back(arguments[1]);
            return DateTimeTransformImpl<DataTypeDateTime64, ToDataType, decltype(transformer)>::execute(temp_args, result_type, input_rows_count, transformer);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0].type->getName(), this->getName());
    }

    inline void convertToDateTime(const ColumnsWithTypeAndName & arguments, ColumnsWithTypeAndName * result, size_t input_rows_count) const
    {
        auto to_func = FunctionFactory::instance().get("toDateTime", context);
        auto to_col = to_func->build(arguments)->execute(arguments, std::make_shared<DataTypeDateTime>(), input_rows_count);
        result->emplace_back(std::move(to_col), std::make_shared<DataTypeDateTime>(), "datetime");
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    IFunction::Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        IFunction::Monotonicity is_monotonic { true };
        IFunction::Monotonicity is_not_monotonic;

        if (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
        {
            is_monotonic.is_always_monotonic = true;
            return is_monotonic;
        }

        /// This method is called only if the function has one argument. Therefore, we do not care about the non-local time zone.
        const DateLUTImpl * date_lut = &DateLUT::sessionInstance();
        if (const auto * timezone = dynamic_cast<const TimezoneMixin *>(&type))
            date_lut = &timezone->getTimeZone();
        if (left.isNull() || right.isNull())
            return is_not_monotonic;

        /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.

        if (checkAndGetDataType<DataTypeDate>(&type))
        {
            return Transform::FactorTransform::execute(UInt16(left.get<UInt64>()), *date_lut)
                == Transform::FactorTransform::execute(UInt16(right.get<UInt64>()), *date_lut)
                ? is_monotonic : is_not_monotonic;
        }
        else
        {
            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), *date_lut)
                == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), *date_lut)
                ? is_monotonic : is_not_monotonic;
        }
    }

    private:
        ContextPtr context;

};

}
