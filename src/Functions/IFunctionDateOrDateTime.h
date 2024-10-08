/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/TransformDateTime64.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Transform>
class IFunctionDateOrDateTime : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        if constexpr (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
            return Monotonicity{.is_monotonic = true, .is_positive = true, .is_always_monotonic = true};

        const IFunction::Monotonicity is_monotonic{.is_monotonic = true, .is_positive = true, .is_always_monotonic = false};
        const IFunction::Monotonicity is_not_monotonic;

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
        else if (checkAndGetDataType<DataTypeDate32>(&type))
        {
            return Transform::FactorTransform::execute(Int32(left.get<UInt64>()), *date_lut)
                   == Transform::FactorTransform::execute(Int32(right.get<UInt64>()), *date_lut)
                   ? is_monotonic : is_not_monotonic;
        }
        else
        {
            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), *date_lut)
                   == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), *date_lut)
                   ? is_monotonic : is_not_monotonic;
        }
    }

protected:
    void checkArguments(const ColumnsWithTypeAndName & arguments, bool is_result_type_date_or_date32, ContextPtr context = nullptr) const
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
        {
            if (arguments.size() != 1 && arguments.size() != 2)
                throw Exception(
                        "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 1 or 2",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            return;
        }

        if (arguments.size() == 1)
        {
            if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type)
                && !isTime(arguments[0].type) && !isString(arguments[0].type))
                throw Exception(
                        "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be Date, Date32, DateTime, DateTime64, Time or String",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if (arguments.size() == 2)
        {
            if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type)
            && !isTime(arguments[0].type) && !isString(arguments[0].type))
                throw Exception(
                        "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be Date, Date32, DateTime, DateTime64, Time or String",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!isString(arguments[1].type))
                throw Exception(
                        "Function " + getName() + " supports 1 or 2 arguments. The optional 2nd argument must be "
                                                  "a constant string with a timezone name",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if ((isDate(arguments[0].type) || isDate32(arguments[0].type)) && is_result_type_date_or_date32)
                throw Exception(
                        "The timezone argument of function " + getName() + " is allowed only when the 1st argument has the type DateTime or DateTime64",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 1 or 2",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
};

}
