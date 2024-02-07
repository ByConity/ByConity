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
#include <Functions/IFunctionDateOrDateTime.h>
#include <Functions/IFunctionMySql.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Transform>
class FunctionDateOrDateTimeToDateOrDate32 : public IFunctionDateOrDateTime<Transform>
{
private:
    const bool enable_extended_results_for_datetime_functions = false;
    ContextPtr context;

public:
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionDateOrDateTimeToDateOrDate32>(context_);
    }

    explicit FunctionDateOrDateTimeToDateOrDate32(ContextPtr context_)
        : enable_extended_results_for_datetime_functions(context_->getSettingsRef().enable_extended_results_for_datetime_functions)
        , context(context_)
    {
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, /*is_result_type_date_or_date32*/ true, context);

        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        /// If the time zone is specified but empty, throw an exception.
        /// only validate the time_zone part if the number of arguments is 2.
        if ((which.isDateTime() || which.isDateTime64()) && arguments.size() == 2
            && extractTimeZoneNameFromFunctionArguments(arguments, 1, 0).empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                this->getName());

        if ((which.isDate32() || which.isDateTime64()) && enable_extended_results_for_datetime_functions)
            return std::make_shared<DataTypeDate32>();
        else
            return std::make_shared<DataTypeDate>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDate32())
        {
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDate32, DataTypeDate32, Transform, /*is_extended_result*/ true>::execute(arguments, result_type, input_rows_count);
            else
                return DateTimeTransformImpl<DataTypeDate32, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        }
        else if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime64() || which.isString())
        {
            const UInt32 scale = which.isString() ? 0 : static_cast<const DataTypeDateTime64 *>(from_type)->getScale();

            const TransformDateTime64<Transform> transformer(scale);
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, decltype(transformer), /*is_extended_result*/ true>::execute(arguments, result_type, input_rows_count, transformer);
            else
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate, decltype(transformer)>::execute(arguments, result_type, input_rows_count, transformer);
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
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, decltype(transformer), /*is_extended_result*/ true>::execute(temp_args, result_type, input_rows_count, transformer);
            else
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate, decltype(transformer)>::execute(temp_args, result_type, input_rows_count, transformer);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}",
                    arguments[0].type->getName(), this->getName());
    }

};

}
