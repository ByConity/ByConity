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

#include <iostream>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/TransformDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunctionMySql.h>
#include <Common/Exception.h>
#include <Common/DateLUTImpl.h>
#include <common/types.h>

/// The default mode value to use for the WEEK() function
#define DEFAULT_WEEK_MODE 0
#define DEFAULT_WEEK_MODE_MYSQL 3
#define DEFAULT_DAY_WEEK_MODE_MYSQL 1


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/**
 * CustomWeek Transformations.
  */

struct ToYearWeekImpl
{
    static constexpr auto name = "toYearWeek";

    static inline UInt32 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        // TODO: ditch toDayNum()
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }

    static inline UInt32 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }
    static inline UInt32 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(ExtendedDayNum (d), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }
    static inline UInt32 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfWeekImpl
{
    static constexpr auto name = "toStartOfWeek";

    static inline UInt16 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        if (t < 0)
            return 0;

        return time_zone.toFirstDayNumOfWeek(DayNum(std::min(Int32(time_zone.toDayNum(t)), Int32(DATE_LUT_MAX_DAY_NUM))), week_mode);
    }
    static inline UInt16 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static inline UInt16 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        if (d < 0)
            return 0;

        return time_zone.toFirstDayNumOfWeek(DayNum(std::min(d, Int32(DATE_LUT_MAX_DAY_NUM))), week_mode);
    }
    static inline UInt16 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d), week_mode);
    }
    static inline Int64 executeExtendedResult(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static inline Int32 executeExtendedResult(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d), week_mode);
    }

    using FactorTransform = ZeroTransform;
};

struct ToWeekImpl
{
    static constexpr auto name = "toWeek";

    static inline UInt8 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        // TODO: ditch conversion to DayNum, since it doesn't support extended range.
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(ExtendedDayNum(d), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode);
        return yw.second;
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToWeekOfYearImpl
{
    static constexpr auto name = "toWeekOfYear";

    static inline UInt8 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        // TODO: ditch conversion to DayNum, since it doesn't support extended range.
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(ExtendedDayNum(d), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode);
        return yw.second;
    }

    using FactorTransform = ToStartOfYearImpl;
};

template <typename FromType, typename ToType, typename Transform, bool is_extended_result = false>
struct WeekTransformer
{
    explicit WeekTransformer(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FromVectorType, typename ToVectorType>
    void
    vector(const FromVectorType & vec_from, ToVectorType & vec_to, UInt8 week_mode, const DateLUTImpl & time_zone) const
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            if constexpr (is_extended_result)
                vec_to[i] = transform.executeExtendedResult(vec_from[i], week_mode, time_zone);
            else
                vec_to[i] = transform.execute(vec_from[i], week_mode, time_zone);
    }

private:
    const Transform transform;
};


template <typename FromDataType, typename ToDataType, bool is_extended_result = false>
struct CustomWeekTransformImpl
{
    template <typename Transform>
    static ColumnPtr execute(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr &,
        size_t /* input_rows_count */,
        Transform transform = {},
        bool mysql_mode_ = false)
    {
        const auto op = WeekTransformer<typename FromDataType::FieldType, typename ToDataType::FieldType, Transform, is_extended_result>{std::move(transform)};

        UInt8 week_mode =  DEFAULT_WEEK_MODE;
        if (mysql_mode_)
        {
            if constexpr(std::is_same_v<Transform, ToDayOfWeekMySQLImpl> || std::is_same_v<Transform, TransformDateTime64<ToDayOfWeekMySQLImpl>>)
                // Weekday in mysql uses mode 1
                week_mode = DEFAULT_DAY_WEEK_MODE_MYSQL;
            else if constexpr(std::is_same_v<Transform, ToWeekImpl> || 
                            std::is_same_v<Transform, TransformDateTime64<ToWeekImpl>> || 
                            std::is_same_v<Transform, ToYearWeekImpl> || 
                            std::is_same_v<Transform, TransformDateTime64<ToYearWeekImpl>>)
                // toWeek and toYearWeek in mysql uses mode 0
                week_mode = DEFAULT_WEEK_MODE;
            else
                // By default mysql uses mode 3
                week_mode = DEFAULT_WEEK_MODE_MYSQL;
        }
        
        if constexpr(std::is_same_v<Transform, ToWeekOfYearImpl> || std::is_same_v<Transform, TransformDateTime64<ToWeekOfYearImpl>>)
            // toWeekOfYear uses mode 3 by default
            week_mode = DEFAULT_WEEK_MODE_MYSQL;

        size_t timezone_index = 2;
        if (arguments.size() > 1)
        {
            if (const auto * week_mode_column = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
                week_mode = week_mode_column->getValue<UInt8>();
            else if (const auto * timezone_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
                // for backward compatibility - try to get 2nd column as timezone
                timezone_index = 1;
        }

        // for backward compatibility - try to get 2nd column as timezone
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, timezone_index, 0);
        const ColumnPtr source_col = arguments[0].column;
        if (const auto * sources = checkAndGetColumn<typename FromDataType::ColumnType>(source_col.get()))
        {
            auto col_to = ToDataType::ColumnType::create();
            op.vector(sources->getData(), col_to->getData(), week_mode, time_zone);
            return col_to;
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), Transform::name);
        }
    }
};

}
