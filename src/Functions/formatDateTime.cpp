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

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Functions/TransformDateTime64.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <Core/DecimalFunctions.h>
#include <IO/WriteHelpers.h>

#include <iostream>
#include <type_traits>
#include <IO/WriteHelpers.h>
#include <common/DateLUTImpl.h>
#include <common/find_symbols.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_PARAMETER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using Pos = const char *;

    template <class T>
    concept Integral = std::is_integral_v<T>;

    template <typename DataType>
    struct InstructionValueTypeMap
    {
    };
    template <>
    struct InstructionValueTypeMap<DataTypeInt8>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeUInt8>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeInt16>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeUInt16>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeInt32>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeUInt32>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeInt64>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeUInt64>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeDate>
    {
        using InstructionValueType = UInt16;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeDate32>
    {
        using InstructionValueType = Int32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeDateTime>
    {
        using InstructionValueType = UInt32;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeDateTime64>
    {
        using InstructionValueType = Int64;
    };
    template <>
    struct InstructionValueTypeMap<DataTypeTime>
    {
        using InstructionValueType = Decimal64;
    };

    struct NameFormatDateTime
    {
        static constexpr auto name = "formatDateTime";
    };

    struct NameFromUnixTime
    {
        static constexpr auto name = "FROM_UNIXTIME";
    };

    struct NameFromUnixTimeAdaptive
    {
        static constexpr auto name = "FROM_UNIXTIME_ADAPTIVE";
    };

    struct NameDateFormat
    {
        static constexpr auto name = "DATE_FORMAT_MYSQL";
    };


    struct NameTimeFormat
    {
        static constexpr auto name = "TIME_FORMAT";
    };

    /// Cast value from integer to string, making sure digits number in result string is no less than total_digits by padding leading '0'.
    [[maybe_unused]] String padValue(UInt32 val, size_t min_digits)
    {
        String str = std::to_string(val);
        auto length = str.size();
        if (length >= min_digits)
            return str;

        String paddings(min_digits - length, '0');
        return str.insert(0, paddings);
    }

    constexpr std::string_view weekdaysFull[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

    constexpr std::string_view weekdaysShort[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

    constexpr std::string_view monthsFull[]
        = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

    constexpr std::string_view monthsShort[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    /** formatDateTime(time, 'pattern')
  * Performs formatting of time, according to provided pattern.
  *
  * This function is optimized with an assumption, that the resulting strings are fixed width.
  * (This assumption is fulfilled for currently supported formatting options).
  *
  * It is implemented in two steps.
  * At first step, it creates a pattern of zeros, literal characters, whitespaces, etc.
  *  and quickly fills resulting character array (string column) with this pattern.
  * At second step, it walks across the resulting character array and modifies/replaces specific characters,
  *  by calling some functions by pointers and shifting cursor by specified amount.
  *
  * Advantages:
  * - memcpy is mostly unrolled;
  * - low number of arithmetic ops due to pre-filled pattern;
  * - for somewhat reason, function by pointer call is faster than switch/case.
  *
  * Possible further optimization options:
  * - slightly interleave first and second step for better cache locality
  *   (but it has no sense when character array fits in L1d cache);
  * - avoid indirect function calls and inline functions with JIT compilation.
  *
  * Performance on Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz:
  *
  * WITH formatDateTime(now() + number, '%H:%M:%S') AS x SELECT count() FROM system.numbers WHERE NOT ignore(x);
  * - 97 million rows per second per core;
  *
  * WITH formatDateTime(toDateTime('2018-01-01 00:00:00') + number, '%F %T') AS x SELECT count() FROM system.numbers WHERE NOT ignore(x)
  * - 71 million rows per second per core;
  *
  * select count() from (select formatDateTime(t, '%m/%d/%Y %H:%M:%S') from (select toDateTime('2018-01-01 00:00:00')+number as t from numbers(100000000)));
  * - 53 million rows per second per core;
  *
  * select count() from (select formatDateTime(t, 'Hello %Y World') from (select toDateTime('2018-01-01 00:00:00')+number as t from numbers(100000000)));
  * - 138 million rows per second per core;
  *
  * PS. We can make this function to return FixedString. Currently it returns String.
  */
    template <typename Name, bool support_integer, bool forceAdaptive = false>
    class FunctionFormatDateTimeImpl : public IFunction
    {
    protected:
        /// Time is either UInt32 for DateTime or UInt16 for Date.
        template <typename F>
        static bool castType(const IDataType * type, F && f)
        {
            return castTypeToEither<
                DataTypeInt8,
                DataTypeUInt8,
                DataTypeInt16,
                DataTypeUInt16,
                DataTypeInt32,
                DataTypeUInt32,
                DataTypeInt64,
                DataTypeUInt64>(type, std::forward<F>(f));
        }

        template <typename Time>
        class Instruction
        {
        public:
            using FuncMysql = size_t (Instruction<Time>::*)(char *, Time, UInt64, UInt32, const DateLUTImpl &);
            FuncMysql func_mysql = nullptr;

            // Func func;
            /// extra_shift is only used in MySQL format syntax. It is always 0 in Joda format syntax.
            size_t extra_shift = 0;

            // Holds literal characters that will be copied into the output. Used by the mysqlLiteral instruction.
            String literal;

            bool mysql_with_only_fixed_length_formatters;

            Instruction() = default;

            void setMysqlFunc(FuncMysql && func) { func_mysql = std::move(func); }
            void setLiteral(std::string_view literal_) { literal = literal_; }
            void setFixedFormat(bool mysql_with_only_fixed_length_formatters_)
            {
                mysql_with_only_fixed_length_formatters = mysql_with_only_fixed_length_formatters_;
            }

            void perform(char *& dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
            {
                size_t shift = std::invoke(func_mysql, this, dest, source, fractional_second, scale, timezone);
                dest += shift + extra_shift;
            }

        private:
            inline static size_t writeChar1(char * p, char v)
            {
                memcpy(p, &v, 1);
                return 1;
            }

            template <typename T>
            inline static size_t writeNumber2(char * p, T v)
            {
                memcpy(p, &digits100[v * 2], 2);
                return 2;
            }

            template <typename T>
            inline static size_t writeNumber3(char * p, T v)
            {
                writeNumber2(p, v / 10);
                p[2] = '0' + v % 10;
                return 3;
            }

            template <typename T>
            inline static size_t writeNumber4(char * p, T v)
            {
                writeNumber2(p, v / 100);
                writeNumber2(p + 2, v % 100);
                return 4;
            }

            /// Cast content from integer to string, and append result string to buffer.
            /// Make sure digits number in result string is no less than total_digits by padding leading '0'
            /// Notice: '-' is not counted as digit.
            /// For example:
            /// val = -123, total_digits = 2 => dest = "-123"
            /// val = -123, total_digits = 3 => dest = "-123"
            /// val = -123, total_digits = 4 => dest = "-0123"
            template <typename T>
            std::enable_if_t<Integral<T>, size_t> writeNumberWithPadding(char * dest, T val, size_t min_digits)
            {
                using WeightType = typename NumberTraits::Construct<is_signed_v<T>, /*is_floating*/ false, sizeof(T)>::Type;
                WeightType w = 1;
                WeightType n = val;
                size_t digits = 0;
                while (n)
                {
                    w *= 10;
                    n /= 10;
                    ++digits;
                }

                /// Possible sign
                size_t pos = 0;
                n = val;
                if constexpr (is_signed_v<T>)
                    if (val < 0)
                    {
                        n = (~n) + 1;
                        dest[pos] = '-';
                        ++pos;
                    }

                /// Possible leading paddings
                if (min_digits > digits)
                {
                    memset(dest, '0', min_digits - digits);
                    pos += min_digits - digits;
                }

                /// Digits
                while (w >= 100)
                {
                    w /= 100;

                    writeNumber2(dest + pos, n / w);
                    pos += 2;

                    n = n % w;
                }
                if (n)
                {
                    dest[pos] = '0' + n;
                    ++pos;
                }

                return pos;
            }

        public:
            size_t mysqlNoop(char *, Time, UInt64, UInt32, const DateLUTImpl &) { return 0; }

            size_t mysqlLiteral(char * dest, Time, UInt64, UInt32, const DateLUTImpl &)
            {
                memcpy(dest, literal.data(), literal.size());
                return literal.size();
            }

            size_t mysqlCentury(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                auto year = ToYearImpl::execute(source, timezone);
                auto century = year / 100;
                return writeNumber2(dest, century);
            }

            size_t mysqlDayOfMonth(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToDayOfMonthImpl::execute(source, timezone));
            }

            size_t mysqlAmericanDate(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                writeNumber2(dest, ToMonthImpl::execute(source, timezone));
                writeNumber2(dest + 3, ToDayOfMonthImpl::execute(source, timezone));
                writeNumber2(dest + 6, ToYearImpl::execute(source, timezone) % 100);
                if (!mysql_with_only_fixed_length_formatters)
                {
                    writeChar1(dest + 2, '/');
                    writeChar1(dest + 5, '/');
                }

                return 8;
            }

            size_t mysqlDayOfMonthSpacePadded(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                auto day = ToDayOfMonthImpl::execute(source, timezone);
                if (day < 10)
                    dest[1] = '0' + day;
                else
                    writeNumber2(dest, day);
                return 2;
            }

            size_t mysqlISO8601Date(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                writeNumber4(dest, ToYearImpl::execute(source, timezone));
                writeNumber2(dest + 5, ToMonthImpl::execute(source, timezone));
                writeNumber2(dest + 8, ToDayOfMonthImpl::execute(source, timezone));
                if (!mysql_with_only_fixed_length_formatters)
                {
                    writeChar1(dest + 4, '-');
                    writeChar1(dest + 7, '-');
                }
                return 10;
            }

            size_t mysqlDayOfYear(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber3(dest, ToDayOfYearImpl::execute(source, timezone));
            }

            size_t mysqlMonth(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToMonthImpl::execute(source, timezone));
            }

            static size_t monthOfYearText(char * dest, Time source, bool abbreviate, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                auto month = ToMonthImpl::execute(source, timezone);
                std::string_view str_view = abbreviate ? monthsShort[month - 1] : monthsFull[month - 1];
                memcpy(dest, str_view.data(), str_view.size());
                return str_view.size();
            }

            size_t mysqlMonthOfYearTextShort(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
            {
                return monthOfYearText(dest, source, true, fractional_second, scale, timezone);
            }

            size_t mysqlMonthOfYearTextLong(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
            {
                return monthOfYearText(dest, source, false, fractional_second, scale, timezone);
            }

            size_t mysqlDayOfWeek(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                *dest = '0' + ToDayOfWeekImpl::execute(source, 0, timezone);
                return 1;
            }

            static size_t dayOfWeekText(char * dest, Time source, bool abbreviate, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                auto week_day = ToDayOfWeekImpl::execute(source, 0, timezone);
                if (week_day == 7)
                    week_day = 0;

                std::string_view str_view = abbreviate ? weekdaysShort[week_day] : weekdaysFull[week_day];
                memcpy(dest, str_view.data(), str_view.size());
                return str_view.size();
            }

            size_t mysqlDayOfWeekTextShort(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
            {
                return dayOfWeekText(dest, source, true, fractional_second, scale, timezone);
            }

            size_t mysqlDayOfWeekTextLong(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
            {
                return dayOfWeekText(dest, source, false, fractional_second, scale, timezone);
            }

            size_t mysqlDayOfWeek0To6(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                auto day = ToDayOfWeekImpl::execute(source, 0, timezone);
                *dest = '0' + (day == 7 ? 0 : day);
                return 1;
            }

            size_t mysqlISO8601Week(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToISOWeekImpl::execute(source, timezone));
            }

            size_t mysqlISO8601Year2(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToISOYearImpl::execute(source, timezone) % 100);
            }

            size_t mysqlISO8601Year4(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber4(dest, ToISOYearImpl::execute(source, timezone));
            }

            size_t mysqlYear2(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToYearImpl::execute(source, timezone) % 100);
            }

            size_t mysqlYear4(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber4(dest, ToYearImpl::execute(source, timezone));
            }

            size_t mysqlHour24(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                    return writeNumber2(dest, ToHourImpl::executeTime(source, timezone));
                else
                    return writeNumber2(dest, ToHourImpl::execute(source, timezone));
            }

            size_t mysqlHour12(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                UInt8 x;
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                    x = ToHourImpl::executeTime(source, timezone);
                else
                    x = ToHourImpl::execute(source, timezone);
                return writeNumber2(dest, x == 0 ? 12 : (x > 12 ? x - 12 : x));
            }

            size_t mysqlMinute(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                    return writeNumber2(dest, ToMinuteImpl::executeTime(source, timezone));
                else
                    return writeNumber2(dest, ToMinuteImpl::execute(source, timezone));
            }

            static size_t AMPM(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
            {
                UInt8 hour;
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                    hour = ToHourImpl::executeTime(source, timezone);
                else
                    hour = ToHourImpl::execute(source, timezone);
                dest[0] = hour >= 12 ? 'P' : 'A';
                dest[1] = 'M';
                return 2;
            }

            size_t mysqlAMPM(char * dest, Time source, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & timezone)
            {
                return AMPM(dest, source, fractional_second, scale, timezone);
            }

            size_t mysqlHHMM24(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                {
                    writeNumber2(dest, ToHourImpl::executeTime(source, timezone));
                    writeNumber2(dest + 3, ToMinuteImpl::executeTime(source, timezone));
                }
                else
                {
                    writeNumber2(dest, ToHourImpl::execute(source, timezone));
                    writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));
                }

                if (!mysql_with_only_fixed_length_formatters)
                {
                    writeChar1(dest + 2, ':');
                }

                return 5;
            }

            size_t mysqlHHMM12(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                UInt8 hour;
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                {
                    hour = ToHourImpl::executeTime(source, timezone);
                    writeNumber2(dest, hour == 0 ? 12 : (hour > 12 ? hour - 12 : hour));
                    writeNumber2(dest + 3, ToMinuteImpl::executeTime(source, timezone));
                }
                else
                {
                    hour = ToHourImpl::execute(source, timezone);
                    writeNumber2(dest, hour == 0 ? 12 : (hour > 12 ? hour - 12 : hour));
                    writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));
                }
                dest[6] = hour >= 12 ? 'P' : 'A';

                if (!mysql_with_only_fixed_length_formatters)
                {
                    writeChar1(dest + 2, ':');
                }

                return 8;
            }

            size_t mysqlSecond(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                    return writeNumber2(dest, ToSecondImpl::executeTime(source, timezone));
                else
                    return writeNumber2(dest, ToSecondImpl::execute(source, timezone));
            }

            size_t
            mysqlFractionalSecond(char * dest, Time /*source*/, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & /*timezone*/)
            {
                if (scale == 0)
                    scale = 6;

                if (!mysql_with_only_fixed_length_formatters)
                {
                    for (Int64 i = 0; i < scale; i++)
                    {
                        writeChar1(dest + i, '0');
                    }
                }

                for (Int64 i = scale, value = fractional_second; i > 0; --i)
                {
                    dest[i - 1] += value % 10;
                    value /= 10;
                }
                return scale;
            }

            /// Same as mysqlFractionalSecond but prints a single zero if the value has no fractional seconds
            size_t mysqlFractionalSecondSingleZero(
                char * dest, Time /*source*/, UInt64 fractional_second, UInt32 scale, const DateLUTImpl & /*timezone*/)
            {
                if (scale == 0)
                    scale = 1;

                if (!mysql_with_only_fixed_length_formatters)
                {
                    for (Int64 i = 0; i < scale; i++)
                    {
                        writeChar1(dest + i, '0');
                    }
                }

                for (Int64 i = scale, value = fractional_second; i > 0; --i)
                {
                    dest[i - 1] += value % 10;
                    value /= 10;
                }
                return scale;
            }

            size_t mysqlISO8601Time(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone) // NOLINT
            {
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                {
                    writeNumber2(dest, ToHourImpl::executeTime(source, timezone));
                    writeNumber2(dest + 3, ToMinuteImpl::executeTime(source, timezone));
                    writeNumber2(dest + 6, ToSecondImpl::executeTime(source, timezone));
                }
                else
                {
                    writeNumber2(dest, ToHourImpl::execute(source, timezone));
                    writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));
                    writeNumber2(dest + 6, ToSecondImpl::execute(source, timezone));
                }

                if (!mysql_with_only_fixed_length_formatters)
                {
                    writeChar1(dest + 2, ':');
                    writeChar1(dest + 5, ':');
                }

                return 8;
            }

            size_t mysqlTimezoneOffset(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                auto offset = TimezoneOffsetImpl::execute(source, timezone);

                if (!mysql_with_only_fixed_length_formatters)
                {
                    *dest = '+';
                }

                if (offset < 0)
                {
                    *dest = '-';
                    offset = -offset;
                }

                writeNumber2(dest + 1, offset / 3600);
                writeNumber2(dest + 3, offset % 3600 / 60);
                return 5;
            }

            size_t mysqlQuarter(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                *dest = '0' + ToQuarterImpl::execute(source, timezone);
                return 1;
            }

            /// Extra actions to keep align with MySQL (ANSI SQL)
            size_t mysqlhhmmss12AMPM(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                UInt8 hh;
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                {
                    hh = ToHourImpl::executeTime(source, timezone);
                    writeNumber2(dest, hh == 0 ? 12 : (hh > 12 ? hh - 12 : hh));
                    writeNumber2(dest + 3, ToMinuteImpl::executeTime(source, timezone));
                    writeNumber2(dest + 6, ToSecondImpl::executeTime(source, timezone));
                }
                else
                {
                    hh = ToHourImpl::execute(source, timezone);
                    writeNumber2(dest, hh == 0 ? 12 : (hh > 12 ? hh - 12 : hh));
                    writeNumber2(dest + 3, ToMinuteImpl::execute(source, timezone));
                    writeNumber2(dest + 6, ToSecondImpl::execute(source, timezone));
                }
                if (hh >= 12)
                    *(dest + 9) = 'P';

                if (!mysql_with_only_fixed_length_formatters)
                {
                    writeChar1(dest + 2, ':');
                    writeChar1(dest + 5, ':');
                }

                return 11;
            }

            size_t weekMode0(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToWeekImpl::execute(source, 0, timezone));
            }

            size_t weekMode1(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToWeekImpl::execute(source, 1, timezone));
            }

            size_t weekMode2(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToWeekImpl::execute(source, 2, timezone));
            }

            size_t weekMode3(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                return writeNumber2(dest, ToWeekImpl::execute(source, 3, timezone));
            }

            size_t yearForFirstDayOfWeekSunday(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                const auto year = ToISOYearImpl::execute(ToStartOfWeekImpl::execute(source, 2, timezone), timezone);
                return writeNumber4(dest, year);
            }

            size_t yearForFirstDayOfWeekMonday(char * dest, Time source, UInt64, UInt32, const DateLUTImpl & timezone)
            {
                const auto year = ToISOYearImpl::execute(ToStartOfWeekImpl::execute(source, 3, timezone), timezone);
                return writeNumber4(dest, year);
            }
        };

        [[noreturn]] static void throwLastCharacterIsPercentException()
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'%' must not be the last character in the format string, use '%%' instead");
        }

        static bool containsOnlyFixedWidthMySQLFormatters(std::string_view format, bool mysql_M_is_month_name)
        {
            static constexpr std::array variable_width_formatter = {'W'};
            static constexpr std::array variable_width_formatter_M_is_month_name = {'W', 'M'};

            for (size_t i = 0; i < format.size(); ++i)
            {
                switch (format[i])
                {
                    case '%':
                        if (i + 1 >= format.size())
                            throwLastCharacterIsPercentException();
                        if (mysql_M_is_month_name)
                        {
                            if (std::any_of(
                                    variable_width_formatter_M_is_month_name.begin(),
                                    variable_width_formatter_M_is_month_name.end(),
                                    [&](char c) { return c == format[i + 1]; }))
                                return false;
                        }
                        else
                        {
                            if (std::any_of(variable_width_formatter.begin(), variable_width_formatter.end(), [&](char c) {
                                    return c == format[i + 1];
                                }))
                                return false;
                        }
                        i += 1;
                        continue;
                    default:
                        break;
                }
            }

            return true;
        }
        ContextPtr context_ptr;
        bool adaptive_cast;
        bool mysql_mode;
        bool mysql_M_is_month_name;
        bool mysql_f_prints_single_zero;
        // check if format string is fixed
        mutable bool mysql_with_only_fixed_length_formatters;

    public:
        static constexpr auto name = Name::name;

        explicit FunctionFormatDateTimeImpl(bool is_adaptive_cast, bool is_mysql, bool is_mysql_M_month_name, ContextPtr context)
            : context_ptr(context)
            , adaptive_cast(is_adaptive_cast)
            , mysql_mode(is_mysql)
            , mysql_M_is_month_name(is_mysql || is_mysql_M_month_name)
            , mysql_f_prints_single_zero(context->getSettingsRef().formatdatetime_f_prints_single_zero)
        {
        }

        static FunctionPtr create(ContextPtr context)
        {
            /// Template variable forceAdaptive is to co-work with the rewriting of toDate-like functions
            /// in terms of query parsing stage, so as to enforce the apply of adaptive timestamp.
            /// Here, the original function(FROM_UNIXTIME) shall be replaced with the counterpart built on
            /// forceAdaptive=true: FROM_UNIXTIME_ADAPTIVE
            if constexpr (forceAdaptive)
                return std::make_shared<FunctionFormatDateTimeImpl>(true, false, false, context);
            if constexpr (std::is_same_v<Name, NameDateFormat>)
                return std::make_shared<FunctionFormatDateTimeImpl>(false, true, true, context);
            return std::make_shared<FunctionFormatDateTimeImpl>(
                context->getSettingsRef().adaptive_type_cast,
                context->getSettingsRef().dialect_type == DialectType::MYSQL,
                context->getSettingsRef().formatdatetime_parsedatetime_m_is_month_name,
                context);
        }

        String getName() const override { return name; }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if constexpr (support_integer)
            {
                if (arguments.size() != 1 && arguments.size() != 2 && arguments.size() != 3)
                    throw Exception(
                        "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                            + ", should be 1, 2 or 3",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                if (arguments.size() == 1 && !isInteger(arguments[0].type) && !isStringOrFixedString(arguments[0].type))
                    throw Exception(
                        "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                            + " when arguments size is 1. Should be integer or a string literal of an integer",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                if (arguments.size() > 1
                    && !(
                        isInteger(arguments[0].type) || isDate(arguments[0].type) || isDate32(arguments[0].type)
                        || isDateTime(arguments[0].type) || isDateTime64(arguments[0].type) || isStringOrFixedString(arguments[0].type)))
                    throw Exception(
                        "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                            + " when arguments size is 2 or 3. Should be a integer, a string literal of an integer or a date with time",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            else
            {
                if (arguments.size() != 2 && arguments.size() != 3)
                    throw Exception(
                        "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                            + ", should be 2 or 3",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                /// change from vanilla: If the first argument is UInt type, we should regard it as unix timestamp
                if (!WhichDataType(arguments[0].type).isDateOrDateTime() && !WhichDataType(arguments[0].type).isUInt32()
                    && !WhichDataType(arguments[0].type).isTime() && !WhichDataType(arguments[0].type).isStringOrFixedString())
                    throw Exception(
                        "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                            + ". Should be a date or a date with time",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            if (arguments.size() == 2 && !WhichDataType(arguments[1].type).isString())
                throw Exception(
                    "Illegal type " + arguments[1].type->getName() + " of 2 argument of function " + getName() + ". Must be String.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Illegal type " + arguments[2].type->getName() + " of 3 argument of function " + getName() + ". Must be String.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (arguments.size() == 1)
                return std::make_shared<DataTypeDateTime>();
            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(
            const ColumnsWithTypeAndName & arguments,
            const DataTypePtr & result_type,
            [[maybe_unused]] size_t input_rows_count) const override
        {
            ColumnPtr res;
            ColumnsWithTypeAndName converted;
            if constexpr (support_integer)
            {
                if (isStringOrFixedString(arguments[0].type))
                {
                    ColumnsWithTypeAndName temp{arguments[0]};
                    auto to_int = FunctionFactory::instance().get("toInt64", context_ptr);
                    auto col = to_int->build(temp)->execute(temp, std::make_shared<DataTypeInt64>(), input_rows_count);
                    ColumnWithTypeAndName converted_col(col, std::make_shared<DataTypeInt64>(), "unixtime");
                    converted.emplace_back(converted_col);
                    for (size_t i = 1; i < arguments.size(); i++)
                    {
                        converted.emplace_back(arguments[i]);
                    }
                }
                else
                {
                    converted = arguments;
                }
                if (arguments.size() == 1)
                {
                    if (!castType(converted[0].type.get(), [&](const auto & type) {
                            using FromDataType = std::decay_t<decltype(type)>;
                            if constexpr (std::is_same_v<FromDataType, DataTypeInt64> || std::is_same_v<FromDataType, DataTypeUInt64>)
                            {
                                if (adaptive_cast)
                                    res = ConvertImpl<FromDataType, DataTypeDateTime, Name, ConvertDefaultBehaviorTag, ConvertExceptionMode::Throw, true>::execute(
                                        converted, result_type, input_rows_count);
                                else
                                    res = ConvertImpl<FromDataType, DataTypeDateTime, Name, ConvertDefaultBehaviorTag, ConvertExceptionMode::Throw, false>::execute(
                                        converted, result_type, input_rows_count);
                            }
                            else
                                res = ConvertImpl<FromDataType, DataTypeDateTime, Name>::execute(converted, result_type, input_rows_count);

                            return true;
                        }))
                    {
                        throw Exception(
                            "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                                + ", must be Integer, String or DateTime when arguments size is 1.",
                            ErrorCodes::ILLEGAL_COLUMN);
                    }
                }
                else
                {
                    if (!castType(converted[0].type.get(), [&](const auto & type) {
                            using FromDataType = std::decay_t<decltype(type)>;
                            if (!(res = executeType<FromDataType>(converted, result_type)))
                                throw Exception(
                                    "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                                        + ", must be Integer, String or DateTime.",
                                    ErrorCodes::ILLEGAL_COLUMN);
                            return true;
                        }))
                    {
                        if (!((res = executeType<DataTypeDate>(converted, result_type))
                              || (res = executeType<DataTypeDate32>(converted, result_type))
                              || (res = executeType<DataTypeDateTime>(converted, result_type))
                              || (res = executeType<DataTypeDateTime64>(converted, result_type))))
                            throw Exception(
                                "Illegal column " + arguments[0].column->getName() + " of function " + getName()
                                    + ", must be Integer, String or DateTime.",
                                ErrorCodes::ILLEGAL_COLUMN);
                    }
                }
            }
            else
            {
                if constexpr (std::is_same_v<Name, NameTimeFormat>)
                {
                    if (!isTime(arguments[0].type))
                    {
                        // TODO: handle scale for datetime
                        auto to_int = FunctionFactory::instance().get("toTimeType", context_ptr);
                        const auto scale_type = std::make_shared<DataTypeUInt8>();
                        const auto scale_col = scale_type->createColumnConst(1, Field(0));
                        ColumnWithTypeAndName scale_arg {std::move(scale_col), std::move(scale_type), "scale"};
                        ColumnsWithTypeAndName temp {arguments[0], std::move(scale_arg)};
                        auto col = to_int->build(temp)->execute(temp, std::make_shared<DataTypeTime>(0), input_rows_count);
                        ColumnWithTypeAndName converted_col(col, std::make_shared<DataTypeTime>(0), "unixtime");
                        converted.emplace_back(converted_col);
                        for (size_t i = 1; i < arguments.size(); i++)
                        {
                            converted.emplace_back(arguments[i]);
                        }
                    }
                    else
                    {
                        converted = arguments;
                    }
                }
                else
                {
                    if (isStringOrFixedString(arguments[0].type))
                    {
                        const auto timezone_type = std::make_shared<DataTypeString>();
                        const auto timezone_col = timezone_type->createColumnConst(1, Field("UTC"));
                        ColumnWithTypeAndName timezone_arg{std::move(timezone_col), std::move(timezone_type), "timezone"};
                        const auto scale_type = std::make_shared<DataTypeUInt8>();
                        const auto scale_col = scale_type->createColumnConst(1, Field(6));
                        ColumnWithTypeAndName scale_arg{std::move(scale_col), std::move(scale_type), "scale"};
                        ColumnsWithTypeAndName temp{std::move(arguments[0]), std::move(scale_arg), std::move(timezone_arg)};
                        auto to_datetime = FunctionFactory::instance().get("toDateTime", context_ptr);
                        auto col = to_datetime->build(temp)->execute(temp, std::make_shared<DataTypeDateTime>(), input_rows_count);
                        ColumnWithTypeAndName converted_col(col, std::make_shared<DataTypeDateTime>(), "datetime");
                        converted.emplace_back(converted_col);
                        for (size_t i = 1; i < arguments.size(); i++)
                        {
                            converted.emplace_back(arguments[i]);
                        }
                    }
                    else
                    {
                        converted = arguments;
                    }
                }

                if (!((res = executeType<DataTypeDate>(converted, result_type))
                      || (res = executeType<DataTypeDate32>(converted, result_type))
                      || (res = executeType<DataTypeDateTime>(converted, result_type))
                      || (res = executeType<DataTypeDateTime64>(converted, result_type))
                      || (res = executeType<DataTypeTime>(converted, result_type))))
                    throw Exception(
                        "Illegal column " + arguments[0].column->getName() + " of function " + getName() + ", must be Date or DateTime.",
                        ErrorCodes::ILLEGAL_COLUMN);
            }

            return res;
        }

        template <typename DataType>
        ColumnPtr executeType(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
        {
            auto * times = checkAndGetColumn<typename DataType::ColumnType>(arguments[0].column.get());
            if (!times)
                return nullptr;

            const ColumnConst * format_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
            if (!format_column)
                throw Exception(
                    "Illegal column " + arguments[1].column->getName() + " of second ('format') argument of function " + getName()
                        + ". Must be constant string.",
                    ErrorCodes::ILLEGAL_COLUMN);

            String format = format_column->getValue<String>();
            UInt32 scale [[maybe_unused]] = 0;
            if constexpr (std::is_same_v<DataType, DataTypeDateTime64> || std::is_same_v<DataType, DataTypeTime>)
                scale = times->getScale();

            /// For MySQL, we support two modes of execution:
            ///
            /// - All formatters in the format string are fixed-width. As a result, all output rows will have the same width and structure. We
            ///   take advantage of this and
            ///     1. create a "template" with placeholders from the format string,
            ///     2. allocate a result column large enough to store the template on each row,
            ///     3. copy the template into each result row,
            ///     4. run instructions which replace the formatter placeholders. All other parts of the template (e.g. whitespaces) are already
            ///        as desired and instructions skip over them (see 'extra_shift' in the formatters).
            ///
            /// - The format string contains at least one variable-width formatter. Output rows will potentially be of different size.
            ///   Steps 1. and 2. are performed as above (the result column is allocated based on a worst-case size estimation). The result
            ///   column rows are NOT populated with the template and left uninitialized. We run the normal instructions for formatters AND
            ///   instructions that copy literal characters before/between/after formatters. As a result, each byte of each result row is
            ///   written which is obviously slow.
            mysql_with_only_fixed_length_formatters = containsOnlyFixedWidthMySQLFormatters(format, mysql_M_is_month_name);

            using T = typename InstructionValueTypeMap<DataType>::InstructionValueType;
            std::vector<Instruction<T>> instructions;
            String out_template;
            size_t out_template_size = parseMySQLFormat(format, instructions, scale, out_template);

            const DateLUTImpl * time_zone_tmp = nullptr;
            if (castType(arguments[0].type.get(), [&]([[maybe_unused]] const auto & type) { return true; }))
                time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 0);
            else if (std::is_same_v<DataType, DataTypeDateTime64> || std::is_same_v<DataType, DataTypeDateTime>)
                time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, 2, 0);
            else
                time_zone_tmp = &DateLUT::instance();

            const DateLUTImpl & time_zone = *time_zone_tmp;
            const auto & vec = times->getData();

            auto col_res = ColumnString::create();
            auto & res_data = col_res->getChars();
            auto & res_offsets = col_res->getOffsets();
            res_data.resize(vec.size() * (out_template_size + 1));
            res_offsets.resize(vec.size());


            if (mysql_with_only_fixed_length_formatters)
            {
                /// Fill result with template.
                {
                    const UInt8 * const begin = res_data.data();
                    const UInt8 * const end = res_data.data() + res_data.size();
                    UInt8 * pos = res_data.data();

                    if (pos < end)
                    {
                        memcpy(
                            pos,
                            out_template.data(),
                            out_template_size + 1); /// With zero terminator. mystring[mystring.size()] = '\0' is guaranteed since C++11.
                        pos += out_template_size + 1;
                    }

                    /// Copy exponentially growing ranges.
                    while (pos < end)
                    {
                        size_t bytes_to_copy = std::min(pos - begin, end - pos);
                        memcpy(pos, begin, bytes_to_copy);
                        pos += bytes_to_copy;
                    }
                }
            }


            auto * begin = reinterpret_cast<char *>(res_data.data());
            auto * pos = begin;
            for (size_t i = 0; i < vec.size(); ++i)
            {
                if constexpr (std::is_same_v<DataType, DataTypeDateTime64>)
                {
                    const auto c = DecimalUtils::split(vec[i], scale);
                    for (auto & instruction : instructions)
                        instruction.perform(pos, static_cast<Int64>(c.whole), c.fractional, scale, time_zone);
                }
                else if constexpr (std::is_same_v<DataType, DataTypeTime>)
                {
                    const auto c = DecimalUtils::split(vec[i], scale);
                    for (auto & instruction : instructions)
                        instruction.perform(pos, static_cast<Int64>(c.whole), c.fractional, scale, time_zone);
                }
                else
                {
                    for (auto & instruction : instructions)
                        instruction.perform(pos, static_cast<UInt32>(vec[i]), 0, 0, time_zone);
                }
                *pos++ = '\0';

                res_offsets[i] = pos - begin;
            }

            res_data.resize(pos - begin);
            return col_res;
        }

        template <typename T>
        size_t
        parseMySQLFormat(const String & format, std::vector<Instruction<T>> & instructions, UInt32 scale, String & out_template) const
        {
            auto add_extra_shift = [&](size_t amount) {
                if (instructions.empty())
                {
                    Instruction<T> instruction;
                    instruction.setMysqlFunc(&Instruction<T>::mysqlNoop);
                    instructions.push_back(std::move(instruction));
                }
                instructions.back().extra_shift += amount;
            };

            [[maybe_unused]] auto add_literal_instruction = [&](std::string_view literal) {
                Instruction<T> instruction;
                instruction.setMysqlFunc(&Instruction<T>::mysqlLiteral);
                instruction.setLiteral(literal);
                instructions.push_back(std::move(instruction));
            };

            auto add_extra_shift_or_literal_instruction = [&](std::string_view literal) {
                if (mysql_with_only_fixed_length_formatters)
                    add_extra_shift(literal.size());
                else
                    add_literal_instruction(literal);
            };

            auto add_time_instruction
                = [&]([[maybe_unused]] typename Instruction<T>::FuncMysql && func, [[maybe_unused]] std::string_view literal) {
                      /// DateTime/DateTime64 --> insert instruction
                      /// Other types cannot provide the requested data --> write out template
                      if constexpr (std::is_same<T, UInt32>::value || std::is_same<T, Int64>::value || std::is_same<T, Decimal64>::value)
                      {
                          Instruction<T> instruction;
                          instruction.setMysqlFunc(std::move(func));
                          instruction.setFixedFormat(mysql_with_only_fixed_length_formatters);
                          instructions.push_back(std::move(instruction));
                      }
                      else
                          add_extra_shift_or_literal_instruction(literal);
                  };

            Pos pos = format.data();
            Pos const end = format.data() + format.size();

            while (true)
            {
                Pos const percent_pos = find_first_symbols<'%'>(pos, end);

                if (percent_pos < end)
                {
                    if (pos < percent_pos)
                    {
                        /// Handle characters before next %
                        add_extra_shift_or_literal_instruction(std::string_view(pos, percent_pos - pos));
                        out_template += String(pos, percent_pos - pos);
                    }

                    pos = percent_pos + 1;
                    if (pos >= end)
                        throwLastCharacterIsPercentException();

                    /// Looking for MySQL specific patterns to match :
                    /// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format


                    switch (*pos)
                    {
                        // Abbreviated weekday [Mon-Sun]
                        case 'a': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfWeekTextShort);
                            instructions.push_back(std::move(instruction));
                            out_template += "Mon";
                            break;
                        }

                        // Abbreviated month [Jan-Dec]
                        case 'b': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlMonthOfYearTextShort);
                            instructions.push_back(std::move(instruction));
                            out_template += "Jan";
                            break;
                        }

                        // Month as a integer number (01-12)
                        case 'c': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlMonth);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }

                        // Year, divided by 100, zero-padded
                        case 'C': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlCentury);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }

                        // Day of month, zero-padded (01-31)
                        case 'd': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfMonth);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }

                        // Short MM/DD/YY date, equivalent to %m/%d/%y
                        // TODO: add D implementation for mysql
                        case 'D': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlAmericanDate);
                            instruction.setFixedFormat(mysql_with_only_fixed_length_formatters);
                            instructions.push_back(std::move(instruction));
                            out_template += "00/00/00";
                            break;
                        }

                        // Day of month, space-padded ( 1-31)  23
                        case 'e': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfMonthSpacePadded);
                            instructions.push_back(std::move(std::move(instruction)));
                            out_template += " 0";
                            break;
                        }

                        // Depending on a setting
                        // - Full month [January-December] OR
                        // - Minute of hour range [0, 59]
                        case 'M': {
                            Instruction<T> instruction;
                            if (mysql_M_is_month_name)
                            {
                                instruction.setMysqlFunc(&Instruction<T>::mysqlMonthOfYearTextLong);
                                instructions.push_back(std::move(instruction));
                                out_template += "September"; /// longest possible month name
                            }
                            else
                            {
                                static constexpr std::string_view val = "00";
                                add_time_instruction(&Instruction<T>::mysqlMinute, val);
                                out_template += val;
                            }
                            break;
                        }

                            // Fractional seconds
                        case 'f': {
                            /// If the time data type has no fractional part, we print (default) '000000' or (deprecated) '0' as fractional part.
                            Instruction<T> instruction;
                            if (mysql_f_prints_single_zero)
                            {
                                instruction.setMysqlFunc(&Instruction<T>::mysqlFractionalSecondSingleZero);
                                instruction.setFixedFormat(mysql_with_only_fixed_length_formatters);
                                instructions.push_back(std::move(instruction));
                                out_template += String(scale == 0 ? 1 : scale, '0');
                            }
                            else
                            {
                                instruction.setMysqlFunc(&Instruction<T>::mysqlFractionalSecond);
                                instruction.setFixedFormat(mysql_with_only_fixed_length_formatters);
                                instructions.push_back(std::move(instruction));
                                out_template += String(scale == 0 ? 6 : scale, '0');
                            }
                            break;
                        }

                        // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                        case 'F': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlISO8601Date);
                            instruction.setFixedFormat(mysql_with_only_fixed_length_formatters);
                            instructions.push_back(std::move(instruction));
                            out_template += "0000-00-00";
                            break;
                        }

                        // Last two digits of year of ISO 8601 week number (see %G)
                        case 'g': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlISO8601Year2);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }

                        // Year of ISO 8601 week number (see %V)
                        case 'G': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlISO8601Year4);
                            instructions.push_back(std::move(instruction));
                            out_template += "0000";
                            break;
                        }

                        // Day of the year (001-366)   235
                        case 'j': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfYear);
                            instructions.push_back(std::move(instruction));
                            out_template += "000";
                            break;
                        }

                        // Month as a integer number (01-12)
                        case 'm': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlMonth);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }

                        // ISO 8601 weekday as number with Monday as 1 (1-7)
                        // MySQL mode: 	Week (00..53), where Monday is the first day of the week; WEEK() mode 1
                        case 'u': {
                            if (mysql_mode)
                            {
                                Instruction<T> instruction;
                                instruction.setMysqlFunc(&Instruction<T>::weekMode1);
                                instructions.push_back(std::move(instruction));
                                out_template += "00";
                            }
                            else
                            {
                                Instruction<T> instruction;
                                instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfWeek);
                                instructions.push_back(std::move(instruction));
                                out_template += "0";
                            }
                            break;
                        }

                        // ISO 8601 week number (01-53) week mode 3 by default
                        // weekMode2 for mysql
                        case 'V': {
                            if (mysql_mode)
                            {
                                Instruction<T> instruction;
                                instruction.setMysqlFunc(&Instruction<T>::weekMode2);
                                instructions.push_back(std::move(instruction));
                                out_template += "00";
                            }
                            else
                            {
                                Instruction<T> instruction;
                                instruction.setMysqlFunc(&Instruction<T>::mysqlISO8601Week);
                                instructions.push_back(std::move(instruction));
                                out_template += "00";
                            }
                            break;
                        }

                        // Weekday as a decimal number with Sunday as 0 (0-6)  4
                        case 'w': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfWeek0To6);
                            instructions.push_back(std::move(instruction));
                            out_template += "0";
                            break;
                        }

                        // Full weekday [Monday-Sunday]
                        case 'W': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfWeekTextLong);
                            instructions.push_back(std::move(instruction));
                            out_template += "Wednesday"; /// longest possible weekday name
                            break;
                        }

                        // Two digits year
                        case 'y': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlYear2);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }

                        // Four digits year
                        case 'Y': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlYear4);
                            instructions.push_back(std::move(instruction));
                            out_template += "0000";
                            break;
                        }

                        // Quarter (1-4)
                        case 'Q': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlQuarter);
                            instructions.push_back(std::move(instruction));
                            out_template += "0";
                            break;
                        }

                        // Offset from UTC timezone as +hhmm or -hhmm
                        case 'z': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::mysqlTimezoneOffset);
                            instruction.setFixedFormat(mysql_with_only_fixed_length_formatters);
                            instructions.push_back(std::move(instruction));
                            out_template += "+0000";
                            break;
                        }

                        /// Time components. If the argument is Date, not a DateTime, then this components will have default value.

                        // AM or PM
                        case 'p': {
                            static constexpr std::string_view val = "AM";
                            add_time_instruction(&Instruction<T>::mysqlAMPM, val);
                            out_template += val;
                            break;
                        }

                        // 12-hour HH:MM:SS time, equivalent to %h:%i %p 2:55 PM
                        case 'r': {
                            static constexpr std::string_view val = "12:00:00 AM";
                            add_time_instruction(&Instruction<T>::mysqlhhmmss12AMPM, val);
                            out_template += val;
                            break;
                        }

                        // 24-hour HH:MM time, equivalent to %H:%i 14:55
                        case 'R': {
                            static constexpr std::string_view val = "00:00";
                            add_time_instruction(&Instruction<T>::mysqlHHMM24, val);
                            out_template += val;
                            break;
                        }

                        // Seconds
                        case 's': {
                            static constexpr std::string_view val = "00";
                            add_time_instruction(&Instruction<T>::mysqlSecond, val);
                            out_template += val;
                            break;
                        }

                        // Seconds
                        case 'S': {
                            static constexpr std::string_view val = "00";
                            add_time_instruction(&Instruction<T>::mysqlSecond, val);
                            out_template += val;
                            break;
                        }

                        // ISO 8601 time format (HH:MM:SS), equivalent to %H:%i:%S 14:55:02
                        case 'T': {
                            static constexpr std::string_view val = "00:00:00";
                            add_time_instruction(&Instruction<T>::mysqlISO8601Time, val);
                            out_template += val;
                            break;
                        }

                        // Hour in 12h format (01-12)
                        case 'h': {
                            static constexpr std::string_view val = "12";
                            add_time_instruction(&Instruction<T>::mysqlHour12, val);
                            out_template += val;
                            break;
                        }

                        // Hour in 24h format (00-23)
                        case 'H': {
                            static constexpr std::string_view val = "00";
                            add_time_instruction(&Instruction<T>::mysqlHour24, val);
                            out_template += val;
                            break;
                        }

                        // Minute of hour range [0, 59]
                        case 'i': {
                            static constexpr std::string_view val = "00";
                            add_time_instruction(&Instruction<T>::mysqlMinute, val);
                            out_template += val;
                            break;
                        }

                        // Hour in 12h format (01-12)
                        case 'I': {
                            static constexpr std::string_view val = "12";
                            add_time_instruction(&Instruction<T>::mysqlHour12, val);
                            out_template += val;
                            break;
                        }

                        // Hour in 24h format (00-23)
                        case 'k': {
                            static constexpr std::string_view val = "00";
                            add_time_instruction(&Instruction<T>::mysqlHour24, val);
                            out_template += val;
                            break;
                        }

                        // Hour in 12h format (01-12)
                        case 'l': {
                            static constexpr std::string_view val = "12";
                            add_time_instruction(&Instruction<T>::mysqlHour12, val);
                            out_template += val;
                            break;
                        }

                        case 't': {
                            static constexpr std::string_view val = "\t";
                            add_extra_shift_or_literal_instruction(val);
                            out_template += val;
                            break;
                        }

                        case 'n': {
                            static constexpr std::string_view val = "\n";
                            add_extra_shift_or_literal_instruction(val);
                            out_template += val;
                            break;
                        }

                        // Escaped literal characters.
                        case '%': {
                            static constexpr std::string_view val = "%";
                            add_extra_shift_or_literal_instruction(val);
                            out_template += val;
                            break;
                        }

                        // %U : Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
                        case 'U': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::weekMode0);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }
                        // %v : Week (01..53), where Sunday is the first day of the week; WEEK() mode 3
                        case 'v': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::weekMode3);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                        }
                        // %X : Year for the week, where Sunday is the first day of the week, numeric, four digits
                        case 'X': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::yearForFirstDayOfWeekSunday);
                            instructions.push_back(std::move(instruction));
                            out_template += "0000";
                            break;
                        }
                        // %x : Year for the week, where Monday is the first day of the week, numeric, four digits
                        case 'x': {
                            Instruction<T> instruction;
                            instruction.setMysqlFunc(&Instruction<T>::yearForFirstDayOfWeekMonday);
                            instructions.push_back(std::move(instruction));
                            out_template += "0000";
                            break;
                        }
                        default:
                            out_template += *pos;
                            add_extra_shift(1);
                            break;
                    }
                    ++pos;
                }
                else
                {
                    /// Handle characters after last %
                    add_extra_shift_or_literal_instruction(std::string_view(pos, end - pos));
                    out_template += String(pos, end - pos);
                    break;
                }
            }
            return out_template.size();
        }
    };

    using FunctionFormatDateTime = FunctionFormatDateTimeImpl<NameFormatDateTime, false>;
    using FunctionFROM_UNIXTIME = FunctionFormatDateTimeImpl<NameFromUnixTime, true>;
    using FunctionFROM_UNIXTIMEAdaptive = FunctionFormatDateTimeImpl<NameFromUnixTimeAdaptive, true, true>;
    using FunctionDateFormat = FunctionFormatDateTimeImpl<NameDateFormat, false>;
    using FunctionTimeFormat = FunctionFormatDateTimeImpl<NameTimeFormat, false>;

    /*
 * This function is like date_format in hive.
 */
    class FunctionHiveDateFormat : public FunctionFormatDateTime
    {
        static constexpr std::string_view PATTERN_CHARS = "GyMdkHmsSEDFwWahKzZYuXL";
        static constexpr int TAG_ASCII_CHAR = 100;

        enum PATTERN
        {
            ERA = 0, // G
            YEAR, // y
            MONTH, // M
            DAY_OF_MONTH, // d
            HOUR_OF_DAY1, // k
            HOUR_OF_DAY0, // H
            MINUTE, // m
            SECOND, // s
            MILLISECOND, // S
            DAY_OF_WEEK, // E
            DAY_OF_YEAR, // D
            DAY_OF_WEEK_IN_MONTH, // F
            WEEK_OF_YEAR, // w
            WEEK_OF_MONTH, // W
            AM_PM, // a
            HOUR1, // h
            HOUR0, // K
            ZONE_NAME, // z
            ZONE_VALUE, // Z
            WEEK_YEAR, // Y
            ISO_DAY_OF_WEEK, // u
            ISO_ZONE, // X
            MONTH_STANDALONE // L
        };

        void compilePattern(String & pattern, String & compiled_code) const
        {
            auto encode = [](int tag, int length, String & buffer) {
                if (length < 255)
                {
                    buffer += static_cast<char>(tag);
                    buffer += static_cast<char>(length);
                }
                else
                    throw Exception("Illegal date format pattern. ", ErrorCodes::BAD_ARGUMENTS);
            };

            size_t length = pattern.size();
            int count = 0;
            int last_tag = -1;

            for (size_t i = 0; i < length; i++)
            {
                char c = pattern[i];

                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')))
                {
                    if (count != 0)
                    {
                        encode(last_tag, count, compiled_code);
                        last_tag = -1;
                        count = 0;
                    }

                    size_t j;
                    for (j = i + 1; j < length; j++)
                    {
                        char d = pattern[j];
                        if ((d >= 'a' && d <= 'z') || (d >= 'A' && d <= 'Z'))
                            break;
                    }

                    encode(TAG_ASCII_CHAR, j - i, compiled_code);

                    for (; i < j; i++)
                    {
                        compiled_code += (pattern[i]);
                    }
                    i--;

                    continue;
                }

                auto found = PATTERN_CHARS.find(c);
                if (found == String::npos)
                    throw Exception(String("Unknown pattern character '") + c + "'.", ErrorCodes::UNSUPPORTED_PARAMETER);

                int tag = found;

                if (last_tag == -1 || last_tag == tag)
                {
                    last_tag = tag;
                    count++;
                    continue;
                }

                encode(last_tag, count, compiled_code);
                last_tag = tag;
                count = 1;
            }

            if (count != 0)
                encode(last_tag, count, compiled_code);
        }

        template <typename T>
        String parsePattern(String & pattern, std::vector<FunctionFormatDateTime::Instruction<T>> & instructions) const
        {
            String compiled;
            compilePattern(pattern, compiled);

            auto add_extra_shift = [&](size_t amount) {
                if (instructions.empty())
                {
                    Instruction<T> instruction;
                    instruction.setMysqlFunc(&Instruction<T>::mysqlNoop);
                    instructions.push_back(std::move(instruction));
                }
                instructions.back().extra_shift += amount;
            };

            auto add_instruction_or_shift
                = [&](typename FunctionFormatDateTime::Instruction<T> instruction [[maybe_unused]], size_t shift) {
                      if constexpr (std::is_same_v<T, UInt32>)
                          instructions.push_back(std::move(instruction));
                      else
                          add_extra_shift(shift);
            };


            [[maybe_unused]] auto add_literal_instruction = [&](std::string_view literal) {
                Instruction<T> instruction;
                instruction.setMysqlFunc(&Instruction<T>::mysqlLiteral);
                instruction.setLiteral(literal);
                instructions.push_back(std::move(instruction));
            };

            String out_template;

            size_t length = compiled.size();

            int tag;
            int count;

            for (size_t i = 0; i < length;)
            {
                if ((tag = compiled[i++]) == TAG_ASCII_CHAR)
                {
                    count = compiled[i++];
                    out_template.append(compiled, i, count);
                    add_extra_shift(count);
                    i += count;
                }
                else
                {
                    count = compiled[i++];
                    Instruction<T> instruction;
                    switch (tag)
                    {
                        case PATTERN::WEEK_YEAR:
                        case PATTERN::YEAR:
                            if (count != 2)
                            {
                                instruction.setMysqlFunc(&Instruction<T>::mysqlYear4);
                                instructions.push_back(std::move(instruction));
                                out_template += "0000";
                            }
                            else
                            {
                                instruction.setMysqlFunc(&Instruction<T>::mysqlYear2);
                                instructions.push_back(std::move(instruction));
                                out_template += "00";
                            }
                            break;

                        case PATTERN::MONTH:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlMonth);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;

                        case PATTERN::DAY_OF_MONTH:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfMonth);
                            instructions.push_back(std::move(instruction));
                            out_template += "00";
                            break;
                            break;

                        case PATTERN::HOUR_OF_DAY0:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlHour24);
                            add_instruction_or_shift(instruction, 2);
                            out_template += "00";
                            break;

                        case PATTERN::HOUR0:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlHour12);
                            add_instruction_or_shift(instruction, 2);
                            out_template += "00";
                            break;

                        case PATTERN::MINUTE:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlMinute);
                            add_instruction_or_shift(instruction, 2);
                            out_template += "00";
                            break;

                        case PATTERN::SECOND:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlSecond);
                            add_instruction_or_shift(instruction, 2);
                            out_template += "00";
                            break;

                        case PATTERN::DAY_OF_YEAR:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlDayOfYear);
                            instructions.push_back(std::move(instruction));
                            out_template += "000";
                            break;

                        case PATTERN::AM_PM:
                            instruction.setMysqlFunc(&Instruction<T>::mysqlAMPM);
                            add_instruction_or_shift(instruction, 2);
                            out_template += "AM";
                            break;

                        default:
                            throw Exception("Not supported pattern: " + std::to_string(tag), ErrorCodes::NOT_IMPLEMENTED);
                    }
                }
            }

            add_extra_shift(1);
            return out_template;
        }

    public:
        static constexpr auto name = "date_format";

        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionHiveDateFormat>(context); }

        explicit FunctionHiveDateFormat(ContextPtr context) : FunctionFormatDateTime(false, false, false, context) { }

        String getName() const override { return name; }

        bool useDefaultImplementationForConstants() const override { return true; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (arguments.size() != 2 && arguments.size() != 3)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be 2 or 3",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!WhichDataType(arguments[0].type).isDateOrDateTime() && !WhichDataType(arguments[0].type).isUInt32()
                && !WhichDataType(arguments[0].type).isString())
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName()
                        + ". Should be datetime or datetime format string",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (!WhichDataType(arguments[1].type).isString())
                throw Exception(
                    "Illegal type " + arguments[1].type->getName() + " of 2 argument of function " + getName() + ". Must be String.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (arguments.size() == 3)
            {
                if (!WhichDataType(arguments[2].type).isString())
                    throw Exception(
                        "Illegal type " + arguments[2].type->getName() + " of 3 argument of function " + getName() + ". Must be String.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(
            const ColumnsWithTypeAndName & arguments,
            [[maybe_unused]] const DataTypePtr & result_type,
            [[maybe_unused]] size_t input_rows_count) const override
        {
            auto date_time_type = std::make_shared<DataTypeDateTime>();
            ColumnsWithTypeAndName tmp_arguments;
            tmp_arguments.push_back(arguments[0]); /* from data */
            if (arguments.size() == 3) /* time zone */
                tmp_arguments.push_back(arguments[2]);

            FunctionPtr convert = std::make_shared<FunctionToDateTime>();
            ColumnPtr times = convert->executeImpl(tmp_arguments, date_time_type, input_rows_count);

            const ColumnConst * pattern_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

            if (!pattern_column)
                throw Exception(
                    "Illegal column " + arguments[1].column->getName() + " of second ('format') argument of function " + getName()
                        + ". Must be constant string.",
                    ErrorCodes::ILLEGAL_COLUMN);

            auto pattern = pattern_column->getValue<String>();

            std::vector<FunctionFormatDateTime::Instruction<UInt32>> instructions;
            String pattern_to_fill = parsePattern(pattern, instructions);
            size_t result_size = pattern_to_fill.size();

            auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

            auto col_res = ColumnString::create();
            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();
            dst_data.resize(times->size() * (result_size + 1));
            dst_offsets.resize(times->size());

            /// Fill result with literals.
            {
                UInt8 * begin = dst_data.data();
                UInt8 * end = begin + dst_data.size();
                UInt8 * pos = begin;

                if (pos < end)
                {
                    memcpy(pos, pattern_to_fill.data(), result_size + 1); /// With zero terminator.
                    pos += result_size + 1;
                }

                /// Fill by copying exponential growing ranges.
                while (pos < end)
                {
                    size_t bytes_to_copy = std::min(pos - begin, end - pos);
                    memcpy(pos, begin, bytes_to_copy);
                    pos += bytes_to_copy;
                }
            }

            auto begin = reinterpret_cast<char *>(dst_data.data());
            auto pos = begin;

            for (size_t i = 0; i < times->size(); ++i)
            {
                for (auto & instruction : instructions)
                    instruction.perform(pos, (*times)[i].get<UInt32>(), 0, 0, time_zone);

                dst_offsets[i] = pos - begin;
            }

            dst_data.resize(pos - begin);

            return col_res;
        }
    };

}

void registerFunctionFormatDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatDateTime>();
    factory.registerFunction<FunctionFROM_UNIXTIME>();
    factory.registerFunction<FunctionDateFormat>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTimeFormat>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("fromUnixTimestamp", "FROM_UNIXTIME");
    factory.registerAlias("from_unixtime", "FROM_UNIXTIME");
    factory.registerFunction<FunctionFROM_UNIXTIMEAdaptive>();
    factory.registerFunction<FunctionHiveDateFormat>();
}

}
