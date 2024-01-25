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

#include <Common/FieldVisitorToString.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}


template <typename T>
static inline String formatQuoted(T x)
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline void writeQuoted(const DecimalField<T> & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x.getValue(), x.getScale(), buf, {});
    writeChar('\'', buf);
}

/** In contrast to writeFloatText (and writeQuoted),
  *  even if number looks like integer after formatting, prints decimal point nevertheless (for example, Float64(1) is printed as 1.).
  * - because resulting text must be able to be parsed back as Float64 by query parser (otherwise it will be parsed as integer).
  *
  * Trailing zeros after decimal point are omitted.
  *
  * NOTE: Roundtrip may lead to loss of precision.
  */
static String formatFloat(const Float64 x)
{
    DoubleConverter<true>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

    if (!result)
        throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    return { buffer, buffer + builder.position() };
}


String FieldVisitorToString::operator() (const Null &) const { return "NULL"; }
String FieldVisitorToString::operator() (const NegativeInfinity &) const { return "-Inf"; }
String FieldVisitorToString::operator() (const PositiveInfinity &) const { return "+Inf"; }
String FieldVisitorToString::operator() (const UInt64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int64 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Float64 & x) const { return formatFloat(x); }
String FieldVisitorToString::operator() (const String & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal32> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal64> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal128> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const DecimalField<Decimal256> & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int128 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UInt128 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UInt256 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const Int256 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const UUID & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const IPv4 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const IPv6 & x) const { return formatQuoted(x); }
String FieldVisitorToString::operator() (const AggregateFunctionStateData & x) const { return formatQuoted(x.data); }

String FieldVisitorToString::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << '[';
    for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb.write(", ", 2);
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorToString::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    // For single-element tuples we must use the explicit tuple() function,
    // or they will be parsed back as plain literals.
    if (x.size() > 1)
    {
        wb << '(';
    }
    else
    {
        wb << "tuple(";
    }

    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorToString::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << '{';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, it->first);
        wb << ":";
        wb << applyVisitor(*this, it->second);
    }
    wb << '}';

    return wb.str();
}

String FieldVisitorToString::operator() (const BitMap64 & x) const
{
    WriteBufferFromOwnString wb;
    wb << "BitMap64_" << x.toString();
    return wb.str();
}

String FieldVisitorToString::operator() (const Object & x) const
{
    WriteBufferFromOwnString wb;

    wb << '{';
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";

        writeDoubleQuoted(it->first, wb);
        wb << ": " << applyVisitor(*this, it->second);
    }
    wb << '}';

    return wb.str();

}

}
