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

#include <Common/FieldVisitorDump.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

template <typename T>
static inline String formatQuotedWithPrefix(T x, const char * prefix)
{
    WriteBufferFromOwnString wb;
    writeCString(prefix, wb);
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

String FieldVisitorDump::operator() (const Null &) const { return "NULL"; }
String FieldVisitorDump::operator() (const NegativeInfinity &) const { return "-Inf"; }
String FieldVisitorDump::operator() (const PositiveInfinity &) const { return "+Inf"; }
String FieldVisitorDump::operator() (const UInt64 & x) const { return formatQuotedWithPrefix(x, "UInt64_"); }
String FieldVisitorDump::operator() (const Int64 & x) const { return formatQuotedWithPrefix(x, "Int64_"); }
String FieldVisitorDump::operator() (const Float64 & x) const { return formatQuotedWithPrefix(x, "Float64_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal32> & x) const { return formatQuotedWithPrefix(x, "Decimal32_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal64> & x) const { return formatQuotedWithPrefix(x, "Decimal64_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal128> & x) const { return formatQuotedWithPrefix(x, "Decimal128_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal256> & x) const { return formatQuotedWithPrefix(x, "Decimal256_"); }
String FieldVisitorDump::operator() (const UInt128 & x) const { return formatQuotedWithPrefix(x, "UInt128_"); }
String FieldVisitorDump::operator() (const UInt256 & x) const { return formatQuotedWithPrefix(x, "UInt256_"); }
String FieldVisitorDump::operator() (const Int128 & x) const { return formatQuotedWithPrefix(x, "Int128_"); }
String FieldVisitorDump::operator() (const Int256 & x) const { return formatQuotedWithPrefix(x, "Int256_"); }
String FieldVisitorDump::operator() (const UUID & x) const { return formatQuotedWithPrefix(x, "UUID_"); }
String FieldVisitorDump::operator() (const IPv4 & x) const { return formatQuotedWithPrefix(x, "IPv4_"); }
String FieldVisitorDump::operator() (const IPv6 & x) const { return formatQuotedWithPrefix(x, "IPv6_"); }


String FieldVisitorDump::operator() (const String & x) const
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

String FieldVisitorDump::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Array_[";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorDump::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Tuple_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Map_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        Field tuple = Tuple{it->first, it->second};
        wb << applyVisitor(*this, tuple);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const AggregateFunctionStateData & x) const
{
    WriteBufferFromOwnString wb;
    wb << "AggregateFunctionState_(";
    writeQuoted(x.name, wb);
    wb << ", ";
    writeQuoted(x.data, wb);
    wb << ')';
    return wb.str();
}

String FieldVisitorDump::operator() (const BitMap64 & x) const
{
    WriteBufferFromOwnString wb;
    wb << "BitMap64_" << x.toString();
    return wb.str();
}

String FieldVisitorDump::operator() (const Object & x) const
{
    WriteBufferFromOwnString wb;

    wb << "Object_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << "(" << it->first << ", " << applyVisitor(*this, it->second) << ")";
    }
    wb << ')';

    return wb.str();

}

}
