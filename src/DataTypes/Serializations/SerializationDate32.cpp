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

#include <DataTypes/Serializations/SerializationDate32.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}


void SerializationDate32::checkDataOverflow(const FormatSettings & settings)
{
    if (!settings.check_data_overflow)
        return;

    if(!current_thread || !current_thread->getOverflow(ThreadStatus::OverflowFlag::Date))
        return;

    current_thread->unsetOverflow(ThreadStatus::OverflowFlag::Date);
    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Under MYSQL dialect or check_data_overflow = 1, the Date32 value should be within [1900-01-01, 2299-12-31]");

}

void SerializationDate32::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(ExtendedDayNum(assert_cast<const ColumnInt32 &>(column).getData()[row_num]), ostr, time_zone);
}

void SerializationDate32::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Date32");
}

void SerializationDate32::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    readDateText(x, istr, time_zone);
    checkDataOverflow(settings);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

void SerializationDate32::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate32::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate32::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    assertChar('\'', istr);
    readDateText(x, istr, time_zone);
    assertChar('\'', istr);
    checkDataOverflow(settings);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDate32::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate32::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    assertChar('"', istr);
    readDateText(x, istr, time_zone);
    assertChar('"', istr);
    checkDataOverflow(settings);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

void SerializationDate32::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate32::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (!settings.check_data_overflow)
    {
        LocalDate value;
        readCSV(value, istr);
        const auto x = value.getExtendedDayNum();
        assert_cast<ColumnInt32 &>(column).getData().push_back(x);

    }
    else
    {
        ExtendedDayNum x;
        readCSV(x, istr);
        checkDataOverflow(settings);
        assert_cast<ColumnInt32 &>(column).getData().push_back(x);
    }
}
}
