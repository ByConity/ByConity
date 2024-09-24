#include <DataTypes/Serializations/SerializationDate.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>

#include <Common/assert_cast.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

void SerializationDate::checkDataOverflow(const FormatSettings & settings)
{
    if (!settings.check_data_overflow)
        return;

    if (!current_thread || !current_thread->getOverflow(ThreadStatus::OverflowFlag::Date))
        return;

    current_thread->unsetOverflow(ThreadStatus::OverflowFlag::Date);
    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Under MYSQL dialect or check_data_overflow = 1, the Date value should be within [1970-01-01, 2149-06-06]");
}

void SerializationDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(DayNum(assert_cast<const ColumnUInt16 &>(column).getData()[row_num]), ostr, time_zone);
}

void SerializationDate::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void SerializationDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    readDateText(x, istr, time_zone);
    checkDataOverflow(settings);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void SerializationDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    assertChar('\'', istr);
    readDateText(x, istr, time_zone);
    assertChar('\'', istr);
    checkDataOverflow(settings);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    assertChar('"', istr);
    readDateText(x, istr, time_zone);
    assertChar('"', istr);
    checkDataOverflow(settings);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void SerializationDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (!settings.check_data_overflow)
    {
        LocalDate value;
        readCSV(value, istr);
        assert_cast<ColumnUInt16 &>(column).getData().push_back(value.getDayNum());
    }
    else
    {
        DayNum x;
        readCSV(x, istr);
        checkDataOverflow(settings);
        assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    }
}
}
