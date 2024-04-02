#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/ErrorCodes.h>
#include <Common/DateLUT.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

/// DateTime is stored as UInt32 for the seconds since unix epoch
/// Under mysql dialect the allowed range is [1970-01-01 00:00:00, 2106-02-07 06:28:15]
/// constexpr time_t MIN_DATETIME_TIMESTAMP = 0;
constexpr time_t MAX_DATETIME_TIMESTAMP = std::numeric_limits<uint32_t>::max();

inline void readText(time_t & x, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTimeText(x, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTimeBestEffort(x, istr, time_zone, utc_time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            parseDateTimeBestEffortUS(x, istr, time_zone, utc_time_zone);
            return;
    }
}

}

SerializationDateTime::SerializationDateTime(
    const DateLUTImpl & time_zone_, const DateLUTImpl & utc_time_zone_)
    : time_zone(time_zone_), utc_time_zone(utc_time_zone_)
{
}

void SerializationDateTime::checkDataOverflow(const time_t & x, const FormatSettings & settings)
{
    if (!settings.check_data_overflow || !current_thread)
        return;

    /// there is a special case: when year=0000, x is set to 0 and overflow bit is turned on
    if (std::make_unsigned_t<time_t>(x) <= MAX_DATETIME_TIMESTAMP && !current_thread->getOverflow(ThreadStatus::OverflowFlag::Date))
        return;

    current_thread->unsetOverflow(ThreadStatus::OverflowFlag::Date);
    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Under MYSQL dialect or check_data_overflow = 1, the DateTime value should be within UTC [1970-01-01 00:00:00, 2106-02-07 06:28:15]");
}

void SerializationDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    switch (settings.date_time_output_format)
    {
        case FormatSettings::DateTimeOutputFormat::Simple:
            writeDateTimeText(value, ostr, time_zone);
            return;
        case FormatSettings::DateTimeOutputFormat::UnixTimestamp:
            writeIntText(value, ostr);
            return;
        case FormatSettings::DateTimeOutputFormat::ISO:
            writeDateTimeTextISO(value, ostr, utc_time_zone);
            return;
    }
}

void SerializationDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDateTime::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void SerializationDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    readText(x, istr, settings, time_zone, utc_time_zone);
    checkDataOverflow(x, settings);
    if (x < 0)
        x = 0;
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void SerializationDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    checkDataOverflow(x, settings);
    if (x < 0)
        x = 0;
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDateTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (checkChar('"', istr))
    {
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    checkDataOverflow(x, settings);
    if (x < 0)
        x = 0;
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void SerializationDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    readText(x, istr, settings, time_zone, utc_time_zone);
    checkDataOverflow(x, settings);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    if (x < 0)
        x = 0;

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

}
