#include <DataTypes/Serializations/SerializationDateTime64.h>

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/DateLUT.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Common/CurrentThread.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

SerializationDateTime64::SerializationDateTime64(
    const DateLUTImpl & time_zone_, const DateLUTImpl & utc_time_zone_, UInt32 scale_)
    : SerializationDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_)
    , time_zone(time_zone_), utc_time_zone(utc_time_zone_)
{
}


void SerializationDateTime64::checkDateOverflow(DateTime64 & x, const FormatSettings & settings) const
{
    if (!settings.check_date_overflow || !current_thread)
        return;

    /// y is the seconds since 1970-01-01, negative for date before
    auto y = extractWholePart(x, scale);
    const time_t min_datetime_timestamp = utc_time_zone.makeDateTime(1900, 1, 1, 0, 0, 0);

    /// not overflow
    /// max side check is done during parsing and it would set has_truncated_date flag
    if (!current_thread->has_truncated_date && y >= min_datetime_timestamp)
        return;

    /// adjusted seconds
    time_t z = 0;
    DateTime64 fraction = 0;

    /// e.g., 1800-01-01 23:59:59 GMT+12 is truncated to 1900-01-01 23:59:59 GMT+12
    /// the timestamp is min_datetime_timestamp + DATE_SECONDS_PER_DAY * 3/2 - 1.
    /// here use DATE_SECONDS_PER_DAY * 2 just as a loose upper bound
    /// y == 0 is a special case when the year==0
    if (y <= (min_datetime_timestamp + DATE_SECONDS_PER_DAY * 2) || y == 0)
    {
        /// for 1900-01-01 00:00:00
        z = min_datetime_timestamp;
    }
    else
    {
        z = utc_time_zone.makeDateTime(2299, 12, 31, 23, 59, 59);
        /// for the fraction, make the time as 2299-12-31 23:59:59.999...
        fraction = common::exp10_i32(scale + 1) - 1;
    }

    if (settings.throw_on_date_overflow)
    {
        current_thread->has_truncated_date = false;
        throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Under MYSQL dialect or check_date_overflow = 1, the Datetime64 value should be within [1900-01-01 00:00:00, 2299-12-31 23:59:59.99...]");
    }

    DB::DecimalUtils::DecimalComponents<DateTime64> components{static_cast<DateTime64::NativeType>(z), fraction};
    x = DecimalUtils::decimalFromComponents<DateTime64>(components, scale);
    current_thread->has_truncated_date = in_serialization_nullable;
}

void SerializationDateTime64::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    switch (settings.date_time_output_format)
    {
        case FormatSettings::DateTimeOutputFormat::Simple:
            writeDateTimeText(value, scale, ostr, time_zone);
            return;
        case FormatSettings::DateTimeOutputFormat::UnixTimestamp:
            writeDateTimeUnixTimestamp(value, scale, ostr);
            return;
        case FormatSettings::DateTimeOutputFormat::ISO:
            writeDateTimeTextISO(value, scale, ostr, utc_time_zone);
            return;
    }
}

void SerializationDateTime64::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 result = 0;
    readDateTime64Text(result, scale, istr, time_zone);
    checkDateOverflow(result, settings);
    assert_cast<ColumnType &>(column).getData().push_back(result);
}

void SerializationDateTime64::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void SerializationDateTime64::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

static inline void readText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTime64Text(x, scale, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTime64BestEffort(x, scale, istr, time_zone, utc_time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            parseDateTime64BestEffortUS(x, scale, istr, time_zone, utc_time_zone);
            return;
    }
}

void SerializationDateTime64::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    readText(x, scale, istr, settings, time_zone, utc_time_zone);
    checkDateOverflow(x, settings);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void SerializationDateTime64::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDateTime64::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    checkDateOverflow(x, settings);
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDateTime64::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime64::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('"', istr))
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    checkDateOverflow(x, settings);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void SerializationDateTime64::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime64::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    readText(x, scale, istr, settings, time_zone, utc_time_zone);
    checkDateOverflow(x, settings);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

}
