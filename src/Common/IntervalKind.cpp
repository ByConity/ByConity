#include <Common/IntervalKind.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

const char * IntervalKind::toString() const
{
    switch (kind)
    {
        case IntervalKind::Second: return "Second";
        case IntervalKind::Minute: return "Minute";
        case IntervalKind::Hour: return "Hour";
        case IntervalKind::Day: return "Day";
        case IntervalKind::Week: return "Week";
        case IntervalKind::Month: return "Month";
        case IntervalKind::Quarter: return "Quarter";
        case IntervalKind::Year: return "Year";
        case IntervalKind::MinuteSecond:
            return "MinuteSecond";
        case IntervalKind::HourSecond:
            return "HourSecond";
        case IntervalKind::HourMinute:
            return "HourMinute";
        case IntervalKind::DaySecond:
            return "DaySecond";
        case IntervalKind::DayMinute:
            return "DayMinute";
        case IntervalKind::DayHour:
            return "DayHour";
        case IntervalKind::YearMonth:
            return "YearMonth";
    }
    __builtin_unreachable();
}


Int32 IntervalKind::toAvgSeconds() const
{
    switch (kind)
    {
        case IntervalKind::Second: return 1;
        case IntervalKind::Minute: return 60;
        case IntervalKind::Hour: return 3600;
        case IntervalKind::Day: return 86400;
        case IntervalKind::Week: return 604800;
        case IntervalKind::Month: return 2629746;   /// Exactly 1/12 of a year.
        case IntervalKind::Quarter: return 7889238; /// Exactly 1/4 of a year.
        case IntervalKind::Year: return 31556952;   /// The average length of a Gregorian year is equal to 365.2425 days
        case IntervalKind::MinuteSecond:
        case IntervalKind::HourSecond:
        case IntervalKind::HourMinute:
        case IntervalKind::DaySecond:
        case IntervalKind::DayMinute:
        case IntervalKind::DayHour:
        case IntervalKind::YearMonth:
            throw Exception("The method toAvgSeconds is not supported for {}", kind, ErrorCodes::UNSUPPORTED_METHOD);
    }
    __builtin_unreachable();
}


IntervalKind IntervalKind::fromAvgSeconds(Int64 num_seconds)
{
    if (num_seconds)
    {
        if (!(num_seconds % 31556952))
            return IntervalKind::Year;
        if (!(num_seconds % 7889238))
            return IntervalKind::Quarter;
        if (!(num_seconds % 2629746))
            return IntervalKind::Month;
        if (!(num_seconds % 604800))
            return IntervalKind::Week;
        if (!(num_seconds % 86400))
            return IntervalKind::Day;
        if (!(num_seconds % 3600))
            return IntervalKind::Hour;
        if (!(num_seconds % 60))
            return IntervalKind::Minute;
    }
    return IntervalKind::Second;
}


const char * IntervalKind::toKeyword() const
{
    switch (kind)
    {
        case IntervalKind::Second: return "SECOND";
        case IntervalKind::Minute: return "MINUTE";
        case IntervalKind::Hour: return "HOUR";
        case IntervalKind::Day: return "DAY";
        case IntervalKind::Week: return "WEEK";
        case IntervalKind::Month: return "MONTH";
        case IntervalKind::Quarter: return "QUARTER";
        case IntervalKind::Year: return "YEAR";
        case IntervalKind::MinuteSecond:
            return "MINUTE_SECOND";
        case IntervalKind::HourSecond:
            return "HOUR_SECOND";
        case IntervalKind::HourMinute:
            return "HOUR_MINUTE";
        case IntervalKind::DaySecond:
            return "DAY_SECOND";
        case IntervalKind::DayMinute:
            return "DAY_MINUTE";
        case IntervalKind::DayHour:
            return "DAY_HOUR";
        case IntervalKind::YearMonth:
            return "YEAR_MONTH";
    }
    __builtin_unreachable();
}


const char * IntervalKind::toLowercasedKeyword() const
{
    switch (kind)
    {
        case IntervalKind::Second: return "second";
        case IntervalKind::Minute: return "minute";
        case IntervalKind::Hour: return "hour";
        case IntervalKind::Day: return "day";
        case IntervalKind::Week: return "week";
        case IntervalKind::Month: return "month";
        case IntervalKind::Quarter: return "quarter";
        case IntervalKind::Year: return "year";
        case IntervalKind::MinuteSecond:
            return "minute_second";
        case IntervalKind::HourSecond:
            return "hour_second";
        case IntervalKind::HourMinute:
            return "hour_minute";
        case IntervalKind::DaySecond:
            return "day_second";
        case IntervalKind::DayMinute:
            return "day_minute";
        case IntervalKind::DayHour:
            return "day_hour";
        case IntervalKind::YearMonth:
            return "year_month";
    }
    __builtin_unreachable();
}


const char * IntervalKind::toDateDiffUnit() const
{
    switch (kind)
    {
        case IntervalKind::Second:
            return "second";
        case IntervalKind::Minute:
            return "minute";
        case IntervalKind::Hour:
            return "hour";
        case IntervalKind::Day:
            return "day";
        case IntervalKind::Week:
            return "week";
        case IntervalKind::Month:
            return "month";
        case IntervalKind::Quarter:
            return "quarter";
        case IntervalKind::Year:
            return "year";
        case IntervalKind::MinuteSecond:
            return "minute_second";
        case IntervalKind::HourSecond:
            return "hour_second";
        case IntervalKind::HourMinute:
            return "hour_minute";
        case IntervalKind::DaySecond:
            return "day_second";
        case IntervalKind::DayMinute:
            return "day_minute";
        case IntervalKind::DayHour:
            return "day_hour";
        case IntervalKind::YearMonth:
            return "year_month";
    }
    __builtin_unreachable();
}


const char * IntervalKind::toNameOfFunctionToIntervalDataType() const
{
    switch (kind)
    {
        case IntervalKind::Second:
            return "toIntervalSecond";
        case IntervalKind::Minute:
            return "toIntervalMinute";
        case IntervalKind::Hour:
            return "toIntervalHour";
        case IntervalKind::Day:
            return "toIntervalDay";
        case IntervalKind::Week:
            return "toIntervalWeek";
        case IntervalKind::Month:
            return "toIntervalMonth";
        case IntervalKind::Quarter:
            return "toIntervalQuarter";
        case IntervalKind::Year:
            return "toIntervalYear";
        case IntervalKind::MinuteSecond:
            return "toIntervalMinuteSecond";
        case IntervalKind::HourSecond:
            return "toIntervalHourSecond";
        case IntervalKind::HourMinute:
            return "toIntervalHourMinute";
        case IntervalKind::DaySecond:
            return "toIntervalDaySecond";
        case IntervalKind::DayMinute:
            return "toIntervalDay_Minute";
        case IntervalKind::DayHour:
            return "toIntervalDay_Hour";
        case IntervalKind::YearMonth:
            return "toIntervalYear_Month";
    }
    __builtin_unreachable();
}


const char * IntervalKind::toNameOfFunctionExtractTimePart() const
{
    switch (kind)
    {
        case IntervalKind::Second:
            return "toSecond";
        case IntervalKind::Minute:
            return "toMinute";
        case IntervalKind::Hour:
            return "toHour";
        case IntervalKind::Day:
            return "toDayOfMonth";
        case IntervalKind::Week:
            // TODO: SELECT toRelativeWeekNum(toDate('2017-06-15')) - toRelativeWeekNum(toStartOfYear(toDate('2017-06-15')))
            // else if (ParserKeyword("WEEK").ignore(pos, expected))
            //    function_name = "toRelativeWeekNum";
            throw Exception("The syntax 'EXTRACT(WEEK FROM date)' is not supported, cannot extract the number of a week", ErrorCodes::SYNTAX_ERROR);
        case IntervalKind::Month:
            return "toMonth";
        case IntervalKind::Quarter:
            return "toQuarter";
        case IntervalKind::Year:
            return "toYear";
        case IntervalKind::MinuteSecond:
            return "toMinuteSecond";
        case IntervalKind::HourSecond:
            return "toHourSecond";
        case IntervalKind::HourMinute:
            return "toHourMinute";
        case IntervalKind::DaySecond:
            return "toDaySecond";
        case IntervalKind::DayMinute:
            return "toDayMinute";
        case IntervalKind::DayHour:
            return "toDayHour";
        case IntervalKind::YearMonth:
            return "toYearMonth";
    }
    __builtin_unreachable();
}


bool IntervalKind::tryParseString(const std::string & kind, IntervalKind::Kind & result)
{
    if ("second" == kind)
    {
        result = IntervalKind::Second;
        return true;
    }
    if ("minute" == kind)
    {
        result = IntervalKind::Minute;
        return true;
    }
    if ("hour" == kind)
    {
        result = IntervalKind::Hour;
        return true;
    }
    if ("day" == kind)
    {
        result = IntervalKind::Day;
        return true;
    }
    if ("week" == kind)
    {
        result = IntervalKind::Week;
        return true;
    }
    if ("month" == kind)
    {
        result = IntervalKind::Month;
        return true;
    }
    if ("quarter" == kind)
    {
        result = IntervalKind::Quarter;
        return true;
    }
    if ("year" == kind)
    {
        result = IntervalKind::Year;
        return true;
    }
    if ("minute_second" == kind)
    {
        result = IntervalKind::MinuteSecond;
        return true;
    }
    if ("hour_second" == kind)
    {
        result = IntervalKind::HourSecond;
        return true;
    }
    if ("hour_minute" == kind)
    {
        result = IntervalKind::HourMinute;
        return true;
    }
    if ("day_second" == kind)
    {
        result = IntervalKind::DaySecond;
        return true;
    }
    if ("day_minute" == kind)
    {
        result = IntervalKind::DayMinute;
        return true;
    }
    if ("day_hour" == kind)
    {
        result = IntervalKind::DayHour;
        return true;
    }
    if ("year_month" == kind)
    {
        result = IntervalKind::YearMonth;
        return true;
    }
    return false;
}
}
