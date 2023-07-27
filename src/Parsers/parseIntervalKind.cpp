#include <Parsers/parseIntervalKind.h>
#include <Parsers/CommonParsers.h>
#include <Common/IntervalKind.h>


namespace DB
{
bool parseIntervalKind(IParser::Pos & pos, Expected & expected, IntervalKind & result)
{
    if (ParserKeyword("SECOND").ignore(pos, expected) || ParserKeyword("SQL_TSI_SECOND").ignore(pos, expected)
        || ParserKeyword("SS").ignore(pos, expected) || ParserKeyword("S").ignore(pos, expected))
    {
        result = IntervalKind::Second;
        return true;
    }

    if (ParserKeyword("MINUTE").ignore(pos, expected) || ParserKeyword("SQL_TSI_MINUTE").ignore(pos, expected)
        || ParserKeyword("MI").ignore(pos, expected) || ParserKeyword("N").ignore(pos, expected))
    {
        result = IntervalKind::Minute;
        return true;
    }

    if (ParserKeyword("HOUR").ignore(pos, expected) || ParserKeyword("SQL_TSI_HOUR").ignore(pos, expected)
        || ParserKeyword("HH").ignore(pos, expected))
    {
        result = IntervalKind::Hour;
        return true;
    }

    if (ParserKeyword("DAY").ignore(pos, expected) || ParserKeyword("SQL_TSI_DAY").ignore(pos, expected)
        || ParserKeyword("DD").ignore(pos, expected) || ParserKeyword("D").ignore(pos, expected))
    {
        result = IntervalKind::Day;
        return true;
    }

    if (ParserKeyword("WEEK").ignore(pos, expected) || ParserKeyword("SQL_TSI_WEEK").ignore(pos, expected)
        || ParserKeyword("WK").ignore(pos, expected) || ParserKeyword("WW").ignore(pos, expected))
    {
        result = IntervalKind::Week;
        return true;
    }

    if (ParserKeyword("MONTH").ignore(pos, expected) || ParserKeyword("SQL_TSI_MONTH").ignore(pos, expected)
        || ParserKeyword("MM").ignore(pos, expected) || ParserKeyword("M").ignore(pos, expected))
    {
        result = IntervalKind::Month;
        return true;
    }

    if (ParserKeyword("QUARTER").ignore(pos, expected) || ParserKeyword("SQL_TSI_QUARTER").ignore(pos, expected)
        || ParserKeyword("QQ").ignore(pos, expected) || ParserKeyword("Q").ignore(pos, expected))
    {
        result = IntervalKind::Quarter;
        return true;
    }

    if (ParserKeyword("YEAR").ignore(pos, expected) || ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected))
    {
        result = IntervalKind::Year;
        return true;
    }

    if (ParserKeyword("MINUTE_SECOND").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::MinuteSecond;
        return true;
    }

    if (ParserKeyword("HOUR_SECOND").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::HourSecond;
        return true;
    }

    if (ParserKeyword("HOUR_MINUTE").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::HourMinute;
        return true;
    }

    if (ParserKeyword("DAY_SECOND").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::DaySecond;
        return true;
    }

    if (ParserKeyword("DAY_MINUTE").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::DayMinute;
        return true;
    }

    if (ParserKeyword("DAY_HOUR").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::DayHour;
        return true;
    }

    if (ParserKeyword("YEAR_MONTH").ignore(pos, expected) /*|| ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
        || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected)*/)
    {
        result = IntervalKind::YearMonth;
        return true;
    }

    return false;
}
}
