#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/IParserBase.h>

namespace DB
{

/** Parse specified keyword such as SELECT or compound keyword such as ORDER BY.
  * All case insensitive. Requires word boundary.
  * For compound keywords, any whitespace characters and comments could be in the middle.
  */
/// Example: ORDER/* Hello */BY
class ParserKeyword : public IParserBase
{
private:
    const char * s;

public:
    ParserKeyword(const char * s_);

protected:
    const char * getName() const override;

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserToken : public IParserBase
{
private:
    TokenType token_type;
public:
    ParserToken(TokenType token_type_) : token_type(token_type_) {}
protected:
    const char * getName() const override { return "token"; }

    bool parseImpl(Pos & pos, ASTPtr & /*node*/, Expected & expected) override
    {
        if (pos->type != token_type)
        {
            expected.add(pos, getTokenName(token_type));
            return false;
        }
        ++pos;
        return true;
    }
};


// Parser always returns true and do nothing.
class ParserNothing : public IParserBase
{
public:
    const char * getName() const override { return "nothing"; }

    bool parseImpl(Pos & /*pos*/, ASTPtr & /*node*/, Expected & /*expected*/) override { return true; }
};

class ParserInterval : public IParserBase
{
public:
    enum class IntervalKind
    {
        Incorrect,
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month,
        Quarter,
        Year
    };

    IntervalKind interval_kind;

    ParserInterval() : interval_kind(IntervalKind::Incorrect) { }

    const char * getToIntervalKindFunctionName() const
    {
        switch (interval_kind)
        {
            case ParserInterval::IntervalKind::Second:
                return "toIntervalSecond";
            case ParserInterval::IntervalKind::Minute:
                return "toIntervalMinute";
            case ParserInterval::IntervalKind::Hour:
                return "toIntervalHour";
            case ParserInterval::IntervalKind::Day:
                return "toIntervalDay";
            case ParserInterval::IntervalKind::Week:
                return "toIntervalWeek";
            case ParserInterval::IntervalKind::Month:
                return "toIntervalMonth";
            case ParserInterval::IntervalKind::Quarter:
                return "toIntervalQuarter";
            case ParserInterval::IntervalKind::Year:
                return "toIntervalYear";
            default:
                return nullptr;
        }
    }

protected:
    const char * getName() const override { return "interval"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        auto convert_node = std::make_shared<ASTFunction>();
        if (ParserKeyword("SECOND").ignore(pos, expected) || ParserKeyword("SQL_TSI_SECOND").ignore(pos, expected)
            || ParserKeyword("SS").ignore(pos, expected) || ParserKeyword("S").ignore(pos, expected))
            interval_kind = IntervalKind::Second;
        else if (
            ParserKeyword("MINUTE").ignore(pos, expected) || ParserKeyword("SQL_TSI_MINUTE").ignore(pos, expected)
            || ParserKeyword("MI").ignore(pos, expected) || ParserKeyword("N").ignore(pos, expected))
            interval_kind = IntervalKind::Minute;
        else if (
            ParserKeyword("HOUR").ignore(pos, expected) || ParserKeyword("SQL_TSI_HOUR").ignore(pos, expected)
            || ParserKeyword("HH").ignore(pos, expected))
            interval_kind = IntervalKind::Hour;
        else if (
            ParserKeyword("DAY").ignore(pos, expected) || ParserKeyword("SQL_TSI_DAY").ignore(pos, expected)
            || ParserKeyword("DD").ignore(pos, expected) || ParserKeyword("D").ignore(pos, expected))
            interval_kind = IntervalKind::Day;
        else if (
            ParserKeyword("WEEK").ignore(pos, expected) || ParserKeyword("SQL_TSI_WEEK").ignore(pos, expected)
            || ParserKeyword("WK").ignore(pos, expected) || ParserKeyword("WW").ignore(pos, expected))
            interval_kind = IntervalKind::Week;
        else if (
            ParserKeyword("MONTH").ignore(pos, expected) || ParserKeyword("SQL_TSI_MONTH").ignore(pos, expected)
            || ParserKeyword("MM").ignore(pos, expected) || ParserKeyword("M").ignore(pos, expected))
            interval_kind = IntervalKind::Month;
        else if (
            ParserKeyword("QUARTER").ignore(pos, expected) || ParserKeyword("SQL_TSI_QUARTER").ignore(pos, expected)
            || ParserKeyword("QQ").ignore(pos, expected) || ParserKeyword("Q").ignore(pos, expected))
            interval_kind = IntervalKind::Quarter;
        else if (
            ParserKeyword("YEAR").ignore(pos, expected) || ParserKeyword("SQL_TSI_YEAR").ignore(pos, expected)
            || ParserKeyword("YYYY").ignore(pos, expected) || ParserKeyword("YY").ignore(pos, expected))
            interval_kind = IntervalKind::Year;
        else if (
            ParserKeyword("MINUTE_SECOND").ignore(pos, expected) || ParserKeyword("MINUTESECOND").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Second;
            convert_node->name = "convertMinuteSecondToSecond";
        }
        else if (
            ParserKeyword("HOUR_SECOND").ignore(pos, expected) || ParserKeyword("HOURSECOND").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Second;
            convert_node->name = "convertHourSecondToSecond";
        }
        else if (
            ParserKeyword("HOUR_MINUTE").ignore(pos, expected) || ParserKeyword("HOURMINUTE").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Minute;
            convert_node->name = "convertHourMinuteToMinute";
        }
        else if (
            ParserKeyword("DAY_SECOND").ignore(pos, expected) || ParserKeyword("DAYSECOND").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Second;
            convert_node->name = "convertDaySecondToSecond";
        }
        else if (
            ParserKeyword("DAY_MINUTE").ignore(pos, expected) || ParserKeyword("DAYMINUTE").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Minute;
            convert_node->name = "convertDayMinuteToMinute";
        }
        else if (
            ParserKeyword("DAY_HOUR").ignore(pos, expected) || ParserKeyword("DAYHOUR").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Hour;
            convert_node->name = "convertDayHourToHour";
        }
        else if (
            ParserKeyword("YEAR_MONTH").ignore(pos, expected) || ParserKeyword("YEARMONTH").ignore(pos, expected))
        {
            interval_kind = IntervalKind::Month;
            convert_node->name = "convertYearMonthToMonth";
        }
        else
            interval_kind = IntervalKind::Incorrect;
        
        if (!convert_node->name.empty())
            node = std::move(convert_node);

        if (interval_kind == IntervalKind::Incorrect)
        {
            expected.add(pos, "YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE or SECOND");
            return false;
        }
        /// one of ParserKeyword already made ++pos
        return true;
    }
};
}
