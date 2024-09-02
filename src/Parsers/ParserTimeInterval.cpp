#include <Parsers/ParserTimeInterval.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIntervalKind.h>

#include <Parsers/ASTTimeInterval.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

ParserTimeInterval::ParserTimeInterval(Options opt) : options(opt) {}
ParserTimeInterval::ParserTimeInterval() = default;

bool ParserTimeInterval::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    CalendarTimeInterval::Intervals intervals;
    while (true)
    {
        ASTPtr value;
        IntervalKind kind;
        if (!ParserNumber{}.parse(pos, value, expected))
            break;
        if (!parseIntervalKind(pos, expected, kind))
            return false;

        UInt64 val;
        if (!value->as<ASTLiteral &>().value.tryGet(val))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Time interval must be an integer");
        intervals.emplace_back(kind, val);
    }

    if (intervals.empty())
        return false;

    if (options.allow_one_interval && intervals.size() != 1)
        return true;

    CalendarTimeInterval interval(intervals);

    if (!options.allow_zero)
        interval.assertPositive();
    if (!options.allow_mixing_calendar_and_clock_units)
        interval.assertSingleUnit();
    if (!options.allow_refresh_interval_unit)
        interval.assertRefreshUnit();

    auto time_interval = std::make_shared<ASTTimeInterval>();
    time_interval->interval = interval;

    node = time_interval;
    return true;
}

}
