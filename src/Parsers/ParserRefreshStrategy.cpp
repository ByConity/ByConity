#include <Parsers/ParserRefreshStrategy.h>

#include <Parsers/ASTRefreshStrategy.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTimeInterval.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool ParserRefreshStrategy::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto refresh = std::make_shared<ASTRefreshStrategy>();

    if (ParserKeyword{"ASYNC"}.ignore(pos, expected))
    {
        refresh->schedule_kind = RefreshScheduleKind::ASYNC;

        ParserToken s_lparen(TokenType::OpeningRoundBracket);
        ParserToken s_rparen(TokenType::ClosingRoundBracket);

        /// Start time is optional
        if (ParserKeyword{"START"}.ignore(pos, expected))
        {
            if (!s_lparen.ignore(pos, expected))
                return false;

            ASTPtr start_time;
            ParserStringLiteral parser_start_time_literal;
            if (!parser_start_time_literal.parse(pos, start_time, expected))
                return false;
            
            refresh->set(refresh->start, start_time);

            if (!s_rparen.ignore(pos, expected))
                return false;
        }
        
        /// every time interval is required
        if (!ParserKeyword{"EVERY"}.ignore(pos, expected))
            return false;
        
        if (!s_lparen.ignore(pos, expected))
            return false;

        if (!ParserKeyword("INTERVAL").ignore(pos, expected))
            return false;

        ASTPtr time_interval;
        if (!ParserTimeInterval{{.allow_mixing_calendar_and_clock_units = false, .allow_one_interval = true, 
             .allow_refresh_interval_unit = true}}.parse(pos, time_interval, expected))
            return false;
        refresh->set(refresh->time_interval, time_interval);

        if (!s_rparen.ignore(pos, expected))
            return false;
    }
    else if (ParserKeyword{"SYNC"}.ignore(pos, expected))
    {
        refresh->schedule_kind = RefreshScheduleKind::SYNC;
    }
    else if (ParserKeyword{"MANUAL"}.ignore(pos, expected))
    {
        refresh->schedule_kind = RefreshScheduleKind::MANUAL;
    }

    node = refresh;
    return true;
}

}
