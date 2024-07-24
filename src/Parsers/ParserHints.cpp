#include <Parsers/ASTSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserHints.h>
#include <Parsers/IParser.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/IAST.h>

#include <string>

namespace DB
{

void ParserHints::parse(IParser::Pos & pos, SqlHints & hints, Expected & expected)
{
    auto * token = pos.getLastInsignificantToken();
    if (!token || token->type != TokenType::Comment || !(*token->begin == '/' && *(token->begin + 1) == '*'))
        return;

    Tokens tokens(token->begin + 2, token->end - 2);
    IParser::Pos hints_pos(tokens, 0);
    if (!ParserToken(TokenType::Plus).ignore(hints_pos, expected))
        return;

    bool first = true;
    while (true)
    {
        if (first)
        {
            if(!parseHint(hints_pos, hints, expected))
                return;
            first = false;
        }
        else
        {
            if (!ParserToken(TokenType::Comma).ignore(hints_pos, expected))
                break;

            if(!parseHint(hints_pos, hints, expected))
                return;
        }
    }
}

bool ParserHints::parseHint(IParser::Pos & pos, SqlHints & hints, Expected & expected)
{
    String hint_name;
    if (!parsehintName(pos, hint_name, expected))
        return false;

    SqlHint hint{hint_name};
    auto begin = pos;
    if (!parseHintOptions(pos, hint, expected))
    {
        pos = begin;
        if (Poco::toLower(hint_name) != "leading" || !parseJoinLevelOptions(pos, hint, expected))
            return false;
    }

    hints.emplace_back(hint);
    return true;
}

bool ParserHints::parseJoinLevelOptions(IParser::Pos & pos, SqlHint & hint, Expected & expected)
{
    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        return false;

    if (!parseJoinPair(pos, hint, expected))
        return false;

    if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
        return false;

    return true;
}

bool ParserHints::parseJoinPair(IParser::Pos & pos, SqlHint & hint, Expected & expected)
{
    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        return false;
    hint.setOption(String("("));

    std::string name1;
    if (pos->type == TokenType::OpeningRoundBracket)
        parseJoinPair(pos, hint, expected);
    else if (!parseOptionName(pos, name1, expected))
        return false;

    if (!name1.empty())
        hint.setOption(name1);

    if (!ParserToken(TokenType::Comma).ignore(pos, expected))
        return false;
    hint.setOption(String(","));

    std::string name2;
    if (pos->type == TokenType::OpeningRoundBracket)
        parseJoinPair(pos, hint, expected);
    else if (!parseOptionName(pos, name2, expected))
        return false;

    if (!name2.empty())
        hint.setOption(name2);

    if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
        return false;
    hint.setOption(String(")"));

    return true;
}

bool ParserHints::parseHintOptions(IParser::Pos & pos, SqlHint & hint, Expected & expected)
{
    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        return true;
    bool first = true;
    bool is_kv_option = false;
    while (true)
    {
        if (first)
        {
            if(!parseOption(pos, hint, first, is_kv_option, expected))
                return false;

            first = false;
        }
        else
        {
            if (!ParserToken(TokenType::Comma).ignore(pos, expected))
                break;

            if(!parseOption(pos, hint, first, is_kv_option, expected))
                return false;
        }
    }
    if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
        return false;

    return true;
}

bool ParserHints::parseOption(IParser::Pos & pos, SqlHint & hint, bool & first, bool & is_kv_option, Expected & expected)
{
    String key_name;
    if (!parseOptionName(pos, key_name, expected))
        return false;

    if (first && pos->type == TokenType::Equals)
        is_kv_option = true;

    if (is_kv_option)
    {
        if (pos->type != TokenType::Equals)
            return false;
        ++pos;
        String value_name;
        if (!parseOptionName(pos, value_name, expected))
            return false;

        hint.setKvOption(key_name, value_name);
    }
    else
        hint.setOption(key_name);

    return true;
}

bool ParserHints::parsehintName(IParser::Pos & pos, String & name, Expected &)
{
    if (pos->type == TokenType::BackQuotedIdentifier || pos->type == TokenType::DoubleQuotedIdentifier)
    {
        ReadBufferFromMemory buf(pos->begin, pos->size());

        if (*pos->begin == '`')
            readBackQuotedStringWithSQLStyle(name, buf);
        else
            readDoubleQuotedStringWithSQLStyle(name, buf);

        if (name.empty())    /// Identifiers "empty string" are not allowed.
            return false;

        ++pos;
        return true;
    }
    else if (pos->type == TokenType::BareWord)
    {
        name = String(pos->begin, pos->end);
        ++pos;
        return true;
    }
    return false;
}

bool ParserHints::parseOptionName(IParser::Pos & pos, String & name, Expected & )
{
    /// Identifier in backquotes or in double quotes
    if (pos->type == TokenType::BackQuotedIdentifier || pos->type == TokenType::DoubleQuotedIdentifier)
    {
        ReadBufferFromMemory buf(pos->begin, pos->size());

        if (*pos->begin == '`')
            readBackQuotedStringWithSQLStyle(name, buf);
        else
            readDoubleQuotedStringWithSQLStyle(name, buf);

        if (name.empty())
            return false;
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::StringLiteral)
    {
        /// Identifier single quote
        ReadBufferFromMemory buf(pos->begin, pos->size());
        readQuotedStringWithSQLStyle(name, buf);

        if (name.empty())
            return false;
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::BareWord)
    {
        name = String(pos->begin, pos->end);

        if (name.empty())
            return false;
        ++pos;
        return true;
    }
    return false;
}
}
