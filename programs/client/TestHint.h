#pragma once

#include <memory>
#include <sstream>
#include <iostream>
#include <Core/Types.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Parsers/Lexer.h>
#include <charconv>
#include <string_view>
#include <unordered_set>

namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

namespace DB
{

/// Checks expected server and client error codes in --testmode.
///
/// The following comment hints are supported:
///
/// - "-- { serverError 60 }" -- in case of you are expecting server error.
///
/// - "-- { clientError 20 }" -- in case of you are expecting client error.
///
///   Remember that the client parse the query first (not the server), so for
///   example if you are expecting syntax error, then you should use
///   clientError not serverError.
///
/// Examples:
///
/// - echo 'select / -- { clientError 62 }' | clickhouse-client --testmode -nm
///
//    Here the client parses the query but it is incorrect, so it expects
///   SYNTAX_ERROR (62).
///
/// - echo 'select foo -- { serverError 47 }' | clickhouse-client --testmode -nm
///
///   But here the query is correct, but there is no such column "foo", so it
///   is UNKNOWN_IDENTIFIER server error.
///
/// The following hints will control the query echo mode (i.e print each query):
///
/// - "-- { echo }"
/// - "-- { echoOn }"
/// - "-- { echoOff }"
class TestHint
{
public:
    using ErrorVector = std::vector<int>;
    TestHint(bool enabled_, const String & query_) :
        query(query_)
    {
        if (!enabled_)
            return;

        // Don't parse error hints in leading comments, because it feels weird.
        // Leading 'echo' hint is OK.
        bool is_leading_hint = true;

        Lexer lexer(query.data(), query.data() + query.size());

        for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        {
            if (token.type != TokenType::Comment
                && token.type != TokenType::Whitespace)
            {
                is_leading_hint = false;
            }
            else if (token.type == TokenType::Comment)
            {
                String comment(token.begin, token.begin + token.size());

                if (!comment.empty())
                {
                    size_t pos_start = comment.find('{', 0);
                    if (pos_start != String::npos)
                    {
                        size_t pos_end = comment.find('}', pos_start);
                        if (pos_end != String::npos)
                        {
                            Lexer comment_lexer(comment.c_str() + pos_start + 1, comment.c_str() + pos_end, 0);
                            parse(comment_lexer, is_leading_hint);
                        }
                    }
                }
            }
        }
    }

    int serverError() const { return server_errors.empty() ? 0 : server_errors[0]; }
    int clientError() const { return client_errors.empty() ? 0 : client_errors[0]; }
    std::optional<bool> echoQueries() const { return echo; }

private:
    const String & query;
    ErrorVector server_errors{};
    ErrorVector client_errors{};
    std::optional<bool> echo;

    void parse(Lexer & comment_lexer, bool is_leading_hint)
    {
        std::unordered_set<std::string_view> commands{"echo", "echoOn", "echoOff"};

        std::unordered_set<std::string_view> command_errors{
            "serverError",
            "clientError",
        };

        for (Token token = comment_lexer.nextToken(); !token.isEnd(); token = comment_lexer.nextToken())
        {
            String item = String(token.begin, token.end);
            if (token.type == TokenType::BareWord && commands.contains(item))
            {
                if (item == "echo")
                    echo.emplace(true);
                if (item == "echoOn")
                    echo.emplace(true);
                if (item == "echoOff")
                    echo.emplace(false);
            }
            else if (!is_leading_hint && token.type == TokenType::BareWord && command_errors.contains(item))
            {
                /// Everything after this must be a list of errors separated by comma
                ErrorVector error_codes;
                while (!token.isEnd())
                {
                    token = comment_lexer.nextToken();
                    if (token.type == TokenType::Whitespace)
                        continue;
                    if (token.type == TokenType::Number)
                    {
                        int code;
                        auto [p, ec] = std::from_chars(token.begin, token.end, code);
                        if (p == token.begin)
                            throw DB::Exception(
                                DB::ErrorCodes::CANNOT_PARSE_TEXT,
                                "Could not parse integer number for errorcode: {}",
                                String(token.begin, token.end));
                        error_codes.push_back(code);
                    }
                    else if (token.type == TokenType::BareWord)
                    {
                        int code = DB::ErrorCodes::getErrorCodeByName(String(token.begin, token.end));
                        error_codes.push_back(code);
                    }
                    else
                        throw DB::Exception(
                            DB::ErrorCodes::CANNOT_PARSE_TEXT,
                            "Could not parse error code in {}: {}",
                            getTokenName(token.type),
                            String(token.begin, token.end));
                    do
                    {
                        token = comment_lexer.nextToken();
                    } while (!token.isEnd() && token.type == TokenType::Whitespace);

                    if (!token.isEnd() && token.type != TokenType::Comma)
                        throw DB::Exception(
                            DB::ErrorCodes::CANNOT_PARSE_TEXT,
                            "Could not parse error code. Expected ','. Got '{}'",
                            String(token.begin, token.end));
                }

                if (item == "serverError")
                    server_errors = error_codes;
                else
                    client_errors = error_codes;
                break;
            }
        }
    }

    bool allErrorsExpected(int actual_server_error, int actual_client_error) const
    {
        if (actual_server_error && std::find(server_errors.begin(), server_errors.end(), actual_server_error) == server_errors.end())
            return false;
        if (!actual_server_error && server_errors.size())
            return false;

        if (actual_client_error && std::find(client_errors.begin(), client_errors.end(), actual_client_error) == client_errors.end())
            return false;
        if (!actual_client_error && client_errors.size())
            return false;

        return true;
    }

    bool lostExpectedError(int actual_server_error, int actual_client_error) const
    {
        return (server_errors.size() && !actual_server_error) || (client_errors.size() && !actual_client_error);
    }
};

}
