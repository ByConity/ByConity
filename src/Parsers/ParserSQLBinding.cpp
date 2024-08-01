#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTSQLBinding.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSQLBinding.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{
bool ParserCreateBinding::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_global("GLOBAL");
    ParserKeyword s_session("SESSION");
    ParserKeyword s_binding("BINDING");
    ParserKeyword s_using("USING");
    ParserKeyword s_settings("SETTINGS");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_or_replace("OR REPLACE");

    String query_pattern;
    String re_expression;
    ASTPtr pattern;
    ASTPtr target;
    ASTPtr settings;
    bool if_not_exists = false;
    bool or_replace = false;

    BindingLevel level;
    if (!s_create.ignore(pos, expected))
        return false;

    if (s_session.ignore(pos, expected))
        level = BindingLevel::SESSION;
    else if (s_global.ignore(pos, expected))
        level = BindingLevel::GLOBAL;
    else
        level = BindingLevel::SESSION;

    if (!s_binding.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;
    else if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    ParserSelectWithUnionQuery select_p(dt);
    const auto * begin = pos->begin;
    if (pos->type == TokenType::StringLiteral || pos->type == TokenType::DoubleQuotedIdentifier)
    {
        /// Identifier single quot
        ReadBufferFromMemory buf(pos->begin, pos->size());
        if (pos->type == TokenType::StringLiteral)
            readQuotedStringWithSQLStyle(re_expression, buf);
        else
            readDoubleQuotedStringWithSQLStyle(re_expression, buf);

        if (re_expression.empty())
            return false;
        ++pos;

        /// SETTINGS key1 = value1, key2 = value2, ...
        if (!s_settings.ignore(pos, expected))
            return false;

        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, settings, expected))
            return false;
    }
    else if (select_p.parse(pos, pattern, expected))
    {
        query_pattern = String(begin, pos->begin - begin);
        if (!s_using.ignore(pos, expected))
            return false;

        if (!select_p.parse(pos, target, expected))
            return false;
    }
    else
        return false;

    auto create_binding_query = std::make_shared<ASTCreateBinding>();
    create_binding_query->level = level;
    create_binding_query->query_pattern = query_pattern;
    create_binding_query->re_expression = re_expression;
    create_binding_query->pattern = pattern;
    create_binding_query->target = target;
    create_binding_query->settings = settings;
    create_binding_query->if_not_exists = if_not_exists;
    create_binding_query->or_replace = or_replace;

    node = create_binding_query;
    return true;
}

bool ParserShowBindings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_bindings("BINDINGS");
    if (s_show.ignore(pos, expected) && s_bindings.ignore(pos, expected))
    {
        node = std::make_shared<ASTShowBindings>();
        return true;
    }
    return false;
}

bool ParserDropBinding::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_global("GLOBAL");
    ParserKeyword s_session("SESSION");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_binding("BINDING");
    ParserKeyword s_uuid("UUID");

    bool drop_uuid = false;
    String str_value;
    ASTPtr binding_pattern;
    bool if_exists = false;

    BindingLevel level;
    if (!s_drop.ignore(pos, expected))
        return false;

    if (s_session.ignore(pos, expected))
        level = BindingLevel::SESSION;
    else if (s_global.ignore(pos, expected))
        level = BindingLevel::GLOBAL;
    else
        level = BindingLevel::SESSION;

    if (!s_binding.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (s_uuid.ignore(pos, expected))
        drop_uuid = true;

    const auto * begin = pos->begin;
    String query_pattern;
    ParserSelectWithUnionQuery select_p(dt);
    if (pos->type == TokenType::StringLiteral || pos->type == TokenType::DoubleQuotedIdentifier)
    {
        /// Identifier single quot
        ReadBufferFromMemory buf(pos->begin, pos->size());
        if (pos->type == TokenType::StringLiteral)
            readQuotedStringWithSQLStyle(str_value, buf);
        else
            readDoubleQuotedStringWithSQLStyle(str_value, buf);

        if (str_value.empty())
            return false;
        ++pos;
    }
    else if (select_p.parse(pos, binding_pattern, expected))
    {
        query_pattern = String(begin, pos->begin - begin);
    }
    else
        return false;

    auto drop_binding_query = std::make_shared<ASTDropBinding>();
    drop_binding_query->level = level;
    if (!drop_uuid)
        drop_binding_query->re_expression = str_value;
    else
        drop_binding_query->uuid = str_value;

    drop_binding_query->pattern = query_pattern;
    drop_binding_query->pattern_ast = binding_pattern;
    drop_binding_query->if_exists = if_exists;
    node = drop_binding_query;
    return true;
}
}
