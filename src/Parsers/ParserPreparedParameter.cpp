#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTPreparedParameter.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserPreparedParameter.h>

namespace DB
{
bool ParserPreparedParameter::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserToken(TokenType::OpeningSquareBracket).ignore(pos, expected))
        return false;

    ASTPtr type_node;
    auto prepared_parameter = std::make_shared<ASTPreparedParameter>();

    ParserIdentifier name_p;
    ASTPtr identifier;

    if (!name_p.parse(pos, identifier, expected))
        return false;

    if (!ParserToken(TokenType::Colon).ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, type_node, expected))
        return false;

    if (!ParserToken(TokenType::ClosingSquareBracket).ignore(pos, expected))
        return false;

    tryGetIdentifierNameInto(identifier, prepared_parameter->name);
    tryGetIdentifierNameInto(type_node, prepared_parameter->type);
    node = std::move(prepared_parameter);
    return true;
}


}
