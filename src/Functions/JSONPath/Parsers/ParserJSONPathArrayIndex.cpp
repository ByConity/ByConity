#include <Functions/JSONPath/Parsers/ParserJSONPathArrayIndex.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/Lexer.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/NumberParser.h>

namespace DB
{
/**
 *
 * pos token iterator
 * node node of ParserJSONPathArrayIndex
 * expected stuff for logging
 * @return was parse successful
 * '$.a.1' -> is_start_with_dot = true
 * '$.1' -> is_start_with_dot = false
 */
bool ParserJSONPathArrayIndex::parseImpl(Pos & pos, ASTPtr & node, Expected & /*expected*/)
{
    bool is_start_with_dot = false;
    if (pos->type == TokenType::Dot)
    {
        is_start_with_dot = true;
        ++pos;
    }

    if (pos->type != TokenType::Number)
        return false;

    auto range = std::make_shared<ASTJSONPathRange>();
    node = range;

    std::pair<UInt32, UInt32> range_indices;

    std::string number_str;
    number_str.assign(is_start_with_dot ? pos->begin : pos->begin + 1, pos->end);
    UInt32 index;
    if (!Poco::NumberParser::tryParseUnsigned(number_str, index))
        return false;
    range_indices.first = index;
    range_indices.second = range_indices.first + 1;
    range->ranges.push_back(std::move(range_indices));

    ++pos;

    return !range->ranges.empty();
}

}
