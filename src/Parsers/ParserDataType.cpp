/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Parsers/ParserDataType.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>


namespace DB
{

namespace
{

/// Wrapper to allow mixed lists of nested and normal types.
/// Parameters are either:
/// - Nested table elements;
/// - Enum element in form of 'a' = 1;
/// - literal;
/// - another data type (or identifier)
class ParserDataTypeArgument : public IParserBase
{
private:
    const char * getName() const override { return "data type argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserNestedTable nested_parser;
        ParserDataType data_type_parser;
        // Note: community of ClickHouse use ParserAllCollectionsOfLiterals
        // but we don't have it yet, so just handle single, Array and Tuple
        ParserLiteral literal_parser(ParserSettings::CLICKHOUSE);
        ParserArrayOfLiterals array_literal_parser(ParserSettings::CLICKHOUSE);
        ParserTupleOfLiterals tuple_literal_parser(ParserSettings::CLICKHOUSE);

        const char * operators[] = {"=", "equals", nullptr};
        ParserLeftAssociativeBinaryOperatorList enum_parser(operators, std::make_unique<ParserLiteral>(ParserSettings::CLICKHOUSE));

        if (pos->type == TokenType::BareWord && std::string_view(pos->begin, pos->size()) == "Nested")
            return nested_parser.parse(pos, node, expected);

        return enum_parser.parse(pos, node, expected)
            || literal_parser.parse(pos, node, expected)
            || array_literal_parser.parse(pos, node, expected)
            || tuple_literal_parser.parse(pos, node, expected)
            || data_type_parser.parse(pos, node, expected);
    }
};

}

bool ParserDataType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserNestedTable nested;
    if (nested.parse(pos, node, expected))
        return true;

    String type_name;

    ParserIdentifier name_parser;
    ASTPtr identifier;
    if (!name_parser.parse(pos, identifier, expected))
        return false;
    tryGetIdentifierNameInto(identifier, type_name);

    String type_name_upper = Poco::toUpper(type_name);
    String type_name_suffix;

    /// Special cases for compatibility with SQL standard. We can parse several words as type name
    /// only for certain first words, otherwise we don't know how many words to parse
    if (type_name_upper == "NATIONAL")
    {
        if (ParserKeyword("CHARACTER LARGE OBJECT").ignore(pos))
            type_name_suffix = "CHARACTER LARGE OBJECT";
        else if (ParserKeyword("CHARACTER VARYING").ignore(pos))
            type_name_suffix = "CHARACTER VARYING";
        else if (ParserKeyword("CHAR VARYING").ignore(pos))
            type_name_suffix = "CHAR VARYING";
        else if (ParserKeyword("CHARACTER").ignore(pos))
            type_name_suffix = "CHARACTER";
        else if (ParserKeyword("CHAR").ignore(pos))
            type_name_suffix = "CHAR";
    }
    else if (type_name_upper == "BINARY" ||
             type_name_upper == "CHARACTER" ||
             type_name_upper == "CHAR" ||
             type_name_upper == "NCHAR")
    {
        if (ParserKeyword("LARGE OBJECT").ignore(pos))
            type_name_suffix = "LARGE OBJECT";
        else if (ParserKeyword("VARYING").ignore(pos))
            type_name_suffix = "VARYING";
    }
    else if (type_name_upper == "DOUBLE")
    {
        if (ParserKeyword("PRECISION").ignore(pos))
            type_name_suffix = "PRECISION";
    }
    else if (type_name_upper.find("INT") != std::string::npos)
    {
        /// Support SIGNED and UNSIGNED integer type modifiers for compatibility with MySQL
        if (ParserKeyword("SIGNED").ignore(pos))
            type_name_suffix = "SIGNED";
        else if (ParserKeyword("UNSIGNED").ignore(pos))
            type_name_suffix = "UNSIGNED";
    }

    if (!type_name_suffix.empty())
        type_name = type_name_upper + " " + type_name_suffix;

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = type_name;
    function_node->no_empty_args = true;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        node = function_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ParserList args_parser(std::make_unique<ParserDataTypeArgument>(), std::make_unique<ParserToken>(TokenType::Comma));
    ASTPtr expr_list_args;

    if (!args_parser.parse(pos, expr_list_args, expected))
        return false;
    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = function_node;
    return true;
}

}

