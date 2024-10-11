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

#include <Parsers/ASTDataType.h>
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
class ParserDataTypeArgument : public IParserDialectBase
{
private:
    const char * getName() const override { return "data type argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserNestedTable nested_parser(dt);
        ParserDataType data_type_parser(dt, false);
        // Note: community of ClickHouse use ParserAllCollectionsOfLiterals
        // but we don't have it yet, so just handle single, Array and Tuple
        ParserLiteral literal_parser(dt);
        ParserArrayOfLiterals array_literal_parser(dt);
        ParserTupleOfLiterals tuple_literal_parser(dt);

        const char * operators[] = {"=", "equals", nullptr};
        ParserLeftAssociativeBinaryOperatorList enum_parser(operators, std::make_unique<ParserLiteral>(dt));

        if (pos->type == TokenType::BareWord && std::string_view(pos->begin, pos->size()) == "Nested")
            return nested_parser.parse(pos, node, expected);

        return enum_parser.parse(pos, node, expected)
            || literal_parser.parse(pos, node, expected)
            || array_literal_parser.parse(pos, node, expected)
            || tuple_literal_parser.parse(pos, node, expected)
            || data_type_parser.parse(pos, node, expected);
    }

public:
    using IParserDialectBase::IParserDialectBase;
};

}

bool ParserDataType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserNestedTable nested(dt);
    if (nested.parse(pos, node, expected))
        return true;

    String type_name;

    ParserIdentifier name_parser;
    ASTPtr identifier;
    if (!name_parser.parse(pos, identifier, expected))
        return false;
    tryGetIdentifierNameInto(identifier, type_name);

    String type_name_upper = Poco::toUpper(type_name);

    /// ParserNestedTable will parse the pattern `[type name] [(NOT) NULL]` as `[name] [type]`, in case that
    /// we need to detect this kind of wrong patterns and reject them.
    if (type_name_upper == "NULL" || type_name_upper == "NOT")
        return false;

    /// Handle Nullable recursively, which is able to fold multiple Nullables in a row:
    ///      Nullable(Nullable(Nullable(A))) => Nullable(A)
    if (type_name_upper == "NULLABLE")
    {
        if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
            return false;

        ParserDataType parser(dt, false /* is_root_type */, true /* parent_nullable */);
        ASTPtr function_node;
        parser.parse(pos, function_node, expected);

        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;

        node = function_node;
        return true;
    }

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
    else if (type_name_upper == "UNSIGNED" ||
             type_name_upper == "SIGNED")
    {
        if (ParserKeyword("INTEGER").ignore(pos))
            type_name_suffix = "INTEGER";
    }

    if (!type_name_suffix.empty())
        type_name = type_name_upper + " " + type_name_suffix;
    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = type_name;
    function_node->no_empty_args = true;

    ASTPtr expr_list_args;
    if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos))
    {
        /// Parse optional parameters
        ParserList args_parser(std::make_unique<ParserDataTypeArgument>(dt), std::make_unique<ParserToken>(TokenType::Comma));

        if (!args_parser.parse(pos, expr_list_args, expected))
            return false;
    }

    if (expr_list_args)
    {
        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos))
            return false;
        function_node->arguments = expr_list_args;
        function_node->children.push_back(function_node->arguments);
    }

    std::optional<bool> null_modifier;
    if (!is_root_type)
    {
        /// check if parent type is Nullable
        if (parent_nullable)
        {
            /// Null modifier should not exist when parent type is Nullable
            if (ParserKeyword("NOT").ignore(pos) || ParserKeyword("NULL").ignore(pos))
                return false;
            null_modifier.emplace(true);
        }
        /// parse null modifiers for nested types
        else
        {
            bool negate_modifier = ParserKeyword("NOT").ignore(pos, expected);
            if (ParserKeyword("NULL").ignore(pos, expected))
                null_modifier.emplace(!negate_modifier);
            else if (negate_modifier)
                return false;
            /// Add a default null modifier for levels don't have one
            else if (dt.explicit_null_modifiers)
                null_modifier.emplace(false);
        }
    }

    if (null_modifier && *null_modifier)
        node = std::make_shared<ASTDataType>(function_node, null_modifier.value());
    else
        node = function_node;

    return true;
}

}
