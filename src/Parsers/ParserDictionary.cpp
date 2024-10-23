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

#include <Parsers/ParserDictionary.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Parsers/ParserDictionaryAttributeDeclaration.h>

#include <Poco/String.h>

#include <Parsers/ParserSetQuery.h>

namespace DB
{


bool ParserDictionaryLifetime::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserLiteral literal_p(dt);
    ParserKeyValuePairsList key_value_pairs_p(dt);
    ASTPtr ast_lifetime;
    auto res = std::make_shared<ASTDictionaryLifetime>();

    /// simple lifetime with only maximum value e.g. LIFETIME(300)
    if (literal_p.parse(pos, ast_lifetime, expected))
    {
        auto literal = ast_lifetime->as<const ASTLiteral &>();

        if (literal.value.getType() != Field::Types::UInt64)
            return false;

        res->max_sec = literal.value.get<UInt64>();
        node = res;
        return true;
    }

    if (!key_value_pairs_p.parse(pos, ast_lifetime, expected))
        return false;

    const ASTExpressionList & expr_list = ast_lifetime->as<const ASTExpressionList &>();
    if (expr_list.children.size() != 2)
        return false;

    bool initialized_max = false;
    /// should contain both min and max
    for (const auto & elem : expr_list.children)
    {
        const ASTPair & pair = elem->as<const ASTPair &>();
        const ASTLiteral * literal = pair.second->as<ASTLiteral>();
        if (literal == nullptr)
            return false;

        if (literal->value.getType() != Field::Types::UInt64)
            return false;

        if (pair.first == "min")
            res->min_sec = literal->value.get<UInt64>();
        else if (pair.first == "max")
        {
            res->max_sec = literal->value.get<UInt64>();
            initialized_max = true;
        }
        else
            return false;
    }

    if (!initialized_max)
        return false;

    node = res;
    return true;
}


bool ParserDictionaryRange::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyValuePairsList key_value_pairs_p(dt);
    ASTPtr ast_range;
    if (!key_value_pairs_p.parse(pos, ast_range, expected))
        return false;

    const ASTExpressionList & expr_list = ast_range->as<const ASTExpressionList &>();
    if (expr_list.children.size() != 2)
        return false;

    auto res = std::make_shared<ASTDictionaryRange>();
    for (const auto & elem : expr_list.children)
    {
        const ASTPair & pair = elem->as<const ASTPair &>();
        const ASTIdentifier * identifier = pair.second->as<ASTIdentifier>();
        if (identifier == nullptr)
            return false;

        if (pair.first == "min")
            res->min_attr_name = identifier->name();
        else if (pair.first == "max")
            res->max_attr_name = identifier->name();
        else
            return false;
    }

    if (res->min_attr_name.empty() || res->max_attr_name.empty())
        return false;

    node = res;
    return true;
}

bool ParserDictionaryLayout::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserFunctionWithKeyValueArguments key_value_func_p(dt, /* brackets_can_be_omitted = */ true);
    ASTPtr ast_func;
    if (!key_value_func_p.parse(pos, ast_func, expected))
        return false;

    const ASTFunctionWithKeyValueArguments & func = ast_func->as<const ASTFunctionWithKeyValueArguments &>();
    auto res = std::make_shared<ASTDictionaryLayout>();
    /// here must be exactly one argument - layout_type
    if (func.children.size() > 1)
        return false;

    res->layout_type = func.name;
    res->has_brackets = func.has_brackets;
    const ASTExpressionList & type_expr_list = func.elements->as<const ASTExpressionList &>();

    /// if layout has params than brackets must be specified
    if (!type_expr_list.children.empty() && !res->has_brackets)
        return false;

    res->set(res->parameters, func.elements);

    node = res;
    return true;
}

bool ParserDictionarySettings::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_comma(TokenType::Comma);

    SettingsChanges changes;

    while (true)
    {
        if (!changes.empty() && !s_comma.ignore(pos))
            break;

        changes.push_back(SettingChange{});

        if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTDictionarySettings>();
    query->changes = std::move(changes);

    node = query;

    return true;
}


bool ParserDictionary::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword primary_key_keyword("PRIMARY KEY");
    ParserKeyword source_keyword("SOURCE");
    ParserKeyword lifetime_keyword("LIFETIME");
    ParserKeyword range_keyword("RANGE");
    ParserKeyword layout_keyword("LAYOUT");
    ParserKeyword settings_keyword("SETTINGS");
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);
    ParserFunctionWithKeyValueArguments key_value_pairs_p(dt);
    ParserList expression_list_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserDictionaryLifetime lifetime_p(dt);
    ParserDictionaryRange range_p(dt);
    ParserDictionaryLayout layout_p(dt);
    ParserDictionarySettings settings_p(dt);

    ASTPtr primary_key;
    ASTPtr ast_source;
    ASTPtr ast_lifetime;
    ASTPtr ast_layout;
    ASTPtr ast_range;
    ASTPtr ast_settings;

    String db_name;
    String tb_name;
    String refresh_query;
    String invalidate_query;

    /// Primary is required to be the first in dictionary definition
    if (primary_key_keyword.ignore(pos) && !expression_list_p.parse(pos, primary_key, expected))
        return false;

    /// Loop is used to avoid strict order of dictionary properties
    auto get_value = [](ASTPair* kv_pair)
    {
        auto tb = kv_pair->second->as<ASTLiteral>();
        auto tb_identifier = kv_pair->second->as<ASTIdentifier>();
        if (!tb)
        {
            if (tb_identifier)
                return tb_identifier->name();
        }
        else
            return tb->value.get<String>();
        return String();
    };

    while (true)
    {
        if (!ast_source && source_keyword.ignore(pos, expected))
        {

            if (!open.ignore(pos))
                return false;

            if (!key_value_pairs_p.parse(pos, ast_source, expected))
                return false;

            //Rewrite the binded databasename in source!
            auto ast_func = ast_source->as<ASTFunctionWithKeyValueArguments>();
            auto & ele = ast_func->elements;
            if (ele && ast_func->name == "clickhouse")
            {
                for (auto &kv : ele->children)
                {
                    auto kv_pair = kv->as<ASTPair>();
                    if (kv_pair->first == "user" || kv_pair->first == "USER")
                    {
                        String user_name;
                        auto user = kv_pair->second->as<ASTLiteral>();
                        auto user_identifier = kv_pair->second->as<ASTIdentifier>();
                        if (!user)
                        {
                            if (user_identifier)
                                user_name = user_identifier->name();
                        }
                        else
                            user_name = user->value.get<String>();

                        if (!user && !user_identifier)
                        {
                            throw Exception("Invalid user field!", static_cast<int>(kv_pair->second->getType()));
                        }

                        user_name = formatTenantConnectUserName(user_name, true);
                        if (user)
                            user->value = user_name;
                        else if (user_identifier)
                            user_identifier->setShortName(user_name);
                    }
                    else if (kv_pair->first == "db" || kv_pair->first == "DB")
                    {
                        auto db = get_value(kv_pair);
                        if (!db.empty())
                            db_name = formatTenantEntityName(db);
                    }
                    else if (kv_pair->first == "table" || kv_pair->first == "TABLE")
                    {
                        tb_name = get_value(kv_pair);
                    }
                    else if (kv_pair->first == "query" || kv_pair->first == "QUERY")
                    {
                        refresh_query = get_value(kv_pair);
                    }
                    else if (kv_pair->first == "invalidate_query" || kv_pair->first == "INVALIDATE_QUERY")
                    {
                        invalidate_query = get_value(kv_pair);
                    }
                }
            }

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_lifetime && lifetime_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!lifetime_p.parse(pos, ast_lifetime, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_layout && layout_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!layout_p.parse(pos, ast_layout, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_range && range_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!range_p.parse(pos, ast_range, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        if (!ast_settings && settings_keyword.ignore(pos, expected))
        {
            if (!open.ignore(pos))
                return false;

            if (!settings_p.parse(pos, ast_settings, expected))
                return false;

            if (!close.ignore(pos))
                return false;

            continue;
        }

        break;
    }

    auto query = std::make_shared<ASTDictionary>();
    node = query;
    query->clickhouse_db = db_name;
    query->clickhouse_tb = tb_name;
    query->clickhouse_query = refresh_query;
    query->clickhouse_invalidate_query = invalidate_query;
    if (primary_key)
        query->set(query->primary_key, primary_key);

    if (ast_source)
        query->set(query->source, ast_source);

    if (ast_lifetime)
        query->set(query->lifetime, ast_lifetime);

    if (ast_layout)
        query->set(query->layout, ast_layout);

    if (ast_range)
        query->set(query->range, ast_range);

    if (ast_settings)
        query->set(query->dict_settings, ast_settings);

    return true;
}

}
