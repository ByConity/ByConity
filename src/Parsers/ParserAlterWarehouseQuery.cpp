/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Parsers/ParserAlterWarehouseQuery.h>
#include <Parsers/ASTAlterWarehouseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool ParserAlterWarehouseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTAlterWarehouseQuery>();
    node = query;

    ParserKeyword s_alter_warehouse("ALTER WAREHOUSE ");
    ParserKeyword s_rename_to("RENAME TO");
    ParserKeyword s_settings("SETTINGS");
    ParserKeyword s_add_rule("ADD RULE");
    ParserKeyword s_delete_rule("DELETE RULE");
    ParserKeyword s_modify_rule("MODIFY RULE");
    ParserKeyword s_set("SET");
    ParserKeyword s_where("WHERE");

    ParserList parser_assignment_list(std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserExpression parser_exp_elem;
    ParserIdentifier rename_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    ASTPtr settings;

    if (!ParserKeyword{"ALTER WAREHOUSE"}.ignore(pos, expected))
        return false;

    ASTPtr warehouse_name_ast;
    if (!ParserIdentifier{}.parse(pos, warehouse_name_ast, expected))
        return false;
    String warehouse_name = getIdentifierName(warehouse_name_ast);

    while (true)
    {
        if (s_rename_to.ignore(pos, expected))
        {
            ASTPtr new_warehouse_name_ast;
            if (query->rename_to.empty() && rename_p.parse(pos, new_warehouse_name_ast, expected))
            {
                query->rename_to = getIdentifierName(new_warehouse_name_ast);
                continue;
            }
            else
                return false;
        }

        break;
    }

    if (s_add_rule.ignore(pos, expected))
    {
        query->type = ASTAlterWarehouseQuery::Type::ADD_RULE; 
        if (!s_set.ignore(pos, expected))
            return false;

        if (!parser_assignment_list.parse(pos, query->assignment_list, expected))
            return false;
    }
    else if (s_delete_rule.ignore(pos, expected))
    {
        query->type = ASTAlterWarehouseQuery::Type::DELETE_RULE; 
    }
    else if (s_modify_rule.ignore(pos, expected))
    {
        query->type = ASTAlterWarehouseQuery::Type::MODIFY_RULE; 
        if (!s_set.ignore(pos, expected))
            return false;

        if (!parser_assignment_list.parse(pos, query->assignment_list, expected))
            return false;
    }

    if (s_where.ignore(pos, expected))
    {
        if (!parser_exp_elem.parse(pos, query->predicate, expected))
            return false;
    }

    if (s_settings.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, settings, expected))
            return false;
    }

    query->name = std::move(warehouse_name);
    if (settings)
        query->set(query->settings, settings);
    node = query;
    return true;
}

}
