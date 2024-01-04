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

#include <Storages/System/CollectWhereClausePredicate.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWithAlias.h>
#include "Core/Field.h"

namespace DB
{
    void collectWhereClausePredicate(const ASTPtr & ast, std::map<String,String> & columnToValue)
    {
        static String column_name;
        if (!ast)
            return;

        if (ASTFunction * func = ast->as<ASTFunction>())
        {
            if (func->name == "equals" || func->name == "and")
            {
                for (auto & arg : func->arguments->children)
                {
                    collectWhereClausePredicate(arg, columnToValue); // recurse in a depth first fashion
                }
            }
        }
        else if (ASTIdentifier * identifier = ast->as<ASTIdentifier>())
        {
            column_name = identifier->name();
        }
        else if (ASTLiteral * literal = ast->as<ASTLiteral>())
        {
            if (literal->value.getType() == Field::Types::String)
            {
                columnToValue.emplace(column_name,literal->value.get<String>());
            }
        }
    }

    std::pair<String,Field> collectWhereEqualClausePredicate(const ASTPtr & ast, const ContextPtr & context)
    {
        std::pair<String,Field> res;
        if (!ast)
            return res;

        ASTFunction * func = ast->as<ASTFunction>();
        if (!func)
            return res;

        if (func->name != "equals")
            return res;

        const auto * column_name = func->arguments->children[0]->as<ASTIdentifier>();
        if (!column_name)
            return res;

        const ASTPtr ast_value = func->arguments->children[1];
        const ASTFunction * function_value = ast_value->as<ASTFunction>();
        if (function_value && function_value->name == "currentDatabase")
        {
            ASTPtr db_ast = evaluateConstantExpressionForDatabaseName(ast_value , context);
            return std::make_pair(column_name->name(), db_ast->as<const ASTLiteral &>().value);
        }

        const auto * column_value = ast_value->as<ASTLiteral>();
        if (!column_value)
            return res;

        return std::make_pair(column_name->name(), column_value->value);
    }

    std::map<String,Field> collectWhereANDClausePredicate(const ASTPtr & ast, const ContextPtr & context)
    {
        std::map<String,Field> res;
        if (!ast)
            return res;

        ASTFunction * func = ast->as<ASTFunction>();
        if (!func)
            return res;

        else if ((func->name == "and") && func->arguments)
        {
            const auto children = func->arguments->children;
            std::for_each(children.begin(), children.end(), [& res, & context] (const auto & child) {
                std::pair<String, Field> p = collectWhereEqualClausePredicate(child, context);
                if (!p.first.empty())
                    res.insert(p);
            });
        }
        else if (func->name == "equals")
        {
            auto p = collectWhereEqualClausePredicate(ast, context);
            if (!p.first.empty())
                res.insert(p);
        }

        return res;
    }

    // collects columns and values for WHERE condition with OR, AND and EQUALS (case insesitive)
    // e.g for query "select ... where ((db = 'db') AND (name = 'name1')) OR ((db = 'db') AND (name = 'name2')) ", method will return a vector {'name':'name1', 'db':'db'}, {'db' : 'db', 'name':'name2'}
    // if a value of column is not a string, it will has value as an empty string
    std::vector<std::map<String,Field>> collectWhereORClausePredicate(const ASTPtr & ast, const ContextPtr & context, bool or_ret_empty)
    {
        std::vector<std::map<String,Field>> res;
        if (!ast)
            return res;

        ASTFunction * func = ast->as<ASTFunction>();
        if (!func)
            return res;

        if ((func->name == "or") && func->arguments)
        {
            const auto children = func->arguments->children;
            std::for_each(children.begin(), children.end(), [& res, & context] (const auto & child) {
                std::map<String,Field> m = collectWhereANDClausePredicate(child, context);
                if (or_ret_empty || !m.empty())
                    res.push_back(m);
            });
        }
        else if (func->name == "and")
        {
            std::map<String,Field> m = collectWhereANDClausePredicate(ast, context);
            if (!m.empty())
                res.push_back(m);
        }
        else if (func->name == "equals")
        {
            auto p = collectWhereEqualClausePredicate(ast, context);
            if (!p.first.empty())
                res.push_back(std::map<String, Field>{p});
        }

        return res;
    }

    bool extractNameFromWhereClause(const ASTPtr & node, const String & key_name, String & ret)
    {
        if (const auto * func_and = node->as<ASTFunction>(); func_and && func_and->name == "and")
        {
            for (const auto & elem : func_and->arguments->children)
            {
                if (extractNameFromWhereClause(elem, key_name, ret))
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            const auto * func_equal = node->as<ASTFunction>();
            if (!func_equal)
                return false;
            if (func_equal->name != "equals")
                return false;

            auto * left_arg = func_equal->arguments->children.front().get();
            auto * right_arg = func_equal->arguments->children.back().get();
            if (!left_arg->as<ASTIdentifier>() && right_arg->as<ASTIdentifier>())
                std::swap(left_arg, right_arg);
            if (const auto * left_identifier = left_arg->as<ASTIdentifier>())
            {
                const auto & cond_key_name = left_identifier->name();
                if (cond_key_name != key_name)
                    return false;
                if (const auto * literal = right_arg->as<ASTLiteral>())
                {
                    const auto & field = literal->value;
                    const auto type = field.getType();
                    if (type != Field::Types::String)
                        return false;
                    ret = DB::toString(field);
                    return true;
                }
            }
            return false;
        }
    }
}
