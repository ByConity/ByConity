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

#include <Interpreters/replaceForPositionalArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

bool replaceForPositionalArguments(ASTPtr & argument, const ASTSelectQuery * select_query, ASTSelectQuery::Expression expression)
{
    auto columns = select_query->select()->children;

    const auto * expr_with_alias = dynamic_cast<const ASTWithAlias *>(argument.get());

    if (expr_with_alias && !expr_with_alias->alias.empty())
        return false;

    const auto * ast_literal = typeid_cast<const ASTLiteral *>(argument.get());
    if (!ast_literal)
        return false;

    auto which = ast_literal->value.getType();
    if (which != Field::Types::UInt64)
        return false;

    auto pos = ast_literal->value.get<UInt64>();
    if (!pos || pos > columns.size())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Positional argument out of bounds: {} (exprected in range [1, {}]",
                        pos, columns.size());

    const auto & column = columns[--pos];
    if (typeid_cast<const ASTIdentifier *>(column.get()) || typeid_cast<const ASTLiteral *>(column.get()))
    {
        argument = column->clone();
    }
    else if (typeid_cast<const ASTFunction *>(column.get()))
    {
        std::function<void(ASTPtr)> throw_if_aggregate_function = [&](ASTPtr node)
        {
            if (const auto * function = typeid_cast<const ASTFunction *>(node.get()))
            {
                auto is_aggregate_function = AggregateUtils::isAggregateFunction(*function);
                if (is_aggregate_function)
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal value (aggregate function) for positional argument in {}",
                                    ASTSelectQuery::expressionToString(expression));
                }
                else
                {
                    if (function->arguments)
                    {
                        for (const auto & arg : function->arguments->children)
                            throw_if_aggregate_function(arg);
                    }
                }
            }
        };

        if (expression == ASTSelectQuery::Expression::GROUP_BY)
            throw_if_aggregate_function(column);

        argument = column->clone();
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal value for positional argument in {}",
                        ASTSelectQuery::expressionToString(expression));
    }

    return true;
}

}
