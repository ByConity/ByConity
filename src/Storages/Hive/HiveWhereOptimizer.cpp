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

#include "Storages/Hive/HiveWhereOptimizer.h"
#if USE_HIVE

#    include "Interpreters/IdentifierSemantic.h"
#    include "Interpreters/predicateExpressionsUtils.h"
#    include "Parsers/ASTExpressionList.h"
#    include "Parsers/ASTFunction.h"
#    include "Parsers/ASTIdentifier.h"
#    include "Parsers/ASTLiteral.h"
#    include "Parsers/ASTSelectQuery.h"
#    include "Parsers/queryToString.h"
#    include "Storages/SelectQueryInfo.h"
#    include "Storages/StorageInMemoryMetadata.h"

namespace DB
{
namespace ErrorCodes
{
}

HiveWhereOptimizer::HiveWhereOptimizer(
    const StorageMetadataPtr & metadata_snapshot_, const ASTPtr & filter_conditions)
{
    if (!filter_conditions)
        return;

    if (metadata_snapshot_->hasPartitionKey())
    {
        const auto & partition_key_cols = metadata_snapshot_->getPartitionKey().column_names;
        partition_key_names = NameSet(partition_key_cols.begin(), partition_key_cols.end());
    }

    if (metadata_snapshot_->hasClusterByKey())
    {
        const auto & cluster_key_cols = metadata_snapshot_->getColumnsRequiredForClusterByKey();
        cluster_key_names = NameSet(cluster_key_cols.begin(), cluster_key_cols.end());
    }

    Data data;
    extractKeyConditions(data, filter_conditions);

    partition_key_conds = reconstruct(data.partiton_key_conditions);
    cluster_key_conds = reconstruct(data.cluster_key_conditions);
}

ASTPtr HiveWhereOptimizer::reconstruct(const HiveWhereOptimizer::Conditions & conditions)
{
    if (conditions.empty())
        return {};

    if (conditions.size() == 1)
        return conditions.front();

    const auto function = std::make_shared<ASTFunction>();
    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    for (const auto & elem : conditions)
    {
        function->arguments->children.push_back(elem);
    }

    return function;
}

void HiveWhereOptimizer::extractKeyConditions(Data & res, const ASTPtr & node)
{
    if (const auto * func = node->as<ASTFunction>(); func)
    {
        if (func->name == "and")
        {
            for (const auto & elem : func->arguments->children)
                extractKeyConditions(res, elem);
        }
        else
        {
            extractKeyConditions(res, node, *func);
        }
    }
}

/// check if a condition can be used by hive metastore
void HiveWhereOptimizer::extractKeyConditions(Data & res, const ASTPtr & node, const ASTFunction & func)
{
    if (isComparisonFunctionName(func.name))
    {
        auto left_arg = func.arguments->children.front();
        auto right_arg = func.arguments->children.back();

        /// make left_arg always an identifier
        if (!left_arg->as<ASTIdentifier>() && right_arg->as<ASTIdentifier>())
            std::swap(left_arg, right_arg);

        auto * identifier = left_arg->as<ASTIdentifier>();
        auto * literal = right_arg->as<ASTLiteral>();
        if (identifier && literal)
        {
            auto opt_col_name = IdentifierSemantic::getColumnName(*identifier);
            if (opt_col_name && isPartitionKey(*opt_col_name))
            {
                auto cond = makeASTFunction(func.name, ASTs{left_arg, right_arg});
                res.partiton_key_conditions.push_back(std::move(cond));
            }

            if (opt_col_name && isClusterKey(*opt_col_name))
            {
                auto cond = makeASTFunction(func.name, ASTs{left_arg, right_arg});
                res.cluster_key_conditions.push_back(std::move(cond));
            }
        }
    }
    else if (func.name == "in")
    {
        auto left_arg = func.arguments->children.front();
        auto right_arg = func.arguments->children.back();

        auto opt_col = IdentifierSemantic::getColumnName(left_arg);
        if (opt_col.has_value() && isPartitionKey(*opt_col))

        {
            /// convert a in (1, 2) to (a = 1 or a = 2)
            /// (1, 2) is stored as ASTExpressionList with children 1 and 2.
            if (auto * in_parameter = right_arg->as<ASTFunction>())
            {
                auto or_func = makeASTFunction("or", ASTs{});
                if (in_parameter->getChildren().size() != 1)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "cannot convert {}, expecting only one expression list", queryToString(right_arg));
                }
                const auto & literal_list = in_parameter->children[0];

                {
                    if (literal_list->as<ASTExpressionList>())
                    {
                        for (const auto & in_literal : literal_list->getChildren())
                        {
                            auto eq_func = makeASTFunction("equals", ASTs{left_arg, in_literal});
                            or_func->arguments->children.push_back(std::move(eq_func));
                        }
                        res.partiton_key_conditions.push_back(std::move(or_func));
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "cannot convert {},  expecting expression list", queryToString(right_arg));
                    }
                }
            }
            return;
        }

        if (opt_col && isClusterKey(*opt_col))
        {
            if (auto * in_func = right_arg->as<ASTFunction>())
            {
                bool all_literals = std::all_of(
                    in_func->children.begin(), in_func->children.end(), [](const ASTPtr & child) { return child->as<ASTLiteral>(); });
                if (all_literals)
                    res.cluster_key_conditions.push_back(node);
            }
            return;
        }

        bool all_partition_column = false;
        bool all_cluster_column = false;
        auto * expr_tuple = left_arg->as<ASTFunction>();
        auto & identifier_vec = expr_tuple->getChildren()[0]->children;

        if (expr_tuple && expr_tuple->name == "tuple")
        {
            all_partition_column = std::all_of(identifier_vec.begin(), identifier_vec.end(), [this](const ASTPtr & child) {
                auto child_col_name = IdentifierSemantic::getColumnName(child);
                return child_col_name.has_value() && isPartitionKey(*child_col_name);
            });
            all_cluster_column = std::all_of(identifier_vec.begin(), identifier_vec.end(), [this](const ASTPtr & child) {
                auto child_col_name = IdentifierSemantic::getColumnName(child);
                return child_col_name.has_value() && isClusterKey(*child_col_name);
            });
        }
        if (all_partition_column)
        {
            if (auto * in_parameter = right_arg->as<ASTLiteral>(); in_parameter && in_parameter->value.isTuple())
            {
                const auto & literal_tuple_value = in_parameter->value.get<Tuple>();
                auto or_func = makeASTFunction("or", ASTs{});
                for (const auto & literal_field : literal_tuple_value)
                {
                    if (literal_field.isTuple())
                    {
                        const auto & literal_field_vec = literal_field.get<Tuple>();
                        auto and_func = makeASTFunction("and", ASTs{});
                        for (size_t i = 0; i < literal_field_vec.size(); i++)
                        {
                            auto eq_func
                                = makeASTFunction("equals", ASTs{identifier_vec[i], std::make_shared<ASTLiteral>(literal_field_vec[i])});
                            and_func->arguments->children.push_back(std::move(eq_func));
                        }
                        or_func->arguments->children.push_back(std::move(and_func));
                    }
                    else
                    {
                        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "cannot convert {}, wrong expression", right_arg->dumpTree(0));
                    }
                }
                res.partiton_key_conditions.push_back(std::move(or_func));
            }
            return;
        }

        if (all_cluster_column)
        {
            if (auto * in_func = right_arg->as<ASTFunction>())
            {
                bool all_literals = std::all_of(in_func->children.begin(), in_func->children.end(), [](const ASTPtr & child) {
                    return std::all_of(child->children.begin(), child->children.end(), [](const ASTPtr & child_inner) {
                        return child_inner->as<ASTLiteral>();
                    });
                });
                if (all_literals)
                    res.cluster_key_conditions.push_back(node);
            }
            return;
        }
    }
}

bool HiveWhereOptimizer::isPartitionKey(const String & column_name) const
{
    return partition_key_names.count(column_name);
}

bool HiveWhereOptimizer::isClusterKey(const String & column_name) const
{
    return cluster_key_names.count(column_name);
}
}
#endif
