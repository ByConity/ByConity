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
        if (opt_col && isPartitionKey(*opt_col))
        {
            /// convert a in (1, 2) to (a = 1 or a = 2)
            if (auto * in_func = right_arg->as<ASTFunction>())
            {
                auto or_func = makeASTFunction("or", ASTs{});
                for (const auto & child : in_func->children)
                {
                    if (child->as<ASTLiteral>())
                    {
                        auto eq_func = makeASTFunction("equals", ASTs{left_arg, child});
                        or_func->arguments->children.push_back(std::move(eq_func));
                    }
                }
                res.partiton_key_conditions.push_back(std::move(or_func));
            }
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
