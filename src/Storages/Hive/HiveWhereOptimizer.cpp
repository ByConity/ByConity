<<<<<<< HEAD
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

#include <Interpreters/predicateExpressionsUtils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Storages/Hive/HiveWhereOptimizer.h>
#include <Storages/StorageCnchHive.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/typeid_cast.h>

=======
#include "Storages/Hive/HiveWhereOptimizer.h"
#if USE_HIVE

#include "Interpreters/IdentifierSemantic.h"
#include "Interpreters/predicateExpressionsUtils.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/queryToString.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "Storages/SelectQueryInfo.h"
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')

namespace DB
{
namespace ErrorCodes
{
<<<<<<< HEAD
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

HiveWhereOptimizer::HiveWhereOptimizer(const SelectQueryInfo & query_info_, ContextPtr & /*context_*/, const StoragePtr & storage_)
    : select(typeid_cast<ASTSelectQuery &>(*query_info_.query)), storage(storage_)
{
    if (const auto & cnchhive = dynamic_cast<const StorageCnchHive *>(storage.get()))
    {
        NamesAndTypesList available_real_columns = cnchhive->getInMemoryMetadataPtr()->getColumns().getAllPhysical();
        for (const NameAndTypePair & col : available_real_columns)
            table_columns.insert(col.name);

        partition_expr_ast = extractKeyExpressionList(cnchhive->getPartitionKey());
    }
}

// bool HiveWhereOptimizer::hasColumnOfTableColumns(const HiveWhereOptimizer::Conditions & conditions) const
// {
//     for (auto it = conditions.begin(); it != conditions.end(); ++it)
//     {
//         if (isSubsetOfTableColumns(it->identifiers))
//             return true;
//     }
//     return false;
// }

// bool HiveWhereOptimizer::hasColumnOfTableColumns() const
// {
//     if (isInTableColumns(select.select())
//         || isInTableColumns(select.groupBy())
//         || isInTableColumns(select.having())
//         || isInTableColumns(select.orderBy())
//         || isInTableColumns(select.limitByValue())
//         || isInTableColumns(select.limitBy()))
//         return true;
//     return false;
// }

// bool HiveWhereOptimizer::isInTableColumns(const ASTPtr & node) const
// {
//     if (!node)
//         return false;

//     if (const auto * identifier = node->as<ASTIdentifier>())
//     {
//         if (table_columns.count(identifier->name))
//             return true;
//     }
//     if (!node->as<ASTSubquery>())
//     {
//         for (const auto & child : node->children)
//             if(isInTableColumns(child))
//                 return true;
//     }

//     return false;
// }

/// get where condition usefull filter
/// for example:  app_name is partition key.
/// app_name IN ('test', 'test2') will be convert to ((app_name = 'test') or (app_name = 'test2'))
bool HiveWhereOptimizer::convertWhereToUsefullFilter(ASTs & conditions, String & filter)
=======
}

HiveWhereOptimizer::HiveWhereOptimizer(
    const StorageMetadataPtr & metadata_snapshot_, const SelectQueryInfo & query_info_, Poco::Logger * log_): log(log_)
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')
{
    ASTSelectQuery & select = query_info_.query->as<ASTSelectQuery &>();
    if (!select.where())
        return;

    if (metadata_snapshot_->hasPartitionKey())
    {
        const auto & partition_key_cols = metadata_snapshot_->getPartitionKey().column_names;
        partition_key_names = NameSet(partition_key_cols.begin(), partition_key_cols.end());
    }

    if (metadata_snapshot_->hasClusterByKey())
    {
        const auto & cluster_key_cols = metadata_snapshot_->getClusterByKey().column_names;
        cluster_key_names = NameSet(cluster_key_cols.begin(), cluster_key_cols.end());
    }

    Data data;
    extractKeyConditions(data, select.where());

    partition_key_conds = reconstruct(data.partiton_key_conditions);
    cluster_key_conds = reconstruct(data.cluster_key_conditions);
}

ASTPtr HiveWhereOptimizer::reconstruct(const HiveWhereOptimizer::Conditions & conditions)
{
<<<<<<< HEAD
    if (!select.implicitWhere())
        return false;

    ASTs ret_conditions;
    size_t num_conditions = 0;

    if (const auto & function = typeid_cast<const ASTFunction *>(select.implicitWhere().get()))
    {
        if (function->name == "and")
        {
            auto & conditions = function->arguments->children;
            num_conditions = conditions.size();
            for (const auto & condition : conditions)
            {
                if (const auto & func = typeid_cast<const ASTFunction *>(condition.get()))
                {
                    if (func->name == "and")
                        throw Exception("The conditions of implicit_where should be linearized", ErrorCodes::LOGICAL_ERROR);

                    if (func->name == "in")
                    {
                        const ASTPtr left_arg = func->arguments->children[0];
                        const ASTPtr right_arg = func->arguments->children[1];

                        ASTLiteral * literal = typeid_cast<ASTLiteral *>(right_arg.get());
                        if (!literal)
                            throw Exception(
                                "The conditions of implicit_where IN function right arg should be literal", ErrorCodes::LOGICAL_ERROR);

                        const auto new_function = makeASTFunction("equals", ASTs{left_arg, right_arg});
                        ret_conditions.push_back(new_function);

                        continue;
                    }

                    ///
                    if (!isComparisonFunctionName(func->name))
                        continue;
                }

                ret_conditions.push_back(condition);
            }
        }
    }

    /// only have one condition
    if (ret_conditions.empty())
    {
        const auto & func = typeid_cast<const ASTFunction *>(select.implicitWhere().get());
        if (func && func->name == "in")
        {
            const ASTPtr left_arg = func->arguments->children[0];
            const ASTPtr right_arg = func->arguments->children[1];

            ASTLiteral * literal = typeid_cast<ASTLiteral *>(right_arg.get());
            if (!literal)
                throw Exception("The conditions of implicit_where IN function right arg should be literal", ErrorCodes::LOGICAL_ERROR);

            const auto new_function = makeASTFunction("equals", ASTs{left_arg, right_arg});
            filter = queryToString(new_function);
            return true;
        }
=======
    if (conditions.empty())
        return {};
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')

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
<<<<<<< HEAD

    if (ret_conditions.empty())
    {
        const auto & func = typeid_cast<const ASTFunction *>(select.implicitWhere().get());
        if (func)
        {
            String name = func->name;
            if (!isComparisonFunctionName(func->name))
                return false;

            filter = queryToString(select.implicitWhere());
        }

        return true;
    }
    else if (ret_conditions.size() == 1)
    {
        filter = queryToString(ret_conditions[0]);
        return is_all_usefull;
    }

    const auto function = std::make_shared<ASTFunction>();
    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    for (const auto & elem : ret_conditions)
    {
        function->arguments->children.push_back(elem);
    }
    filter = queryToString(function);
    return is_all_usefull;
}

void HiveWhereOptimizer::implicitwhereOptimize() const
{
    if (!select.where() || select.implicitWhere())
        return;

    Conditions where_conditions = implicitAnalyze(select.where());
    Conditions implicitwhere_conditions;

    while (!where_conditions.empty())
    {
        auto it = std::min_element(where_conditions.begin(), where_conditions.end());
        if (!it->viable)
            break;

        implicitwhere_conditions.splice(implicitwhere_conditions.end(), where_conditions, it);
    }

    if (implicitwhere_conditions.empty())
        return;

    select.setExpression(ASTSelectQuery::Expression::IMPLICITWHERE, reconstruct(implicitwhere_conditions));
    select.setExpression(ASTSelectQuery::Expression::WHERE, reconstruct(where_conditions));
}

bool HiveWhereOptimizer::isSubsetOfTableColumns(const NameSet & identifiers) const
{
    for (const auto & identifier : identifiers)
        if (table_columns.count(identifier) == 0)
            return false;

    return true;
}

HiveWhereOptimizer::Conditions HiveWhereOptimizer::implicitAnalyze(const ASTPtr & expression) const
{
    Conditions res;
    implicitAnalyzeImpl(res, expression);

    return res;
}

ASTPtr HiveWhereOptimizer::reconstruct(const Conditions & conditions) const
{
    if (conditions.empty())
        return {};

    if (conditions.size() == 1)
        return conditions.front().node;

    const auto function = std::make_shared<ASTFunction>();
    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    for (const auto & elem : conditions)
    {
        function->arguments->children.push_back(elem.node);
    }

    return function;
=======
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')
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
<<<<<<< HEAD
    }

    if (ret_conditions.empty())
    {
        const auto & func = typeid_cast<const ASTFunction *>(ast.get());
        if (func)
        {
           if (isComparisonFunctionName(func->name) || isInFunctionName(func->name) || isLikeFunctionName(func->name))
               return {ast};
        }
        else
        {
            throw Exception("Query for CnchHive is not support to select with NULL value in WHERE condition.",
                ErrorCodes::BAD_ARGUMENTS);
        }
    }

    return ret_conditions;
}

ASTs HiveWhereOptimizer::getConditions(const ASTPtr & ast) const
{
    if (!ast)
        return {};

    ASTs ret_conditions;
=======
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')

        if (opt_col && isClusterKey(*opt_col))
        {
            if (auto * in_func = right_arg->as<ASTFunction>())
            {
                bool all_literals = std::all_of(in_func->children.begin(), in_func->children.end(), [] (const ASTPtr & child)
                {
                    return child->as<ASTLiteral>();
                });
                if (all_literals)
                    res.cluster_key_conditions.push_back(node);
            }
        }
    }
<<<<<<< HEAD


    if (ret_conditions.empty())
    {
        const auto & func = typeid_cast<const ASTFunction *>(ast.get());
        if (func)
        {
            if (isComparisonFunctionName(func->name) || isInFunctionName(func->name) || isLikeFunctionName(func->name))
                return {ast};
        }
    }

    return ret_conditions;
=======
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')
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
