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

#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/formatAST.h>
#include <Interpreters/misc.h>
#include <Common/typeid_cast.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/NestedUtils.h>
#include <common/map.h>
#include <iterator>
#include <optional>
#include <fmt/core.h>
#include <fmt/format.h>
#include <Core/Names.h>

#include <AggregateFunctions/AggregateBitmapExpression_fwd.h>
#include <Interpreters/PartitionPredicateVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/convertFieldToType.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/queryToString.h>
#include <QueryPlan/SymbolMapper.h>
#include <Storages/MergeTree/MergeTreeCloudData.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>

#include <boost/algorithm/string.hpp>
#include <Poco/String.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Conditions like "x = N" are considered good if abs(N) > threshold.
/// This is used to assume that condition is likely to have good selectivity.
static constexpr auto threshold = 2;

static bool containSubQuery(const ASTPtr & pred)
{
    if (!pred)
        return false;
    if (auto * select = pred->as<ASTSelectQuery>(); select)
        return true;
    for (const auto & child : pred->children)
        if (containSubQuery(child))
            return true;
    return false;
}

static bool containIdentifiers(const ASTPtr & expr)
{
    if (!expr)
        return false;
    if (const auto & identifier = expr->as<ASTIdentifier>(); identifier)
        return true;
    for (const auto & child : expr->children)
        if (containIdentifiers(child))
            return true;
    return false;
}

size_t getNumberOfOrExpression(const ASTPtr & node)
{
    size_t number = 0;
    if (const auto * func_or = node->as<ASTFunction>(); func_or && func_or->name == "or")
    {
        number += func_or->arguments->children.size();
    }

    for (const auto & child : node->children)
    {
        number += getNumberOfOrExpression(child);
    }
    return number;
}

MergeTreeWhereOptimizer::MergeTreeWhereOptimizer(
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    std::unordered_map<std::string, UInt64> column_sizes_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & queried_columns_,
    LoggerPtr log_,
    MaterializeStrategy materialize_strategy_)
    : table_columns{collections::map<std::unordered_set>(
        metadata_snapshot_->getColumns().getAllPhysical(), [](const NameAndTypePair & col) { return col.name; })}
    , queried_columns{queried_columns_}
    , sorting_key_names{NameSet(
          metadata_snapshot_->getSortingKey().column_names.begin(), metadata_snapshot_->getSortingKey().column_names.end())}
    , block_with_constants{KeyCondition::getBlockWithConstants(query_info_.query->clone(), query_info_.syntax_analyzer_result, context_)}
    , log{log_}
    , column_sizes{std::move(column_sizes_)}
    , metadata_snapshot{metadata_snapshot_}
    , enable_ab_index_optimization{context_->getSettingsRef().enable_ab_index_optimization}
    , enable_implicit_column_prewhere_push{context_->getSettingsRef().enable_implicit_column_prewhere_push}
    , materialize_strategy{materialize_strategy_}
    , aggresive_pushdown{context_->getSettingsRef().late_materialize_aggressive_push_down}
    , partition_columns(metadata_snapshot_->getPartitionKey().column_names)
    , max_prewhere_or_expression_size{context_->getSettingsRef().max_prewhere_or_expression_size}
{
    boost::split(skip_functions, Poco::toLower(context_->getSettingsRef().prewhere_skip_functions.value), boost::is_any_of(","));
    ASTSelectQuery & query = query_info_.query->as<ASTSelectQuery &>();

    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    if (!primary_key.column_names.empty())
        first_primary_key_column = primary_key.column_names[0];

    for (const auto & name : queried_columns)
    {
        auto it = column_sizes.find(name);
        if (it != column_sizes.end())
            total_size_of_queried_columns += it->second;
    }

    determineArrayJoinedNames(query);
    optimize(query);
}


static void collectIdentifiersNoSubqueries(const ASTPtr & ast, NameSet & set)
{
    if (auto opt_name = tryGetIdentifierName(ast))
        return (void)set.insert(*opt_name);

    if (ast->as<ASTSubquery>())
        return;

    for (const auto & child : ast->children)
        collectIdentifiersNoSubqueries(child, set);
}

static bool isConditionGood(const ASTPtr & condition)
{
    const auto * function = condition->as<ASTFunction>();
    if (!function)
        return false;

    /** we are only considering conditions of form `equals(one, another)` or `one = another`,
        * especially if either `one` or `another` is ASTIdentifier */
    if (function->name != "equals")
        return false;

    auto * left_arg = function->arguments->children.front().get();
    auto * right_arg = function->arguments->children.back().get();

    /// try to ensure left_arg points to ASTIdentifier
    if (!left_arg->as<ASTIdentifier>() && right_arg->as<ASTIdentifier>())
        std::swap(left_arg, right_arg);

    if (left_arg->as<ASTIdentifier>())
    {
        /// condition may be "good" if only right_arg is a constant and its value is outside the threshold
        if (const auto * literal = right_arg->as<ASTLiteral>())
        {
            const auto & field = literal->value;
            const auto type = field.getType();

            /// check the value with respect to threshold
            if (type == Field::Types::UInt64)
            {
                const auto value = field.get<UInt64>();
                return value > threshold;
            }
            else if (type == Field::Types::Int64)
            {
                const auto value = field.get<Int64>();
                return value < -threshold || threshold < value;
            }
            else if (type == Field::Types::Float64)
            {
                const auto value = field.get<Float64>();
                return value < threshold || threshold < value;
            }
        }
    }

    return false;
}


void MergeTreeWhereOptimizer::analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final) const
{
    if (const auto * func_and = node->as<ASTFunction>(); func_and && func_and->name == "and")
    {
        for (const auto & elem : func_and->arguments->children)
            analyzeImpl(res, elem, is_final);
    }
    else
    {
        Condition cond;
        cond.node = node;

        collectIdentifiersNoSubqueries(node, cond.identifiers);

        cond.columns_size = getIdentifiersColumnSize(cond.identifiers);

        cond.viable =
            /// Condition depend on some column. Constant expressions are not moved.
            !cond.identifiers.empty()
            && !cannotBeMoved(node, is_final)
            /// Do not take into consideration the conditions consisting only of the first primary key column
            && !hasPrimaryKeyAtoms(node)
            /// Only table columns are considered. Not array joined columns. NOTE We're assuming that aliases was expanded.
            && isSubsetOfTableColumns(cond.identifiers)
            /// Do not move conditions involving all queried columns.
            && ((materialize_strategy == MaterializeStrategy::LATE_MATERIALIZE && aggresive_pushdown)
                || cond.identifiers.size() < queried_columns.size());

        if (cond.viable)
        {
            cond.good = isConditionGood(node);
            LOG_DEBUG(log, "MergeTreeWhereOptimizer: analyzeImpl identifiers: {}, column_size:{}", boost::join(cond.identifiers, ","), std::to_string(cond.columns_size));
        }
        res.emplace_back(std::move(cond));
    }
}

/// Transform conjunctions chain in WHERE expression to Conditions list.
MergeTreeWhereOptimizer::Conditions MergeTreeWhereOptimizer::analyze(const ASTPtr & expression, bool is_final) const
{
    Conditions res;
    analyzeImpl(res, expression, is_final);
    return res;
}

/// Transform Conditions list to WHERE or PREWHERE expression.
ASTPtr MergeTreeWhereOptimizer::reconstruct(const Conditions & conditions)
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
        function->arguments->children.push_back(elem.node);

    return function;
}

bool MergeTreeWhereOptimizer::containsArraySetCheck(const ASTPtr & condition) const
{
    if (!condition)
        return false;

    const auto * const function = typeid_cast<const ASTFunction *>(condition.get());

    if (function)
    {
        if (function->name == "not")
        {
            return containsArraySetCheck(function->arguments->children.front());
        }
        if (function->name == "and" or function->name == "or")
        {
            bool result = false;
            for (const auto & children_ast : function->arguments->children)
            {
                result |= containsArraySetCheck(children_ast);
            }
            return result;
        }
        else if (BitmapIndexHelper::isArraySetFunctions(function->name))
        {
            return true;
        }
    }

    return false;
}

// A expression is arraySetCheck if
// 1. single arraySetCheck function with column argument is a BLOOM column
// 2. not arraySetCheck function
bool MergeTreeWhereOptimizer::isArraySetCheck(const ASTPtr & condition, bool) const
{
    if (!condition)
        return false;

    const auto * const function = typeid_cast<const ASTFunction *>(condition.get());

    if (function)
    {
        if (BitmapIndexHelper::isArraySetFunctions(function->name))
        {
            size_t arg_size = function->arguments->children.size();
            if (arg_size % 2)
                throw Exception("Wrong number of arguments of arraySetCheck", ErrorCodes::LOGICAL_ERROR);

            for (size_t i = 0; i < arg_size; i += 2)
            {
                auto * left_arg = function->arguments->children.at(i).get();
                auto * right_arg = function->arguments->children.at(i + 1).get();
                auto * identifier = left_arg->as<ASTIdentifier>();
                if (!identifier || right_arg->as<ASTIdentifier>())
                    return false;

                String identifier_name = identifier->getColumnName();
                if (isMapImplicitKey(identifier_name))
                    identifier_name = parseMapNameFromImplicitFileName(identifier_name);

                auto columns = metadata_snapshot->getColumns();
                if (!columns.has(identifier_name))
                    return false;

                // unlikely
                {
                    // Cannot handle column like 'map.key'
                    auto [maybe_reserved_map_keys, name_without_suffix] = mayBeMapKVReservedKeys(identifier_name);
                    if (maybe_reserved_map_keys)
                    {
                        if (!columns.has(name_without_suffix))
                            return false;

                        const ColumnDescription & column = columns.get(name_without_suffix);
                        if (column.type->isMap())
                            return false;
                    }
                }

                const ColumnDescription & column = columns.get(identifier_name);

                if (!column.type->isBitmapIndex() && !column.type->isSegmentBitmapIndex())
                    return false;
            }

            return true;
        }
    }

    return false;
}

void MergeTreeWhereOptimizer::optimize(ASTSelectQuery & select) const
{
    if (!select.where() || select.prewhere())
        return;

    Conditions where_conditions = analyze(select.where(), select.final());

    if (materialize_strategy == MaterializeStrategy::PREWHERE)
        optimizePrewhere(where_conditions, select);
    else if (materialize_strategy == MaterializeStrategy::LATE_MATERIALIZE)
        optimizeLateMaterialize(where_conditions, select);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown materialize strategy");
}

void MergeTreeWhereOptimizer::optimizePrewhere(Conditions & where_conditions, ASTSelectQuery & select) const
{
    Conditions prewhere_conditions;

    UInt64 total_size_of_moved_conditions = 0;
    UInt64 total_number_of_moved_columns = 0;

    /// Move condition and all other conditions depend on the same set of columns.
    auto move_condition = [&](Conditions::iterator cond_it)
    {
        prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, cond_it);
        total_size_of_moved_conditions += cond_it->columns_size;
        total_number_of_moved_columns += cond_it->identifiers.size();

        // for bitmap index, same column with diffetent value will use different index
        if (isArraySetCheck(cond_it->node))
            return;

        /// Move all other viable conditions that depend on the same set of columns.
        for (auto jt = where_conditions.begin(); jt != where_conditions.end();)
        {
            if (jt->viable && jt->columns_size == cond_it->columns_size && jt->identifiers == cond_it->identifiers)
                prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, jt++);
            else
                ++jt;
        }
    };

    /// @ab-opt, move ab check index if there is
    if (enable_ab_index_optimization)
    {
        // LOG_DEBUG(log, "MergeTreeWhereOptimizer: try to use ab index optimization.");
        size_t array_set_check_function_numbers = 0;
        for (auto it = where_conditions.begin(); it != where_conditions.end();)
        {
            if (isArraySetCheck(it->node))
            {
                array_set_check_function_numbers++;
            }
            ++it;
        }

        // current only support one array check function
        /// TODO: support complex expressions
        if (array_set_check_function_numbers == 1)
        {
            for (auto it = where_conditions.begin(); it != where_conditions.end();)
            {
                if (isArraySetCheck(it->node))
                {
                    auto move_it = it++;
                    move_condition(move_it);
                    continue;
                }
                ++it;
            }
        }
    }

    /// Move conditions unless the ratio of total_size_of_moved_conditions to the total_size_of_queried_columns is less than some threshold.
    while (!where_conditions.empty())
    {
        /// Move the best condition to PREWHERE if it is viable.

        auto it = std::min_element(where_conditions.begin(), where_conditions.end());

        if (!it->viable)
            break;

        if (isArraySetCheck(it->node))
            break;

        // check conditon depth
        if (max_prewhere_or_expression_size)
        {
            auto number_of_ors = getNumberOfOrExpression(it->node);
            if (number_of_ors > max_prewhere_or_expression_size)
            {
                LOG_DEBUG(
                    log, "MergeTreeWhereOptimizer: condition {}. Number of `or` expressions is {}", it->node->dumpTree(), number_of_ors);
                break;
            }
        }
        bool moved_enough = false;
        if (total_size_of_queried_columns > 0)
        {
            /// If we know size of queried columns use it as threshold. 10% ratio is just a guess.
            moved_enough = total_size_of_moved_conditions > 0
                && (total_size_of_moved_conditions + it->columns_size) * 10 > total_size_of_queried_columns;
        }
        else
        {
            /// Otherwise, use number of moved columns as a fallback.
            /// It can happen, if table has only compact parts. 25% ratio is just a guess.
            moved_enough = total_number_of_moved_columns > 0
                && (total_number_of_moved_columns + it->identifiers.size()) * 4 > queried_columns.size();
        }

        if (moved_enough)
            break;
        move_condition(it);
    }

    /// Nothing was moved.
    if (prewhere_conditions.empty())
        return;

    /// Rewrite the SELECT query.

    select.setExpression(ASTSelectQuery::Expression::WHERE, reconstruct(where_conditions));
    select.setExpression(ASTSelectQuery::Expression::PREWHERE, reconstruct(prewhere_conditions));

    LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition \"{}\" moved to PREWHERE", select.prewhere());
}

void MergeTreeWhereOptimizer::optimizeLateMaterialize(Conditions where_conditions, ASTSelectQuery & select) const
{
    Conditions tmp_conditions;
    std::vector<Condition> conditions;
    UInt64 total_size_of_moved_conditions = 0;

    /// Move condition and all other conditions depend on the same set of columns to
    /// tmp_conditions w/o any restriction of how many condition should we move
    auto move_condition = [&](Conditions::iterator cond_it, bool array_set_check = false) {
        tmp_conditions.splice(tmp_conditions.end(), where_conditions, cond_it);

        if (array_set_check)
            return;

        total_size_of_moved_conditions += cond_it->columns_size;

        /// Move all other viable conditions that depend on the same set of columns.
        for (auto jt = where_conditions.begin(); jt != where_conditions.end();)
        {
            if (jt->viable && jt->columns_size == cond_it->columns_size && jt->identifiers == cond_it->identifiers)
                tmp_conditions.splice(tmp_conditions.end(), where_conditions, jt++);
            else
                ++jt;
        }
    };

    /// Firstly, move ab index if there is any, always use for early materialize
    LOG_DEBUG(log, "MergeTreeWhereOptimizer: try to use bitmap index");
    NameSet indexed_columns; /// indexed columns, will always be read at the last stage in chain, so we won't move condition that required these columns
    for (auto it = where_conditions.begin(); it != where_conditions.end();)
    {
        if (isArraySetCheck(it->node))
        {
            auto move_it = it++;
            indexed_columns.insert(move_it->identifiers.begin(), move_it->identifiers.end());
            move_condition(move_it, true);
            continue;
        }
        ++it;
    }

    /// When moving for EMP, following conditions applied:
    /// - Condition with >= 2 identifiers can't be moved (may relax later)
    /// - Condition that is subset of partition expression can't be moved
    /// - Condition that required any column in indexed_columns
    evaluateConditionsForEMP(where_conditions, indexed_columns);

    /// Seperate bitmap index from other conditions. The reason is when we have a condition like
    /// `len(x) = 10 and arraySetCheck(x,1)`, if we keep both condition in a stage then in that
    /// stage we need to read column `x`, which is not necessary for `arraySetCheck`. The better
    /// strategy is making `arraySetCheck(x,1)` the first stage in chain and `len(x) = 10` should
    /// be kept in WHERE. If we have many conditions, we have high change to skip reading some
    /// granules of `x`
    auto bitmap_conditions = std::move(tmp_conditions);

    while (!where_conditions.empty())
    {
        auto it = std::min_element(where_conditions.begin(), where_conditions.end());

        if (!it->viable)
            break;

        if (containsArraySetCheck(it->node))
            break;

        bool moved_enough = false;
        if (!aggresive_pushdown && total_size_of_queried_columns > 0)
        {
            /// If we know size of queried columns use it as threshold. 40% ratio is just a guess.
            moved_enough = total_size_of_moved_conditions > 0
                && (total_size_of_moved_conditions + it->columns_size) * 2.5 > total_size_of_queried_columns;
        }
        if (moved_enough)
            break;
        move_condition(it);
    }

    for (auto && condition : tmp_conditions)
    {
        conditions.emplace_back(std::move(condition));
    }
//#ifndef NDEBUG
//    fmt::print("Prewhere conditions with single columns\n");
//    for (auto & cond : conditions)
//    {
//        fmt::print("{} {} {}\n", cond.column_names, cond.column_size, cond.node->formatForErrorMessage());
//    }
//#endif

    /// Merge all predicates with same required input columns
    size_t cnt = 0;
    for (size_t i = 0; i < conditions.size();)
    {
        size_t start = i++;
        while (i < conditions.size() && conditions[i].identifiers == conditions[start].identifiers)
            ++i;
        if (i > start + 1) /// More than 2 predicates, merge to `predicates[start]` with a 'and'
        {
            const auto function = std::make_shared<ASTFunction>();
            function->name = "and";
            function->arguments = std::make_shared<ASTExpressionList>();
            function->children.push_back(function->arguments);
            for (size_t j = start; j < i; ++j)
            {
                function->arguments->children.push_back(conditions[j].node);
            }
            conditions[start].node = std::move(function);
        }
        if (start > cnt)
            conditions[cnt] = std::move(conditions[start]);
        ++cnt;
    }
    conditions.erase(conditions.begin() + cnt, conditions.end());

    /// TODO: @canh find the most reasonable way to order columns
    std::reverse(conditions.begin(), conditions.end()); /// Last condition in array is first reader in chain

    /// Construct final column with predicate list
    for (auto & pred : conditions)
        atomic_predicates_expr.emplace_back(pred.node);

    /// If there's any bitmap index, make it the first reader in chain
    for (auto & pred : bitmap_conditions)
        atomic_predicates_expr.emplace_back(pred.node);


#ifndef NDEBUG
    fmt::print(stderr, "MergeTreeWhereOptimizer: prewhere predicate map by column:\n");
    for (const auto & pred : atomic_predicates_expr)
    {
        fmt::print(stderr, "{}\n", pred);
    }
    LOG_DEBUG(log, "Prewhere predicate map by column:\n {}", fmt::join(atomic_predicates_expr, "\n"));
#endif

    select.setExpression(ASTSelectQuery::Expression::WHERE, reconstruct(where_conditions));

}


UInt64 MergeTreeWhereOptimizer::getIdentifiersColumnSize(const NameSet & identifiers) const
{
    UInt64 size = 0;

    for (const auto & identifier : identifiers)
        if (column_sizes.count(identifier))
            size += column_sizes.at(identifier);

    return size;
}


bool MergeTreeWhereOptimizer::hasPrimaryKeyAtoms(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        const auto & args = func->arguments->children;

        if ((func->name == "not" && 1 == args.size()) || func->name == "and" || func->name == "or")
        {
            for (const auto & arg : args)
                if (hasPrimaryKeyAtoms(arg))
                    return true;

            return false;
        }
    }

    return isPrimaryKeyAtom(ast);
}


bool MergeTreeWhereOptimizer::isPrimaryKeyAtom(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (!KeyCondition::atom_map.count(func->name))
            return false;

        const auto & args = func->arguments->children;
        if (functionIsEscapeLikeOperator(func->name))
        {
            if (args.size() != 3)
                return false;
        }
        else {
            if (args.size() != 2)
                return false;
        }

        const auto & first_arg_name = args.front()->getColumnName();
        const auto & second_arg_name = args.back()->getColumnName();

        if ((first_primary_key_column == first_arg_name && isConstant(args[1]))
            || (first_primary_key_column == second_arg_name && isConstant(args[0]))
            || (first_primary_key_column == first_arg_name && functionIsInOrGlobalInOperator(func->name)))
            return true;
    }

    return false;
}


bool MergeTreeWhereOptimizer::isSortingKey(const String & column_name) const
{
    return sorting_key_names.count(column_name);
}


bool MergeTreeWhereOptimizer::isConstant(const ASTPtr & expr) const
{
    const auto column_name = expr->getColumnName();

    return expr->as<ASTLiteral>()
        || (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column));
}


bool MergeTreeWhereOptimizer::isSubsetOfTableColumns(const NameSet & identifiers) const
{
    for (const auto & identifier : identifiers)
        if (table_columns.count(identifier) == 0 && !(enable_implicit_column_prewhere_push && isMapImplicitKey(identifier)))
            return false;

    return true;
}


bool MergeTreeWhereOptimizer::cannotBeMoved(const ASTPtr & ptr, bool is_final) const
{
    if (const auto * function_ptr = ptr->as<ASTFunction>())
    {
        LOG_DEBUG(getLogger("MergeTreeWhereOptimizer"), "[cannotBeMoved]: function: {} tree: {}",
                  function_ptr->name, function_ptr->dumpTree());
        /// disallow arrayJoin expressions to be moved to PREWHERE for now
        if ("arrayJoin" == function_ptr->name)
            return true;

        /// disallow GLOBAL IN, GLOBAL NOT IN
        if ("globalIn" == function_ptr->name
            || "globalNotIn" == function_ptr->name)
            return true;

        /// indexHint is a special function that it does not make sense to transfer to PREWHERE
        if ("indexHint" == function_ptr->name)
            return true;

        // These functions can cause performance degradation
        if ("match" == function_ptr->name || "get_json_object" == function_ptr->name
            || skip_functions.count(Poco::toLower(function_ptr->name)))
            return true;
    }
    else if (auto opt_name = IdentifierSemantic::getColumnName(ptr))
    {
        /// disallow moving result of ARRAY JOIN to PREWHERE
        if (array_joined_names.count(*opt_name) ||
            array_joined_names.count(Nested::extractTableName(*opt_name)) ||
            (is_final && !isSortingKey(*opt_name)))
            return true;
    }

    for (const auto & child : ptr->children)
        if (cannotBeMoved(child, is_final))
            return true;

    return false;
}


void MergeTreeWhereOptimizer::determineArrayJoinedNames(ASTSelectQuery & select)
{
    auto array_join_expression_list = select.arrayJoinExpressionList();

    /// much simplified code from ExpressionAnalyzer::getArrayJoinedColumns()
    if (!array_join_expression_list)
        return;

    for (const auto & ast : array_join_expression_list->children)
        array_joined_names.emplace(ast->getAliasOrColumnName());
}

void MergeTreeWhereOptimizer::evaluateConditionsForEMP(Conditions & conditions, const NameSet & prohibited) const
{
    for (auto & cond : conditions)
    {
        Names intersect;
        std::set_intersection(prohibited.begin(), prohibited.end(), cond.identifiers.begin(), cond.identifiers.end(), std::back_inserter(intersect));
        cond.viable &= intersect.empty() && cond.identifiers.size() == 1 && !isSubsetOfPartitionKeyExpressions(cond.node) && !containSubQuery(cond.node) && !defaultExpressionDependOnOtherColumns(*cond.identifiers.begin());
    }
}

bool MergeTreeWhereOptimizer::isValidPartitionColumn(const IAST * condition) const
{
    const auto *const function = typeid_cast<const ASTFunction *>(condition);
    if (!function || partition_columns.empty())
        return false;

    // Here we consider the expression which only contains one ASTIdentifier/ASTFunction and the other
    // arguments must be ASTLiteral.
    // The flag represents whether we have found a partition-key column (Function or Identifier)
    for (auto & arg_child : function->arguments->children)
    {
        const auto & identifier = typeid_cast<const ASTIdentifier *>(arg_child.get());
        const auto & func = typeid_cast<const ASTFunction *>(arg_child.get());

        // Function or Identifier can only appear once
        if (!identifier && !func) continue;

        // Whether a Function or Identifer is a partition key
        for (const auto & name : partition_columns)
        {
            if (func || identifier)
            {
                if (name == arg_child->getAliasOrColumnName())
                    return true;
            }
        }
    }
    return false;
}

bool MergeTreeWhereOptimizer::isSubsetOfPartitionKeyExpressions(const ASTPtr & node) const
{
    if (partition_columns.empty()) return false;
    if (const auto * func = node->as<ASTFunction>(); func && (func->name == "and" || func->name == "or" || func->name == "not"))
    {
        for (const auto & elem : func->arguments->children)
            if (isSubsetOfPartitionKeyExpressions(elem)) return true;
    }
    else
    {
        NameSet identifiers;

        collectIdentifiersNoSubqueries(node, identifiers);

        if (isSubsetOfTableColumns(identifiers) && isValidPartitionColumn(node.get()))
            return true;
    }
    return false;
}

bool MergeTreeWhereOptimizer::defaultExpressionDependOnOtherColumns(const String & column_name) const
{
    auto column_default = metadata_snapshot->getColumns().getDefault(column_name);
    if (!column_default)
        return false;
    return containIdentifiers(column_default->expression);
}

std::vector<ASTPtr> && MergeTreeWhereOptimizer::getAtomicPredicatesExpressions()
{
    return std::move(atomic_predicates_expr);
}

Names getBitMapParameterValues(String expression)
{
    Names values;
    expression += "#";
    size_t max_index = expression.size();
    size_t pre_index = max_index;
    for (size_t i = 0; i < max_index; ++i)
    {
        if (BITENGINE_EXPRESSION_KEYWORDS.count(expression[i]))
        {
            if (pre_index != max_index)
            {
                if (pre_index == 0 || (pre_index > 0 && expression[pre_index - 1] != '_'))
                    values.push_back(expression.substr(pre_index, i - pre_index));
                pre_index = max_index;
            }
        }
        else if (pre_index == max_index && expression[i] != BITENGINE_SPECIAL_KEYWORD)
            pre_index = i;
    }
    return values;
}

/** For parameters in bitmap**** family aggregation functions, we construct an 'In' functions
  * for example, bitmapCount('1 | 2 & 3')(a, b), we will construct 'a in (1, 2, 3)' expression.
  */
std::pair<ASTPtr, size_t> createInFunctionForBitMapParameter(const String & index_arg, const std::set<Field> & parameter_values)
{
    auto tuple_func = std::make_shared<ASTFunction>();
    tuple_func->name = "tuple";
    tuple_func->arguments = std::make_shared<ASTExpressionList>();
    tuple_func->children.push_back(tuple_func->arguments);

    for (const auto & field : parameter_values)
    {
        tuple_func->arguments->children.push_back(std::make_shared<ASTLiteral>(field));
    }

    size_t total_in_elements = tuple_func->arguments->children.size();

    /// for case like bitmapCount('')(tag_id, bitmap)
    if (total_in_elements == 0U)
        return {nullptr, 0};

    auto in_func = std::make_shared<ASTFunction>();
    in_func->name = "in";
    in_func->arguments = std::make_shared<ASTExpressionList>();
    in_func->arguments->children.push_back(std::make_shared<ASTIdentifier>(index_arg));
    in_func->arguments->children.push_back(tuple_func);
    in_func->children.push_back(in_func->arguments);

    return {in_func, total_in_elements};
}

void optimizePartitionPredicate(ASTPtr & query, StoragePtr storage, SelectQueryInfo & query_info, ContextPtr context)
{
    ASTSelectQuery * select = query->as<ASTSelectQuery>();
    if (!select || !select->where() || query_info.partition_filter || !storage)
        return;

    if (!dynamic_cast<MergeTreeCloudData *>(storage.get()))
        return;

    ASTs conjuncts = PredicateUtils::extractConjuncts(select->where()->clone());

    // construct push filter by mapping symbol to origin column
    std::unordered_map<String, String> column_to_alias;
    for (const auto & item : query_info.syntax_analyzer_result->aliases)
        column_to_alias.emplace(item.second->getColumnName(), item.first);
    auto alias_to_column = Utils::reverseMap(column_to_alias);
    ASTPtr push_filter;
    if (!alias_to_column.empty())
    {
        auto mapper = SymbolMapper::simpleMapper(alias_to_column);
        std::vector<ConstASTPtr> mapped_pushable_conjuncts;
        for (auto & conjunct : conjuncts)
        {
            bool all_in = true;
            auto symbols = SymbolsExtractor::extract(conjunct);
            for (const auto & item : symbols)
                all_in &= alias_to_column.contains(item);
            if (all_in)
               mapped_pushable_conjuncts.push_back(mapper.map(conjunct));
            else
               mapped_pushable_conjuncts.push_back(conjunct);
        }

        push_filter = PredicateUtils::combineConjuncts(mapped_pushable_conjuncts);
    }
    if (push_filter && !PredicateUtils::isTruePredicate(push_filter))
    {
        if (auto * merge_tree_data = dynamic_cast<MergeTreeCloudData *>(storage.get()))
        {
            ASTs push_predicates;
            ASTs remain_predicates;
            Names partition_key_names = merge_tree_data->getInMemoryMetadataPtr()->getPartitionKey().column_names;
            Names virtual_key_names = merge_tree_data->getSampleBlockWithVirtualColumns().getNames();
            partition_key_names.insert(partition_key_names.end(), virtual_key_names.begin(), virtual_key_names.end());
            auto iter = std::stable_partition(conjuncts.begin(), conjuncts.end(), [&](const auto & predicate) {
                PartitionPredicateVisitor::Data visitor_data{context, storage, predicate};
                PartitionPredicateVisitor(visitor_data).visit(predicate);
                return visitor_data.getMatch();
            });

            push_predicates.insert(push_predicates.end(), conjuncts.begin(), iter);
            remain_predicates.insert(remain_predicates.end(), iter, conjuncts.end());

            ASTPtr new_partition_filter;

            if (query_info.partition_filter)
            {
                push_predicates.push_back(query_info.partition_filter);
                new_partition_filter = PredicateUtils::combineConjuncts(push_predicates);
            }
            else
            {
                new_partition_filter = PredicateUtils::combineConjuncts<false>(push_predicates);
            }

            if (!PredicateUtils::isTruePredicate(new_partition_filter))
                query_info.partition_filter = std::move(new_partition_filter);

            ASTPtr new_where = PredicateUtils::combineConjuncts<false>(remain_predicates);
            if (!PredicateUtils::isTruePredicate(new_where))
                select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_where));
            else
                select->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
            query_info.query = query;
        }
    }
    if (query_info.partition_filter)
    {
        LOG_TRACE(getLogger("optimizePartitionPredicate"), "Optimize partition prediate push down query rewrited to {} , partiton filter-{} ",
                queryToString(query), queryToString(query_info.partition_filter));
    }
}

void getTargetFunctions(const ASTPtr & ast, ASTs & functions)
{
    if (!ast)
        return;

    if (ASTFunction * func = ast->as<ASTFunction>())
    {
        String func_name = Poco::toLower(func->name);
        if (BITMAP_EXPRESSION_AGGREGATE_FUNCTIONS.count(func_name))
        {
            functions.push_back(func->clone());
            return;
        }
    }

    for (const auto & child : ast->children)
        getTargetFunctions(child, functions);
}

ASTPtr constructAndFunction(ASTs functions)
{
    if (functions.size() == 1)
        return functions.front();

    const auto and_func = std::make_shared<ASTFunction>();
    and_func->name = "and";
    and_func->arguments = std::make_shared<ASTExpressionList>();
    and_func->children.push_back(and_func->arguments);
    for (const auto & func : functions)
        and_func->arguments->children.push_back(func);

    return and_func;
}

ASTPtr constructOrFunction(ASTs functions)
{
    if (functions.size() == 1)
        return functions.front();

    const auto and_func = std::make_shared<ASTFunction>();
    and_func->name = "or";
    and_func->arguments = std::make_shared<ASTExpressionList>();
    and_func->children.push_back(and_func->arguments);
    for (const auto & func : functions)
        and_func->arguments->children.push_back(func);

    return and_func;
}

void collectionParameterValueInFunction(
    const ASTPtr & ast, std::unordered_map<String, std::set<Field>> & parameters_map, const StorageMetadataPtr & metadata_snapshot)
{
    if (!ast)
        return;

    ASTFunction * func = ast->as<ASTFunction>();

    if (!func)
        return;

    ASTPtr func_arguments = func->arguments;
    ASTPtr func_parameters = func->parameters;

    if (!func_arguments || !func_parameters)
        return;

    if (func_arguments->children.size() < 2 || func_parameters->children.empty())
        return;

    ASTIdentifier * index_arg = func_arguments->children.front()->as<ASTIdentifier>();

    if (!index_arg)
        return;
    /// not a physical column in the table, no need to optimize
    DataTypePtr data_type{nullptr};
    if (metadata_snapshot)
        data_type = metadata_snapshot->getColumns().tryGetPhysical(index_arg->name()).value_or(NameAndTypePair{}).type;

    if (!data_type)
        return;

    auto & parameters = parameters_map[index_arg->name()];
    String exp;
    for (const auto & child : func_parameters->children)
    {
        ASTLiteral * param = child->as<ASTLiteral>();
        if (param && param->value.tryGet<String>(exp))
        {
            Names values = getBitMapParameterValues(exp);
            for (auto & node : values)
            {
                Field field(node);
                field = convertFieldToType(field, *data_type);
                parameters.emplace(field);
            }
        }
    }
}

void optimizeBitMapParametersToWhere(ASTPtr & query, const StorageMetadataPtr & metadata_snapshot)
{
    ASTSelectQuery * select = query->as<ASTSelectQuery>();
    if (!select || !select->select())
        return;

    ASTs target_functions;
    getTargetFunctions(select->select(), target_functions);

    if (target_functions.empty())
        return;

    std::unordered_map<String, std::set<Field>> parameters_map;
    for (const auto & function : target_functions)
    {
        collectionParameterValueInFunction(function, parameters_map, metadata_snapshot);
    }

    if (parameters_map.empty())
        return;

    size_t total_in_elems{0};
    ASTs functions;
    /// create in function for those parametered values
    for (const auto & parameter : parameters_map)
    {
        auto [in_ast, elem_size] = createInFunctionForBitMapParameter(parameter.first, parameter.second);
        functions.push_back(in_ast);
        total_in_elems += elem_size;
    }

    size_t found_parameters = functions.size();
    /// for example, `bitmapCount('1 | 2 & 3')(a, b), bitmapCount('3&4')(c,b)`,
    // we need construct 'a in (1, 2, 3) OR c in (3, 4)' predicate expression.
    auto parameter_func = constructOrFunction(functions);
    functions.clear();
    functions.emplace_back(parameter_func);

    if (select->where())
    {
        functions.push_back(select->where());
    }

    // use AND function to combine the newly add IN functions and WHERE expression
    ASTPtr new_predicate = constructAndFunction(functions);

    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_predicate));

    LOG_TRACE(
        getLogger("optimizeBitMapParametersToWhere"),
        "Optimize {} bitmap function arguments to where expression, "
        "with {} IN elements",
        found_parameters,
        total_in_elems);
}
}
