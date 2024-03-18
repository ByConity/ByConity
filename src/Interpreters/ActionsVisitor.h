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

#pragma once

#include <string_view>
#include <Core/ColumnNumbers.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SubqueryForSet.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/Index/MergeTreeIndexHelper.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTPreparedParameter.h>
#include <Parsers/IAST.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

class ASTFunction;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

/// The case of an explicit enumeration of values.
SetPtr makeExplicitSet(
    const ASTFunction * node, const ActionsDAG & actions, bool create_ordered_set,
    ContextPtr context, const SizeLimits & limits, PreparedSets & prepared_sets);

/** Create a block for set from expression.
  * 'set_element_types' - types of what are on the left hand side of IN.
  * 'right_arg' - list of values: 1, 2, 3 or list of tuples: (1, 2), (3, 4), (5, 6).
  *
  *  We need special implementation for ASTFunction, because in case, when we interpret
  *  large tuple or array as function, `evaluateConstantExpression` works extremely slow.
  *
  *  Note: this and following functions are used in third-party applications in Arcadia, so
  *  they should be declared in header file.
  *
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context);

/** Create a block for set from literal.
  * 'set_element_types' - types of what are on the left hand side of IN.
  * 'right_arg' - Literal - Tuple or Array.
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const ASTPtr & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context);

/** For ActionsVisitor
  * A stack of ExpressionActions corresponding to nested lambda expressions.
  * The new action should be added to the highest possible level.
  * For example, in the expression "select arrayMap(x -> x + column1 * column2, array1)"
  *  calculation of the product must be done outside the lambda expression (it does not depend on x),
  *  and the calculation of the sum is inside (depends on x).
  */
struct ScopeStack : WithContext
{
    class Index;
    using IndexPtr = std::unique_ptr<Index>;

    struct Level
    {
        ActionsDAGPtr actions_dag;
        IndexPtr index;
        NameSet inputs;

        Level();
        Level(Level &&);
        ~Level();
    };

    using Levels = std::vector<Level>;

    Levels stack;

    ScopeStack(ActionsDAGPtr actions_dag, ContextPtr context_);

    void pushLevel(const NamesAndTypesList & input_columns);

    size_t getColumnLevel(const std::string & name);

    void addColumn(ColumnWithTypeAndName column);
    void addAlias(const std::string & name, std::string alias);
    void addArrayJoin(const std::string & source_name, std::string result_name);
    void addFunction(
            const FunctionOverloadResolverPtr & function,
            const Names & argument_names,
            std::string result_name);

    ActionsDAGPtr popLevel();

    const ActionsDAG & getLastActions() const;
    const Index & getLastActionsIndex() const;
    std::string dumpNames() const;
};

class ASTIdentifier;
class ASTFunction;
class ASTLiteral;

enum class GroupByKind
{
    NONE,
    ORDINARY,
    ROLLUP,
    CUBE,
    GROUPING_SETS,
};

/*
 * This class stores information about aggregation keys used in GROUP BY clause.
 * It's used for providing information about aggregation to GROUPING function
 * implementation.
*/
struct AggregationKeysInfo
{
    AggregationKeysInfo(
        std::reference_wrapper<const NamesAndTypesList> aggregation_keys_,
        std::reference_wrapper<const ColumnNumbersList> grouping_set_keys_,
        GroupByKind group_by_kind_)
        : aggregation_keys(aggregation_keys_)
        , grouping_set_keys(grouping_set_keys_)
        , group_by_kind(group_by_kind_)
    {}

    AggregationKeysInfo(const AggregationKeysInfo &) = default;
    AggregationKeysInfo(AggregationKeysInfo &&) = default;

    // Names and types of all used keys
    const NamesAndTypesList & aggregation_keys;
    // Indexes of aggregation keys used in each grouping set (only for GROUP BY GROUPING SETS)
    const ColumnNumbersList & grouping_set_keys;

    GroupByKind group_by_kind;
};

/// Collect ExpressionAction from AST. Returns PreparedSets and SubqueriesForSets too.
class ActionsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<ActionsMatcher, true>;

    struct Data : public WithContext
    {
        SizeLimits set_size_limit;
        size_t subquery_depth;
        const NamesAndTypesList & source_columns;
        PreparedSets & prepared_sets;
        SubqueriesForSets & subqueries_for_sets;
        bool no_subqueries;
        bool no_makeset;
        bool only_consts;
        bool create_source_for_in;
        size_t visit_depth;
        ScopeStack actions_stack;
        AggregationKeysInfo aggregation_keys_info;
        bool build_expression_with_window_functions;

        MergeTreeIndexContextPtr index_context;
        StorageMetadataPtr metadata_snapshot;

        /*
         * Remember the last unique column suffix to avoid quadratic behavior
         * when we add lots of column with same prefix. One counter for all
         * prefixes is good enough.
         */
        int next_unique_suffix;

        Data(
            ContextPtr context_,
            SizeLimits set_size_limit_,
            size_t subquery_depth_,
            const NamesAndTypesList & source_columns_,
            ActionsDAGPtr actions_dag,
            PreparedSets & prepared_sets_,
            SubqueriesForSets & subqueries_for_sets_,
            bool no_subqueries_,
            bool no_makeset_,
            bool only_consts_,
            bool create_source_for_in_,
            AggregationKeysInfo aggregation_keys_info_,
            bool build_expression_with_window_functions_ = false,
            MergeTreeIndexContextPtr index_context_ = nullptr,
            StorageMetadataPtr metadata_snapshot = nullptr);

        /// Does result of the calculation already exists in the block.
        bool hasColumn(const String & column_name) const;
        std::vector<std::string_view> getAllColumnNames() const;

        void addColumn(ColumnWithTypeAndName column)
        {
            actions_stack.addColumn(std::move(column));
        }

        void addAlias(const std::string & name, std::string alias)
        {
            actions_stack.addAlias(name, std::move(alias));
        }

        void addArrayJoin(const std::string & source_name, std::string result_name)
        {
            actions_stack.addArrayJoin(source_name, std::move(result_name));
        }

        void addFunction(const FunctionOverloadResolverPtr & function,
                         const Names & argument_names,
                         std::string result_name)
        {
            actions_stack.addFunction(function, argument_names, std::move(result_name));
        }

        ActionsDAGPtr getActions()
        {
            return actions_stack.popLevel();
        }

        /*
         * Generate a column name that is not present in the sample block, using
         * the given prefix and an optional numeric suffix.
         */
        String getUniqueName(const String & prefix)
        {
            auto result = prefix;

            // First, try the name without any suffix, because it is currently
            // used both as a display name and a column id.
            while (hasColumn(result))
            {
                result = prefix + "_" + toString(next_unique_suffix);
                ++next_unique_suffix;
            }

            return result;
        }
    };

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:

    static void visit(const ASTIdentifier & identifier, const ASTPtr & ast, Data & data);
    static void visit(const ASTFunction & node, const ASTPtr & ast, Data & data);
    static void visit(const ASTLiteral & literal, const ASTPtr & ast, Data & data);
    static void visit(const ASTPreparedParameter & prepared_param, const ASTPtr & ast, Data & data);
    static void visit(ASTExpressionList & expression_list, const ASTPtr & ast, Data & data);

    static SetPtr tryMakeSet(const ASTFunction & node, Data & data, bool no_subqueries, bool create_ordered_set = false);
    static SetPtr makeSet(const ASTFunction & node, Data & data, bool no_subqueries, bool create_ordered_set = false);
    static ASTs doUntuple(const ASTFunction * function, ActionsMatcher::Data & data);
    static std::optional<NameAndTypePair> getNameAndTypeFromAST(const ASTPtr & ast, Data & data);
};

using ActionsVisitor = ActionsMatcher::Visitor;

}
