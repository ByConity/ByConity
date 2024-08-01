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

#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Analyzers/ASTEquals.h>
#include <Analyzers/ResolvedWindow.h>
#include <Analyzers/Scope.h>
#include <Analyzers/SubColumnID.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/asof.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Storages/IStorage_fwd.h>
#include <Common/LinkedHashSet.h>
#include <common/logger_useful.h>

#include <utility>
#include <vector>
#include <unordered_map>

namespace DB
{

struct Analysis;
using AnalysisPtr = std::shared_ptr<Analysis>;

using ASTFunctionPtr = std::shared_ptr<ASTFunction>;

using ExpressionTypes = std::unordered_map<ASTPtr, DataTypePtr>;

using CTEId = UInt32;

struct JoinUsingAnalysis
{
    std::vector<ASTPtr> join_key_asts;
    std::vector<size_t> left_join_fields;
    DataTypes left_coercions;
    std::vector<size_t> right_join_fields;
    DataTypes right_coercions;
    // field index of scope -> join key index of using list
    std::unordered_map<size_t, size_t> left_join_field_reverse_map;
    std::unordered_map<size_t, size_t> right_join_field_reverse_map;
    std::vector<bool> require_right_keys;
};

struct JoinEqualityCondition
{
    ASTPtr left_ast;
    ASTPtr right_ast;
    DataTypePtr left_coercion;
    DataTypePtr right_coercion;

    JoinEqualityCondition(ASTPtr left_ast_, ASTPtr right_ast_, DataTypePtr left_coercion_, DataTypePtr right_coercion_)
        : left_ast(std::move(left_ast_))
        , right_ast(std::move(right_ast_))
        , left_coercion(std::move(left_coercion_))
        , right_coercion(std::move(right_coercion_))
    {}
};

struct JoinInequalityCondition
{
    ASTPtr left_ast;
    ASTPtr right_ast;
    ASOF::Inequality inequality;
    DataTypePtr left_coercion;
    DataTypePtr right_coercion;

    JoinInequalityCondition(ASTPtr left_ast_, ASTPtr right_ast_, ASOF::Inequality inequality_, DataTypePtr left_coercion_, DataTypePtr right_coercion_)
        : left_ast(std::move(left_ast_))
        , right_ast(std::move(right_ast_))
        , inequality(inequality_)
        , left_coercion(std::move(left_coercion_))
        , right_coercion(std::move(right_coercion_))
    {}
};

// statement:
//   t JOIN s ON t.a = s.b AND s.a < t.b AND s.a + t.a > 10
//
// analyze result:
//   equality conditions: [(t.a, s.b)]
//   inequality conditions: [(t.b, s.a, GREATER)]
//   complex expressions: [ s.a + t.a > 10 ]
struct JoinOnAnalysis
{
    std::vector<JoinEqualityCondition> equality_conditions;
    std::vector<JoinInequalityCondition> inequality_conditions;
    std::vector<ASTPtr> complex_expressions;

    ASOF::Inequality getAsofInequality()
    {
        return inequality_conditions.front().inequality;
    }
};

// statement:
//   GROUP BY GROUPING SETS (
//     (a, b)
//     (b, c)
//     (a)
//   )
//
// analyze result:
//   grouping_expressions: [a, b, c]
//   grouping_sets: [[a, b], [b, c], [a]]
struct GroupByAnalysis
{
    std::vector<ASTPtr> grouping_expressions;
    std::vector<std::vector<ASTPtr>> grouping_sets;
};

struct AggregateAnalysis
{
    ASTFunctionPtr expression;
    AggregateFunctionPtr function;
    Array parameters;
};

struct WindowAnalysis
{
    ASTFunctionPtr expression;
    String window_name;
    ResolvedWindowPtr resolved_window;
    AggregateFunctionPtr aggregator;
    Array parameters;
};

using WindowAnalysisPtr = std::shared_ptr<WindowAnalysis>;

struct StorageAnalysis
{
    String database;
    String table;
    StoragePtr storage;
};

struct CTEAnalysis
{
    CTEId id;
    ASTSubquery * representative;
    UInt64 ref_count;

    bool isSharable() const
    {
        return ref_count >= 2;
    }
};

using SubColumnIDSet = std::unordered_set<SubColumnID, SubColumnID::Hash>;

struct SubColumnReference
{
    ResolvedField field;
    SubColumnID column_id;

    ScopePtr getScope() const
    {
        return field.scope;
    }

    size_t getFieldHierarchyIndex() const
    {
        return field.hierarchy_index;
    }

    const SubColumnID & getColumnID() const
    {
        return column_id;
    }
};

struct HintAnalysis
{
    size_t leading_hint_count = 0;
};

struct ArrayJoinDescription
{
    ASTPtr expr;
    bool create_new_field = false;
};
using ArrayJoinDescriptions = std::vector<ArrayJoinDescription>;
struct ArrayJoinAnalysis
{
    bool is_left_array_join;
    ArrayJoinDescriptions descriptions;
};

struct InsertAnalysis
{
    StoragePtr storage;
    StorageID storage_id;
    NamesAndTypes columns;
};

struct OutfileAnalysis
{
    String out_file;
    String format;
    String compression_method;
    size_t compression_level;
};

struct ColumnWithType
{
    DataTypePtr type;
    ColumnPtr column;

    ColumnWithType() { }
    ColumnWithType(const DataTypePtr & type_, const ColumnPtr & column_)
        : type(type_), column(column_)
    {}
    ColumnWithType(const DataTypePtr & type_) : type(type_){}
};

template<typename Key, typename Val>
using ListMultimap = std::unordered_map<Key, std::vector<Val>>;

struct Analysis
{
    ScopeFactory scope_factory;
    Poco::Logger * logger = &Poco::Logger::get("Analysis");

    /// Scopes
    // Regular scopes in an ASTSelectQuery, kept by below convention:
    //     ASTTableExpression::database_and_table_name -> table scope
    //     ASTTableExpression::subquery -> subquery scope
    //     ASTTableExpression::table_function -> table function scope
    //     ASTTableJoin -> joined scope
    //     ASTTablesInSelectQuery -> source scope
    //     ASTFunction for lambda expression -> lambda scope
    std::unordered_map<IAST *, ScopePtr> scopes;
    void setScope(IAST &, ScopePtr);
    ScopePtr getScope(IAST &);

    std::unordered_map<ASTSelectQuery *, ScopePtr> query_without_from_scopes;
    void setQueryWithoutFromScope(ASTSelectQuery &, ScopePtr);
    ScopePtr getQueryWithoutFromScope(ASTSelectQuery &);

    // table storage scopes doesn't contain alias columns
    std::unordered_map<ASTIdentifier *, ScopePtr> table_storage_scopes;
    void setTableStorageScope(ASTIdentifier &, ScopePtr);
    ScopePtr getTableStorageScope(ASTIdentifier &);
    std::unordered_map<ASTIdentifier *, ScopePtr> & getTableStorageScopeMap();

    std::unordered_map<ASTIdentifier *, ASTs> table_alias_columns;
    void setTableAliasColumns(ASTIdentifier &, ASTs);
    ASTs & getTableAliasColumns(ASTIdentifier &);

    /*
    std::unordered_map<ASTIdentifier *, ASTs> table_column_masks;
    void setTableColumnMasks(ASTIdentifier &, ASTs);
    ASTs & getTableColumnMasks(ASTIdentifier &);
    */

    /// Expressions
    std::unordered_map<ASTPtr, ColumnWithType> expression_column_with_types;
    bool hasExpressionColumnWithType(const ASTPtr & expression);
    void setExpressionColumnWithType(const ASTPtr & expression, const ColumnWithType & column_with_type);
    std::optional<ColumnWithType> tryGetExpressionColumnWithType(const ASTPtr & expression);
    ExpressionTypes getExpressionTypes();
    std::unordered_map<ASTPtr, ColumnWithType> getExpressionColumnWithTypes()
    {
        return expression_column_with_types;
    }
    DataTypePtr getExpressionType(const ASTPtr & expression);

    // ASTIdentifier, ASTFieldReference
    std::unordered_map<ASTPtr, ResolvedField> column_references;
    void setColumnReference(const ASTPtr & ast, const ResolvedField & resolved);
    std::optional<ResolvedField> tryGetColumnReference(const ASTPtr & ast);

    // Which columns are needed to read, used for planning phase column pruning.
    // Columns used in alias columns will be included regardless of whether their
    // alias columns are used.
    //
    // ASTTableIdentifier -> index of table storage scope
    std::unordered_map<const IAST *, std::set<size_t>> read_columns;
    void addReadColumn(const IAST * table_ast, size_t field_index);
    void addReadColumn(const ResolvedField & resolved_field, bool add_used);
    const std::set<size_t> & getReadColumns(const IAST & table_ast);

    // ASTIdentifier
    std::unordered_map<ASTPtr, ResolvedField> lambda_argument_references;
    void setLambdaArgumentReference(const ASTPtr & ast, const ResolvedField & resolved);
    std::optional<ResolvedField> tryGetLambdaArgumentReference(const ASTPtr & ast);

    /// Aggregates
    ListMultimap<ASTSelectQuery *, AggregateAnalysis> aggregate_results;
    std::vector<AggregateAnalysis> & getAggregateAnalysis(ASTSelectQuery & select_query);
    bool needAggregate(ASTSelectQuery & select_query);

    ListMultimap<ASTSelectQuery *, std::pair<String, UInt16>> interest_events;
    std::vector<std::pair<String, UInt16>> & getInterestEvents(ASTSelectQuery & select_query);

    ListMultimap<ASTSelectQuery *, ASTFunctionPtr> grouping_operations;
    std::vector<ASTFunctionPtr> & getGroupingOperations(ASTSelectQuery & select_query);

    /// Windows
    ListMultimap<ASTSelectQuery *, WindowAnalysisPtr> window_results_by_select_query;
    std::unordered_map<ASTPtr, WindowAnalysisPtr> window_results_by_ast;
    void addWindowAnalysis(ASTSelectQuery & select_query, WindowAnalysisPtr analysis);
    WindowAnalysisPtr getWindowAnalysis(const ASTPtr & ast);
    std::vector<WindowAnalysisPtr> & getWindowAnalysisOfSelectQuery(ASTSelectQuery & select_query);

    /// Subqueries
    std::unordered_map<ASTPtr, bool> subquery_support_semi_anti;
    ListMultimap<ASTSelectQuery * , ASTPtr> scalar_subqueries;
    std::vector<ASTPtr> & getScalarSubqueries(ASTSelectQuery & select_query);

    ListMultimap<ASTSelectQuery *, ASTPtr> in_subqueries;
    std::vector<ASTPtr> & getInSubqueries(ASTSelectQuery & select_query);

    ListMultimap<ASTSelectQuery *, ASTPtr> exists_subqueries;
    std::vector<ASTPtr> & getExistsSubqueries(ASTSelectQuery & select_query);

    ListMultimap<ASTSelectQuery *, ASTPtr> quantified_comparison_subqueries;
    std::vector<ASTPtr> & getQuantifiedComparisonSubqueries(ASTSelectQuery & select_query);

    // CTE(common table expressions)
    ASTMap<CTEAnalysis> common_table_expressions;
    void registerCTE(ASTSubquery & subquery);
    std::optional<CTEAnalysis> tryGetCTEAnalysis(ASTSubquery & subquery);

    // Prewhere
    std::unordered_map<ASTSelectQuery *, ASTPtr> prewheres;
    void setPrewhere(ASTSelectQuery & select_query, const ASTPtr & prewhere);
    ASTPtr tryGetPrewhere(ASTSelectQuery & select_query);

    /// Join
    std::unordered_map<ASTTableJoin *, JoinUsingAnalysis> join_using_results;
    JoinUsingAnalysis & getJoinUsingAnalysis(ASTTableJoin &);

    std::unordered_map<ASTTableJoin *, JoinOnAnalysis> join_on_results;
    JoinOnAnalysis & getJoinOnAnalysis(ASTTableJoin &);

    // ASTTableExpression::database_and_table/ASTTableExpression::table_function
    LinkedHashMap<const IAST *, StorageAnalysis> storage_results;
    const StorageAnalysis & getStorageAnalysis(const IAST &);
    const LinkedHashMap<const IAST *, StorageAnalysis> & getStorages() const;

    /// Select
    std::unordered_map<ASTSelectQuery *, ASTs> select_expressions;
    ASTs & getSelectExpressions(ASTSelectQuery & select_query);

    /// Group by
    std::unordered_map<ASTSelectQuery *, GroupByAnalysis> group_by_results;
    GroupByAnalysis & getGroupByAnalysis(ASTSelectQuery & select_query);

    /// Order by
    std::unordered_map<ASTSelectQuery *, std::vector<std::shared_ptr<ASTOrderByElement>>> order_by_results;
    std::vector<std::shared_ptr<ASTOrderByElement>> & getOrderByAnalysis(ASTSelectQuery & select_query);

    /// Limit By
    std::unordered_map<ASTSelectQuery *, UInt64> limit_by_values;
    UInt64 getLimitByValue(ASTSelectQuery & select_query);

    ListMultimap<ASTSelectQuery *, ASTPtr> limit_by_items;
    std::vector<ASTPtr> & getLimitByItem(ASTSelectQuery & select_query);

    /// Limit By Offset
    std::unordered_map<ASTSelectQuery *, UInt64> limit_by_offset_values;
    UInt64 getLimitByOffsetValue(ASTSelectQuery & select_query);

    /// Limit
    std::unordered_map<ASTSelectQuery *, UInt64> limit_lengths;
    UInt64 getLimitLength(ASTSelectQuery & select_query);

    /// Offset
    std::unordered_map<ASTSelectQuery *, UInt64> limit_offsets;
    UInt64 getLimitOffset(ASTSelectQuery & select_query);

    /// Windows
    // ASTSelectQuery -> (window name -> ASTWindow)
    std::unordered_map<ASTSelectQuery *, std::unordered_map<String, ResolvedWindowPtr>> registered_windows;
    void setRegisteredWindow(ASTSelectQuery &, const String &, ResolvedWindowPtr &);
    ResolvedWindowPtr getRegisteredWindow(ASTSelectQuery &, const String &);
    const std::unordered_map<String, ResolvedWindowPtr> & getRegisteredWindows(ASTSelectQuery &);

    /// Output format for ASTSelectQuery/ASTSelectWithUnionQuery
    std::unordered_map<IAST *, FieldDescriptions> output_descriptions;
    void setOutputDescription(IAST & ast, const FieldDescriptions & field_descs);
    FieldDescriptions & getOutputDescription(IAST & ast);
    bool hasOutputDescription(IAST & ast);

    /// Sub column optimization
    std::unordered_map<ASTPtr, SubColumnReference> sub_column_references;
    void setSubColumnReference(const ASTPtr & ast, const SubColumnReference & reference);
    std::optional<SubColumnReference> tryGetSubColumnReference(const ASTPtr & ast);

    std::unordered_map<const IAST *, std::vector<SubColumnIDSet>> read_sub_columns;
    void addReadSubColumn(const IAST * table_ast, size_t field_index, const SubColumnID & sub_column_id);
    const std::vector<SubColumnIDSet> & getReadSubColumns(const IAST & table_ast);

    /// Type coercion
    // expression-level coercion
    std::unordered_map<ASTPtr, DataTypePtr> type_coercions;
    void setTypeCoercion(const ASTPtr & expression, const DataTypePtr & coerced_type);
    DataTypePtr getTypeCoercion(const ASTPtr & expression);

    // Type coercion for set operation element
    std::unordered_map<IAST *, DataTypes> relation_type_coercions;
    void setRelationTypeCoercion(IAST &, const DataTypes &);
    bool hasRelationTypeCoercion(IAST &);
    const DataTypes & getRelationTypeCoercion(IAST &);

    /// Non-deterministic functions
    std::unordered_set<IAST *> non_deterministic_functions;
    void addNonDeterministicFunctions(IAST & ast);

    /// record hints info
    HintAnalysis hint_analysis;
    HintAnalysis & getHintInfo() { return hint_analysis; }

    /// outfile info
    std::optional<OutfileAnalysis> outfile_analysis;
    std::optional<OutfileAnalysis> & getOutfileInfo() { return outfile_analysis; }

    std::unordered_map<ASTSelectQuery *, ArrayJoinAnalysis> array_join_analysis;
    ArrayJoinAnalysis & getArrayJoinAnalysis(ASTSelectQuery & select_query);

    /// Insert
    std::optional<InsertAnalysis> insert_analysis;
    std::optional<InsertAnalysis> & getInsert() { return insert_analysis; }

    // Which columns are used in query, used for EXPLAIN ANALYSIS reporting.
    // A difference with read_columns is, columns used in alias columns are not included.
    std::unordered_map<StorageID, LinkedHashSet<String>> used_columns;
    void addUsedColumn(const StorageID & storage_id, const String & column)
    {
        used_columns[storage_id].emplace(column);
    }
    const std::unordered_map<StorageID, LinkedHashSet<String>> & getUsedColumns() const
    {
        return used_columns;
    }

    // Which functions are used in query.
    std::set<String> used_functions;
    void addUsedFunction(const String & function)
    {
        used_functions.emplace(function);
    }
    const std::set<String> & getUsedFunctions() const
    {
        return used_functions;
    }

    std::unordered_map<String, std::vector<String>> function_arguments;
    void addUsedFunctionArgument(const String & func_name, ColumnsWithTypeAndName & processed_arguments);
};

}
