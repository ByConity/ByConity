#pragma once

#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace MaterializedView
{
    using ExpressionEquivalences = Equivalences<ConstASTPtr, EqualityASTMap>;
    using TableInputRefMap = std::unordered_map<TableInputRef, TableInputRef, TableInputRefHash, TableInputRefEqual>;
    using AggregateDefaultValueProvider = std::function<std::optional<Field>(const ASTFunction &)>;

    /**
     * First, it rewrite identifiers in expression recursively, get lineage-form expression.
     * Then, it swaps the all TableInputRefs using table_mapping.
     * Finally, it rewrite equivalent expressions to unified expression.
     *
     * <p> it is used to rewrite expression from query to view-based uniform expression.
     */
    ASTPtr normalizeExpression(
        const ConstASTPtr & expression,
        const SymbolTransformMap & symbol_transform_map,
        const TableInputRefMap & table_mapping,
        const ExpressionEquivalences & expression_equivalences);

    ASTPtr normalizeExpression(
        const ConstASTPtr & expression,
        const SymbolTransformMap & symbol_transform_map,
        const ExpressionEquivalences & expression_equivalences);

    ASTPtr normalizeExpression(const ConstASTPtr & expression, const SymbolTransformMap & symbol_transform_map);

    ASTPtr normalizeExpression(const ConstASTPtr & expression, const ExpressionEquivalences & expression_equivalences);

    ASTPtr normalizeExpression(
        const ConstASTPtr & expression, const SymbolTransformMap & symbol_transform_map, const TableInputRefMap & table_mapping);

    ASTPtr normalizeExpression(const ConstASTPtr & expression, const TableInputRefMap & table_mapping);

    /**
     * It rewrite input expression using output column.
     * If any of the expressions in the input expression cannot be mapped, it will return null.
     */
    std::optional<ASTPtr> rewriteExpression(
        ASTPtr expression,
        const EqualityASTMap<ConstASTPtr> & view_output_columns_map,
        const std::unordered_set<String> & output_columns,
        bool allow_aggregate = false);

    /**
     * Checks whether there are identifiers in input expression are mapped into output columns,
     * or there are unmapped table references,
     * or there are nested aggregate functions.
     */
    bool isValidExpression(const ConstASTPtr & expression, const std::unordered_set<String> & output_columns, bool allow_aggregate = false);

    /**
     * It rewrite input expression using output column.
     * If any of the expressions in the input expression cannot be mapped, it will return null.
     *
     * <p> Compared with rewriteExpression, it supports expression contains aggregates, and do rollup rewrite.
     */
    std::optional<ASTPtr> rewriteExpressionContainsAggregates(
        ASTPtr expression,
        const EqualityASTMap<ConstASTPtr> & view_output_columns_map,
        const std::unordered_set<String> & output_columns,
        bool view_contains_aggregates,
        bool need_rollup,
        bool empty_groupings,
        AggregateDefaultValueProvider & default_value_provider);


    AggregateDefaultValueProvider
    getAggregateDefaultValueProvider(const std::shared_ptr<const AggregatingStep> & query_step, ContextMutablePtr context);
}
}
