#include <Optimizer/MaterializedView/ExpressionSubstitution.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Core/NameToType.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/EqualityASTMap.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanNode.h>

#include <unordered_map>

namespace DB
{

namespace MaterializedView
{
    using ConstASTMap = EqualityASTMap<ConstASTPtr>;
    class EquivalencesRewriter : public SimpleExpressionRewriter<const ConstASTMap>
    {
    public:
        static ASTPtr rewrite(ASTPtr & expression, const ExpressionEquivalences & expression_equivalences)
        {
            EquivalencesRewriter rewriter;
            const auto map = expression_equivalences.representMap();
            return ASTVisitorUtil::accept(expression, rewriter, map);
        }

        ASTPtr visitNode(ASTPtr & node, const ConstASTMap & context) override
        {
            auto it = context.find(node);
            if (it != context.end())
                return it->second->clone();
            return SimpleExpressionRewriter::visitNode(node, context);
        }
    };

    class ExpressionNormalizer : public SimpleExpressionRewriter<const Void>
    {
    public:
        static ASTPtr normalize(ASTPtr expression)
        {
            ExpressionNormalizer normalizer;
            const Void c;
            return ASTVisitorUtil::accept(expression, normalizer, c);
        }

        ASTPtr visitASTFunction(ASTPtr & node, const Void & c) override
        {
            auto res = visitNode(node, c);
            auto & function = res->as<ASTFunction &>();
            if (function.name.ends_with("SimpleState"))
                function.name = function.name.substr(0, function.name.size() - 11);
            return res;
        }
    };

    class SwapTableInputRefRewriter : public SimpleExpressionRewriter<const TableInputRefMap>
    {
    public:
        static ASTPtr rewrite(ASTPtr & expression, const TableInputRefMap & table_mapping)
        {
            SwapTableInputRefRewriter rewriter;
            return ASTVisitorUtil::accept(expression, rewriter, table_mapping);
        }

        ASTPtr visitASTTableColumnReference(ASTPtr & node, const TableInputRefMap & context) override
        {
            auto & table_column_ref = node->as<ASTTableColumnReference &>();
            auto storage_ptr = (const_cast<IStorage *>(table_column_ref.storage))->shared_from_this();
            TableInputRef ref{std::move(storage_ptr), table_column_ref.unique_id};
            if (!context.contains(ref))
                throw Exception("table ref not exists: " + ref.toString(), ErrorCodes::LOGICAL_ERROR);
            const auto & replace_table_ref = context.at(ref);
            return std::make_shared<ASTTableColumnReference>(
                replace_table_ref.storage.get(), replace_table_ref.unique_id, table_column_ref.column_name);
        }
    };

    ASTPtr normalizeExpression(
        const ConstASTPtr & expression,
        const SymbolTransformMap & symbol_transform_map,
        const TableInputRefMap & table_mapping,
        const ExpressionEquivalences & expression_equivalences)
    {
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = ExpressionNormalizer::normalize(lineage);
        lineage = SwapTableInputRefRewriter::rewrite(lineage, table_mapping);
        lineage = EquivalencesRewriter::rewrite(lineage, expression_equivalences);
        return lineage;
    }

    /**
     * it is used to rewrite expression from view to uniform expression.
     */
    ASTPtr normalizeExpression(
        const ConstASTPtr & expression,
        const SymbolTransformMap & symbol_transform_map,
        const ExpressionEquivalences & expression_equivalences)
    {
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = ExpressionNormalizer::normalize(lineage);
        lineage = EquivalencesRewriter::rewrite(lineage, expression_equivalences);
        return lineage;
    }

    ASTPtr normalizeExpression(const ConstASTPtr & expression, const ExpressionEquivalences & expression_equivalences)
    {
        auto expr = expression->clone();
        expr = ExpressionNormalizer::normalize(expr);
        expr = EquivalencesRewriter::rewrite(expr, expression_equivalences);
        return expr;
    }

    ASTPtr normalizeExpression(const ConstASTPtr & expression, const SymbolTransformMap & symbol_transform_map)
    {
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = ExpressionNormalizer::normalize(lineage);
        return lineage;
    }

    ASTPtr normalizeExpression(const ConstASTPtr & expression, const TableInputRefMap & table_mapping)
    {
        auto expr = expression->clone();
        expr = ExpressionNormalizer::normalize(expr);
        return SwapTableInputRefRewriter::rewrite(expr, table_mapping);
    }

    ASTPtr normalizeExpression(
        const ConstASTPtr & expression, const SymbolTransformMap & symbol_transform_map, const TableInputRefMap & table_mapping)
    {
        auto lineage = symbol_transform_map.inlineReferences(expression);
        lineage = ExpressionNormalizer::normalize(lineage);
        lineage = SwapTableInputRefRewriter::rewrite(lineage, table_mapping);
        return lineage;
    }

    bool isValidExpression(const ConstASTPtr & expression, const std::unordered_set<String> & output_columns, bool allow_aggregate)
    {
        class ExpressionChecker : public ConstASTVisitor<bool, bool>
        {
        public:
            explicit ExpressionChecker(const std::unordered_set<String> & output_columns_) : output_columns(output_columns_) { }

            bool visitNode(const ConstASTPtr & ast, bool & c) override
            {
                return std::all_of(ast->children.begin(), ast->children.end(), [&](const auto & child) {
                    return ASTVisitorUtil::accept(child, *this, c);
                });
            }

            bool visitASTTableColumnReference(const ConstASTPtr &, bool &) override { return false; }

            bool visitASTIdentifier(const ConstASTPtr & node, bool &) override
            {
                return output_columns.count(node->as<const ASTIdentifier &>().name());
            }

            bool visitASTFunction(const ConstASTPtr & node, bool & allow_aggregate) override
            {
                if (!AggregateFunctionFactory::instance().isAggregateFunctionName(node->as<ASTFunction &>().name))
                    return visitNode(node, allow_aggregate);
                if (!allow_aggregate)
                    return false;
                bool allow_nested_aggregate = false;
                return visitNode(node, allow_nested_aggregate);
            }

        private:
            const std::unordered_set<String> & output_columns;
        };
        ExpressionChecker checker{output_columns};
        return ASTVisitorUtil::accept(expression, checker, allow_aggregate);
    }

    std::optional<ASTPtr> rewriteExpression(
        ASTPtr expression,
        const EqualityASTMap<ConstASTPtr> & view_output_columns_map,
        const std::unordered_set<String> & output_columns,
        bool allow_aggregate)
    {
        EquivalencesRewriter rewriter;
        auto rewrite_expression = ASTVisitorUtil::accept(expression, rewriter, view_output_columns_map);
        if (isValidExpression(rewrite_expression, output_columns, allow_aggregate))
            return rewrite_expression;
        return {}; // rewrite fail, bail out
    }

    AggregateDefaultValueProvider
    getAggregateDefaultValueProvider(const std::shared_ptr<const AggregatingStep> & query_step, ContextMutablePtr context)
    {
        return [&, context](const ASTFunction & aggregate_ast_function) -> std::optional<Field> {
            if (!query_step)
                return {};
            const auto & input_types = query_step->getInputStreams()[0].header.getNamesAndTypes();
            DataTypes agg_argument_types;
            if (aggregate_ast_function.arguments)
            {
                for (auto & argument : aggregate_ast_function.arguments->children)
                {
                    auto type = TypeAnalyzer::getType(argument, context, input_types);
                    agg_argument_types.emplace_back(type);
                }
            }

            Array parameters;
            if (aggregate_ast_function.parameters)
            {
                for (auto & argument : aggregate_ast_function.parameters->children)
                    parameters.emplace_back(argument->as<ASTLiteral &>().value);
            }

            AggregateFunctionProperties properties;
            auto aggregate_function
                = AggregateFunctionFactory::instance().tryGet(aggregate_ast_function.name, agg_argument_types, parameters, properties);
            if (!aggregate_function)
            {
                return {};
            }

            AlignedBuffer place_buffer(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
            AggregateDataPtr place = place_buffer.data();
            aggregate_function->create(place);

            auto column = aggregate_function->getReturnType()->createColumn();
            auto arena = std::make_unique<Arena>();
            aggregate_function->insertResultInto(place, *column, arena.get());
            Field default_value;
            column->get(0, default_value);
            return default_value;
        };
    }

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
        AggregateDefaultValueProvider & default_value_provider)
    {
        class ExpressionWithAggregateRewriter : public ASTVisitor<std::optional<ASTPtr>, Void>
        {
        public:
            ExpressionWithAggregateRewriter(
                const EqualityASTMap<ConstASTPtr> & view_output_columns_map_,
                const std::unordered_set<String> & output_columns_,
                bool view_contains_aggregates_,
                bool need_rollup_,
                bool empty_groupings_,
                AggregateDefaultValueProvider & default_value_provider_)
                : view_output_columns_map(view_output_columns_map_)
                , output_columns(output_columns_)
                , view_contains_aggregates(view_contains_aggregates_)
                , need_rollup(need_rollup_)
                , empty_groupings(empty_groupings_)
                , default_value_provider(default_value_provider_)
            {
            }

            std::optional<ASTPtr> visitNode(ASTPtr & node, Void & c) override
            {
                if (!need_rollup || !Utils::containsAggregateFunction(node))
                {
                    if (view_output_columns_map.contains(node))
                    {
                        const auto & result = view_output_columns_map.at(node);
                        return result->clone();
                    }
                }

                for (auto & child : node->children)
                {
                    auto result = ASTVisitorUtil::accept(child, *this, c);
                    if (!result)
                        return {};
                    if (*result != child)
                        child = std::move(*result);
                }
                return node;
            }

            std::optional<ASTPtr> visitASTFunction(ASTPtr & node, Void & c) override
            {
                auto * function = node->as<ASTFunction>();
                if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
                    return visitNode(node, c);

                // 0. view is not aggregate
                if (!view_contains_aggregates)
                {
                    return rewriteExpression(node, view_output_columns_map, output_columns, true);
                }

                // try to rewrite the expression with the following rules:
                // 1. state aggregate support rollup directly.
                if (!function->name.ends_with("State"))
                {
                    auto state_function = function->clone();
                    if (function->name.ends_with("Merge"))
                    {
                        function->name = function->name.substr(0, function->name.size() - 5);
                    }
                    state_function->as<ASTFunction &>().name = function->name + "State";
                    auto rewrite_expression = rewriteExpression(state_function, view_output_columns_map, output_columns);
                    if (rewrite_expression)
                    {
                        auto rewritten_function = std::make_shared<ASTFunction>();
                        rewritten_function->name = need_rollup ? (function->name + "Merge") : ("finalizeAggregation");
                        rewritten_function->arguments = std::make_shared<ASTExpressionList>();
                        rewritten_function->children.emplace_back(rewritten_function->arguments);
                        rewritten_function->arguments->children.push_back(*rewrite_expression);
                        rewritten_function->parameters = function->parameters;
                        return rewritten_function;
                    }
                }

                // 2. rewrite with direct match
                auto rewrite = rewriteExpression(node, view_output_columns_map, output_columns, true);
                if (!rewrite)
                    return {};
                if (isAggregateFunction(*rewrite))
                {
                    // special case, some aggregate functions allow to be calculated from group by keys results:
                    //  MV:      select empid from emps group by empid
                    //  Query:   select max(empid) from emps group by empid
                    //  rewrite: select max(empid) from emps group by empid
                    if (!canRollupOnGroupByResults(rewrite.value()->as<const ASTFunction>()->name))
                        return {};
                    return rewrite;
                }
                else
                {
                    if (need_rollup)
                        rewrite = getRollupAggregate(function, *rewrite);
                    if (!rewrite)
                        return {};
                    if (empty_groupings)
                        rewrite = rewriteEmptyGroupings(*rewrite, *function, default_value_provider);
                    return rewrite;
                }
            }

            static std::optional<ASTPtr> getRollupAggregate(const ASTFunction * original_function, const ASTPtr & rewrite)
            {
                String rollup_aggregate_name = getRollupAggregateName(original_function->name);
                if (rollup_aggregate_name.empty())
                    return {};
                auto rollup_function = std::make_shared<ASTFunction>();
                rollup_function->name = std::move(rollup_aggregate_name);
                rollup_function->arguments = std::make_shared<ASTExpressionList>();
                rollup_function->arguments->children.emplace_back(rewrite);
                rollup_function->parameters = original_function->parameters;
                rollup_function->children.emplace_back(rollup_function->arguments);
                return rollup_function;
            }

            static bool isAggregateFunction(const ASTPtr & ast)
            {
                if (const auto * function = ast->as<const ASTFunction>())
                    if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
                        return true;
                return false;
            }

            static String getRollupAggregateName(const String & name)
            {
                if (name == "count")
                    return "sum";
                else if (name == "min" || name == "max" || name == "sum")
                    return name;
                return {};
            }

            static bool canRollupOnGroupByResults(const String & name)
            {
                return name.ends_with("distinct") || name.starts_with("uniq") || name == "min" || name == "max";
            }

            /**
             * if query is aggregate with empty groupings,
             * rewrite result is not aggregate or aggregate changed (eg. rollup),
             * we need add coalesce to assign default value from origin aggregate function for empty result.
             */
            static std::optional<ASTPtr> rewriteEmptyGroupings(
                const ASTPtr & rewrite,
                const ASTFunction & original_aggregate_function,
                AggregateDefaultValueProvider & default_value_provider)
            {
                if (rewrite->getType() == ASTType::ASTFunction && getFunctionName(rewrite) == original_aggregate_function.name)
                {
                    return rewrite;
                }

                ASTPtr any_aggregate_function;
                if (rewrite->getType() == ASTType::ASTFunction
                    && AggregateFunctionFactory::instance().isAggregateFunctionName(getFunctionName(rewrite)))
                {
                    any_aggregate_function = rewrite;
                }
                else
                {
                    any_aggregate_function = makeASTFunction("any", rewrite);
                }
                auto default_value = default_value_provider(original_aggregate_function);
                if (!default_value)
                    return {};
                return std::make_optional(
                    makeASTFunction("coalesce", any_aggregate_function, std::make_shared<ASTLiteral>(*default_value)));
            }

            const EqualityASTMap<ConstASTPtr> & view_output_columns_map;
            const std::unordered_set<String> & output_columns;
            bool view_contains_aggregates;
            bool need_rollup;
            bool empty_groupings;
            AggregateDefaultValueProvider & default_value_provider;
        };
        ExpressionWithAggregateRewriter rewriter{
            view_output_columns_map, output_columns, view_contains_aggregates, need_rollup, empty_groupings, default_value_provider};
        Void c;
        auto rewrite = ASTVisitorUtil::accept(expression, rewriter, c);
        if (rewrite && isValidExpression(*rewrite, output_columns, true))
            return rewrite;
        return {};
    }

    class ColumnReferenceRewriter : public SimpleExpressionRewriter<const Void>
    {
    public:
        ASTPtr visitASTTableColumnReference(ASTPtr & node, const Void &) override
        {
            auto & column_ref = node->as<ASTTableColumnReference &>();
            if (!belonging.has_value())
                belonging = column_ref.unique_id;
            else if (*belonging != column_ref.unique_id)
                belonging = std::nullopt;
            return std::make_shared<ASTIdentifier>(column_ref.column_name);
        }

        std::optional<UInt32> belonging;
    };
}
}
