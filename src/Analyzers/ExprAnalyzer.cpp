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

#include <Analyzers/ExprAnalyzer.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/function_utils.h>
#include <Analyzers/postExprAnalyze.h>
#include <Analyzers/tryEvaluateConstantExpression.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/InternalFunctionRuntimeFilter.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/join_common.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/ASTVisitor.h>
#include <QueryPlan/Void.h>
#include <Poco/String.h>
#include <Common/StringUtils/StringUtils.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int ILLEGAL_AGGREGATION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

struct AnalyzeContext
{
    bool only_and;
};

class ExprAnalyzerVisitor : public ASTVisitor<ColumnWithTypeAndName, AnalyzeContext>
{
public:
    // Call `process` instead of `ASTVisitorUtil::accept` to process a node,
    // as there are some common logics in `process` method.
    ColumnWithTypeAndName process(ASTPtr & node, AnalyzeContext &);
    ColumnsWithTypeAndName processNodes(ASTs & nodes, AnalyzeContext &);

    ColumnWithTypeAndName visitNode(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTFunction(ASTPtr & node, AnalyzeContext &) override;
    // Note that this method is only used for processing scalar subquery.
    // For generic subquery expression processing, see `handleSubquery`.
    ColumnWithTypeAndName visitASTSubquery(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTIdentifier(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTFieldReference(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTLiteral(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTOrderByElement(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTQuantifiedComparison(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTTableColumnReference(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTPreparedParameter(ASTPtr & node, AnalyzeContext &) override;
    ColumnWithTypeAndName visitASTExpressionList(ASTPtr & node, AnalyzeContext &) override;

    ExprAnalyzerVisitor(ContextPtr context_, Analysis & analysis_, ScopePtr scope_, ExprAnalyzerOptions options_)
        : context(std::move(context_))
        , analysis(analysis_)
        , options(std::move(options_))
        , use_ansi_semantic(context->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
        , enable_implicit_type_conversion(context->getSettingsRef().enable_implicit_type_conversion)
        , allow_extended_conversion(context->getSettingsRef().allow_extended_type_conversion)
        , scopes({scope_})
    {
    }

    void setInWindow(bool x) { in_window = x; }

private:
    ContextPtr context;
    Analysis & analysis;
    const ExprAnalyzerOptions options;
    const bool use_ansi_semantic;
    const bool enable_implicit_type_conversion;
    const bool allow_extended_conversion;

    std::vector<ScopePtr> scopes;
    // whether we are in an aggregate function
    bool in_aggregate = false;
    // whether we are in a window function
    bool in_window = false;

    ScopePtr currentScope() const { return scopes.back(); }

    ScopePtr baseScope() const { return scopes.front(); }

    void enterLambda(ScopePtr lambda_scope)
    {
        if (!lambda_scope || lambda_scope->getType() != Scope::ScopeType::LAMBDA)
            throw Exception("Provided scope is not a lambda scope", ErrorCodes::LOGICAL_ERROR);

        scopes.push_back(lambda_scope);
    }

    void exitLambda()
    {
        if (scopes.size() <= 1)
            throw Exception("No outer scope.", ErrorCodes::LOGICAL_ERROR);

        if (currentScope()->getType() != Scope::ScopeType::LAMBDA)
            throw Exception("Current scope is not a lambda scope", ErrorCodes::LOGICAL_ERROR);

        scopes.pop_back();
    }

    bool isInLambda() const { return currentScope()->getType() == Scope::ScopeType::LAMBDA; }

    ColumnWithTypeAndName analyzeWindowFunction(ASTFunctionPtr & function);
    ColumnWithTypeAndName analyzeAggregateFunction(ASTFunctionPtr & function);
    ColumnWithTypeAndName analyzeGroupingOperation(ASTFunctionPtr & function);
    ColumnWithTypeAndName analyzeInSubquery(ASTFunctionPtr & function, AnalyzeContext &);
    ColumnWithTypeAndName analyzeExistsSubquery(ASTFunctionPtr & function, AnalyzeContext &);
    ColumnWithTypeAndName analyzeOrdinaryFunction(ASTFunctionPtr & function, AnalyzeContext &);

    std::tuple<AggregateFunctionPtr, Array, String> resolveAggregateFunction(ASTFunction & function);
    DataTypePtr handleSubquery(const ASTPtr & subquery, bool use_explicit_named_tuple);
    ColumnWithTypeAndName handleResolvedField(ASTPtr & node, const ResolvedField & field);

    void processSubqueryArgsWithCoercion(ASTPtr & lhs_ast, ASTPtr & rhs_ast);
    void expandUntuple(ASTs & nodes);
    void expandAsterisk(ASTs & nodes);
    static String getFunctionColumnName(const String & func_name, const ColumnsWithTypeAndName & arguments);
};

DataTypePtr ExprAnalyzer::analyze(ASTPtr & expression, ScopePtr scope, ContextPtr context, Analysis & analysis, ExprAnalyzerOptions options)
{
    ExprAnalyzerVisitor expr_visitor{context, analysis, scope, options};
    AnalyzeContext ac{.only_and = options.subquery_to_semi_anti};
    return expr_visitor.process(expression, ac).type;
}

ColumnWithTypeAndName ExprAnalyzerVisitor::process(ASTPtr & node, AnalyzeContext & ac)
{
    if (auto processed = analysis.tryGetExpressionColumnWithType(node))
    {
        if (processed->column)
            return {processed->column, processed->type, ""};
        else
            return {processed->type, ""};
    }

    auto result = ASTVisitorUtil::accept(node, *this, ac);

    if (!result.type)
        throw Exception("Can not determine expression type: " + serializeAST(*node), ErrorCodes::LOGICAL_ERROR);

    analysis.setExpressionColumnWithType(node, {result.type, result.column});
    return result;
}

ColumnsWithTypeAndName ExprAnalyzerVisitor::processNodes(ASTs & nodes, AnalyzeContext & ac)
{
    ColumnsWithTypeAndName processed(nodes.size());
    std::transform(nodes.begin(), nodes.end(), processed.begin(), [&](auto && node) { return process(node, ac); });
    return processed;
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitNode(ASTPtr & node, AnalyzeContext &)
{
    throw Exception("Unsupported Node" + node->getID(), ErrorCodes::NOT_IMPLEMENTED);
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTExpressionList(ASTPtr &, AnalyzeContext &)
{
    return ColumnWithTypeAndName{nullptr, std::make_shared<DataTypeNothing>(), "list"};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTTableColumnReference(ASTPtr & node, AnalyzeContext &)
{
    auto & ref = node->as<ASTTableColumnReference &>();
    auto name_and_type = ref.storage->getInMemoryMetadataPtr()->getColumns().getPhysical(ref.column_name);
    return {nullptr, name_and_type.type, name_and_type.name};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTLiteral(ASTPtr & node, AnalyzeContext &)
{
    auto & literal = node->as<ASTLiteral &>();
    DataTypePtr type = applyVisitor(FieldToDataType(), literal.value);
    // TODO: remove convertFieldToType if we have done in parser/rewriter phase
    ColumnPtr column = type->createColumnConst(1, convertFieldToType(literal.value, *type));
    return {column, type, node->getColumnName()};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTIdentifier(ASTPtr & node, AnalyzeContext &)
{
    auto & identifier = node->as<ASTIdentifier &>();
    std::optional<ResolvedField> resolved;

    if (use_ansi_semantic)
        resolved = currentScope()->resolveFieldByAnsi(QualifiedName::extractQualifiedName(identifier));
    else
        resolved = currentScope()->resolveFieldByClickhouse(identifier.name());

    if (!resolved)
        throw Exception("Can not resolve identifier: " + identifier.name(), ErrorCodes::UNKNOWN_IDENTIFIER);

    return handleResolvedField(node, *resolved);
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTFieldReference(ASTPtr & node, AnalyzeContext &)
{
    // FieldReference is only used to refer to a relation field.
    auto & field_ref = node->as<ASTFieldReference &>();

    if (field_ref.field_index >= baseScope()->size())
        throw Exception("Illegal field reference node", ErrorCodes::LOGICAL_ERROR);

    ResolvedField resolved{baseScope(), field_ref.field_index};

    return handleResolvedField(node, resolved);
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTOrderByElement(ASTPtr & node, AnalyzeContext & ac)
{
    auto & order_by = node->as<ASTOrderByElement &>();
    return process(order_by.children.front(), ac);
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTFunction(ASTPtr & node, AnalyzeContext & ac)
{
    ASTFunctionPtr function_ptr = std::dynamic_pointer_cast<ASTFunction>(node);
    expandUntuple(function_ptr->arguments->children);
    expandAsterisk(function_ptr->arguments->children);
    if (options.record_used_object)
        analysis.addUsedFunction(function_ptr->name);
    auto function_type = getFunctionType(*function_ptr, context);

    if (function_type == FunctionType::WINDOW_FUNCTION)
        return analyzeWindowFunction(function_ptr);
    if (function_type == FunctionType::AGGREGATE_FUNCTION)
        return analyzeAggregateFunction(function_ptr);
    if (function_type == FunctionType::GROUPING_OPERATION)
        return analyzeGroupingOperation(function_ptr);
    if (function_type == FunctionType::IN_SUBQUERY)
        return analyzeInSubquery(function_ptr, ac);
    if (function_type == FunctionType::EXISTS_SUBQUERY)
        return analyzeExistsSubquery(function_ptr, ac);
    if (function_type == FunctionType::FUNCTION)
        return analyzeOrdinaryFunction(function_ptr, ac);

    Exception ex("Unknown function " + function_ptr->name, ErrorCodes::UNKNOWN_FUNCTION);

    auto hints = FunctionFactory::instance().getHints(function_ptr->name);
    if (!hints.empty())
        ex.addMessage(" Maybe you meant: " + toString(hints) + ".");

    hints = AggregateFunctionFactory::instance().getHints(function_ptr->name);
    if (!hints.empty())
        ex.addMessage("Or unknown aggregate function " + function_ptr->name + ". Maybe you meant: " + toString(hints));

    throw Exception(ex);
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTQuantifiedComparison(ASTPtr & node, AnalyzeContext & ac)
{
    auto quantified_comparison = std::dynamic_pointer_cast<ASTQuantifiedComparison>(node);
    auto & lhs_ast = quantified_comparison->children[0];
    auto & rhs_ast = quantified_comparison->children[1];
    processSubqueryArgsWithCoercion(lhs_ast, rhs_ast);
    analysis.quantified_comparison_subqueries[options.select_query].push_back(quantified_comparison);
analysis.subquery_support_semi_anti[quantified_comparison] = ac.only_and;
    return {nullptr, std::make_shared<DataTypeUInt8>(), node->getColumnName()};
}


ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTSubquery(ASTPtr & node, AnalyzeContext & ac)
{
    auto type = handleSubquery(node, true);
    // TODO: we should only execute uncorrelated subqueries
    bool early_execute_subquery
        = context->getSettingsRef().early_execute_scalar_subquery || (isInLambda() && context->getSettingsRef().execute_subquery_in_lambda);

    if (early_execute_subquery)
    {
        const auto & block = analysis.getScalarSubqueryResult(node, context);
        const auto & col_with_type = block.getByPosition(0);
        auto lit = std::make_shared<ASTLiteral>(col_with_type.column->operator[](0));
        lit->alias = node->tryGetAlias();
        lit->prefer_alias_to_column_name = node->as<ASTSubquery &>().prefer_alias_to_column_name;
        node = addTypeConversionToAST(std::move(lit), col_with_type.type->getName());
        return {col_with_type.column, col_with_type.type, ""};
    }

    if (isInLambda())
        throw Exception("Subquery in lambda is not supported by ApplyStep", ErrorCodes::SYNTAX_ERROR);

    // when a scalar subquery has 0 rows, it returns NULL, hence we change its type to Nullable type
    // note that this feature is not compatible with subquery with multiple output returning Tuple type
    // see test 00420_null_in_scalar_subqueries
    if (!type->isNullable() && JoinCommon::canBecomeNullable(type))
    {
        type = JoinCommon::tryConvertTypeToNullable(type);
        analysis.setTypeCoercion(node, type);
    }

    analysis.scalar_subqueries[options.select_query].push_back(node);
    analysis.subquery_support_semi_anti[node] = ac.only_and;
    return {nullptr, type, node->getColumnName()};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::visitASTPreparedParameter(ASTPtr & node, AnalyzeContext &)
{
    const auto & prepared_param = node->as<ASTPreparedParameter &>();
    return {nullptr, DataTypeFactory::instance().get(prepared_param.type), node->getColumnName()};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::analyzeOrdinaryFunction(ASTFunctionPtr & function, AnalyzeContext & ac)
{
    auto overload_resolver = FunctionFactory::instance().get(function->name, context);
    ASTs & arguments = function->arguments->children;
    ColumnsWithTypeAndName processed_arguments(arguments.size());
    DataTypes arguments_types(arguments.size());
    std::vector<size_t> lambda_arg_index;
    bool all_const = true;

    AnalyzeContext child_context{.only_and = ac.only_and && function->name == "and"};
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        auto * lambda = arguments[i]->as<ASTFunction>();
        // for high-ordered functions, we analyze lambda expression args in next cycle
        if (lambda && lambda->name == "lambda")
        {
            auto lambda_arg_size = getLambdaExpressionArguments(*lambda).size();
            arguments_types[i] = std::make_shared<DataTypeFunction>(DataTypes(lambda_arg_size));
            lambda_arg_index.emplace_back(i);
        }
        else
        {
            processed_arguments[i] = process(arguments[i], child_context);

            // fix 01457_int256_hashing.sql
            if (checkFunctionIsInOrGlobalInOperator(*function) && i == 1)
            {
                processed_arguments[i].type = std::make_shared<DataTypeSet>();
            }

            arguments_types[i] = processed_arguments[i].type;
        }
        if (!processed_arguments[i].column)
            all_const = false;
    }

    if (!lambda_arg_index.empty())
    {
        // resolve lambda expressions' argument types
        overload_resolver->getLambdaArgumentTypes(arguments_types);
        for (const auto & index : lambda_arg_index)
        {
            auto & lambda = arguments[index]->as<ASTFunction &>();
            const auto * lambda_type = typeid_cast<const DataTypeFunction *>(arguments_types[index].get());
            auto lambda_args = getLambdaExpressionArguments(lambda);
            auto lambda_body = getLambdaExpressionBody(lambda);

            FieldDescriptions lambda_fields;

            for (size_t j = 0; j < lambda_args.size(); ++j)
            {
                auto lambda_arg_name = tryGetIdentifierName(lambda_args[j]);
                if (!lambda_arg_name)
                    throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                lambda_fields.emplace_back(*lambda_arg_name, lambda_type->getArgumentTypes()[j]);
            }

            const auto * lambda_scope = analysis.scope_factory.createLambdaScope(currentScope(), lambda_fields);

            analysis.setScope(lambda, lambda_scope);

            enterLambda(lambda_scope);
            auto lambda_ret_type = process(lambda_body, child_context).type;
            exitLambda();

            auto resolved_lambda_type = std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(), lambda_ret_type);
            // since we don't call `process` for lambda argument, register its type manually
            analysis.setExpressionColumnWithType(arguments[index], {resolved_lambda_type});
            processed_arguments[index] = {nullptr, resolved_lambda_type, ""};
        }
    }

    auto function_base = overload_resolver->build(processed_arguments);
    auto column_name = getFunctionColumnName(function->name, processed_arguments);

    ColumnPtr res_col;
    auto function_ret_type = function_base->getResultType();
    if (options.evaluate_constant_expression && function_base->isSuitableForConstantFolding()
        && !functionIsDictGet(function->name) // 01852_dictionary_found_rate_long
        && !functionIsInOrGlobalInOperator(function->name))
    {
        if (all_const)
            res_col = function_base->execute(processed_arguments, function_ret_type, 1, false);
        else
            res_col = function_base->getConstantResultForNonConstArguments(processed_arguments, function_ret_type);

        if (res_col && isColumnConst(*res_col) && res_col->empty())
            res_col = res_col->cloneResized(1);
    }

    // post analysis for sub column optimization
    postExprAnalyze(function, processed_arguments, analysis, context);

    if (!function_base->isDeterministicInScopeOfQuery())
    {
        analysis.addNonDeterministicFunctions(*function);
        context->addNonDeterministicFunction(function->name, true);
    } else if (!function_base->isDeterministic())
    {
        context->addNonDeterministicFunction(function->name, false);
    }

    analysis.addUsedFunctionArgument(function->name, processed_arguments);
    return {res_col, function_ret_type, column_name};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::analyzeAggregateFunction(ASTFunctionPtr & function)
{
    if (options.aggregate_support == ExprAnalyzerOptions::AggregateSupport::DISALLOWED)
        throw Exception("Aggregate function is not supported in " + options.statement_name, ErrorCodes::SYNTAX_ERROR);

    if (in_aggregate)
        throw Exception("Nested aggregate function is not supported", ErrorCodes::ILLEGAL_AGGREGATION);

    in_aggregate = true;
    AggregateFunctionPtr aggregator;
    Array parameters;
    String column_name;
    std::tie(aggregator, parameters, column_name) = resolveAggregateFunction(*function);

    AggregateAnalysis aggregate_analysis;
    aggregate_analysis.expression = function;
    aggregate_analysis.function = aggregator;
    aggregate_analysis.parameters = parameters;
    if (options.select_query)
        analysis.aggregate_results[options.select_query].push_back(aggregate_analysis);
    in_aggregate = false;
    return {nullptr, aggregate_analysis.function->getReturnType(), column_name};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::analyzeWindowFunction(ASTFunctionPtr & function)
{
    if (options.window_support == ExprAnalyzerOptions::WindowSupport::DISALLOWED)
        throw Exception("Window function is not supported in " + options.statement_name, ErrorCodes::SYNTAX_ERROR);

    if (!options.select_query)
        throw Exception("Provide query node if window function is allowed", ErrorCodes::LOGICAL_ERROR);

    if (in_window)
        throw Exception("Nested window function is not supported", ErrorCodes::SYNTAX_ERROR);

    if (in_aggregate)
        throw Exception("Window function under an aggregate function is not supported", ErrorCodes::SYNTAX_ERROR);

    in_window = true;

    String window_name;
    ResolvedWindowPtr resolved_window;
    AggregateFunctionPtr aggregator;
    Array parameters;
    String column_name;

    if (!function->window_name.empty())
    {
        window_name = function->window_name;
        resolved_window = analysis.getRegisteredWindow(*options.select_query, function->window_name);
        // Lead and lag
        if (function->window_definition)
        {
            const struct WindowFrame def = {
                .is_default = false,
                .type = WindowFrame::FrameType::Rows,
                .end_type = WindowFrame::BoundaryType::Unbounded,
            };

            if (resolved_window->frame != def)
            {
                window_name = function->window_name + "(ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
                resolved_window = std::make_shared<ResolvedWindow>(*resolved_window);
                resolved_window->frame = def;
            }
        }
    }
    else
    {
        window_name = function->window_definition->as<ASTWindowDefinition &>().getDefaultWindowName();
        resolved_window = resolveWindow(function->window_definition, analysis.getRegisteredWindows(*options.select_query), context);
    }

    AnalyzeContext ac{.only_and = false};
    if (resolved_window->partition_by)
    {
        expandUntuple(resolved_window->partition_by->children);
        expandAsterisk(resolved_window->partition_by->children);
        processNodes(resolved_window->partition_by->children, ac);
    }

    if (resolved_window->order_by)
    {
        expandUntuple(resolved_window->order_by->children);
        expandAsterisk(resolved_window->order_by->children);
        processNodes(resolved_window->order_by->children, ac);
    }

    std::tie(aggregator, parameters, column_name) = resolveAggregateFunction(*function);

    auto window_analysis = std::make_shared<WindowAnalysis>();
    window_analysis->expression = function;
    window_analysis->window_name = window_name;
    window_analysis->resolved_window = resolved_window;
    window_analysis->aggregator = aggregator;
    window_analysis->parameters = parameters;
    analysis.addWindowAnalysis(*options.select_query, std::move(window_analysis));
    in_window = false;
    return {nullptr, aggregator->getReturnType(), column_name};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::analyzeGroupingOperation(ASTFunctionPtr & function)
{
    if (options.aggregate_support == ExprAnalyzerOptions::AggregateSupport::DISALLOWED)
        throw Exception("Grouping operation is not supported in " + options.statement_name, ErrorCodes::SYNTAX_ERROR);

    if (!options.select_query)
        throw Exception("Provide query node if grouping operation is allowed", ErrorCodes::LOGICAL_ERROR);

    if (in_aggregate)
        throw Exception("Nested aggregate function is not supported", ErrorCodes::SYNTAX_ERROR);

    in_aggregate = true;

    if (!function->arguments || function->arguments->children.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function GROUPING expects at least one argument");

    if (function->arguments->children.size() > 64)
        throw Exception(
            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
            "Function GROUPING can have up to 64 arguments, but {} provided",
            function->arguments->children.size());
    AnalyzeContext ac{.only_and = false};
    auto processed_arguments = processNodes(function->arguments->children, ac);
    auto column_name = getFunctionColumnName(function->name, processed_arguments);

    analysis.grouping_operations[options.select_query].push_back(function);
    in_aggregate = false;
    return {nullptr, std::make_shared<DataTypeUInt64>(), column_name};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::analyzeInSubquery(ASTFunctionPtr & function, AnalyzeContext & ac)
{
    auto & lhs_ast = function->arguments->children[0];
    auto & rhs_ast = function->arguments->children[1];
    processSubqueryArgsWithCoercion(lhs_ast, rhs_ast);
    analysis.in_subqueries[options.select_query].push_back(function);
    analysis.subquery_support_semi_anti[function] = ac.only_and;
    return {nullptr, std::make_shared<DataTypeUInt8>(), function->getColumnName()};
}

ColumnWithTypeAndName ExprAnalyzerVisitor::analyzeExistsSubquery(ASTFunctionPtr & function, AnalyzeContext & ac)
{
    if (function->children.size() != 1)
        throw Exception("Invalid exists subquery expression: " + serializeAST(*function), ErrorCodes::SYNTAX_ERROR);
    if (function->name == "not")
    {
        auto * not_func = function->arguments->children[0]->as<ASTFunction>();
        handleSubquery(not_func->arguments->children[0], false);
    }
    else
    {
        handleSubquery(function->arguments->children[0], false);
    }

    if (isInLambda())
        throw Exception("Subquery in lambda is not supported by ApplyStep", ErrorCodes::SYNTAX_ERROR);

    analysis.exists_subqueries[options.select_query].push_back(function);
    analysis.subquery_support_semi_anti[function] = ac.only_and;
    return {nullptr, std::make_shared<DataTypeUInt8>(), function->getColumnName()};
}

void ExprAnalyzerVisitor::processSubqueryArgsWithCoercion(ASTPtr & lhs_ast, ASTPtr & rhs_ast)
{
    AnalyzeContext ac{.only_and = false};
    auto lhs_type = process(lhs_ast, ac).type;
    auto rhs_type = handleSubquery(rhs_ast, false);
    // TODO: we should only execute uncorrelated subqueries
    // TODO: handle type mismatch
    bool early_execute_subquery
        = context->getSettingsRef().early_execute_in_subquery || (isInLambda() && context->getSettingsRef().execute_subquery_in_lambda);

    if (early_execute_subquery)
    {
        auto set = analysis.getInSubqueryResult(rhs_ast, context);
        auto set_columns = set->getSetElements();
        Tuple coll;
        for (size_t row_id = 0; row_id < set_columns[0]->size(); ++row_id)
        {
            if (set_columns.size() == 1)
            {
                coll.push_back(set_columns[0]->operator[](row_id));
            }
            else
            {
                Tuple nested_tuple;
                for (const auto & set_column : set_columns)
                    nested_tuple.push_back(set_column->operator[](row_id));
                coll.push_back(std::move(nested_tuple));
            }
        }
        // TODO: better handle empty set
        if (coll.empty())
            coll.push_back(Null{});
        rhs_ast = std::make_shared<ASTLiteral>(std::move(coll));
        return;
    }

    if (isInLambda())
        throw Exception("Subquery in lambda is not supported by ApplyStep", ErrorCodes::SYNTAX_ERROR);

    if (!JoinCommon::isJoinCompatibleTypes(lhs_type, rhs_type))
    {
        DataTypePtr super_type = nullptr;
        if (enable_implicit_type_conversion)
        {
            if (context->getSettingsRef().convert_to_right_type_for_in_subquery)
            {
                if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(rhs_type.get()))
                {
                    DataTypes elem_types = type_tuple->getElements();
                    std::transform(elem_types.begin(), elem_types.end(), elem_types.begin(), &JoinCommon::convertTypeToNullable);
                    super_type = std::make_shared<DataTypeTuple>(elem_types, type_tuple->getElementNames());
                }
                else
                {
                    super_type = JoinCommon::convertTypeToNullable(rhs_type);
                }
            }
            else
                super_type = getLeastSupertype(DataTypes{lhs_type, rhs_type}, allow_extended_conversion);
        }
        if (!super_type)
            throw Exception("Incompatible types for IN prediacte", ErrorCodes::TYPE_MISMATCH);
        if (!lhs_type->equals(*super_type))
            analysis.setTypeCoercion(lhs_ast, super_type);
        if (!rhs_type->equals(*super_type))
            analysis.setTypeCoercion(rhs_ast, super_type);
    }
}

void ExprAnalyzerVisitor::expandAsterisk(ASTs & nodes)
{
    if (!options.expand_untuple_and_asterisk)
        return;

    ASTs new_nodes;
    new_nodes.reserve(nodes.size());
    bool has_asterisk = false;

    for (auto & node : nodes)
    {
        if (auto * asterisk = node->as<ASTAsterisk>())
        {
            has_asterisk = true;
            for (size_t field_index = 0; field_index < baseScope()->size(); ++field_index)
            {
                if (baseScope()->at(field_index).substituted_by_asterisk)
                {
                    auto field_reference = std::make_shared<ASTFieldReference>(field_index);
                    field_reference->setFieldName(baseScope()->at(field_index).name);
                    new_nodes.push_back(field_reference);
                }
            }
         }
        else if (auto * qualified_asterisk = node->as<ASTQualifiedAsterisk>())
        {
            if (qualified_asterisk->children.empty() || !qualified_asterisk->getChildren()[0]->as<ASTTableIdentifier>())
                throw Exception("Unable to resolve qualified asterisk", ErrorCodes::UNKNOWN_IDENTIFIER);

            has_asterisk = true;
            ASTIdentifier & astidentifier = qualified_asterisk->getChildren()[0]->as<ASTTableIdentifier &>();
            auto prefix = QualifiedName::extractQualifiedName(astidentifier);
            bool matched = false;
            for (size_t field_index = 0; field_index < baseScope()->size(); ++field_index)
            {
                if (baseScope()->at(field_index).substituted_by_asterisk && baseScope()->at(field_index).prefix.hasSuffix(prefix))
                {
                    matched = true;
                    auto field_reference = std::make_shared<ASTFieldReference>(field_index);
                    field_reference->setFieldName(baseScope()->at(field_index).name);
                    new_nodes.push_back(field_reference);
                }
            }
            if (!matched)
                throw Exception("Can not find column of " + prefix.toString() + " in Scope", ErrorCodes::UNKNOWN_IDENTIFIER);
        }
        else
            new_nodes.push_back(node);
    }
    if (has_asterisk)
        nodes.swap(new_nodes);
}

void ExprAnalyzerVisitor::expandUntuple(ASTs & nodes)
{
    if (!options.expand_untuple_and_asterisk)
        return;

    ASTs new_nodes;
    new_nodes.reserve(nodes.size());
    bool has_untuple = false;

    for (auto & node : nodes)
    {
        if (auto * untuple = node->as<ASTFunction>(); untuple && untuple->name == "untuple")
        {
            if (untuple->arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function untuple doesn't match. Passed {}, should be 1",
                    untuple->arguments->children.size());

            auto & tuple_ast = untuple->arguments->children[0];
            /// Get type and name for tuple argument
            AnalyzeContext ac{.only_and = false};
            auto argument_type = process(tuple_ast, ac);
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(argument_type.type.get());
            if (!tuple_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function untuple expect tuple argument, got {}", argument_type.type->getName());

            size_t tid = 0;
            for (const auto & name [[maybe_unused]] : tuple_type->getElementNames())
            {
                auto literal = std::make_shared<ASTLiteral>(UInt64(++tid));
                auto func = makeASTFunction("tupleElement", tuple_ast, literal);
                new_nodes.push_back(std::move(func));
            }

            has_untuple = true;
        }
        else
            new_nodes.push_back(node);
    }

    if (has_untuple)
        nodes.swap(new_nodes);
}

std::tuple<AggregateFunctionPtr, Array, String> ExprAnalyzerVisitor::resolveAggregateFunction(ASTFunction & function)
{
    AnalyzeContext ac{.only_and = false};
    auto processed_arguments = processNodes(function.arguments->children, ac);
    Array parameters = (function.parameters) ? getAggregateFunctionParametersArray(function.parameters, "", context) : Array();
    DataTypes argument_types(processed_arguments.size());
    std::transform(processed_arguments.begin(), processed_arguments.end(), argument_types.begin(), [](auto && arg) { return arg.type; });

    for (auto & item : argument_types)
        item = recursiveRemoveLowCardinality(item);

    AggregateFunctionProperties properties;
    auto aggregate = AggregateFunctionFactory::instance().get(function.name, argument_types, parameters, properties);

    auto column_name = getFunctionColumnName(function.name, processed_arguments);
    return {aggregate, parameters, column_name};
}

DataTypePtr ExprAnalyzerVisitor::handleSubquery(const ASTPtr & subquery, bool use_explicit_named_tuple)
{
    if (auto * s = subquery->as<ASTSubquery>(); !s)
        throw Exception("Invalid subquery expression", ErrorCodes::LOGICAL_ERROR);

    if (options.subquery_support == ExprAnalyzerOptions::SubquerySupport::DISALLOWED)
        throw Exception("Subquery is not supported in " + options.statement_name, ErrorCodes::SYNTAX_ERROR);

    if (!options.select_query)
        throw Exception("Provide query node if subquery is allowed", ErrorCodes::LOGICAL_ERROR);

    QueryAnalyzer::analyze(subquery->children[0], currentScope(), context, analysis);
    auto & output_columns = analysis.getOutputDescription(*subquery);

    DataTypePtr type;

    if (output_columns.size() == 1)
    {
        type = output_columns[0].type;
    }
    else
    {
        // if subquery has multiple output column, its return type should be the tuple of columns
        DataTypes column_types(output_columns.size());
        std::transform(output_columns.begin(), output_columns.end(), column_types.begin(), std::mem_fn(&FieldDescription::type));

        if (use_explicit_named_tuple)
        {
            Strings names(output_columns.size());
            std::transform(
                output_columns.begin(), output_columns.end(), names.begin(), [](FieldDescription & v1) -> String { return v1.name; });
            if (!DataTypeTuple::checkTupleNames(names))
                type = std::make_shared<DataTypeTuple>(column_types, names);
        }

        if (!type)
            type = std::make_shared<DataTypeTuple>(column_types);
    }

    analysis.setExpressionColumnWithType(subquery, {type});
    return type;
}

ColumnWithTypeAndName ExprAnalyzerVisitor::handleResolvedField(ASTPtr & node, const ResolvedField & field)
{
    if (field.scope->getType() == Scope::ScopeType::RELATION)
    {
        analysis.setColumnReference(node, field);
        analysis.addReadColumn(field, options.record_used_object);
    }
    else if (field.scope->getType() == Scope::ScopeType::LAMBDA)
        analysis.setLambdaArgumentReference(node, field);

    return {nullptr, field.getFieldDescription().type, node->getColumnName()};
}

String ExprAnalyzerVisitor::getFunctionColumnName(const String & func_name, const ColumnsWithTypeAndName & arguments)
{
    Strings arg_result_names;
    for (const auto & arg : arguments)
        arg_result_names.emplace_back(arg.name);
    return getFunctionResultName(func_name, arg_result_names);
}

}
