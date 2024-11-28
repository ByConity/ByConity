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

#include <Analyzers/Analysis.h>

#include <DataStreams/materializeBlock.h>
#include <Interpreters/executeSubQuery.h>

namespace DB
{

#define MAP_GET(container, key)                                                                            \
            do {                                                                                           \
                if(auto iter = (container).find(key); iter != (container).end())                           \
                    return iter->second;                                                                   \
                else                                                                                       \
                    throw Exception("Object not found in " #container, ErrorCodes::LOGICAL_ERROR);         \
            } while(false)

#define MAP_SET(container, key, val)                                                                       \
            do {                                                                                           \
                if(!(container).emplace((key), (val)).second)                                              \
                    throw Exception("Object already exists in " #container, ErrorCodes::LOGICAL_ERROR);    \
            } while(false)                                                                                 \

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

void Analysis::setScope(IAST & statement, ScopePtr scope)
{
    LOG_TRACE(logger, "scope of ast {}: {}", statement.dumpTree(0), scope->toString());
    MAP_SET(scopes, &statement, scope);
}

ScopePtr Analysis::getScope(IAST & statement)
{
    MAP_GET(scopes, &statement);
}

void Analysis::setQueryWithoutFromScope(ASTSelectQuery & query, ScopePtr scope)
{
    MAP_SET(query_without_from_scopes, &query, scope);
}

ScopePtr Analysis::getQueryWithoutFromScope(ASTSelectQuery & query)
{
    MAP_GET(query_without_from_scopes, &query);
}

void Analysis::setTableStorageScope(ASTIdentifier & db_and_table, ScopePtr scope)
{
    LOG_TRACE(logger, "scope of table ast {}: {}", db_and_table.dumpTree(0), scope->toString());
    MAP_SET(table_storage_scopes, &db_and_table, scope);
}

ScopePtr Analysis::getTableStorageScope(ASTIdentifier & db_and_table)
{
    MAP_GET(table_storage_scopes, &db_and_table);
}

std::unordered_map<ASTIdentifier *, ScopePtr> & Analysis::getTableStorageScopeMap()
{
    return table_storage_scopes;
}

/*
void Analysis::setTableColumnMasks(ASTIdentifier & db_and_table, ASTs column_masks)
{
    MAP_SET(table_column_masks, &db_and_table, std::move(column_masks));
}

ASTs & Analysis::getTableColumnMasks(ASTIdentifier & db_and_table)
{
    MAP_GET(table_column_masks, &db_and_table);
}
*/

void Analysis::setTableAliasColumns(ASTIdentifier & db_and_table, ASTs alias_columns)
{
    MAP_SET(table_alias_columns, &db_and_table, std::move(alias_columns));
}

ASTs & Analysis::getTableAliasColumns(ASTIdentifier & db_and_table)
{
    MAP_GET(table_alias_columns, &db_and_table);
}

bool Analysis::hasExpressionColumnWithType(const ASTPtr & expression)
{
    return expression_column_with_types.find(expression) != expression_column_with_types.end();
}

void Analysis::setExpressionColumnWithType(const ASTPtr & expression, const ColumnWithType & column_with_type)
{
    expression_column_with_types[expression] = column_with_type;
}

std::optional<ColumnWithType> Analysis::tryGetExpressionColumnWithType(const ASTPtr & expression)
{
    if (auto it = expression_column_with_types.find(expression); it != expression_column_with_types.end())
        return it->second;

    return std::nullopt;
}

DataTypePtr Analysis::getExpressionType(const ASTPtr & expression)
{
    if(auto it = expression_column_with_types.find(expression); it != expression_column_with_types.end())
        return expression_column_with_types[expression].type;
    else
        throw Exception("Object not found in expression_column_with_types", ErrorCodes::LOGICAL_ERROR);
}

ExpressionTypes Analysis::getExpressionTypes()
{
    ExpressionTypes expression_types;
    for(auto & it : expression_column_with_types)
        expression_types[it.first] = it.second.type;
    return expression_types;
}

void Analysis::setPrewhere(ASTSelectQuery & select_query, const ASTPtr & prewhere)
{
    MAP_SET(prewheres, &select_query, prewhere);
}

ASTPtr Analysis::tryGetPrewhere(ASTSelectQuery & select_query)
{
    if (auto it = prewheres.find(&select_query); it != prewheres.end())
        return it->second;

    return nullptr;
}

JoinUsingAnalysis & Analysis::getJoinUsingAnalysis(ASTTableJoin & table_join)
{
    MAP_GET(join_using_results, &table_join);
}

JoinOnAnalysis & Analysis::getJoinOnAnalysis(ASTTableJoin & table_join)
{
    MAP_GET(join_on_results, &table_join);
}

const StorageAnalysis & Analysis::getStorageAnalysis(const IAST & ast)
{
    if (storage_results.count(&ast) == 0)
        throw Exception("storage not found in storage_results", ErrorCodes::LOGICAL_ERROR);
    return storage_results[&ast];
}

const LinkedHashMap<const IAST *, StorageAnalysis> & Analysis::getStorages() const
{
    return storage_results;
}

UInt64 Analysis::getLimitByValue(ASTSelectQuery & select_query)
{
    MAP_GET(limit_by_values, &select_query);
}

std::vector<ASTPtr> & Analysis::getLimitByItem(ASTSelectQuery & select_query)
{
    return limit_by_items[&select_query];
}

UInt64 Analysis::getLimitByOffsetValue(ASTSelectQuery & select_query)
{
    MAP_GET(limit_by_offset_values, &select_query);
}

UInt64 Analysis::getLimitLength(ASTSelectQuery & select_query)
{
    MAP_GET(limit_lengths, &select_query);
}

UInt64 Analysis::getLimitOffset(ASTSelectQuery & select_query)
{
    MAP_GET(limit_offsets, &select_query);
}

void Analysis::setColumnReference(const ASTPtr & ast, const ResolvedField & resolved)
{
    column_references[ast] = resolved;
}

std::optional<ResolvedField> Analysis::tryGetColumnReference(const ASTPtr & ast)
{
    if (column_references.find(ast) != column_references.end())
        return column_references.at(ast);

    return std::nullopt;
}

void Analysis::addReadColumn(const IAST * table_ast, size_t field_index)
{
    read_columns[table_ast].emplace(field_index);
}

void Analysis::addReadColumn(const ResolvedField & resolved_field, bool add_used)
{
    const auto & field_desc = resolved_field.getFieldDescription();
    // only need do this in the initial SELECT query
    if (field_desc.origin_columns.size() == 1)
    {
        const auto & origin_column = field_desc.origin_columns.front();
        addReadColumn(origin_column.table_ast, origin_column.index_of_scope);
        if (add_used)
            addUsedColumn(origin_column.storage->getStorageID(), origin_column.column);
    }
}

const std::set<size_t> & Analysis::getReadColumns(const IAST & table_ast)
{
    return read_columns[&table_ast];
}

void Analysis::setLambdaArgumentReference(const ASTPtr & ast, const ResolvedField & resolved)
{
    lambda_argument_references[ast] = resolved;
}

std::optional<ResolvedField> Analysis::tryGetLambdaArgumentReference(const ASTPtr & ast)
{
    if (lambda_argument_references.find(ast) != lambda_argument_references.end())
        return lambda_argument_references.at(ast);

    return std::nullopt;
}

std::vector<AggregateAnalysis> & Analysis::getAggregateAnalysis(ASTSelectQuery & select_query)
{
    return aggregate_results[&select_query];
}

std::vector<std::pair<String, UInt16>> & Analysis::getInterestEvents(ASTSelectQuery & select_query)
{
    return interest_events[&select_query];
}

std::vector<ASTFunctionPtr> & Analysis::getGroupingOperations(ASTSelectQuery & select_query)
{
    return grouping_operations[&select_query];
}

void Analysis::addWindowAnalysis(ASTSelectQuery & select_query, WindowAnalysisPtr analysis)
{
    window_results_by_select_query[&select_query].push_back(analysis);
    MAP_SET(window_results_by_ast, analysis->expression, analysis);
}

WindowAnalysisPtr Analysis::getWindowAnalysis(const ASTPtr & ast)
{
    MAP_GET(window_results_by_ast, ast);
}

std::vector<WindowAnalysisPtr> & Analysis::getWindowAnalysisOfSelectQuery(ASTSelectQuery & select_query)
{
    return window_results_by_select_query[&select_query];
}

bool Analysis::needAggregate(ASTSelectQuery & select_query)
{
    return !getAggregateAnalysis(select_query).empty() || select_query.groupBy();
}

std::vector<ASTPtr> & Analysis::getScalarSubqueries(ASTSelectQuery & select_query)
{
    return scalar_subqueries[&select_query];
}

std::vector<ASTPtr> & Analysis::getInSubqueries(ASTSelectQuery & select_query)
{
    return in_subqueries[&select_query];
}

std::vector<ASTPtr> & Analysis::getExistsSubqueries(ASTSelectQuery & select_query)
{
    return exists_subqueries[&select_query];
}

std::vector<ASTPtr> & Analysis::getQuantifiedComparisonSubqueries(ASTSelectQuery & select_query)
{
    return quantified_comparison_subqueries[&select_query];
}

void Analysis::registerCTE(ASTSubquery & subquery)
{
    auto clone = std::make_shared<ASTSubquery>(subquery);
    clone->alias.clear();
    auto iter = common_table_expressions.find(clone);
    if (iter == common_table_expressions.end())
        common_table_expressions.emplace(
            clone, CTEAnalysis{.id = static_cast<CTEId>(common_table_expressions.size()), .representative = &subquery, .ref_count = 1});
    else
        iter->second.ref_count++;
}

std::optional<CTEAnalysis> Analysis::tryGetCTEAnalysis(ASTSubquery & subquery)
{
    auto clone = std::make_shared<ASTSubquery>(subquery);
    clone->alias.clear();

    if (auto it = common_table_expressions.find(clone); it != common_table_expressions.end())
        return it->second;

    return std::nullopt;
}

ASTs & Analysis::getSelectExpressions(ASTSelectQuery & select_query)
{
    return select_expressions[&select_query];
}

GroupByAnalysis & Analysis::getGroupByAnalysis(ASTSelectQuery & select_query)
{
    return group_by_results[&select_query];
}

std::vector<std::shared_ptr<ASTOrderByElement>> & Analysis::getOrderByAnalysis(ASTSelectQuery & select_query)
{
    return order_by_results[&select_query];
}

void Analysis::setOutputDescription(IAST & ast, const FieldDescriptions & field_descs)
{
    if (auto * subquery = ast.as<ASTSubquery>())
        setOutputDescription(*subquery->children[0], field_descs);

    MAP_SET(output_descriptions, &ast, field_descs);
}

FieldDescriptions & Analysis::getOutputDescription(IAST & ast)
{
    if (auto * subquery = ast.as<ASTSubquery>())
        return getOutputDescription(*subquery->children[0]);

    MAP_GET(output_descriptions, &ast);
}

bool Analysis::hasOutputDescription(IAST & ast)
{
    if (auto * subquery = ast.as<ASTSubquery>())
        return hasOutputDescription(*subquery->children[0]);

    return output_descriptions.contains(&ast);
}

void Analysis::setRegisteredWindow(ASTSelectQuery & select_query, const String & name, ResolvedWindowPtr & window)
{
    MAP_SET(registered_windows[&select_query], name, window);
}

ResolvedWindowPtr Analysis::getRegisteredWindow(ASTSelectQuery & select_query, const String & name)
{
    MAP_GET(registered_windows[&select_query], name);
}

const std::unordered_map<String, ResolvedWindowPtr> & Analysis::getRegisteredWindows(ASTSelectQuery & select_query)
{
    return registered_windows[&select_query];
}

void Analysis::setTypeCoercion(const ASTPtr & expression, const DataTypePtr & coerced_type)
{
    type_coercions[expression] = coerced_type;
}

DataTypePtr Analysis::getTypeCoercion(const ASTPtr & expression)
{
    return type_coercions.count(expression) ? type_coercions[expression] : nullptr;
}

void Analysis::setRelationTypeCoercion(IAST & ast, const DataTypes & coerced_types)
{
    MAP_SET(relation_type_coercions, &ast, coerced_types);
}

bool Analysis::hasRelationTypeCoercion(IAST & ast)
{
    return relation_type_coercions.count(&ast);
}

const DataTypes & Analysis::getRelationTypeCoercion(IAST & ast)
{
    MAP_GET(relation_type_coercions, &ast);
}

void Analysis::setSubColumnReference(const ASTPtr & ast, const SubColumnReference & reference)
{
    MAP_SET(sub_column_references, ast, reference);
}

std::optional<SubColumnReference> Analysis::tryGetSubColumnReference(const ASTPtr & ast)
{
    if (sub_column_references.find(ast) != sub_column_references.end())
        return sub_column_references.at(ast);

    return std::nullopt;
}

void Analysis::addReadSubColumn(const IAST * table_ast, size_t field_index, const SubColumnID & sub_column_id)
{
    auto & vec = read_sub_columns[table_ast];
    vec.resize(std::max(field_index + 1, vec.size()));
    vec[field_index].insert(sub_column_id);
}

const std::vector<SubColumnIDSet> & Analysis::getReadSubColumns(const IAST & table_ast)
{
    return read_sub_columns[&table_ast];
}

void Analysis::addNonDeterministicFunctions(IAST & ast)
{
    non_deterministic_functions.insert(&ast);
}

ArrayJoinAnalysis & Analysis::getArrayJoinAnalysis(ASTSelectQuery & select_query)
{
    return array_join_analysis[&select_query];
}

void Analysis::addUsedFunctionArgument(const String & func_name, ColumnsWithTypeAndName & processed_arguments)
{
    if (func_name == "getSetting" && !processed_arguments.empty())
    {
        auto & arg = processed_arguments[0];
        if (arg.column && !arg.column->empty())
        function_arguments[func_name].emplace_back((*arg.column)[0].toString());
    }
}

const Block & Analysis::getScalarSubqueryResult(const ASTPtr & subquery, ContextPtr context)
{
    auto hash = subquery->getTreeHash();
    String hash_str = toString(hash.first) + "_" + toString(hash.second);

    if (!executed_scalar_subqueries.count(hash_str))
    {
        auto & ast_subquery = subquery->as<ASTSubquery &>();
        auto & inner_query = ast_subquery.children.front();

        DataTypes types;
        auto pre_execute
            = [&types](InterpreterSelectQueryUseOptimizer & interpreter) { types = interpreter.getSampleBlock().getDataTypes(); };

        auto query_context = createContextForSubQuery(context);
        SettingsChanges changes;
        changes.emplace_back("max_result_rows", 1);
        changes.emplace_back("result_overflow_mode", "throw");
        changes.emplace_back("extremes", false);
        changes.emplace_back("limit", 0);
        changes.emplace_back("offset", 0);
        changes.emplace_back("final_order_by_all_direction", 0);
        query_context->applySettingsChanges(changes);
        auto block = executeSubPipelineWithOneRow(inner_query, query_context, pre_execute);

        if (block.rows() > 1)
            throw Exception(
                ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                "Scalar subquery returned more than one row: {}",
                subquery->formatForErrorMessage());

        if (block.rows() == 0)
        {
            if (types.size() != 1)
                types = {std::make_shared<DataTypeTuple>(types)};

            auto & type = types[0];
            if (!type->isNullable())
            {
                if (!type->canBeInsideNullable())
                    throw Exception(
                        ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                        "Scalar subquery returned empty result of type {} which cannot be Nullable",
                        type->getName());

                type = makeNullable(type);
            }

            auto null_column = type->createColumn();
            null_column->insert(Null{});
            block.clear();
            block.insert(ColumnWithTypeAndName{ColumnPtr{std::move(null_column)}, type, ""});
        }
        else
        {
            block = materializeBlock(block);
            size_t columns = block.columns();

            if (columns == 1)
            {
                auto & column = block.getByPosition(0);
                /// Here we wrap type to nullable if we can.
                /// It is needed cause if subquery return no rows, it's result will be Null.
                /// In case of many columns, do not check it cause tuple can't be nullable.
                if (!column.type->isNullable() && column.type->canBeInsideNullable())
                {
                    column.type = makeNullable(column.type);
                    column.column = makeNullable(column.column);
                }
            }
            else
            {
                ColumnWithTypeAndName ctn;
                ctn.type = std::make_shared<DataTypeTuple>(block.getDataTypes());
                ctn.column = ColumnTuple::create(block.getColumns());
                block = Block{ctn};
            }
        }

        executed_scalar_subqueries.emplace(hash_str, std::move(block));
    }

    return executed_scalar_subqueries.at(hash_str);
}

SetPtr Analysis::getInSubqueryResult(const ASTPtr & subquery, ContextPtr context)
{
    auto hash = subquery->getTreeHash();
    String hash_str = toString(hash.first) + "_" + toString(hash.second);

    if (!executed_in_subqueries.count(hash_str))
    {
        auto & ast_subquery = subquery->as<ASTSubquery &>();
        auto & inner_query = ast_subquery.children.front();

        SizeLimits limites(context->getSettingsRef().max_rows_in_set, context->getSettingsRef().max_bytes_in_set, OverflowMode::THROW);
        SetPtr set = std::make_shared<Set>(limites, true, context->getSettingsRef().transform_null_in);
        auto pre_execute = [&set](InterpreterSelectQueryUseOptimizer & interpreter) { set->setHeader(interpreter.getSampleBlock()); };
        auto proc_block = [&set](Block & block) { set->insertFromBlock(block); };

        auto query_context = createContextForSubQuery(context);
        SettingsChanges changes;
        changes.emplace_back("limit", 0);
        changes.emplace_back("offset", 0);
        changes.emplace_back("final_order_by_all_direction", 0);
        query_context->applySettingsChanges(changes);
        executeSubPipeline(inner_query, query_context, pre_execute, proc_block);

        set->finishInsert();
        executed_in_subqueries.emplace(hash_str, set);
    }

    return executed_in_subqueries.at(hash_str);
}
}
