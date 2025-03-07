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

#include <memory>
#include <Core/Block.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/DumpASTNode.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsConversion.h>

#include <Interpreters/Aggregator.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/DictionaryReader.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/NestedLoopJoin.h>
#include <Interpreters/Set.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/replaceForPositionalArguments.h>

#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ExpressionStep.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDictionary.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/copyData.h>

#include <Dictionaries/DictionaryStructure.h>

#include <Common/checkStackSize.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Parsers/IAST_fwd.h>
#include <Core/NamesAndTypes.h>
#include <common/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/join_common.h>
#include <Interpreters/misc.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Parsers/formatAST.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Parsers/queryToString.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_PREWHERE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
}

static ASTPtr createPredicateFromArrays(const std::vector<ASTPtr> & exprs)
{
    if (exprs.empty()) return nullptr;
    if (exprs.size() == 1) return exprs[0];
    auto function = std::make_shared<ASTFunction>();

    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);
    for (const auto & expr : exprs)
        function->arguments->children.push_back(expr);

    return function;
}

namespace
{

    /// Check if there is an ignore function. It's used for disabling constant folding in query
    ///  predicates because some performance tests use ignore function as a non-optimize guard.
    bool allowEarlyConstantFolding(const ActionsDAG & actions, const Settings & settings)
    {
        if (!settings.enable_early_constant_folding)
            return false;

        for (const auto & node : actions.getNodes())
        {
            if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
            {
                if (!node.function_base->isSuitableForConstantFolding())
                    return false;
            }
        }
        return true;
    }

}

bool sanitizeBlock(Block & block, bool throw_if_cannot_create_column)
{
    for (auto & col : block)
    {
        if (!col.column)
        {
            if (isNotCreatable(col.type->getTypeId()))
            {
                if (throw_if_cannot_create_column)
                    throw Exception("Cannot create column of type " + col.type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                return false;
            }

            col.column = col.type->createColumn();
        }
        else if (!col.column->empty())
            col.column = col.column->cloneEmpty();
    }
    return true;
}

void collectColumnsForDefaultEvaluation(const String & column_name, const ColumnsDescription & storage_columns, NameSet & required_for_default)
{
    checkStackSize();
    const auto column_default = storage_columns.getDefault(column_name);
    if (!column_default)
        return;
        /// collect identifiers required for evaluation
    IdentifierNameSet identifiers;
    column_default->expression->collectIdentifierNames(identifiers);
    required_for_default.insert(identifiers.begin(), identifiers.end());
    for (const auto & identifier : identifiers)
    {
        collectColumnsForDefaultEvaluation(identifier, storage_columns, required_for_default);
    }
}

void sanitizeDataType(const DataTypePtr & type)
{
    if (!type)
        throw Exception("Invalid null type for filter:, must be UInt8 or Nullable(UInt8)",
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

    if (type->getTypeId() == TypeIndex::Nullable)
    {
        const auto * nullable = dynamic_cast<const DataTypeNullable *>(type.get());
        sanitizeDataType(nullable->getNestedType());
        return;
    }

    if (type->getTypeId() == TypeIndex::LowCardinality)
    {
        const auto * lc = dynamic_cast<const DataTypeLowCardinality *>(type.get());
        sanitizeDataType(lc->getDictionaryType());
        return;
    }

    if(type->getTypeId() == TypeIndex::UInt8)
        return;

    throw Exception("Invalid type for filter: " + type->getName() + ", must be UInt8 or Nullable(UInt8)",
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
}

ExpressionAnalyzerData::~ExpressionAnalyzerData() = default;

ExpressionAnalyzer::ExtractedSettings::ExtractedSettings(const Settings & settings_)
    : use_index_for_in_with_subqueries(settings_.use_index_for_in_with_subqueries)
    , size_limits_for_set(settings_.max_rows_in_set, settings_.max_bytes_in_set, settings_.set_overflow_mode)
{
}

ExpressionAnalyzer::~ExpressionAnalyzer() = default;

ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const TreeRewriterResultPtr & syntax_analyzer_result_,
    ContextPtr context_,
    size_t subquery_depth_,
    bool do_global,
    SubqueriesForSets subqueries_for_sets_,
    PreparedSets prepared_sets_,
    BitmapIndexInfoPtr bitmap_index_info_,
    const StorageMetadataPtr & metadata_snapshot_)
    : WithContext(context_)
    , query(query_)
    , settings(getContext()->getSettingsRef())
    , subquery_depth(subquery_depth_)
    , index_context(std::make_shared<MergeTreeIndexContext>())
    , metadata_snapshot(metadata_snapshot_)
    , syntax(syntax_analyzer_result_)
{
    if (bitmap_index_info_)
        index_context->add(bitmap_index_info_);

    /// Cache prepared sets because we might run analysis multiple times
    subqueries_for_sets = std::move(subqueries_for_sets_);
    prepared_sets = std::move(prepared_sets_);

    /// external_tables, subqueries_for_sets for global subqueries.
    /// Replaces global subqueries with the generated names of temporary tables that will be sent to remote servers.
    initGlobalSubqueriesAndExternalTables(do_global);

    /// init columns_after_bitmap_index before analyzeAggregation
    analyzeBitmapIndex();

    /// has_aggregation, aggregation_keys, aggregate_descriptions, aggregated_columns.
    /// This analysis should be performed after processing global subqueries, because otherwise,
    /// if the aggregate function contains a global subquery, then `analyzeAggregation` method will save
    /// in `aggregate_descriptions` the information about the parameters of this aggregate function, among which
    /// global subquery. Then, when you call `initGlobalSubqueriesAndExternalTables` method, this
    /// the global subquery will be replaced with a temporary table, resulting in aggregate_descriptions
    /// will contain out-of-date information, which will lead to an error when the query is executed.
    analyzeAggregation();

    /**
     * check query that satisfing some preconditions
     * 1. sample optimization is legal
     **/
    checkQuery();
}

void ExpressionAnalyzer::analyzeAggregation()
{
    /** Find aggregation keys (aggregation_keys), information about aggregate functions (aggregate_descriptions),
     *  as well as a set of columns obtained after the aggregation, if any,
     *  or after all the actions that are usually performed before aggregation (aggregated_columns).
     *
     * Everything below (compiling temporary ExpressionActions) - only for the purpose of query analysis (type output).
     */

    auto * select_query = query->as<ASTSelectQuery>();

    auto temp_actions = std::make_shared<ActionsDAG>(columns_after_bitmap_index);

    if (select_query)
    {
        NamesAndTypesList array_join_columns;
        columns_after_array_join = columns_after_bitmap_index;

        bool is_array_join_left;
        if (ASTPtr array_join_expression_list = select_query->arrayJoinExpressionList(is_array_join_left))
        {
            getRootActionsNoMakeSet(array_join_expression_list, true, temp_actions, false);

            auto array_join = addMultipleArrayJoinAction(temp_actions, is_array_join_left);
            auto sample_columns = temp_actions->getResultColumns();
            array_join->prepare(sample_columns);
            temp_actions = std::make_shared<ActionsDAG>(sample_columns);

            NamesAndTypesList new_columns_after_array_join;
            NameSet added_columns;

            for (auto & column : temp_actions->getResultColumns())
            {
                if (syntax->array_join_result_to_source.count(column.name))
                {
                    new_columns_after_array_join.emplace_back(column.name, column.type);
                    added_columns.emplace(column.name);
                }
            }

            for (auto & column : columns_after_array_join)
                if (added_columns.count(column.name) == 0)
                    new_columns_after_array_join.emplace_back(column.name, column.type);

            columns_after_array_join.swap(new_columns_after_array_join);
        }

        columns_after_array_join.insert(columns_after_array_join.end(), array_join_columns.begin(), array_join_columns.end());

        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            getRootActionsNoMakeSet(analyzedJoin().leftKeysList(), true, temp_actions, false);
            auto sample_columns = temp_actions->getResultColumns();
            analyzedJoin().addJoinedColumnsAndCorrectTypes(sample_columns);
            temp_actions = std::make_shared<ActionsDAG>(sample_columns);
        }

        columns_after_join = columns_after_array_join;
        analyzedJoin().addJoinedColumnsAndCorrectTypes(columns_after_join, false);
    }

    /// delay the agg function creation because some argument column types may change
    has_aggregation = !aggregates().empty();
    if (select_query && (select_query->groupBy() || select_query->having()))
        has_aggregation = true;

    if (has_aggregation)
    {
        /// Find out aggregation keys.
        if (select_query)
        {
            if (select_query->groupBy())
            {
                NameToIndexMap unique_keys;
                ASTs & group_asts = select_query->groupBy()->children;

                if (select_query->group_by_with_rollup)
                    group_by_kind = GroupByKind::ROLLUP;
                else if (select_query->group_by_with_cube)
                    group_by_kind = GroupByKind::CUBE;
                else if (select_query->group_by_with_grouping_sets && group_asts.size() > 1)
                    group_by_kind = GroupByKind::GROUPING_SETS;
                else
                    group_by_kind = GroupByKind::ORDINARY;

                /// For GROUPING SETS with multiple groups we always add virtual __grouping_set column
                /// With set number, which is used as an additional key at the stage of merging aggregating data.
                if (group_by_kind != GroupByKind::ORDINARY)
                    aggregated_columns.emplace_back("__grouping_set", std::make_shared<DataTypeUInt64>());

                bool ansi_enabled = getContext()->getSettingsRef().dialect_type != DialectType::CLICKHOUSE;
                const bool non_trivial_grouping_sets = group_by_kind != GroupByKind::ORDINARY;
                for (ssize_t i = 0; i < static_cast<ssize_t>(group_asts.size()); ++i)
                {
                    ssize_t size = group_asts.size();

                    if (select_query->group_by_with_grouping_sets)
                    {
                        ASTs group_elements_ast;
                        const ASTExpressionList * group_ast_element = group_asts[i]->as<const ASTExpressionList>();
                        group_elements_ast = group_ast_element->children;

                        NamesAndTypesList grouping_set_list;
                        ColumnNumbers grouping_set_indexes_list;

                        for (ssize_t j = 0; j < ssize_t(group_elements_ast.size()); ++j)
                        {
                            if (getContext()->getSettingsRef().enable_positional_arguments)
                                replaceForPositionalArguments(group_elements_ast[j], select_query, ASTSelectQuery::Expression::GROUP_BY);

                            getRootActionsNoMakeSet(group_elements_ast[j], true, temp_actions, false);

                            ssize_t group_size = group_elements_ast.size();
                            const auto & column_name = group_elements_ast[j]->getColumnName();
                            const auto & column_ast = group_elements_ast[j];
                            const auto * node = temp_actions->tryFindInOutputs(column_name);
                            if (!node)
                                throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                            /// Only removes constant keys if it's an initiator or distributed_group_by_no_merge is enabled.
                            if (getContext()->getClientInfo().distributed_depth == 0
                                || getContext()->getSettingsRef().distributed_group_by_no_merge)
                            {
                                /// Constant expressions have non-null column pointer at this stage.
                                if (node->column && isColumnConst(*node->column))
                                {
                                    select_query->group_by_with_constant_keys = true;

                                    /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                                    if (!aggregates().empty() || group_size > 1)
                                    {
                                        if (j + 1 < static_cast<ssize_t>(group_size))
                                            group_elements_ast[j] = std::move(group_elements_ast.back());

                                        group_elements_ast.pop_back();

                                        --j;
                                        continue;
                                    }
                                }
                            }

                            /// When ANSI mode is on, aggregation keys must be nullable.
                            NameAndTypePair key{column_name, node->result_type};
                            if (ansi_enabled && non_trivial_grouping_sets && JoinCommon::canBecomeNullable(key.type))
                                key.type = JoinCommon::convertTypeToNullable(key.type);

                            grouping_set_list.push_back(key);

                            /// Aggregation keys are unique.
                            if (!unique_keys.contains(key.name))
                            {
                                unique_keys[key.name] = aggregation_keys.size();
                                grouping_set_indexes_list.push_back(aggregation_keys.size());
                                aggregation_keys.push_back(key);
                                aggregation_key_asts.push_back(column_ast);

                                /// Key is no longer needed, therefore we can save a little by moving it.
                                aggregated_columns.push_back(std::move(key));
                            }
                            else
                            {
                                grouping_set_indexes_list.push_back(unique_keys[key.name]);
                            }
                        }

                        aggregation_keys_list.push_back(std::move(grouping_set_list));
                        aggregation_keys_indexes_list.push_back(std::move(grouping_set_indexes_list));
                    }
                    else
                    {
                        if (getContext()->getSettingsRef().enable_positional_arguments)
                            replaceForPositionalArguments(group_asts[i], select_query, ASTSelectQuery::Expression::GROUP_BY);

                        getRootActionsNoMakeSet(group_asts[i], true, temp_actions, false);

                        const auto & column_name = group_asts[i]->getColumnName();
                        const auto & column_ast = group_asts[i];
                        const auto * node = temp_actions->tryFindInOutputs(column_name);
                        if (!node)
                            throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                        /// Only removes constant keys if it's an initiator.
                        if (getContext()->getClientInfo().distributed_depth == 0)
                        {
                            /// Constant expressions have non-null column pointer at this stage.
                            if (node->column && isColumnConst(*node->column))
                            {
                                select_query->group_by_with_constant_keys = true;

                                /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                                if (!aggregates().empty() || size > 1)
                                {
                                    if (i + 1 < static_cast<ssize_t>(size))
                                        group_asts[i] = std::move(group_asts.back());

                                    group_asts.pop_back();

                                    --i;
                                    continue;
                                }
                            }
                        }

                        /// When ANSI mode is on, aggregation keys must be nullable.
                        NameAndTypePair key{column_name, node->result_type};
                        if (ansi_enabled && non_trivial_grouping_sets && JoinCommon::canBecomeNullable(key.type))
                            key.type = JoinCommon::convertTypeToNullable(key.type);

                        /// Aggregation keys are uniqued.
                        if (!unique_keys.contains(key.name))
                        {
                            unique_keys[key.name] = aggregation_keys.size();
                            aggregation_keys.push_back(key);
                            aggregation_key_asts.push_back(column_ast);
                            /// Key is no longer needed, therefore we can save a little by moving it.
                            aggregated_columns.push_back(std::move(key));
                        }
                    }
                }

                /// the key columns, e.g, aggregation_keys, have been extracted;
                /// we are able to decide whether to make argument columns nullable
                /// and then create the correct agg funcs
                makeAggregateDescriptions(temp_actions);

                // tmpfix, ANSI features should be implemented in the optimizer
                if (ansi_enabled && non_trivial_grouping_sets)
                {
                    NameSet aggregate_arguments;
                    for (const auto & agg : aggregate_descriptions)
                        aggregate_arguments.insert(agg.argument_names.begin(), agg.argument_names.end());

                    for (const auto & key : aggregation_keys)
                        if (aggregate_arguments.count(key.name) && !isNullableOrLowCardinalityNullable(key.type))
                            throw Exception(
                                "In ANSI mode grouping keys will be converted to nullable types, aggregate description should be rebuilt",
                                ErrorCodes::NOT_IMPLEMENTED);
                }

                if (!select_query->group_by_with_grouping_sets)
                {
                    auto & list = aggregation_keys_indexes_list.emplace_back();
                    for (size_t i = 0; i < aggregation_keys.size(); ++i)
                        list.push_back(i);
                }

                if (group_asts.empty())
                {
                    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
                    has_aggregation = select_query->having() || !aggregate_descriptions.empty();
                }
            }
        }
        else
            aggregated_columns = temp_actions->getNamesAndTypesList();

        /// Constant expressions are already removed during first 'analyze' run.
        /// So for second `analyze` information is taken from select_query.
        if (select_query)
            has_const_aggregation_keys = select_query->group_by_with_constant_keys;

        /// if makeAggregateDescriptions has not been invoked
        if (aggregate_descriptions.empty())
            makeAggregateDescriptions(temp_actions);

        for (const auto & desc : aggregate_descriptions)
            aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
    }
    else
    {
        aggregated_columns = temp_actions->getNamesAndTypesList();
    }

}

void ExpressionAnalyzer::analyzeBitmapIndex()
{
    columns_after_bitmap_index = sourceColumns();
    auto * bitmap_index_info = dynamic_cast<BitmapIndexInfo *>(index_context->get(MergeTreeIndexInfo::Type::BITMAP).get());
    if (!bitmap_index_info->return_types.empty())
    {
        for (auto & ele : bitmap_index_info->return_types)
        {
            if (ele.second == BitmapIndexReturnType::EXPRESSION)
            {
                auto type = std::make_shared<DataTypeUInt8>();
                columns_after_bitmap_index.emplace_back(ele.first, type);
            }
        }
        columns_after_bitmap_index.remove_if([&bitmap_index_info](auto & col) {
            // it's index column, and not in non-removable
            return bitmap_index_info->remove_on_header_column_name_set.count(col.name) > 0 && bitmap_index_info->non_removable_index_columns.count(col.name) <= 0;
        });
    }
}

void ExpressionAnalyzer::checkQuery()
{
    const auto * select_query = query->as<ASTSelectQuery>();

    if (!select_query)
        return;
    /// check whether to optimize sample
    checkSampleOptimizeIsLegal();
}

void ExpressionAnalyzer::checkSampleOptimizeIsLegal()
{
    auto * select_query = query->as<ASTSelectQuery>();

    if (!storage() || !select_query)
        return;

    std::map<String, size_t> sampled_table;

    checkSample(query, sampled_table);

    for (const auto & item : sampled_table)
    {
        if (item.second > 1)
        {
            const Settings & settings = getContext()->getSettings();
            const_cast<Settings &>(settings).enable_sample_by_range = false;
            break;
        }
    }
}

void ExpressionAnalyzer::checkSample(ASTPtr & ast, std::map<String, size_t> & sampled_table)
{
    if (!ast)
        return;

    if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        if (select->sampleSize())
        {
            auto db_and_table = getDatabaseAndTable(*select, 0);
            if (!db_and_table.has_value())
                return;
            auto select_database = db_and_table->database;
            auto select_table = db_and_table->table;
            String db_and_table_key = "";

            if (!select_table.empty())
            {
                String database = !select_database.empty() ? select_database : "default";
                db_and_table_key = database + ":" + select_table;
            }

            if (!db_and_table_key.empty())
            {
                sampled_table[db_and_table_key] += 1;

                // If offset is provided, disable sample optimization
                if (select->sampleOffset())
                    sampled_table[db_and_table_key] += 1;
            }
        }
    }

    for (auto & child : ast->children)
        checkSample(child, sampled_table);
}

bool ExpressionAnalyzer::hasByteMapColumn() const
{
    for (auto & col : sourceColumns())
    {
        if (col.type->isByteMap())
            return true;
    }
    return false;
}

void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables(bool do_global)
{
    if (do_global && !getContext()->getSettingsRef().distributed_perfect_shard)
    {
        LOG_DEBUG(getLogger("ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables"), "input query-{}", queryToString(query));
        GlobalSubqueriesVisitor::Data subqueries_data(
            getContext(), subquery_depth, isRemoteStorage(), external_tables, subqueries_for_sets, has_global_subqueries);
        GlobalSubqueriesVisitor(subqueries_data).visit(query);
    }
}


void ExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name, const SelectQueryOptions & query_options)
{
    auto set_key = PreparedSetKey::forSubquery(*subquery_or_table_name);

    if (prepared_sets.count(set_key))
        return; /// Already prepared.

    if (auto set_ptr_from_storage_set = isPlainStorageSetInSubquery(subquery_or_table_name))
    {
        prepared_sets.insert({set_key, set_ptr_from_storage_set});
        return;
    }

    auto interpreter_subquery = interpretSubquery(subquery_or_table_name, getContext(), {}, query_options);
    auto io = interpreter_subquery->execute();
    PullingAsyncPipelineExecutor executor(io.pipeline);

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true, getContext()->getSettingsRef().transform_null_in);
    set->setHeader(executor.getHeader());

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }

    set->finishInsert();

    prepared_sets[set_key] = std::move(set);
}

SetPtr ExpressionAnalyzer::isPlainStorageSetInSubquery(const ASTPtr & subquery_or_table_name)
{
    const auto * table = subquery_or_table_name->as<ASTTableIdentifier>();
    if (!table)
        return nullptr;
    auto table_id = getContext()->resolveStorageID(subquery_or_table_name);
    const auto storage = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (storage->getName() != "Set")
        return nullptr;
    const auto storage_set = std::dynamic_pointer_cast<StorageSet>(storage);
    return storage_set->getSet();
}


/// Performance optimization for IN() if storage supports it.
void SelectQueryExpressionAnalyzer::makeSetsForIndex(const ASTPtr & node)
{
    if (!node || !storage() || !storage()->supportsIndexForIn())
        return;

    for (auto & child : node->children)
    {
        /// Don't descend into subqueries.
        if (child->as<ASTSubquery>())
            continue;

        /// Don't descend into lambda functions
        const auto * func = child->as<ASTFunction>();
        if (func && func->name == "lambda")
            continue;

        makeSetsForIndex(child);
    }

    const auto * func = node->as<ASTFunction>();
    if (func && functionIsInOrGlobalInOperator(func->name))
    {
        const IAST & args = *func->arguments;
        const ASTPtr & left_in_operand = args.children.at(0);

        if (storage()->mayBenefitFromIndexForIn(left_in_operand, getContext(), metadata_snapshot))
        {
            const ASTPtr & arg = args.children.at(1);
            if (arg->as<ASTSubquery>() || arg->as<ASTTableIdentifier>())
            {
                if (settings.use_index_for_in_with_subqueries)
                    tryMakeSetForIndexFromSubquery(arg, query_options);
            }
            else
            {
                auto temp_actions = std::make_shared<ActionsDAG>(columns_after_join);
                getRootActions(left_in_operand, true, temp_actions);

                if (temp_actions->tryFindInOutputs(left_in_operand->getColumnName()))
                    makeExplicitSet(func, *temp_actions, true, getContext(), settings.size_limits_for_set, prepared_sets);
            }
        }
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        no_makeset_for_subqueries,
        false /* no_makeset */,
        only_consts,
        !isRemoteStorage() /* create_source_for_in */,
        getAggregationKeysInfo(),
        false /* build_expression_with_window_functions */,
        index_context,
        metadata_snapshot);
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}

void ExpressionAnalyzer::getRootActionsWithOwnBitmapInfo(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, MergeTreeIndexContextPtr & own_index_context, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(getContext(), settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), std::move(actions), prepared_sets, subqueries_for_sets,
                                   no_subqueries, false, only_consts, !isRemoteStorage(), getAggregationKeysInfo(), false, own_index_context, metadata_snapshot);
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::getRootActionsNoMakeSet(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        no_subqueries,
        true,
        only_consts,
        !isRemoteStorage(),
        getAggregationKeysInfo());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}

void ExpressionAnalyzer::getRootActionsForHaving(
    const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        no_makeset_for_subqueries,
        false /* no_makeset */,
        only_consts,
        true /* create_source_for_in */,
        getAggregationKeysInfo());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::getRootActionsForWindowFunctions(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAGPtr & actions)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        no_makeset_for_subqueries,
        false /* no_makeset */,
        false /*only_consts */,
        !isRemoteStorage() /* create_source_for_in */,
        getAggregationKeysInfo(),
        true);
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


bool ExpressionAnalyzer::makeAggregateDescriptions(ActionsDAGPtr & actions)
{
    for (const ASTFunction * node : aggregates())
    {
        AggregateDescription aggregate;
        if (node->arguments)
            getRootActionsNoMakeSet(node->arguments, true, actions);

        aggregate.column_name = node->getColumnName();

        const ASTs & arguments = node->arguments ? node->arguments->children : ASTs();
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const std::string & name = arguments[i]->getColumnName();
            const auto * dag_node = actions->tryFindInOutputs(name);
            if (!dag_node)
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Unknown identifier '{}' in aggregate function '{}'",
                    name,
                    node->formatForErrorMessage());
            }

            types[i] = dag_node->result_type;
            aggregate.argument_names[i] = name;
        }

        /// To be consistent with appendGroupBy, which converts agg key columns to nullable;
        /// otherwise the functions expect non-nullable columns when key columns are agg arguments,
        /// but nullable columns are present
        if (getContext()->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
            for (size_t i = 0; i < arguments.size(); ++i)
                if (aggregation_keys.contains(aggregate.argument_names[i]) && JoinCommon::canBecomeNullable(types[i]))
                    types[i] = JoinCommon::convertTypeToNullable(types[i]);

        AggregateFunctionProperties properties;
        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters, "", getContext()) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters, properties);

        aggregate_descriptions.push_back(aggregate);
    }

    return !aggregates().empty();
}

void makeWindowDescriptionFromAST(
    const Context & context, const WindowDescriptions & existing_descriptions, WindowDescription & desc, const IAST * ast)
{
    const auto & definition = ast->as<const ASTWindowDefinition &>();

    if (!definition.parent_window_name.empty())
    {
        auto it = existing_descriptions.find(definition.parent_window_name);
        if (it == existing_descriptions.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Window definition '{}' references an unknown window '{}'",
                definition.formatForErrorMessage(),
                definition.parent_window_name);
        }

        const auto & parent = it->second;
        desc.partition_by = parent.partition_by;
        desc.order_by = parent.order_by;
        desc.frame = parent.frame;

        // If an existing_window_name is specified it must refer to an earlier
        // entry in the WINDOW list; the new window copies its partitioning clause
        // from that entry, as well as its ordering clause if any. In this case
        // the new window cannot specify its own PARTITION BY clause, and it can
        // specify ORDER BY only if the copied window does not have one. The new
        // window always uses its own frame clause; the copied window must not
        // specify a frame clause.
        // -- https://www.postgresql.org/docs/current/sql-select.html
        if (definition.partition_by)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Derived window definition '{}' is not allowed to override PARTITION BY",
                definition.formatForErrorMessage());
        }

        if (definition.order_by && !parent.order_by.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Derived window definition '{}' is not allowed to override a non-empty ORDER BY",
                definition.formatForErrorMessage());
        }

        if (!parent.frame.is_default)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Parent window '{}' is not allowed to define a frame: while processing derived window definition '{}'",
                definition.parent_window_name,
                definition.formatForErrorMessage());
        }
    }

    if (definition.partition_by)
    {
        for (const auto & column_ast : definition.partition_by->children)
        {
            const auto * with_alias = dynamic_cast<const ASTWithAlias *>(column_ast.get());
            if (!with_alias)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Expected a column in PARTITION BY in window definition,"
                    " got '{}'",
                    column_ast->formatForErrorMessage());
            }
            desc.partition_by.push_back(SortColumnDescription(with_alias->getColumnName(), 1 /* direction */, 1 /* nulls_direction */));
        }
    }

    if (definition.order_by)
    {
        for (const auto & column_ast : definition.order_by->children)
        {
            // Parser should have checked that we have a proper element here.
            const auto & order_by_element = column_ast->as<ASTOrderByElement &>();
            // Ignore collation for now.
            desc.order_by.push_back(SortColumnDescription(
                order_by_element.children.front()->getColumnName(), order_by_element.direction, order_by_element.nulls_direction));
        }
    }

    desc.full_sort_description = desc.partition_by;
    desc.full_sort_description.insert(desc.full_sort_description.end(), desc.order_by.begin(), desc.order_by.end());

    /* all frame types are supported, type check is ont needed */

    desc.frame.is_default = definition.frame_is_default;
    desc.frame.type = definition.frame_type;
    desc.frame.begin_type = definition.frame_begin_type;
    desc.frame.begin_preceding = definition.frame_begin_preceding;
    desc.frame.end_type = definition.frame_end_type;
    desc.frame.end_preceding = definition.frame_end_preceding;

    if (definition.frame_end_type == WindowFrame::BoundaryType::Offset)
    {
        auto [value, _] = evaluateConstantExpression(definition.frame_end_offset, context.shared_from_this());
        desc.frame.end_offset = value;
    }

    if (definition.frame_begin_type == WindowFrame::BoundaryType::Offset)
    {
        auto [value, _] = evaluateConstantExpression(definition.frame_begin_offset, context.shared_from_this());
        desc.frame.begin_offset = value;
    }
}

void ExpressionAnalyzer::makeWindowDescriptions(ActionsDAGPtr actions)
{
    // Convenient to check here because at least we have the Context.
    if (!syntax->window_function_asts.empty() && !getContext()->getSettingsRef().allow_experimental_window_functions)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The support for window functions is experimental and will change"
            " in backwards-incompatible ways in the future releases. Set"
            " allow_experimental_window_functions = 1 to enable it."
            " While processing '{}'",
            syntax->window_function_asts[0]->formatForErrorMessage());
    }

    // Window definitions from the WINDOW clause
    const auto * select_query = query->as<ASTSelectQuery>();
    if (select_query && select_query->window())
    {
        for (const auto & ptr : select_query->window()->children)
        {
            const auto & elem = ptr->as<const ASTWindowListElement &>();
            WindowDescription desc;
            desc.window_name = elem.name;
            makeWindowDescriptionFromAST(*getContext(), window_descriptions, desc, elem.definition.get());

            auto [it, inserted] = window_descriptions.insert({desc.window_name, desc});

            if (!inserted)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window '{}' is defined twice in the WINDOW clause", desc.window_name);
            }
        }
    }

    // Window functions
    for (const ASTFunction * function_node : syntax->window_function_asts)
    {
        assert(function_node->is_window_function);

        WindowFunctionDescription window_function;
        window_function.function_node = function_node;
        window_function.column_name = window_function.function_node->getColumnName();
        window_function.function_parameters = window_function.function_node->parameters
            ? getAggregateFunctionParametersArray(window_function.function_node->parameters, "", getContext())
            : Array();

        // Requiring a constant reference to a shared pointer to non-const AST
        // doesn't really look sane, but the visitor does indeed require it.
        // Hence we clone the node (not very sane either, I know).
        getRootActionsNoMakeSet(window_function.function_node->clone(), true, actions);

        const ASTs & arguments = window_function.function_node->arguments->children;
        window_function.argument_types.resize(arguments.size());
        window_function.argument_names.resize(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const std::string & name = arguments[i]->getColumnName();
            const auto * node = actions->tryFindInOutputs(name);

            if (!node)
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Unknown identifier '{}' in window function '{}'",
                    name,
                    window_function.function_node->formatForErrorMessage());
            }

            window_function.argument_types[i] = node->result_type;
            window_function.argument_names[i] = name;
        }

        AggregateFunctionProperties properties;
        window_function.aggregate_function = AggregateFunctionFactory::instance().get(
            window_function.function_node->name, window_function.argument_types, window_function.function_parameters, properties);


        // Find the window corresponding to this function. It may be either
        // referenced by name and previously defined in WINDOW clause, or it
        // may be defined inline.
        if (!function_node->window_name.empty())
        {
            auto it = window_descriptions.find(function_node->window_name);
            if (it == std::end(window_descriptions))
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Window '{}' is not defined (referenced by '{}')",
                    function_node->window_name,
                    function_node->formatForErrorMessage());
            }

            // a default window definition is allocated for lead and lag
            if (function_node->window_definition)
            {
                const struct WindowFrame def =
                {
                    .is_default = false,
                    .type = WindowFrame::FrameType::Rows,
                    .end_type = WindowFrame::BoundaryType::Unbounded,
                };

                if (it->second.frame != def)
                {
                    WindowDescription desc = it->second;
                    desc.window_name = desc.window_name + "(ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
                    desc.window_functions.clear();
                    desc.frame = def;

                    auto [iter, inserted] = window_descriptions.insert(
                        {desc.window_name, desc});
                    if (!inserted)
                    {
                        chassert(iter->second.full_sort_description
                            == desc.full_sort_description);
                    }
                    it = iter;
                }
            }

            it->second.window_functions.push_back(window_function);
        }
        else
        {
            const auto & definition = function_node->window_definition->as<const ASTWindowDefinition &>();
            WindowDescription desc;
            desc.window_name = definition.getDefaultWindowName();
            makeWindowDescriptionFromAST(*getContext(), window_descriptions, desc, &definition);

            auto [it, inserted] = window_descriptions.insert({desc.window_name, desc});

            if (!inserted)
            {
                assert(it->second.full_sort_description == desc.full_sort_description);
            }

            it->second.window_functions.push_back(window_function);
        }
    }
}


const ASTSelectQuery * ExpressionAnalyzer::getSelectQuery() const
{
    const auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
    return select_query;
}

const ASTSelectQuery * SelectQueryExpressionAnalyzer::getAggregatingQuery() const
{
    if (!has_aggregation)
        throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
    return getSelectQuery();
}

/// "Big" ARRAY JOIN.
ArrayJoinActionPtr ExpressionAnalyzer::addMultipleArrayJoinAction(ActionsDAGPtr & actions, bool array_join_is_left) const
{
    NameSet result_columns;
    for (const auto & result_source : syntax->array_join_result_to_source)
    {
        /// Assign new names to columns, if needed.
        if (result_source.first != result_source.second)
        {
            const auto & node = actions->findInOutputs(result_source.second);
            actions->getOutputs().push_back(&actions->addAlias(node, result_source.first));
        }

        /// Make ARRAY JOIN (replace arrays with their insides) for the columns in these new names.
        result_columns.insert(result_source.first);
    }

    return std::make_shared<ArrayJoinAction>(result_columns, array_join_is_left, getContext());
}

ArrayJoinActionPtr
SelectQueryExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, ActionsDAGPtr & before_array_join, bool only_types)
{
    const auto * select_query = getSelectQuery();

    bool is_array_join_left;
    ASTPtr array_join_expression_list = select_query->arrayJoinExpressionList(is_array_join_left);
    if (!array_join_expression_list)
        return nullptr;

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_bitmap_index);

    getRootActions(array_join_expression_list, only_types, step.actions());

    auto array_join = addMultipleArrayJoinAction(step.actions(), is_array_join_left);
    before_array_join = chain.getLastActions();

    chain.steps.push_back(std::make_unique<ExpressionActionsChain::ArrayJoinStep>(array_join, step.getResultColumns()));

    chain.addStep();

    return array_join;
}

bool SelectQueryExpressionAnalyzer::appendJoinLeftKeys(ExpressionActionsChain & chain, bool only_types)
{
    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);

    getRootActions(analyzedJoin().leftKeysList(), only_types, step.actions());
    return true;
}

JoinPtr SelectQueryExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain)
{
    const ColumnsWithTypeAndName & left_sample_columns = chain.getLastStep().getResultColumns();
    JoinPtr table_join = makeTableJoin(*syntax->ast_join, left_sample_columns);

    if (syntax->analyzed_join->needConvert())
    {
        chain.steps.push_back(
            std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(syntax->analyzed_join->leftConvertingActions()));
        chain.addStep();
    }

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);
    chain.steps.push_back(std::make_unique<ExpressionActionsChain::JoinStep>(syntax->analyzed_join, table_join, step.getResultColumns()));
    chain.addStep();
    return table_join;
}

static JoinPtr tryGetStorageJoin(std::shared_ptr<TableJoin> analyzed_join)
{
    if (auto * table = analyzed_join->joined_storage.get())
        if (auto * storage_join = dynamic_cast<StorageJoin *>(table))
            return storage_join->getJoinLocked(analyzed_join);
    return {};
}

static ActionsDAGPtr createJoinedBlockActions(ContextPtr context, const TableJoin & analyzed_join)
{
    ASTPtr expression_list = analyzed_join.rightKeysList();
    auto syntax_result = TreeRewriter(context).analyze(expression_list, analyzed_join.columnsFromJoinedTable());
    return ExpressionAnalyzer(expression_list, syntax_result, context).getActionsDAG(true, false);
}

static bool allowDictJoin(StoragePtr joined_storage, ContextPtr context, String & dict_name, String & key_name)
{
    if (!joined_storage->isDictionary())
        return false;

    StorageDictionary & storage_dictionary = static_cast<StorageDictionary &>(*joined_storage);
    dict_name = storage_dictionary.getDictionaryName();
    auto dictionary = context->getExternalDictionariesLoader().getDictionary(dict_name, context);
    if (!dictionary)
        return false;

    const DictionaryStructure & structure = dictionary->getStructure();
    if (structure.id)
    {
        key_name = structure.id->name;
        return true;
    }
    return false;
}

static std::shared_ptr<IJoin> makeJoin(std::shared_ptr<TableJoin> analyzed_join, const Block & l_sample_block, const Block & r_sample_block, ContextPtr context)
{
    bool allow_merge_join = analyzed_join->allowMergeJoin();

    /// HashJoin with Dictionary optimisation
    String dict_name;
    String key_name;
    if (analyzed_join->joined_storage && allowDictJoin(analyzed_join->joined_storage, context, dict_name, key_name))
    {
        Names original_names;
        NamesAndTypesList result_columns;
        if (analyzed_join->allowDictJoin(key_name, r_sample_block, original_names, result_columns))
        {
            analyzed_join->dictionary_reader = std::make_shared<DictionaryReader>(dict_name, original_names, result_columns, context);
            return std::make_shared<HashJoin>(analyzed_join, r_sample_block);
        }
    }

    if (analyzed_join->forceHashJoin() || (analyzed_join->preferMergeJoin() && !allow_merge_join))
    {
        if (analyzed_join->allowParallelHashJoin())
        {
            LOG_TRACE(getLogger("SelectQueryExpressionAnalyzer::makeJoin"), "will use ConcurrentHashJoin");
            return std::make_shared<ConcurrentHashJoin>(analyzed_join, context->getSettings().max_threads, context->getSettings().parallel_join_rows_batch_threshold, r_sample_block);
        }
        return std::make_shared<HashJoin>(analyzed_join, r_sample_block);
    }
    else if (analyzed_join->forceMergeJoin() || (analyzed_join->preferMergeJoin() && allow_merge_join))
        return std::make_shared<MergeJoin>(analyzed_join, r_sample_block);
    else if (analyzed_join->forceNestedLoopJoin())
        return std::make_shared<NestedLoopJoin>(analyzed_join, r_sample_block, context);
    else if (analyzed_join->forceGraceHashJoin())
    {
        if (GraceHashJoin::isSupported(analyzed_join)) {
            auto parallel = (context->getSettingsRef().grace_hash_join_left_side_parallel != 0 ? context->getSettingsRef().grace_hash_join_left_side_parallel: context->getSettings().max_threads);
            return std::make_shared<GraceHashJoin>(context, analyzed_join, l_sample_block, r_sample_block, context->getTempDataOnDisk(), parallel, context->getSettingsRef().spill_mode == SpillMode::AUTO, false, context->getSettings().max_threads);
        }  else if (allow_merge_join) {  // fallback into merge join
            LOG_WARNING(getLogger("SelectQueryExpressionAnalyzer::makeJoin"), "Grace hash join is not support, fallback into merge join.");
            return {std::make_shared<JoinSwitcher>(analyzed_join, r_sample_block)};
        } else { // fallback into hash join when grace hash and merge join not supported
            LOG_WARNING(getLogger("SelectQueryExpressionAnalyzer::makeJoin"), "Grace hash join and merge join is not support, fallback into hash join.");
            return {std::make_shared<HashJoin>(analyzed_join, r_sample_block)};
        }
    }
    return std::make_shared<JoinSwitcher>(analyzed_join, r_sample_block);
}

JoinPtr SelectQueryExpressionAnalyzer::makeTableJoin(
    const ASTTablesInSelectQueryElement & join_element, const ColumnsWithTypeAndName & left_sample_columns)
{
    /// Two JOINs are not supported with the same subquery, but different USINGs.

    if (joined_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table join was already created for query");

    /// Use StorageJoin if any.
    JoinPtr join = tryGetStorageJoin(syntax->analyzed_join);

    if (!join)
    {
        /// Actions which need to be calculated on joined block.
        auto joined_block_actions = createJoinedBlockActions(getContext(), analyzedJoin());

        Names original_right_columns;

        NamesWithAliases required_columns_with_aliases = analyzedJoin().getRequiredColumns(
            Block(joined_block_actions->getResultColumns()), joined_block_actions->getRequiredColumns().getNames());
        for (auto & pr : required_columns_with_aliases)
            original_right_columns.push_back(pr.first);

        /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
            * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
            *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
            * - this function shows the expression JOIN _data1.
            */
        auto interpreter = interpretSubquery(
            join_element.table_expression, getContext(), original_right_columns, query_options.copy().setWithAllColumns().ignoreProjections(false).ignoreAlias(false));
        {
            joined_plan = std::make_unique<QueryPlan>();
            interpreter->buildQueryPlan(*joined_plan);

            auto sample_block = interpreter->getSampleBlock();

            auto rename_dag = std::make_unique<ActionsDAG>(sample_block.getColumnsWithTypeAndName());
            for (const auto & name_with_alias : required_columns_with_aliases)
            {
                if (sample_block.has(name_with_alias.first))
                {
                    auto pos = sample_block.getPositionByName(name_with_alias.first);
                    const auto & alias = rename_dag->addAlias(*rename_dag->getInputs()[pos], name_with_alias.second);
                    rename_dag->getOutputs()[pos] = &alias;
                }
            }

            auto rename_step = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), std::move(rename_dag));
            rename_step->setStepDescription("Rename joined columns");
            joined_plan->addStep(std::move(rename_step));
        }

        auto joined_actions_step = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), std::move(joined_block_actions));
        joined_actions_step->setStepDescription("Joined actions");
        joined_plan->addStep(std::move(joined_actions_step));

        const ColumnsWithTypeAndName & right_sample_columns = joined_plan->getCurrentDataStream().header.getColumnsWithTypeAndName();
        bool need_convert = syntax->analyzed_join->applyJoinKeyConvert(left_sample_columns, right_sample_columns);
        if (need_convert)
        {
            auto converting_step
                = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), syntax->analyzed_join->rightConvertingActions());
            converting_step->setStepDescription("Convert joined columns");
            joined_plan->addStep(std::move(converting_step));
        }

        Block left_sample_block(left_sample_columns);
        for (auto & column : left_sample_block)
        {
            if (!column.column)
                column.column = column.type->createColumn();
        }
        join = makeJoin(syntax->analyzed_join, left_sample_block, joined_plan->getCurrentDataStream().header, getContext());

        /// Do not make subquery for join over dictionary.
        if (syntax->analyzed_join->dictionary_reader)
            joined_plan.reset();
    }
    else
        syntax->analyzed_join->applyJoinKeyConvert(left_sample_columns, {});

    return join;
}

ActionsDAGPtr
SelectQueryExpressionAnalyzer::appendPrewhere(ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns)
{
    const auto * select_query = getSelectQuery();
    if (!select_query->prewhere())
        return nullptr;

    Names first_action_names;
    if (!chain.steps.empty())
        first_action_names = chain.steps.front()->getRequiredColumns().getNames();

    auto & step = chain.lastStep(columns_after_bitmap_index);
    getRootActions(select_query->prewhere(), only_types, step.actions());
    String prewhere_column_name = select_query->prewhere()->getColumnName();
    step.addRequiredOutput(prewhere_column_name);

    const auto & node = step.actions()->findInOutputs(prewhere_column_name);
    auto filter_type = node.result_type;
    if (!filter_type->canBeUsedInBooleanContext())
        throw Exception("Invalid type for filter in PREWHERE: " + filter_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

    ActionsDAGPtr prewhere_actions;
    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions_dag = std::make_shared<ActionsDAG>(columns_after_bitmap_index);
        getRootActions(select_query->prewhere(), only_types, tmp_actions_dag);
        /// Constants cannot be removed since they can be used in other parts of the query.
        /// And if they are not used anywhere, except PREWHERE, they will be removed on the next step.
        /// follow change: https://github.com/ClickHouse/ClickHouse/commit/fbf98bea0ba56440b973fabbfe755aeb13f078ef
        tmp_actions_dag->removeUnusedActions(NameSet{prewhere_column_name}, false);

        auto required_columns = tmp_actions_dag->getRequiredColumnsNames();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());
        required_source_columns.insert(first_action_names.begin(), first_action_names.end());

        /// Add required columns to required output in order not to remove them after prewhere execution.
        /// TODO: add sampling and final execution to common chain.
        for (const auto & column : additional_required_columns)
        {
            if (required_source_columns.count(column))
                step.addRequiredOutput(column);
        }

        auto names = step.actions()->getNames();
        NameSet name_set(names.begin(), names.end());

        for (const auto & column : columns_after_bitmap_index)
            if (required_source_columns.count(column.name) == 0)
                name_set.erase(column.name);

        Names required_output(name_set.begin(), name_set.end());
        prewhere_actions = chain.getLastActions();
        prewhere_actions->removeUnusedActions(required_output);
    }

    {
        /// Add empty action with input = {prewhere actions output} + {unused source columns}
        /// Reasons:
        /// 1. Remove remove source columns which are used only in prewhere actions during prewhere actions execution.
        ///    Example: select A prewhere B > 0. B can be removed at prewhere step.
        /// 2. Store side columns which were calculated during prewhere actions execution if they are used.
        ///    Example: select F(A) prewhere F(A) > 0. F(A) can be saved from prewhere step.
        /// 3. Check if we can remove filter column at prewhere step. If we can, action will store single REMOVE_COLUMN.
        ColumnsWithTypeAndName columns = prewhere_actions->getResultColumns();
        auto required_columns = prewhere_actions->getRequiredColumns();
        NameSet prewhere_input_names;
        NameSet unused_source_columns;

        for (const auto & col : required_columns)
            prewhere_input_names.insert(col.name);

        for (const auto & column : columns_after_bitmap_index)
        {
            if (prewhere_input_names.count(column.name) == 0)
            {
                columns.emplace_back(column.type, column.name);
                unused_source_columns.emplace(column.name);
            }
        }

        chain.steps.emplace_back(
            std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(std::make_shared<ActionsDAG>(std::move(columns))));
        chain.steps.back()->additional_input = std::move(unused_source_columns);
        chain.getLastActions();
        chain.addStep();
    }

    return prewhere_actions;
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendMaterializeStep(ExpressionActionsChain & chain, const ASTPtr & predicate, bool only_types, const Names & additional_required_columns)
{
    Names first_action_names;
    if (!chain.steps.empty())
        first_action_names = chain.steps.front()->getRequiredColumns().getNames();

    auto & step = chain.lastStep(columns_after_bitmap_index);
    getRootActions(predicate, only_types, step.actions());
    String filter_column_name = predicate->getColumnName();
    step.addRequiredOutput(filter_column_name);

    ActionsDAGPtr filter_actions;
    {
        /// Remove unused source_columns from actions.
        auto tmp_actions_dag = std::make_shared<ActionsDAG>(columns_after_bitmap_index);
        getRootActions(predicate, only_types, tmp_actions_dag);
        tmp_actions_dag->removeUnusedActions(NameSet{filter_column_name});

        auto required_columns = tmp_actions_dag->getRequiredColumnsNames();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());
        required_source_columns.insert(first_action_names.begin(), first_action_names.end());

        /// Add required columns to required output in order not to remove them after prewhere execution.
        /// TODO: add sampling and final execution to common chain.
        for (const auto & column : additional_required_columns)
        {
            if (required_source_columns.count(column))
                step.addRequiredOutput(column);
        }

        auto names = step.actions()->getNames();
        NameSet name_set(names.begin(), names.end());

        for (const auto & column : columns_after_bitmap_index)
            if (required_source_columns.count(column.name) == 0)
                name_set.erase(column.name);

        Names required_output(name_set.begin(), name_set.end());
        filter_actions = chain.getLastActions();
        filter_actions->removeUnusedActions(required_output);
    }

    {
        /// Add empty action with input = {filter actions output} + {unused source columns}
        /// Reasons:
        /// 1. Remove remove source columns which are used only in prewhere actions during prewhere actions execution.
        ///    Example: select A prewhere B > 0. B can be removed at prewhere step.
        /// 2. Store side columns which were calculated during prewhere actions execution if they are used.
        ///    Example: select F(A) prewhere F(A) > 0. F(A) can be saved from prewhere step.
        /// 3. Check if we can remove filter column at prewhere step. If we can, action will store single REMOVE_COLUMN.
        ColumnsWithTypeAndName columns = filter_actions->getResultColumns();
        auto required_columns = filter_actions->getRequiredColumns();
        NameSet input_names;
        NameSet unused_source_columns;

        for (const auto & col : required_columns)
            input_names.insert(col.name);

        for (const auto & column : columns_after_bitmap_index)
        {
            if (input_names.count(column.name) == 0)
            {
                columns.emplace_back(column.type, column.name);
                unused_source_columns.emplace(column.name);
            }
        }

        chain.steps.emplace_back(std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(
            std::make_shared<ActionsDAG>(std::move(columns))));
        chain.steps.back()->additional_input = std::move(unused_source_columns);
        chain.getLastActions();
        chain.addStep();
    }

    // fmt::print("Chain after adding em step: \n{}\n", chain.dumpChain());

    return filter_actions;
}

bool SelectQueryExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->where())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    getRootActions(select_query->where(), only_types, step.actions());

    auto where_column_name = select_query->where()->getColumnName();
    step.addRequiredOutput(where_column_name);

    const auto & node = step.actions()->findInOutputs(where_column_name);
    auto filter_type = node.result_type;
    if (!filter_type->canBeUsedInBooleanContext())
        throw Exception("Invalid type for filter in WHERE: " + filter_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

    return true;
}

bool SelectQueryExpressionAnalyzer::appendGroupBy(
    ExpressionActionsChain & chain, bool only_types, bool optimize_aggregation_in_order, ManyExpressionActions & group_by_elements_actions)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->groupBy())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    ASTs asts = select_query->groupBy()->children;
    if (select_query->group_by_with_grouping_sets)
    {
        for (const auto & ast : asts)
        {
            for (const auto & ast_element : ast->children)
            {
                step.addRequiredOutput(ast_element->getColumnName());
                getRootActions(ast_element, only_types, step.actions());
            }
        }
    }
    else
    {
        for (const auto & ast : asts)
        {
            step.addRequiredOutput(ast->getColumnName());
            getRootActions(ast, only_types, step.actions());
        }
    }

    /// When ANSI mode is on, converts group keys into nullable types if they are not. The purpose of conversion is to
    /// ensure that (default) values of empty keys are NULLs with the modifiers as GROUPING SETS, ROLLUP and CUBE.
    /// The conversion occurs before the aggregation to adapt different aggregation variants.
    const bool non_trivial_grouping_sets = select_query->group_by_with_grouping_sets || select_query->group_by_with_rollup || select_query->group_by_with_cube;
    if (non_trivial_grouping_sets)
    {
        if (getContext()->getSettingsRef().dialect_type != DialectType::CLICKHOUSE)
        {
            const auto & src_columns = step.actions()->getResultColumns();
            ColumnsWithTypeAndName dst_columns;
            dst_columns.reserve(src_columns.size());

            for (const auto &src: src_columns)
            {
                DataTypePtr type = src.type;
                if (aggregation_keys.contains(src.name) && JoinCommon::canBecomeNullable(type))
                    type = JoinCommon::convertTypeToNullable(type);
                dst_columns.emplace_back(src.column, type, src.name);
            }

            auto nullify_dag = ActionsDAG::makeConvertingActions(
                    src_columns, dst_columns, ActionsDAG::MatchColumnsMode::Position);

            auto ret = ActionsDAG::merge(std::move(*step.actions()), std::move(*nullify_dag));

            step.actions().swap(ret);
        }
    }
    if (optimize_aggregation_in_order)
    {
        for (auto & child : asts)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            group_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(
                actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes)));
        }
    }

    return true;
}

void SelectQueryExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    for (const auto & desc : aggregate_descriptions)
        for (const auto & name : desc.argument_names)
            step.addRequiredOutput(name);

    /// Collect aggregates removing duplicates by node.getColumnName()
    /// It's not clear why we recollect aggregates (for query parts) while we're able to use previously collected ones (for entire query)
    /// @note The original recollection logic didn't remove duplicates.
    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(select_query->select());

    if (select_query->having())
        GetAggregatesVisitor(data).visit(select_query->having());

    if (select_query->orderBy())
        GetAggregatesVisitor(data).visit(select_query->orderBy());

    /// TODO: data.aggregates -> aggregates()
    for (const ASTFunction * node : data.aggregates)
        if (node->arguments)
            for (auto & argument : node->arguments->children)
                getRootActions(argument, only_types, step.actions());
}

void SelectQueryExpressionAnalyzer::appendWindowFunctionsArguments(ExpressionActionsChain & chain, bool /* only_types */)
{
    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    // (1) Add actions for window functions and the columns they require.
    // (2) Mark the columns that are really required. We have to mark them as
    //     required because we finish the expression chain before processing the
    //     window functions.
    // The required columns are:
    //  (a) window function arguments,
    //  (b) the columns from PARTITION BY and ORDER BY.

    // (1a) Actions for PARTITION BY and ORDER BY for windows defined in the
    // WINDOW clause. The inline window definitions will be processed
    // recursively together with (1b) as ASTFunction::window_definition.
    if (getSelectQuery()->window())
    {
        getRootActionsNoMakeSet(getSelectQuery()->window(), true /* no_subqueries */, step.actions());
    }

    for (const auto & [_, w] : window_descriptions)
    {
        for (const auto & f : w.window_functions)
        {
            // (1b) Actions for function arguments, and also the inline window
            // definitions (1a).
            // Requiring a constant reference to a shared pointer to non-const AST
            // doesn't really look sane, but the visitor does indeed require it.
            getRootActionsNoMakeSet(f.function_node->clone(), true /* no_subqueries */, step.actions());

            // (2b) Required function argument columns.
            for (const auto & a : f.function_node->arguments->children)
            {
                step.addRequiredOutput(a->getColumnName());
            }
        }

        // (2a) Required PARTITION BY and ORDER BY columns.
        for (const auto & c : w.full_sort_description)
        {
            step.addRequiredOutput(c.column_name);
        }
    }
}

void SelectQueryExpressionAnalyzer::appendExpressionsAfterWindowFunctions(ExpressionActionsChain & chain, bool /* only_types */)
{
    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_window);
    for (const auto & expression : syntax->expressions_with_window_function)
    {
        getRootActionsForWindowFunctions(expression->clone(), true, step.actions());
    }
}

void SelectQueryExpressionAnalyzer::appendSelectSkipWindowExpressions(ExpressionActionsChain::Step & step, ASTPtr const & node)
{
    if (auto * function = node->as<ASTFunction>())
    {
        // Skip window function columns here -- they are calculated after
        // other SELECT expressions by a special step.
        // Also skipping lambda functions because they can't be explicitly evaluated.
        if (function->is_window_function || function->name == "lambda")
            return;
        if (function->compute_after_window_functions)
        {
            for (auto & arg : function->arguments->children)
                appendSelectSkipWindowExpressions(step, arg);
            return;
        }
    }
    step.addRequiredOutput(node->getColumnName());
}

bool SelectQueryExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->having())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActionsForHaving(select_query->having(), only_types, step.actions());
    step.addRequiredOutput(select_query->having()->getColumnName());

    return true;
}

void SelectQueryExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActions(select_query->select(), only_types, step.actions());

    for (const auto & child : select_query->select()->children)
        appendSelectSkipWindowExpressions(step, child);
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendOrderBy(
    ExpressionActionsChain & chain, bool only_types, bool optimize_read_in_order, ManyExpressionActions & order_by_elements_actions)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->orderBy())
    {
        auto actions = chain.getLastActions();
        chain.addStep();
        return actions;
    }

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    for (auto & child : select_query->orderBy()->children)
    {
        auto * ast = child->as<ASTOrderByElement>();
        if (!ast || ast->children.empty())
            throw Exception("Bad ORDER BY expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);

        if (getContext()->getSettingsRef().enable_positional_arguments)
            replaceForPositionalArguments(ast->children.at(0), select_query, ASTSelectQuery::Expression::ORDER_BY);
    }

    getRootActions(select_query->orderBy(), only_types, step.actions());

    bool with_fill = false;
    NameSet order_by_keys;

    for (auto & child : select_query->orderBy()->children)
    {
        auto * ast = child->as<ASTOrderByElement>();
        ASTPtr order_expression = ast->children.at(0);
        step.addRequiredOutput(order_expression->getColumnName());

        if (ast->with_fill)
            with_fill = true;
    }

    if (optimize_read_in_order)
    {
        for (auto & child : select_query->orderBy()->children)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            order_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(
                actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes)));
        }
    }

    NameSet non_constant_inputs;
    if (with_fill)
    {
        for (const auto & column : step.getResultColumns())
            if (!order_by_keys.count(column.name))
                non_constant_inputs.insert(column.name);
    }

    auto actions = chain.getLastActions();
    chain.addStep(non_constant_inputs);
    return actions;
}

bool SelectQueryExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->limitBy())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActions(select_query->limitBy(), only_types, step.actions());

    NameSet aggregated_names;
    for (const auto & column : aggregated_columns)
    {
        step.addRequiredOutput(column.name);
        aggregated_names.insert(column.name);
    }

    auto & children = select_query->limitBy()->children;
    for (auto & child : children)
    {
        if (getContext()->getSettingsRef().enable_positional_arguments)
            replaceForPositionalArguments(child, select_query, ASTSelectQuery::Expression::LIMIT_BY);

        auto child_name = child->getColumnName();
        if (!aggregated_names.count(child_name))
            step.addRequiredOutput(std::move(child_name));
    }

    return true;
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    const auto * select_query = getSelectQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    NamesWithAliases result_columns;
    NameSet required_result_columns_set(required_result_columns.begin(), required_result_columns.end());

    ASTs asts = select_query->select()->children;
    for (const auto & ast : asts)
    {
        String result_name = ast->getAliasOrColumnName();
        auto it = required_result_columns_set.find(result_name);
        if (required_result_columns_set.empty() || it != required_result_columns_set.end())
        {
            std::string source_name = ast->getColumnName();

            /*
             * For temporary columns created by ExpressionAnalyzer for literals,
             * use the correct source column. Using the default display name
             * returned by getColumnName is not enough, and we have to use the
             * column id set by EA. In principle, this logic applies to all kinds
             * of columns, not only literals. Literals are especially problematic
             * for two reasons:
             * 1) confusing different literal columns leads to weird side
             *    effects (see 01101_literal_columns_clash);
             * 2) the disambiguation mechanism in SyntaxAnalyzer, that, among
             *    other things, creates unique aliases for columns with same
             *    names from different tables, is applied before these temporary
             *    columns are created by ExpressionAnalyzer.
             * Similar problems should also manifest for function columns, which
             * are likewise created at a later stage by EA.
             * In general, we need to have explicit separation between display
             * names and identifiers for columns. This code is a workaround for
             * a particular subclass of problems, and not a proper solution.
             */
            if (const auto * as_literal = ast->as<ASTLiteral>())
            {
                source_name = as_literal->unique_column_name;
                assert(!source_name.empty());
            }

            result_columns.emplace_back(source_name, result_name);
            step.addRequiredOutput(result_columns.back().second);
            if (!required_result_columns_set.empty())
                required_result_columns_set.erase(it);
        }
    }

    auto actions = chain.getLastActions();
    actions->project(result_columns);

    if (!required_result_columns.empty())
    {
        const NameSet & required_result_columns_not_present = required_result_columns_set;
        result_columns.clear();
        for (const auto & column : required_result_columns)
        {
            // This is a simple fix for the test case tests/queries/4_cnch_stateless/00998_materialized_view_multiple_joins.sql
            // whereby INSERT INTO `1234.test00998mv`.target_join_448189919111675915_write SELECT t.id, t.s FROM `1234.test00998mv`.source AS t INNER JOIN `1234.test00998mv`.dim AS d ON t.s = d.s WHERE _partition_id = 1
            // results in _partition_id being one of the required_result_columns.
            // But actions does not have the virtual column _partition_id.
            // This is probably not the best way to do it. Should _partition_id even be allowed here?
            if (required_result_columns_not_present.count(column) > 0)
            {
                LOG_DEBUG(getLogger("SelectQueryExpressionAnalyzer::appendProjectResult"), "Column not present: {}", column);
                continue;
            }
            result_columns.emplace_back(column, std::string{});
        }
        actions->project(result_columns);
    }

    return actions;
}


void ExpressionAnalyzer::appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types)
{
    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_bitmap_index);
    getRootActions(expr, only_types, step.actions());
    step.addRequiredOutput(expr->getColumnName());
}


ActionsDAGPtr ExpressionAnalyzer::getActionsDAG(bool add_aliases, bool project_result)
{
    auto actions_dag = std::make_shared<ActionsDAG>(aggregated_columns);
    NamesWithAliases result_columns;
    Names result_names;

    ASTs asts;

    if (const auto * node = query->as<ASTExpressionList>())
        asts = node->children;
    else
        asts = ASTs(1, query);

    for (const auto & ast : asts)
    {
        std::string name = ast->getColumnName();
        std::string alias;
        if (add_aliases)
            alias = ast->getAliasOrColumnName();
        else
            alias = name;
        result_columns.emplace_back(name, alias);
        result_names.push_back(alias);
        getRootActions(ast, false, actions_dag);
    }

    if (add_aliases)
    {
        if (project_result)
            actions_dag->project(result_columns);
        else
            actions_dag->addAliases(result_columns);
    }

    if (!(add_aliases && project_result))
    {
        NameSet name_set(result_names.begin(), result_names.end());
        /// We will not delete the original columns.
        for (const auto & column_name_type : columns_after_bitmap_index)
        {
            if (name_set.count(column_name_type.name) == 0)
            {
                result_names.push_back(column_name_type.name);
                name_set.insert(column_name_type.name);
            }
        }

        actions_dag->removeUnusedActions(name_set);
    }

    return actions_dag;
}

ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result, CompileExpressions compile_expressions)
{
    return std::make_shared<ExpressionActions>(
        getActionsDAG(add_aliases, project_result), ExpressionActionsSettings::fromContext(getContext(), compile_expressions));
}

ActionsDAGPtr ExpressionAnalyzer::getConstActionsDAG(const ColumnsWithTypeAndName & constant_inputs)
{
    auto actions = std::make_shared<ActionsDAG>(constant_inputs);
    getRootActions(query, true /* no_makeset_for_subqueries */, actions, true /* only_consts */);
    return actions;
}

ExpressionActionsPtr ExpressionAnalyzer::getConstActions(const ColumnsWithTypeAndName & constant_inputs)
{
    auto actions = getConstActionsDAG(constant_inputs);
    return std::make_shared<ExpressionActions>(actions, ExpressionActionsSettings::fromContext(getContext()));
}

std::unique_ptr<QueryPlan> SelectQueryExpressionAnalyzer::getJoinedPlan()
{
    return std::move(joined_plan);
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::simpleSelectActions()
{
    ExpressionActionsChain new_chain(getContext());
    appendSelect(new_chain, false);
    return new_chain.getLastActions();
}

ExpressionAnalysisResult::ExpressionAnalysisResult(
        SelectQueryExpressionAnalyzer & query_analyzer,
        const StorageMetadataPtr & metadata_snapshot,
        bool first_stage_,
        bool second_stage_,
        bool only_types,
        const FilterDAGInfoPtr & filter_info_,
        const Block & source_header,
        const std::vector<ASTPtr> & additional_predicates)
    : first_stage(first_stage_)
    , second_stage(second_stage_)
    , need_aggregate(query_analyzer.hasAggregation())
    , has_window(query_analyzer.hasWindow())
    , use_grouping_set_key(query_analyzer.useGroupingSetKey())
{
    /// first_stage: Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    /// second_stage: Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.

    /** First we compose a chain of actions and remember the necessary steps from it.
        *  Regardless of from_stage and to_stage, we will compose a complete sequence of actions to perform optimization and
        *  throw out unnecessary columns based on the entire query. In unnecessary parts of the query, we will not execute subqueries.
        */

    const ASTSelectQuery & query = *query_analyzer.getSelectQuery();
    auto context = query_analyzer.getContext();
    const Settings & settings = context->getSettingsRef();
    const ConstStoragePtr & storage = query_analyzer.storage();
    Block precomputed_header;
    auto index_context = query_analyzer.getIndexContext();
    if (auto * bitmap_index_info = dynamic_cast<BitmapIndexInfo *>(index_context->get(MergeTreeIndexInfo::Type::BITMAP).get()))
    {
        for (auto & ele : bitmap_index_info->return_types)
        {
            if (ele.second == BitmapIndexReturnType::EXPRESSION)
                precomputed_header.insert({std::make_shared<DataTypeUInt8>(), ele.first});
        }
    }

    bool finalized = false;
    size_t where_step_num = 0;

    auto finalize_chain = [&](ExpressionActionsChain & chain) -> ColumnsWithTypeAndName {
        chain.finalize();

        if (!finalized)
        {
            finalize(chain, where_step_num, query);
            finalized = true;
        }

        auto res = chain.getLastStep().getResultColumns();
        chain.clear();
        return res;
    };

    if (storage)
    {
        query_analyzer.makeSetsForIndex(query.where());
        query_analyzer.makeSetsForIndex(query.prewhere());
        query_analyzer.makeSetsForIndex(createPredicateFromArrays(additional_predicates));
    }

    {
        ExpressionActionsChain chain(context);
        Names additional_required_columns_after_prewhere;

        if (storage && (query.sampleSize() || settings.parallel_replicas_count > 1))
        {
            Names columns_for_sampling = metadata_snapshot->getColumnsRequiredForSampling();
            additional_required_columns_after_prewhere.insert(
                additional_required_columns_after_prewhere.end(), columns_for_sampling.begin(), columns_for_sampling.end());
        }

        if (storage && query.final())
        {
            Names columns_for_final = metadata_snapshot->getColumnsRequiredForFinal();
            additional_required_columns_after_prewhere.insert(
                additional_required_columns_after_prewhere.end(), columns_for_final.begin(), columns_for_final.end());
        }

        if (storage && filter_info_)
        {
            filter_info = filter_info_;
            filter_info->do_remove_column = true;
        }

        if (auto actions = query_analyzer.appendPrewhere(chain, !first_stage, additional_required_columns_after_prewhere))
        {
            prewhere_info = std::make_shared<PrewhereInfo>(actions, query.prewhere()->getColumnName());
            if (query_analyzer.index_context->has(MergeTreeIndexInfo::Type::BITMAP))
                prewhere_info->index_context = query_analyzer.index_context->clone();

            if (allowEarlyConstantFolding(*prewhere_info->prewhere_actions, settings))
            {
                Block before_prewhere_sample = source_header;
                if (sanitizeBlock(before_prewhere_sample))
                {
                    ExpressionActions(prewhere_info->prewhere_actions, ExpressionActionsSettings::fromSettings(context->getSettingsRef()))
                        .execute(before_prewhere_sample);
                    auto & column_elem = before_prewhere_sample.getByName(query.prewhere()->getColumnName());
                    /// If the filter column is a constant, record it.
                    if (column_elem.column)
                        prewhere_constant_filter_description = ConstantFilterDescription(*column_elem.column);
                }
            }
        }

        if (!additional_predicates.empty())
        {
            /// Collect columns needed for default evaluation
            const auto & source_columns = query_analyzer.sourceColumns();
            NameSet columns_for_default;
            for (const auto & col : source_columns)
                collectColumnsForDefaultEvaluation(col.name, metadata_snapshot->getColumns(), columns_for_default);

            additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(), columns_for_default.begin(), columns_for_default.end());
            Block sample = source_header;
            ASTPtr current_ast = nullptr;
            for (auto it = additional_predicates.rbegin(); it != additional_predicates.rend(); ++it)
            {
                const auto & predicate = *it;
                /// Collect bitmap index on predicate first, at the same time generate the standalone action
                /// for current predicate
                auto tmp_actions = std::make_shared<ActionsDAG>(query_analyzer.sourceColumns());
                auto own_index_context = std::make_shared<MergeTreeIndexContext>();
                query_analyzer.getRootActionsWithOwnBitmapInfo(predicate, !first_stage, tmp_actions, own_index_context);
                /// Santinizer the filter colums
                const auto & node = tmp_actions->findInOutputs(predicate->getColumnName());
                sanitizeDataType(node.result_type);
                /// Generate actions for `accumulated predicates`
                current_ast = current_ast ? makeASTFunction("and", predicate, std::move(current_ast)) : predicate;
                auto actions = query_analyzer.appendMaterializeStep(chain, current_ast, !first_stage, additional_required_columns_after_prewhere);

                atomic_predicates.emplace_front(std::make_shared<AtomicPredicate>());
                atomic_predicates.front()->predicate_actions = actions;
                atomic_predicates.front()->filter_column_name = current_ast->getColumnName();
                if (own_index_context->has(MergeTreeIndexInfo::Type::BITMAP))
                    atomic_predicates.front()->index_context = std::move(own_index_context);

                /// Constant folding if possible
                if (actions && allowEarlyConstantFolding(*actions, settings))
                {
                    if (sanitizeBlock(sample))
                    {
                        ExpressionActions(
                            actions, ExpressionActionsSettings::fromSettings(context->getSettingsRef())).execute(sample);
                    }
                    auto & column_elem = sample.getByName(predicate->getColumnName());
                    /// If the filter column is a constant, record it.
                    if (column_elem.column)
                    {
                        em_constant_filter_description = ConstantFilterDescription(*column_elem.column);
                    }
                }
            }
        }

        array_join = query_analyzer.appendArrayJoin(chain, before_array_join, only_types || !first_stage);

        if (query_analyzer.hasTableJoin())
        {
            query_analyzer.appendJoinLeftKeys(chain, only_types || !first_stage);
            before_join = chain.getLastActions();
            join = query_analyzer.appendJoin(chain);
            converting_join_columns = query_analyzer.analyzedJoin().leftConvertingActions();
            chain.addStep();
        }

        if (query_analyzer.appendWhere(chain, only_types || !first_stage))
        {
            where_step_num = chain.steps.size() - 1;
            before_where = chain.getLastActions();
            if (allowEarlyConstantFolding(*before_where, settings))
            {
                Block before_where_sample;
                if (chain.steps.size() > 1)
                    before_where_sample = Block(chain.steps[chain.steps.size() - 2]->getResultColumns());
                else
                    before_where_sample = source_header;
                if (sanitizeBlock(before_where_sample))
                {
                    ExpressionActions(before_where, ExpressionActionsSettings::fromSettings(context->getSettingsRef()))
                        .execute(before_where_sample);
                    auto & column_elem = before_where_sample.getByName(query.where()->getColumnName());
                    /// If the filter column is a constant, record it.
                    if (column_elem.column)
                        where_constant_filter_description = ConstantFilterDescription(*column_elem.column);
                }
            }
            chain.addStep();
        }

        if (need_aggregate)
        {
            /// TODO correct conditions
            optimize_aggregation_in_order = context->getSettingsRef().optimize_aggregation_in_order && storage && query.groupBy();

            query_analyzer.appendGroupBy(chain, only_types || !first_stage, optimize_aggregation_in_order, group_by_elements_actions);
            query_analyzer.appendAggregateFunctionsArguments(chain, only_types || !first_stage);
            before_aggregation = chain.getLastActions();

            auto columns_before_aggregation = finalize_chain(chain);

            /// Here we want to check that columns after aggregation have the same type as
            /// were promised in query_analyzer.aggregated_columns
            /// Ideally, they should be equal. In practice, this may be not true.
            /// As an example, we don't build sets for IN inside ExpressionAnalysis::analyzeAggregation,
            /// so that constant folding for expression (1 in 1) will not work. This may change the return type
            /// for functions with LowCardinality argument: function "substr(toLowCardinality('abc'), 1 IN 1)"
            /// should usually return LowCardinality(String) when (1 IN 1) is constant, but without built set
            /// for (1 IN 1) constant is not propagated and "substr" returns String type.
            /// See 02503_in_lc_const_args_bug.sql
            ///
            /// As a temporary solution, we add converting actions to the next chain.
            /// Hopefully, later we can
            /// * use a new analyzer where this issue is absent
            /// * or remove ExpressionActionsChain completely and re-implement its logic on top of the query plan
            {
                for (auto & col : columns_before_aggregation)
                    if (!col.column)
                        col.column = col.type->createColumn();

                Block header_before_aggregation(std::move(columns_before_aggregation));

                auto names = query_analyzer.aggregationKeys().getNames();
                ColumnNumbers keys;
                for (const auto & name : names)
                    keys.push_back(header_before_aggregation.getPositionByName(name));
                const auto & aggregates = query_analyzer.aggregates();

                bool has_grouping = query_analyzer.group_by_kind != GroupByKind::ORDINARY;
                auto actual_header = Aggregator::Params::getHeader(header_before_aggregation, {}, keys, aggregates, /*final*/ true);
                actual_header = AggregatingStep::appendGroupingColumn(std::move(actual_header), has_grouping);

                Block expected_header;
                for (const auto & expected : query_analyzer.aggregated_columns)
                    expected_header.insert(ColumnWithTypeAndName(expected.type, expected.name));

                if (!blocksHaveEqualStructure(actual_header, expected_header))
                {
                    auto converting = ActionsDAG::makeConvertingActions(
                        actual_header.getColumnsWithTypeAndName(),
                        expected_header.getColumnsWithTypeAndName(),
                        ActionsDAG::MatchColumnsMode::Name,
                        true);

                    auto & step = chain.lastStep(query_analyzer.aggregated_columns);
                    auto & actions = step.actions();
                    actions = ActionsDAG::merge(std::move(*actions), std::move(*converting));
                }
            }

            if (query_analyzer.appendHaving(chain, only_types || !second_stage))
            {
                before_having = chain.getLastActions();
                chain.addStep();
            }
        }

        bool join_allow_read_in_order = true;
        if (hasJoin())
        {
            /// You may find it strange but we support read_in_order for HashJoin and do not support for MergeJoin.
            join_has_delayed_stream = query_analyzer.analyzedJoin().needStreamWithNonJoinedRows();
            join_allow_read_in_order = typeid_cast<HashJoin *>(join.get()) && !join_has_delayed_stream;
        }

        optimize_read_in_order = settings.optimize_read_in_order && storage && query.orderBy() && !query_analyzer.hasAggregation()
            && !query_analyzer.hasWindow() && !query.final() && join_allow_read_in_order;

        /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
        query_analyzer.appendSelect(chain, only_types || (need_aggregate ? !second_stage : !first_stage));

        // Window functions are processed in a separate expression chain after
        // the main SELECT, similar to what we do for aggregate functions.
        if (has_window)
        {
            query_analyzer.makeWindowDescriptions(chain.getLastActions());

            query_analyzer.appendWindowFunctionsArguments(chain, only_types || !first_stage);

            // Build a list of output columns of the window step.
            // 1) We need the columns that are the output of ExpressionActions.
            for (const auto & x : chain.getLastActions()->getNamesAndTypesList())
            {
                query_analyzer.columns_after_window.push_back(x);
            }
            // 2) We also have to manually add the output of the window function
            // to the list of the output columns of the window step, because the
            // window functions are not in the ExpressionActions.
            for (const auto & [_, w] : query_analyzer.window_descriptions)
            {
                for (const auto & f : w.window_functions)
                {
                    query_analyzer.columns_after_window.push_back({f.column_name, f.aggregate_function->getReturnType()});
                }
            }

            // Here we need to set order by expression as required output to avoid
            // their removal from the ActionsDAG.
            const auto * select_query = query_analyzer.getSelectQuery();
            if (select_query->orderBy())
            {
                // Set up a HashSet of dependent column names for matching sub expr of order key to their dependencies.
                // If there exists AggregateStep, all potential missing dependencies needed to be appended are included in
                // aggregated columns; Otherwise, all potential dependencies needed to be appended are included in source columns.
                std::unordered_set<DB::String> dep_columns;
                if (query_analyzer.hasAggregation())
                {
                    for (const auto & agg_col : query_analyzer.aggregated_columns)
                        dep_columns.emplace(agg_col.name);
                }
                else
                {
                    for (const auto & src_col : query_analyzer.sourceColumns())
                        dep_columns.emplace(src_col.name);
                }

                // Extract all leaf expr which refers to dependent columns from given orderBy key, and append
                // them into required_columns collection
                std::unordered_set<DB::String> required_columns;

                std::function<void(ASTPtr)> extract_dependent_from_order_key = [&](ASTPtr order_key)
                {
                    if (order_key->children.empty())
                    {
                        const auto & col = order_key->getColumnName();
                        if (dep_columns.find(col) != dep_columns.end())
                            required_columns.emplace(col);
                    }
                    else
                    {
                        for (const auto & ch : order_key->children)
                            extract_dependent_from_order_key(ch);
                    }
                };

                for (auto & child : select_query->orderBy()->children)
                {
                    auto * ast = child->as<ASTOrderByElement>();
                    ASTPtr order_expression = ast->children.at(0);
                    if (auto * function = order_expression->as<ASTFunction>();
                        function && (function->is_window_function || function->compute_after_window_functions))
                        continue;
                    // Collect all dependent columns referred by `order_expression`
                    extract_dependent_from_order_key(order_expression);
                }
                // Instead of adding all window irrelevant orderBy columns as required outputs, only adds used source
                // columns in case of `Unknown column` exception might occur during subsequent `finalize_chain`
                for (const auto & req_col : required_columns)
                    chain.getLastStep().addRequiredOutput(req_col);
            }

            before_window = chain.getLastActions();
            finalize_chain(chain);

            query_analyzer.appendExpressionsAfterWindowFunctions(chain, only_types || !first_stage);
            for (const auto & x : chain.getLastActions()->getNamesAndTypesList())
            {
                query_analyzer.columns_after_window.push_back(x);
            }

            auto & step = chain.lastStep(query_analyzer.columns_after_window);

            // The output of this expression chain is the result of
            // SELECT (before "final projection" i.e. renaming the columns), so
            // we have to mark the expressions that are required in the output,
            // again. We did it for the previous expression chain ("select w/o
            // window functions") earlier, in appendSelect(). But that chain also
            // produced the expressions required to calculate window functions.
            // They are not needed in the final SELECT result. Knowing the correct
            // list of columns is important when we apply SELECT DISTINCT later.
            for (const auto & child : select_query->select()->children)
            {
                step.addRequiredOutput(child->getColumnName());
            }
        }

        selected_columns.clear();
        selected_columns.reserve(chain.getLastStep().required_output.size());
        for (const auto & it : chain.getLastStep().required_output)
            selected_columns.emplace_back(it.first);

        has_order_by = query.orderBy() != nullptr;
        before_order_by = query_analyzer.appendOrderBy(
            chain, only_types || (need_aggregate ? !second_stage : !first_stage), optimize_read_in_order, order_by_elements_actions);

        if (query_analyzer.appendLimitBy(chain, only_types || !second_stage))
        {
            before_limit_by = chain.getLastActions();
            chain.addStep();
        }

        final_projection = query_analyzer.appendProjectResult(chain);

        finalize_chain(chain);
    }

    /// Before executing WHERE and HAVING, remove the extra columns from the block (mostly the aggregation keys).
    removeExtraColumns();

    checkActions();
}

void ExpressionAnalysisResult::finalize(const ExpressionActionsChain & chain, size_t where_step_num, const ASTSelectQuery & query)
{
    size_t next_step_i = 0;

    if (hasPrewhere())
    {
        const ExpressionActionsChain::Step & step = *chain.steps.at(next_step_i++);
        prewhere_info->prewhere_actions->projectInput(false);

        NameSet columns_to_remove;
        for (const auto & [name, can_remove] : step.required_output)
        {
            if (name == prewhere_info->prewhere_column_name)
                prewhere_info->remove_prewhere_column = can_remove;
            else if (can_remove)
                columns_to_remove.insert(name);
        }

        columns_to_remove_after_prewhere = std::move(columns_to_remove);
    }

    if (!atomic_predicates.empty())
    {
        size_t step_id = 0;
        NameSet columns_to_remove;
        for (int i = static_cast<int>(atomic_predicates.size()) - 1; i >= 0; --i, step_id += 2)
        {
            const ExpressionActionsChain::Step & step = *chain.steps.at(step_id);
            atomic_predicates[i]->predicate_actions->projectInput(false);

            for (const auto & [name, can_remove] : step.required_output)
            {
                if (name == atomic_predicates[i]->filter_column_name)
                    atomic_predicates[i]->remove_filter_column = can_remove;
                else if (can_remove)
                    columns_to_remove.insert(name);
            }
        }
        columns_to_remove_after_prewhere = std::move(columns_to_remove);
    }

    if (hasWhere())
    {
        where_column_name = query.where()->getColumnName();
        remove_where_filter = chain.steps.at(where_step_num)->required_output.find(where_column_name)->second;
    }
}

void ExpressionAnalysisResult::removeExtraColumns() const
{
    if (hasWhere())
        before_where->projectInput();
    if (hasHaving())
        before_having->projectInput();
}

void ExpressionAnalysisResult::checkActions() const
{
    /// Check that PREWHERE doesn't contain unusual actions. Unusual actions are that can change number of rows.
    if (hasPrewhere())
    {
        auto check_actions = [](const ActionsDAGPtr & actions) {
            if (actions)
                for (const auto & node : actions->getNodes())
                    if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
                        throw Exception("PREWHERE cannot contain ARRAY JOIN action", ErrorCodes::ILLEGAL_PREWHERE);
        };

        check_actions(prewhere_info->prewhere_actions);
        check_actions(prewhere_info->alias_actions);
    }
}

std::string ExpressionAnalysisResult::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "need_aggregate " << need_aggregate << "\n";
    ss << "has_order_by " << has_order_by << "\n";
    ss << "has_window " << has_window << "\n";

    if (before_array_join)
    {
        ss << "before_array_join " << before_array_join->dumpDAG() << "\n";
    }

    if (array_join)
    {
        ss << "array_join "
           << "FIXME doesn't have dump"
           << "\n";
    }

    if (before_join)
    {
        ss << "before_join " << before_join->dumpDAG() << "\n";
    }

    if (before_where)
    {
        ss << "before_where " << before_where->dumpDAG() << "\n";
    }

    if (prewhere_info)
    {
        ss << "prewhere_info " << prewhere_info->dump() << "\n";
    }

    if (filter_info)
    {
        ss << "filter_info " << filter_info->dump() << "\n";
    }

    if (before_aggregation)
    {
        ss << "before_aggregation " << before_aggregation->dumpDAG() << "\n";
    }

    if (before_having)
    {
        ss << "before_having " << before_having->dumpDAG() << "\n";
    }

    if (before_window)
    {
        ss << "before_window " << before_window->dumpDAG() << "\n";
    }

    if (before_order_by)
    {
        ss << "before_order_by " << before_order_by->dumpDAG() << "\n";
    }

    if (before_limit_by)
    {
        ss << "before_limit_by " << before_limit_by->dumpDAG() << "\n";
    }

    if (final_projection)
    {
        ss << "final_projection " << final_projection->dumpDAG() << "\n";
    }

    if (!selected_columns.empty())
    {
        ss << "selected_columns ";
        for (size_t i = 0; i < selected_columns.size(); i++)
        {
            if (i > 0)
            {
                ss << ", ";
            }
            ss << backQuote(selected_columns[i]);
        }
        ss << "\n";
    }

    return ss.str();
}

}
