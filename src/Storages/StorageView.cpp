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

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/MapHelpers.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>

#include <Processors/Pipe.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPlan/ExpressionStep.h>
#include <QueryPlan/SettingQuotaAndLimitsStep.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace
{

bool isNullableOrLcNullable(DataTypePtr type)
{
    if (type->isNullable())
        return true;

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return lc_type->getDictionaryType()->isNullable();

    return false;
}

/// Returns `true` if there are nullable column in src but corresponding column in dst is not
bool changedNullabilityOneWay(const Block & src_block, const Block & dst_block)
{
    std::unordered_map<String, bool> src_nullable;
    for (const auto & col : src_block)
        src_nullable[col.name] = isNullableOrLcNullable(col.type);

    for (const auto & col : dst_block)
    {
        if (!isNullableOrLcNullable(col.type) && src_nullable[col.name])
            return true;
    }
    return false;
}

bool hasJoin(const ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    return joined_table.table_join != nullptr;
}

bool hasJoin(const ASTSelectWithUnionQuery & ast)
{
    for (const auto & child : ast.list_of_selects->children)
    {
        if (const auto * select = child->as<ASTSelectQuery>(); select && hasJoin(*select))
            return true;
    }
    return false;
}

}

StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);
    SelectQueryDescription description;

    description.inner_query = query.select->ptr();
    storage_metadata.setSelectQuery(description);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageView::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context));
}

void StorageView::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const unsigned /*num_streams*/)
{
    ASTPtr current_inner_query = storage_snapshot->metadata->getSelectQuery().inner_query;

    if (query_info.view_query)
    {
        if (!query_info.view_query->as<ASTSelectWithUnionQuery>())
            throw Exception("Unexpected optimized VIEW query", ErrorCodes::LOGICAL_ERROR);
        current_inner_query = query_info.view_query->clone();
    }

    appendColumns(current_inner_query, column_names);

    InterpreterSelectWithUnionQuery interpreter(current_inner_query, context, {}, column_names);
    interpreter.buildQueryPlan(query_plan);

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    auto materializing_actions = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    materializing_actions->addMaterializingOutputActions();

    auto materializing = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(materializing_actions));
    materializing->setStepDescription("Materialize constants after VIEW subquery");
    query_plan.addStep(std::move(materializing));

    /// And also convert to expected structure.
    const auto & expected_header = storage_snapshot->getSampleBlockForColumns(column_names);
    const auto & header = query_plan.getCurrentDataStream().header;

    const auto * select_with_union = current_inner_query->as<ASTSelectWithUnionQuery>();
    if (select_with_union && hasJoin(*select_with_union) && changedNullabilityOneWay(header, expected_header))
    {
        throw DB::Exception(ErrorCodes::INCORRECT_QUERY,
                            "Query from view {} returned Nullable column having not Nullable type in structure. "
                            "If query from view has JOIN, it may be caused by different values of 'join_use_nulls' setting. "
                            "You may explicitly specify 'join_use_nulls' in 'CREATE VIEW' query to avoid this error",
                            getStorageID().getFullTableName());
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            header.getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
    converting->setStepDescription("Convert VIEW subquery result to VIEW table structure");
    query_plan.addStep(std::move(converting));
}

/**
 * append column to select list so that we can pruning unnecessary columns from query in SyntaxAnalyzer.
 * if there are map column, we append map-key column to select list to pruning map column.
 */
void StorageView::appendColumns(ASTPtr & query, const Names & column_names)
{
    if (auto * select_with_union = query->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union->list_of_selects->children)
            appendColumns(child, column_names);
    }
    else if (auto * select = query->as<ASTSelectQuery>())
    {
        if (select->select() && select->tables())
        {
            auto * select_list = select->refSelect()->as<ASTExpressionList>();
            if (!select_list)
                return;

            NameSet visited_names;
            for (auto & column : select_list->children)
            {
                if (auto * column_with_alias = dynamic_cast<ASTWithAlias *>(column.get()))
                {
                    visited_names.insert(column_with_alias->getAliasOrColumnName());
                }
            }

            for (const auto & name : column_names)
            {
                /**
                 * only support map column right now
                 */
                if (!visited_names.count(name) && isMapImplicitKey(name))
                {
                    select_list->children.push_back(std::make_shared<ASTIdentifier>(name));
                }
            }
        }
    }
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    if (!select_query.tables() || select_query.tables()->children.empty())
        throw Exception("Logical error: no table expression in view select AST", ErrorCodes::LOGICAL_ERROR);

    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    return select_element->table_expression->as<ASTTableExpression>();
}

void StorageView::replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->database_and_table_name)
    {
        // If it's a view table function, add a fake db.table name.
        if (table_expression->table_function)
        {
            auto table_function_name = table_expression->table_function->as<ASTFunction>()->name;
            if ((table_function_name == "view") || (table_function_name == "viewIfPermitted"))
                table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>("__view");
        }
        if (!table_expression->database_and_table_name)
            throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);
    }

    DatabaseAndTableWithAlias db_table(table_expression->database_and_table_name);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    view_name = table_expression->database_and_table_name;
    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->children.push_back(view_query);
    table_expression->subquery->setAlias(alias);

    for (auto & child : table_expression->children)
        if (child.get() == view_name.get())
            child = view_query;
}

String StorageView::replaceValueWithQueryParameter(const String & column_name, const NameToNameMap & parameter_values)
{
    String name = column_name;
    std::string::size_type pos = 0u;
    for (const auto & parameter : parameter_values)
    {
        if ((pos = name.find("_CAST(" + parameter.second)) != std::string::npos)
        {
            name = name.substr(0,pos) + parameter.first + ")";
            break;
        }
    }
    return name;
}

ASTPtr StorageView::restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(select_query);

    if (!table_expression->subquery)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->database_and_table_name = view_name;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = view_name;
    return subquery->children[0];
}

void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return StorageView::create(args.table_id, args.query, args.columns, args.comment);
    });
}

}
