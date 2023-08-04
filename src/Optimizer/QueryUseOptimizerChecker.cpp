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

#include <Optimizer/QueryUseOptimizerChecker.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Interpreters/SegmentScheduler.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/misc.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Parsers/ASTDumpInfoQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageView.h>
#include <common/logger_useful.h>
#include "Storages/Hive/StorageCnchHive.h"
#include "Storages/RemoteFile/IStorageCnchFile.h"
#include "Storages/StorageCnchMergeTree.h"
#include "Storages/StorageMaterializedView.h"
//#include <Common/TestLog.h>

namespace DB
{
static void changeDistributedStages(ASTPtr & node)
{
    if (!node)
        return;

    if (auto * select = node->as<ASTSelectQuery>())
    {
        const auto & settings_ptr = select->settings();
        if (!settings_ptr)
            return;
        auto & ast = settings_ptr->as<ASTSetQuery &>();
        for (auto & change : ast.changes)
        {
            if (change.name == "enable_distributed_stages")
            {
                change.value = Field(false);
                return;
            }
        }
    }
    else
    {
        for (auto & child : node->children)
            changeDistributedStages(child);
    }
}

void turnOffOptimizer(ContextMutablePtr context, ASTPtr & node)
{
    SettingsChanges setting_changes;

    setting_changes.emplace_back("enable_optimizer", false);

    context->applySettingsChanges(setting_changes);
    changeDistributedStages(node);
}

static bool checkDatabaseAndTable(String database_name, String table_name, ContextMutablePtr context, const NameSet & ctes)
{
    /// not with table
    if (database_name.empty() && ctes.contains(table_name))
        return true;

    /// If the database is not specified - use the current database.
    auto table_id = context->tryResolveStorageID(StorageID(database_name, table_name));
    auto storage_table = DatabaseCatalog::instance().tryGetTable(table_id, context);
    if (database_name.empty() && !storage_table)
        database_name = context->getCurrentDatabase();

    if (!storage_table)
        return false;

    if (database_name == "system")
        return true;

    if (dynamic_cast<const StorageView *>(storage_table.get()))
    {
        auto table_metadata_snapshot = storage_table->getInMemoryMetadataPtr();
        auto subquery = table_metadata_snapshot->getSelectQuery().inner_query;

        QueryUseOptimizerVisitor checker;
        QueryUseOptimizerContext check_context{.context = context};
        return ASTVisitorUtil::accept(subquery, checker, check_context);
    }

    return dynamic_cast<const MergeTreeMetaBase *>(storage_table.get()) || dynamic_cast<const StorageCnchHive *>(storage_table.get())
        || dynamic_cast<const StorageMaterializedView *>(storage_table.get())
        || dynamic_cast<const IStorageCnchFile *>(storage_table.get());
}

bool QueryUseOptimizerChecker::check(ASTPtr node, ContextMutablePtr context, bool insert_select_from_table)
{
    if (!node || (!context->getSettingsRef().enable_optimizer && !insert_select_from_table))
    {
        turnOffOptimizer(context, node);
        return false;
    }

    // Optimizer only work for Server.
    // for example INSERT INTO parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM parallel_replicas;
    // will execute query : INSERT INTO test.parallel_replicas_backup_4313395779120660490 (d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas )
    // will execute query : SELECT d, x, u, s FROM test.parallel_replicas_4313395779120660490
    // in worker.
    if (context->getApplicationType() != Context::ApplicationType::SERVER)
    {
        turnOffOptimizer(context, node);
        return false;
    }

    if (auto * explain = node->as<ASTExplainQuery>())
    {
        bool explain_plan = explain->getKind() == ASTExplainQuery::ExplainKind::OptimizerPlan
            || explain->getKind() == ASTExplainQuery::ExplainKind::QueryPlan
            || explain->getKind() == ASTExplainQuery::ExplainKind::QueryPipeline
            || explain->getKind() ==  ASTExplainQuery::AnalyzedSyntax
            || explain->getKind() ==  ASTExplainQuery::DistributedAnalyze
            || explain->getKind() ==  ASTExplainQuery::LogicalAnalyze
            || explain->getKind() ==  ASTExplainQuery::Distributed
            || explain->getKind() ==  ASTExplainQuery::TraceOptimizerRule
            || explain->getKind() ==  ASTExplainQuery::TraceOptimizer
            || explain->getKind() ==  ASTExplainQuery::Analysis;
        return explain_plan && check(explain->getExplainedQuery(), context);
    }

    if (auto * dump = node->as<ASTDumpInfoQuery>())
    {
        return check(dump->dump_query, context);
    }

    bool support = false;

    if (node->as<ASTSelectQuery>() || node->as<ASTSelectWithUnionQuery>() || node->as<ASTSelectIntersectExceptQuery>())
    {
        // disable system query, array join, table function, no merge tree table
        NameSet with_tables;

        QueryUseOptimizerVisitor checker;
        QueryUseOptimizerContext check_context{.context = context};
        try
        {
            support = ASTVisitorUtil::accept(node, checker, check_context);
        }
        catch (Exception &)
        {
            //            if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
            throw;
            //            support = false;
        }


        if (support && (context->getSettingsRef().enable_optimizer_white_list || insert_select_from_table))
        {
            QuerySupportOptimizerVisitor support_checker;
            try
            {
                support = ASTVisitorUtil::accept(node, support_checker, context);
            }
            catch (Exception &)
            {
                //            if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
                throw;
                //            support = false;
            }
        }
        if (!support)
            LOG_INFO(&Poco::Logger::get("QueryUseOptimizerChecker"), "query is unsupported for optimizer, reason: " + checker.getReason());
    }
    else if (node->as<ASTInsertQuery>())
    {
        support = true;
        auto * insert_query = node->as<ASTInsertQuery>();
        if (insert_query->in_file || insert_query->table_function || !insert_query->select)
            support = false;
        else
        {
            auto database = insert_query->table_id.database_name;
            if (database.empty())
                database = context->getCurrentDatabase();

            if (!checkDatabaseAndTable(database, insert_query->table_id.getTableName(), context, {}))
                support = false;
        }

        LOG_DEBUG(
            &Poco::Logger::get("QueryUseOptimizerChecker"),
            fmt::format("support: {}, check: {}", support, check(insert_query->select, context)));
        if (support)
            support = check(insert_query->select, context, true);
    }

    if (!support)
        turnOffOptimizer(context, node);

    return support;
}

bool QueryUseOptimizerVisitor::visitNode(ASTPtr & node, QueryUseOptimizerContext & context)
{
    for (auto & child : node->children)
    {
        if (!ASTVisitorUtil::accept(child, *this, context))
        {
            return false;
        }
    }
    return true;
}

static bool checkDatabaseAndTable(const ASTTableExpression & table_expression, const ContextMutablePtr & context, const NameSet & ctes)
{
    if (table_expression.database_and_table_name)
    {
        auto db_and_table = DatabaseAndTableWithAlias(table_expression.database_and_table_name);
        return checkDatabaseAndTable(db_and_table.database, db_and_table.table, context, ctes);
    }
    return true;
}

bool QueryUseOptimizerVisitor::visitASTSelectQuery(ASTPtr & node, QueryUseOptimizerContext & context)
{
    auto * select = node->as<ASTSelectQuery>();

    if (select->limit_with_ties)
    {
        reason = "LIMIT/OFFSET FETCH WITH TIES not implemented";
        return false;
    }

    if (select->group_by_with_totals)
    {
        reason = "group by with totals";
        return false;
    }

    QueryUseOptimizerContext child_context{.context = context.context, .ctes = context.ctes};
    collectWithTableNames(*select, child_context.ctes);

    for (const auto * table_expression : getTableExpressions(*select))
    {
        if (!checkDatabaseAndTable(*table_expression, child_context.context, child_context.ctes))
        {
            reason = "unsupported storage";
            return false;
        }
        if (table_expression->table_function)
        {
            reason = "table function";
            return false;
        }
    }

    return visitNode(node, child_context);
}

bool QueryUseOptimizerVisitor::visitASTTableJoin(ASTPtr & node, QueryUseOptimizerContext & context)
{
    return visitNode(node, context);
}

bool QueryUseOptimizerVisitor::visitASTIdentifier(ASTPtr & node, QueryUseOptimizerContext & context)
{
    bool support = !context.context->getExternalTables().contains(node->as<ASTIdentifier>()->name());
    if (!support)
        reason = "external table";
    return support;
}

bool QueryUseOptimizerVisitor::visitASTFunction(ASTPtr & node, QueryUseOptimizerContext & context)
{
    auto & fun = node->as<ASTFunction &>();
    if (fun.name == "untuple")
    {
        reason = "unsupported function";
        return false;
    }
    else if (functionIsInOrGlobalInOperator(fun.name) && fun.arguments->getChildren().size() == 2)
    {
        if (auto * identifier = fun.arguments->getChildren()[1]->as<ASTIdentifier>())
        {
            ASTTableExpression table_expression;
            table_expression.database_and_table_name = std::make_shared<ASTTableIdentifier>(identifier->name());
            if (!checkDatabaseAndTable(table_expression, context.context, context.ctes))
            {
                reason = "unsupported storage";
                return false;
            }
        }
    }
    return visitNode(node, context);
}

bool QueryUseOptimizerVisitor::visitASTQuantifiedComparison(ASTPtr & node, QueryUseOptimizerContext & context)
{
    return visitNode(node, context);
}

void QueryUseOptimizerVisitor::collectWithTableNames(ASTSelectQuery & query, NameSet & with_tables)
{
    if (auto with = query.with())
    {
        for (const auto & child : with->children)
            if (auto * with_elem = child->as<ASTWithElement>())
                with_tables.emplace(with_elem->name);
    }
}

bool QuerySupportOptimizerVisitor::visitNode(ASTPtr & node, ContextMutablePtr & context)
{
    for (auto & child : node->children)
    {
        if (ASTVisitorUtil::accept(child, *this, context))
        {
            return true;
        }
    }
    return false;
}

bool QuerySupportOptimizerVisitor::visitASTTableJoin(ASTPtr &, ContextMutablePtr &)
{
    return true;
}

bool QuerySupportOptimizerVisitor::visitASTSelectQuery(ASTPtr & node, ContextMutablePtr & context)
{
    auto * select = node->as<ASTSelectQuery>();

    if (select->groupBy())
        return true;

    return visitNode(node, context);
}

bool QuerySupportOptimizerVisitor::visitASTSelectIntersectExceptQuery(ASTPtr & node, ContextMutablePtr & context)
{
    auto & intersect_or_except = node->as<ASTSelectIntersectExceptQuery &>();
    switch (intersect_or_except.final_operator)
    {
        case ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL:
        case ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT:
        case ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL:
        case ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT:
            return true;
        default:
            break;
    }
    return visitNode(node, context);
}

bool QuerySupportOptimizerVisitor::visitASTSelectWithUnionQuery(ASTPtr & node, ContextMutablePtr & context)
{
    auto * select = node->as<ASTSelectWithUnionQuery>();

    switch (select->union_mode)
    {
        case ASTSelectWithUnionQuery::Mode::INTERSECT_ALL:
        case ASTSelectWithUnionQuery::Mode::INTERSECT_DISTINCT:
        case ASTSelectWithUnionQuery::Mode::EXCEPT_ALL:
        case ASTSelectWithUnionQuery::Mode::EXCEPT_DISTINCT:
            return true;
        case ASTSelectWithUnionQuery::Mode::UNION_ALL:
        case ASTSelectWithUnionQuery::Mode::UNION_DISTINCT:
        default:
            break;
    }
    return visitNode(node, context);
}


bool QuerySupportOptimizerVisitor::visitASTFunction(ASTPtr & node, ContextMutablePtr & context)
{
    auto & fun = node->as<ASTFunction &>();
    static const std::set<String> distinct_func{"uniqexact", "countdistinct"};
    if (distinct_func.contains(Poco::toLower(fun.name)))
    {
        return true;
    }
    if (fun.is_window_function && context->getSettingsRef().enable_optimizer_support_window)
    {
        return true;
    }
    return visitNode(node, context);
}

bool QuerySupportOptimizerVisitor::visitASTQuantifiedComparison(ASTPtr &, ContextMutablePtr &)
{
    return true;
}

// don't turn off optimizer if subquery expression exists
bool QuerySupportOptimizerVisitor::visitASTSubquery(ASTPtr &, ContextMutablePtr &)
{
    return true;
}

// skip subquery in FROM
bool QuerySupportOptimizerVisitor::visitASTTableExpression(ASTPtr & node, ContextMutablePtr & context)
{
    auto & table_expr = node->as<ASTTableExpression &>();
    if (table_expr.database_and_table_name)
    {
        /// If the database is not specified - use the current database.
        auto db_and_table = DatabaseAndTableWithAlias(table_expr.database_and_table_name);
        auto table_id = context->tryResolveStorageID(StorageID(db_and_table.database, db_and_table.table));
        auto storage_table = DatabaseCatalog::instance().tryGetTable(table_id, context);
        if (storage_table != nullptr && dynamic_cast<const StorageView *>(storage_table.get()))
        {
            auto table_metadata_snapshot = storage_table->getInMemoryMetadataPtr();
            auto subquery = table_metadata_snapshot->getSelectQuery().inner_query->clone();
            return ASTVisitorUtil::accept(subquery, *this, context);
        }
    }

    for (auto child : table_expr.children)
    {
        if (child == table_expr.subquery)
            child = table_expr.subquery->children.at(0);

        if (ASTVisitorUtil::accept(child, *this, context))
        {
            return true;
        }
    }
    return false;
}
}
