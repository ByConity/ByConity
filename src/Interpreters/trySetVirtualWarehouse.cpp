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

#include <memory>
#include <Interpreters/trySetVirtualWarehouse.h>

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Optimizer/QueryUseOptimizerChecker.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/RemoteFile/IStorageCnchFile.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageView.h>
#include <unicode/tzfmt.h>
#include "Interpreters/DatabaseCatalog.h"
#include "Interpreters/StorageID.h"
#include "Storages/StorageCnchMergeTree.h"


namespace DB
{
/// Forward declaration
static bool trySetVirtualWarehouseFromAST(const ASTPtr & ast, ContextMutablePtr & context);
static String tryGetVirtualWarehouseNameFromAST(const ASTPtr & ast, ContextMutablePtr & context);

static void setVirtualWarehouseByName(const String & vw_name, ContextMutablePtr & context)
{
    auto vw_handle = context->getVirtualWarehousePool().get(vw_name);
    context->setCurrentVW(std::move(vw_handle));
}

static bool trySetVirtualWarehouseFromTable(
    const String & database,
    const String & table,
    ContextMutablePtr & context,
    VirtualWarehouseType vw_type = VirtualWarehouseType::Default)
{
    auto & database_catalog = DatabaseCatalog::instance();
    StorageID table_id(database, table);
    auto storage = database_catalog.tryGetTable(table_id, context);
    if (!storage)
        return false;

    if (auto cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
    {
        String vw_name = vw_type == VirtualWarehouseType::Write ? cnch_table->getSettings()->cnch_vw_write
                                                                : cnch_table->getSettings()->cnch_vw_default;

        LOG_DEBUG(
            &Poco::Logger::get("trySetVirtualWarehouse"),
            "try get warehouse from {}, type is WRITE {}",
            vw_name,
            VirtualWarehouseType::Write == vw_type);
        setVirtualWarehouseByName(vw_name, context);
        return true;
    }
    else if (auto vw_name = storage->getVirtualWarehouseName(vw_type); vw_name)
    {
        /// I hate dynamic_cast
        setVirtualWarehouseByName(*vw_name, context);
        return true;
    }
    else if (auto * cnchfile = dynamic_cast<IStorageCnchFile *>(storage.get()))
    {
        String vw_name = vw_type == VirtualWarehouseType::Write ? cnchfile->settings.cnch_vw_write : cnchfile->settings.cnch_vw_default;

        setVirtualWarehouseByName(vw_name, context);
        LOG_DEBUG(
            &Poco::Logger::get("VirtualWarehouse"),
            "CnchHDFS/CnchS3 Set virtual warehouse {} from {}",
            context->getCurrentVW()->getName(),
            storage->getStorageID().getNameForLogs());
        return true;
    }
    else if (auto * view_table = dynamic_cast<StorageView *>(storage.get()))
    {
        if (trySetVirtualWarehouseFromAST(view_table->getInnerQuery(), context))
            return true;
    }
    else if (auto * mv_table = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        if (trySetVirtualWarehouseFromAST(mv_table->getInnerQuery(), context))
            return true;
    }

    return false;
}

static bool trySetVirtualWarehouseFromAST(const ASTPtr & ast, ContextMutablePtr & context)
{
    do
    {
        auto & database_catalog = DatabaseCatalog::instance();
        if (auto * delete_query = ast->as<ASTDeleteQuery>())
        {
            auto database = delete_query->database.empty() ? context->getCurrentDatabase() : delete_query->database;
            if (trySetVirtualWarehouseFromTable(database, delete_query->table, context, VirtualWarehouseType::Write))
                return true;
        }
        else if (auto * update = ast->as<ASTUpdateQuery>())
        {
            auto database = update->database.empty() ? context->getCurrentDatabase() : update->database;
            if (trySetVirtualWarehouseFromTable(database, update->table, context, VirtualWarehouseType::Write))
                return true;
        }
        else if (auto * insert = ast->as<ASTInsertQuery>())
        {
            //this means the query is function insert, for example `insert into function CnchS3(...)
            if (insert->table_id.empty())
                break;

            auto table_id = insert->table_id;
            if (table_id.database_name.empty())
                table_id.database_name = context->getCurrentDatabase();
            if (QueryUseOptimizerChecker::check(ast, context)
                && trySetVirtualWarehouseFromTable(table_id.database_name, table_id.table_name, context, VirtualWarehouseType::Read))
                return true;
<<<<<<< HEAD
=======

>>>>>>> 2b998bba116 (Merge branch 'cnch-20-session-context' into 'cnch-ce-merge')
            if (trySetVirtualWarehouseFromTable(table_id.database_name, table_id.table_name, context, VirtualWarehouseType::Write))
                return true;
        }
        else if (auto * table_expr = ast->as<ASTTableExpression>())
        {
            if (!table_expr->database_and_table_name)
                break;

            DatabaseAndTableWithAlias db_and_table(table_expr->database_and_table_name);
            if (db_and_table.database.empty())
                db_and_table.database = context->getCurrentDatabase();
            if (trySetVirtualWarehouseFromTable(db_and_table.database, db_and_table.table, context))
                return true;
        }
        /// XXX: This is a hack solution for an uncommonn query `SELECT ... WHERE x IN table`
        /// which may cause some rare bugs if the identifiers confict with table names.
        /// But I don't want to make the problem complex ..
        else if (auto * func_expr = ast->as<ASTFunction>())
        {
            if (!func_expr->arguments || func_expr->arguments->children.size() != 2)
                break;
            if (func_expr->name != "in" && func_expr->name != "notIn")
                break;

            auto * id = func_expr->arguments->children.back()->as<ASTIdentifier>();
            if (!id || id->nameParts().size() > 2)
                break;

            String database = id->nameParts().empty() ? "" : id->nameParts().front();
            String table = id->nameParts().empty() ? id->name() : id->nameParts().back();
            if (trySetVirtualWarehouseFromTable(database, table, context))
                return true;
        }
        else if (auto * refresh_mv = ast->as<ASTRefreshQuery>())
        {
            auto storage = database_catalog.tryGetTable(StorageID(refresh_mv->database, refresh_mv->table), context);
            auto * view_table = dynamic_cast<StorageMaterializedView *>(storage.get());
            if (!view_table)
                break;

            if (trySetVirtualWarehouseFromTable(refresh_mv->database, refresh_mv->table, context))
                return true;
        }

    } while (false);

    for (auto & child : ast->children)
    {
        if (trySetVirtualWarehouseFromAST(child, context))
            return true;
    }

    return false;
}

static String tryGetVirtualWarehouseNameFromTable(
    const String & database,
    const String & table,
    ContextMutablePtr & context,
    VirtualWarehouseType vw_type = VirtualWarehouseType::Default)
{
    auto & database_catalog = DatabaseCatalog::instance();
    StorageID table_id(database, table);
    auto storage = database_catalog.tryGetTable(table_id, context);
    if (!storage)
        return EMPTY_VIRTUAL_WAREHOUSE_NAME;

    if (auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
    {
        String vw_name = vw_type == VirtualWarehouseType::Write ? cnch_table->getSettings()->cnch_vw_write
                                                                : cnch_table->getSettings()->cnch_vw_default;

        return vw_name;
    }
    else if (auto vw_name = storage->getVirtualWarehouseName(vw_type); vw_name)
    {
        return *vw_name;
    }
    else if (auto * cnhfile = dynamic_cast<IStorageCnchFile *>(storage.get()))
    {
        String vw_name = vw_type == VirtualWarehouseType::Write ? cnhfile->settings.cnch_vw_write : cnhfile->settings.cnch_vw_default;

        return vw_name;
    }
    else if (auto * view_table = dynamic_cast<StorageView *>(storage.get()))
    {
        return tryGetVirtualWarehouseNameFromAST(view_table->getInnerQuery(), context);
    }
    else if (auto * mv_table = dynamic_cast<StorageMaterializedView *>(storage.get()))
    {
        return tryGetVirtualWarehouseNameFromAST(mv_table->getInnerQuery(), context);
    }

    return EMPTY_VIRTUAL_WAREHOUSE_NAME;
}

static String tryGetVirtualWarehouseNameFromAST(const ASTPtr & ast, ContextMutablePtr & context)
{
    do
    {
        auto & database_catalog = DatabaseCatalog::instance();
        if (auto * delete_query = ast->as<ASTDeleteQuery>())
        {
            auto database = delete_query->database.empty() ? context->getCurrentDatabase() : delete_query->database;
            return tryGetVirtualWarehouseNameFromTable(database, delete_query->table, context, VirtualWarehouseType::Write);
        }
        else if (auto * update = ast->as<ASTUpdateQuery>())
        {
            auto database = update->database.empty() ? context->getCurrentDatabase() : update->database;
            return tryGetVirtualWarehouseNameFromTable(database, update->table, context, VirtualWarehouseType::Write);
        }
        else if (auto * insert = ast->as<ASTInsertQuery>())
        {
            //this means the query is function insert, for example `insert into function CnchS3(...)
            if (insert->table_id.empty())
                break;
            auto table_id = insert->table_id;
            if (table_id.database_name.empty())
                table_id.database_name = context->getCurrentDatabase();
            if (QueryUseOptimizerChecker::check(ast, context))
                return tryGetVirtualWarehouseNameFromTable(
                    table_id.database_name, table_id.table_name, context, VirtualWarehouseType::Read);
            return tryGetVirtualWarehouseNameFromTable(table_id.database_name, table_id.table_name, context, VirtualWarehouseType::Write);
        }
        else if (auto * table_expr = ast->as<ASTTableExpression>())
        {
            if (!table_expr->database_and_table_name)
                break;

            DatabaseAndTableWithAlias db_and_table(table_expr->database_and_table_name);
            if (db_and_table.database.empty())
                db_and_table.database = context->getCurrentDatabase();
            return tryGetVirtualWarehouseNameFromTable(db_and_table.database, db_and_table.table, context);
        }
        /// XXX: This is a hack solution for an uncommonn query `SELECT ... WHERE x IN table`
        /// which may cause some rare bugs if the identifiers confict with table names.
        /// But I don't want to make the problem complex ..
        else if (auto * func_expr = ast->as<ASTFunction>())
        {
            if (!func_expr->arguments || func_expr->arguments->children.size() != 2)
                break;
            if (func_expr->name != "in" && func_expr->name != "notIn")
                break;

            auto * id = func_expr->arguments->children.back()->as<ASTIdentifier>();
            if (!id || id->nameParts().size() > 2)
                break;

            String database = id->nameParts().empty() ? "" : id->nameParts().front();
            String table = id->nameParts().empty() ? id->name() : id->nameParts().back();
            return tryGetVirtualWarehouseNameFromTable(database, table, context);
        }
        else if (auto * refresh_mv = ast->as<ASTRefreshQuery>())
        {
            auto storage = database_catalog.tryGetTable(StorageID(refresh_mv->database, refresh_mv->table), context);
            auto * view_table = dynamic_cast<StorageMaterializedView *>(storage.get());
            if (!view_table)
                break;

            return tryGetVirtualWarehouseNameFromTable(refresh_mv->database, refresh_mv->table, context);
        }

    } while (false);

    for (auto & child : ast->children)
    {
        if (const auto & vw_name = tryGetVirtualWarehouseNameFromAST(child, context); vw_name != EMPTY_VIRTUAL_WAREHOUSE_NAME)
            return vw_name;
    }

    return EMPTY_VIRTUAL_WAREHOUSE_NAME;
}

static String getVirtualWarehouseNameFromASTSetQuery(ASTSetQuery & query_set_ast)
{
    for (auto & setting : query_set_ast.changes)
    {
        if (setting.name == "virtual_warehouse")
            return setting.value.get<String>();
    }
    return EMPTY_VIRTUAL_WAREHOUSE_NAME;
}

static String getVirtualWarehouseNameFromASTSelectWithUnionQuery(const ASTSelectWithUnionQuery & select_with_union)
{
    String vw_name = EMPTY_VIRTUAL_WAREHOUSE_NAME;
    if (select_with_union.settings_ast)
    {
        auto query_set_ast = select_with_union.settings_ast->as<ASTSetQuery &>();
        vw_name = getVirtualWarehouseNameFromASTSetQuery(query_set_ast);
    }

    const ASTs & children = select_with_union.list_of_selects->children;
    if (!children.empty())
    {
        const auto * last_select = children.back()->as<ASTSelectQuery>();
        if (last_select && last_select->settings())
        {
            auto query_set_ast = last_select->settings()->as<ASTSetQuery &>();
            vw_name = getVirtualWarehouseNameFromASTSetQuery(query_set_ast);
        }
    }
    return vw_name;
}

static String getVirtualWarehouseNameFromQuerySetExpression(const ASTPtr & ast)
{
    ASTSetQuery query_set_ast;
    if (auto * select_query = ast->as<ASTSelectQuery>())
    {
        if (select_query->settings())
            query_set_ast = select_query->settings()->as<ASTSetQuery &>();
    }
    else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        return getVirtualWarehouseNameFromASTSelectWithUnionQuery(*select_with_union_query);
    }
    else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
    {
        if (query_with_output->settings_ast)
            query_set_ast = query_with_output->settings_ast->as<ASTSetQuery &>();
    }

    auto * insert_query = ast->as<ASTInsertQuery>();

    if (insert_query && insert_query->settings_ast)
        query_set_ast = insert_query->settings_ast->as<ASTSetQuery &>();

    return getVirtualWarehouseNameFromASTSetQuery(query_set_ast);
}

String tryGetVirtualWarehouseName(const ASTPtr & ast, ContextMutablePtr & context)
{
    //1. from query setting
    //2. from xml setting
    //3. from table setting
    if (context->tryGetCurrentVW())
        return context->getCurrentVW()->getName();
    if (const auto & vw_name_from_set_expression = getVirtualWarehouseNameFromQuerySetExpression(ast);
        vw_name_from_set_expression != EMPTY_VIRTUAL_WAREHOUSE_NAME)
        return vw_name_from_set_expression;
    else if (const auto & vw_name = context->getSettingsRef().virtual_warehouse.value; !vw_name.empty())
        return vw_name;
    else
        return tryGetVirtualWarehouseNameFromAST(ast, context);
}

bool trySetVirtualWarehouse(const ASTPtr & ast, ContextMutablePtr & context)
{
    if (context->tryGetCurrentVW())
        return true;

    if (const auto & vw_name = context->getSettingsRef().virtual_warehouse.value; !vw_name.empty())
    {
        setVirtualWarehouseByName(vw_name, context);
        return true;
    }
    else
    {
        return trySetVirtualWarehouseFromAST(ast, context);
    }
}

bool trySetVirtualWarehouseAndWorkerGroup(const ASTPtr & ast, ContextMutablePtr & context)
{
    if (context->tryGetCurrentWorkerGroup())
        return true;

    if (trySetVirtualWarehouse(ast, context))
    {
        auto value = context->getSettingsRef().vw_schedule_algo.value;
        auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
        auto worker_group = context->getCurrentVW()->pickWorkerGroup(algo);

        context->setCurrentWorkerGroup(std::move(worker_group));
        return true;
    }
    else
    {
        return false;
    }
}

VirtualWarehouseHandle getVirtualWarehouseForTable(const MergeTreeMetaBase & storage, const ContextPtr & context)
{
    String vw_name;
    String source;

    if (const auto & name = context->getSettingsRef().virtual_warehouse.value; !name.empty())
    {
        vw_name = name;
        source = "query settings";
    }
    else
    {
        vw_name = storage.getSettings()->cnch_vw_default;
        source = "table settings of " + storage.getStorageID().getNameForLogs();
    }

    auto vw = context->getVirtualWarehousePool().get(vw_name);

    return vw;
}

WorkerGroupHandle getWorkerGroupForTable(const MergeTreeMetaBase & storage, const ContextPtr & context)
{
    auto vw = getVirtualWarehouseForTable(storage, context);
    auto value = context->getSettingsRef().vw_schedule_algo.value;
    auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
    auto worker_group = vw->pickWorkerGroup(algo);

    return worker_group;
}

WorkerGroupHandle getWorkerGroupForTable(ContextPtr local_context, StoragePtr storage)
{
    if (auto wg = local_context->tryGetCurrentWorkerGroup(); wg)
        return wg;

    String vw_name;
    if (const auto & name = local_context->getSettingsRef().virtual_warehouse.value; !name.empty())
    {
        vw_name = name;
    }
    else if (auto opt_vw_name = storage->getVirtualWarehouseName(VirtualWarehouseType::Default); opt_vw_name)
    {
        vw_name = *opt_vw_name;
    }
    else
    {
        throw Exception("Storage does not support virtual warehouse", ErrorCodes::BAD_ARGUMENTS);
    }

    auto vw = local_context->getVirtualWarehousePool().get(vw_name);
    auto value = local_context->getSettingsRef().vw_schedule_algo.value;
    auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
    auto worker_group = vw->pickWorkerGroup(algo);

    local_context->setCurrentWorkerGroup(worker_group);
    return worker_group;
}
}
