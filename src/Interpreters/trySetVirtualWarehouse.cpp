#include <Interpreters/trySetVirtualWarehouse.h>

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRefreshQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
// TODO(zuochuang.zema) MERGE storage
// #include <Storages/StorageCnchMergeTree.h>
// #include <Storages/StorageCnchHive.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageView.h>


namespace DB
{

/// Forward declaration
static bool trySetVirtualWarehouseFromAST(const ASTPtr & ast, Context & context);

static void setVirtualWarehouseByName(const String & vw_name, Context & context)
{
    auto vw_handle = context.getVirtualWarehousePool().get(vw_name);
    context.setCurrentVW(std::move(vw_handle));
}

[[maybe_unused]] static bool trySetVirtualWarehouseFromTable([[maybe_unused]] const String & database, [[maybe_unused]] const String & table, [[maybe_unused]] Context & context)
{
    // auto storage = context.tryGetTable(database, table);
    // if (!storage)
    //     return false;

    // if (auto cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
    // {
    //     String vw_name = cnch_table->settings.cnch_vw_default;

    //     setVirtualWarehouseByName(vw_name, context);
    //     LOG_DEBUG(
    //         &Poco::Logger::get("trySetVirtualWarehouse"),
    //         "Set virtual warehouse {} from {}", context.getCurrentVW()->getName(), storage->getStorageID().getNameForLogs());
    //     return true;
    // }
    // else if (auto cnchhive = dynamic_cast<StorageCnchHive *>(storage.get()))
    // {
    //     String vw_name = cnchhive->settings.cnch_vw_default;

    //     setVirtualWarehouseByName(vw_name, context);
    //     LOG_DEBUG(
    //         &Poco::Logger::get("trySetVirtualWarehouse"),
    //         "CnchHive Set virtual warehouse {} from {}", context.getCurrentVW()->getName(), storage->getStorageID().getNameForLogs());
    //     return true;
    // }
    // else if (auto view_table = dynamic_cast<StorageView *>(storage.get()))
    // {
    //     if (trySetVirtualWarehouseFromAST(view_table->getInnerQuery(), context))
    //         return true;
    // }
    // else if (auto mv_table = dynamic_cast<StorageMaterializedView *>(storage.get()))
    // {
    //     if (trySetVirtualWarehouseFromAST(mv_table->getInnerQuery(), context))
    //         return true;
    // }

    return false;
}

static bool trySetVirtualWarehouseFromAST([[maybe_unused]] const ASTPtr & ast, [[maybe_unused]] Context & context)
{
    // do
    // {
    //     if (auto * insert = ast->as<ASTInsertQuery>())
    //     {
    //         auto storage = context.tryGetTable(insert->database, insert->table);
    //         auto cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get());
    //         if (!cnch_table)
    //             break;

    //         String vw_name = cnch_table->settings.cnch_vw_write;

    //         setVirtualWarehouseByName(vw_name, context);
    //         LOG_DEBUG(
    //             &Poco::Logger::get("trySetVirtualWarehouse"),
    //             "Set virtual warehouse {} from {}", context.getCurrentVW()->getName(), storage->getStorageID().getNameForLogs());
    //         return true;
    //     }
    //     else if (auto * table_expr = ast->as<ASTTableExpression>())
    //     {
    //         if (!table_expr->database_and_table_name)
    //             break;

    //         DatabaseAndTableWithAlias db_and_table(table_expr->database_and_table_name);
    //         if (trySetVirtualWarehouseFromTable(db_and_table.database, db_and_table.table, context))
    //             return true;
    //     }
    //     /// XXX: This is a hack solution for an uncommonn query `SELECT ... WHERE x IN table`
    //     /// which may cause some rare bugs if the identifiers confict with table names.
    //     /// But I don't want to make the problem complex ..
    //     else if (auto * func_expr = ast->as<ASTFunction>())
    //     {
    //         if (!func_expr->arguments || func_expr->arguments->children.size() != 2)
    //             break;
    //         if (func_expr->name != "in" && func_expr->name != "notIn")
    //             break;

    //         auto * id = func_expr->arguments->children.back()->as<ASTIdentifier>();
    //         if (!id || id->getNameParts().size() > 2)
    //             break;

    //         String database = id->getNameParts().empty() ? "" : id->getNameParts().front();
    //         String table = id->getNameParts().empty() ? id->name : id->getNameParts().back();
    //         if (trySetVirtualWarehouseFromTable(database, table, context))
    //             return true;
    //     }
    //     else if (auto * refresh_mv = ast->as<ASTRefreshQuery>())
    //     {
    //         auto storage = context.tryGetTable(refresh_mv->database, refresh_mv->table);
    //         auto view_table = dynamic_cast<StorageMaterializedView *>(storage.get());
    //         if (!view_table)
    //             break;

    //         if (trySetVirtualWarehouseFromTable(refresh_mv->database, refresh_mv->table, context))
    //            return true;
    //     }

    // } while (false);

    // for (auto & child : ast->children)
    // {
    //     if (trySetVirtualWarehouseFromAST(child, context))
    //         return true;
    // }

    return false;
}

bool trySetVirtualWarehouse(const ASTPtr & ast, Context & context)
{
    if (context.tryGetCurrentVW())
        return true;

    LOG_DEBUG(&Poco::Logger::get("trySetVirtualWarehouse"), "Trying to set virtual warehouse...");

    if (const auto & vw_name = context.getSettingsRef().virtual_warehouse.value; !vw_name.empty())
    {
        setVirtualWarehouseByName(vw_name, context);
        LOG_DEBUG(
            &Poco::Logger::get("trySetVirtualWarehouse"),
            "Set virtual warehouse {} from query settings", context.getCurrentVW()->getName());
        return true;
    }
    else
    {
        if (trySetVirtualWarehouseFromAST(ast, context))
            return true;

        return false;
    }
}

bool trySetVirtualWarehouseAndWorkerGroup(const ASTPtr & ast, Context & context)
{
    if (context.tryGetCurrentWorkerGroup())
        return true;

    if (trySetVirtualWarehouse(ast, context))
    {
        auto value = context.getSettingsRef().vw_schedule_algo.value;
        auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
        auto worker_group = context.getCurrentVW()->pickWorkerGroup(algo);
        LOG_DEBUG(&Poco::Logger::get("trySetVirtualWarehouse"), "Picked worker group {}", worker_group->getQualifiedName());

        context.setCurrentWorkerGroup(std::move(worker_group));
        return true;
    }
    else
    {
        return false;
    }
}

VirtualWarehouseHandle getVirtualWarehouseForTable([[maybe_unused]] const MergeTreeMetaBase & storage, [[maybe_unused]] const Context & context)
{
    // String vw_name;
    // String source;

    // if (const auto & name = context.getSettingsRef().virtual_warehouse.value; !name.empty())
    // {
    //     vw_name = name;
    //     source = "query settings";
    // }
    // else
    // {
    //     vw_name = storage.settings.cnch_vw_default;
    //     source = "table settings of " + storage.getStorageID().getNameForLogs();
    // }

    // auto vw = context.getVirtualWarehousePool().get(vw_name);
    // LOG_DEBUG(&Poco::Logger::get("trySetVirtualWarehouse"), "Get virtual warehouse {} from {}", vw->getName(), source);

    // return vw;
    return nullptr;
}

WorkerGroupHandle getWorkerGroupForTable(const MergeTreeMetaBase & storage, const Context & context)
{
    auto vw = getVirtualWarehouseForTable(storage, context);
    auto value = context.getSettingsRef().vw_schedule_algo.value;
    auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
    auto worker_group = vw->pickWorkerGroup(algo);

    LOG_DEBUG(&Poco::Logger::get("trySetVirtualWarehouse"), "Picked worker group {}", worker_group->getQualifiedName());
    return worker_group;
}

}
