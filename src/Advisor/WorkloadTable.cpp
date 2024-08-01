#include <Advisor/WorkloadTable.h>

#include <Advisor/WorkloadTableStats.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTClusterByElement.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageCnchMergeTree.h>

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
}

WorkloadTablePtr WorkloadTables::getTable(const QualifiedTableName & table)
{
    auto it = tables.find(table);
    if (it != tables.end())
        return it->second;

    auto database = DatabaseCatalog::instance().getDatabase(table.database, context);
    if (!database)
        throw Exception("Database " + table.database + " does not exist", ErrorCodes::UNKNOWN_DATABASE);

    ASTPtr create_ddl = database->tryGetCreateTableQuery(table.table, context);
    if (create_ddl)
    {
        auto & ast_create_query = create_ddl->as<ASTCreateQuery &>();
        ast_create_query.uuid = UUIDHelpers::Nil;
    }
    StorageID storage_id{table.database, table.table};
    StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context);
    auto storage_merge_tree = dynamic_pointer_cast<const StorageCnchMergeTree>(storage);
    if (!storage_merge_tree)
        throw Exception("Table " + table.table + " is not CnchMergeTree table", ErrorCodes::UNKNOWN_TABLE);

    // get stats
    WorkloadTableStats stats = WorkloadTableStats::build(context, table.database, table.table);
    //    auto columns = storage->getInMemoryMetadataPtr()->getColumns().getAll();
    //    stats.collectExtendedStats(context, table.database, table.table, columns);
    auto workload_table = std::make_shared<WorkloadTable>(storage, create_ddl, stats);
    tables.emplace(table, workload_table);
    return workload_table;
}

WorkloadTablePtr WorkloadTables::tryGetTable(const QualifiedTableName & table)
{
    try
    {
        WorkloadTablePtr res = getTable(table);
        return res;
    }
    catch (...)
    {
        return nullptr;
    }
}


void WorkloadTables::loadTablesFromDatabase(const String & database_name)
{
    auto database = DatabaseCatalog::instance().getDatabase(database_name, context);
    for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        const auto & table = iterator->table();
        if (!table || !dynamic_pointer_cast<const StorageCnchMergeTree>(table))
            continue;
        getTable(QualifiedTableName{database_name, table->getStorageID().table_name});
    }
}

std::vector<std::pair<QualifiedTableName, String>> WorkloadTables::getOptimizedDDLs() const
{
    std::vector<std::pair<QualifiedTableName, String>> res;
    for (const auto & table : tables)
    {
//        if (!table.second->isOptimized())
//            continue;
        res.push_back(std::make_pair(table.first, serializeAST(*table.second->getDDL())));
    }
    return res;
}

ASTPtr WorkloadTable::getOrderBy()
{
    ASTCreateQuery * create = create_table_ddl->as<ASTCreateQuery>();
    if (auto * order_by = create->storage->order_by)
        return order_by->shared_from_this();
    return nullptr;
}

ASTPtr WorkloadTable::getClusterByKey()
{
    ASTCreateQuery * create = create_table_ddl->as<ASTCreateQuery>();
    if (auto * cluster_by = dynamic_cast<ASTClusterByElement *>(create->storage->cluster_by);
        cluster_by && !cluster_by->getChildren().empty())
        return cluster_by->getChildren().front();
    return nullptr;
}

void WorkloadTable::updateOrderBy(const ASTPtr & order_by)
{
    ASTCreateQuery * create = create_table_ddl->as<ASTCreateQuery>();
    create->storage->set(create->storage->order_by, order_by);
    optimized = true;
}

void WorkloadTable::updateClusterByKey(const ASTPtr & cluster_by_key)
{
    ASTCreateQuery * create = create_table_ddl->as<ASTCreateQuery>();
    auto * cluster_by = create->storage->cluster_by;
    cluster_by->getChildren().front() = cluster_by_key;
    optimized = true;
}

} // namespace DB
