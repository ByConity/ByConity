#pragma once

#include <Advisor/WorkloadTableStats.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class WorkloadTable;
using WorkloadTablePtr = std::shared_ptr<WorkloadTable>;

class WorkloadTables
{
public:
    explicit WorkloadTables(ContextMutablePtr context_) : context(context_) { }

    WorkloadTablePtr getTable(const QualifiedTableName & table);
    WorkloadTablePtr tryGetTable(const QualifiedTableName & table);
    void loadTablesFromDatabase(const String & database);
    std::unordered_map<QualifiedTableName, WorkloadTablePtr> & getTables() { return tables; }
    std::vector<std::pair<QualifiedTableName, String>> getOptimizedDDLs() const;

private:
    ContextMutablePtr context;
    std::unordered_map<QualifiedTableName, WorkloadTablePtr> tables;
};

class WorkloadTable
{
public:
    explicit WorkloadTable(
        const StoragePtr & storage_,
        ASTPtr create_table_ddl_,
        WorkloadTableStats stats_)
        : storage(storage_)
        , create_table_ddl(create_table_ddl_)
        , stats(stats_)
    {
    }

    StoragePtr getTablePtr() const { return storage; }

    ASTPtr getDDL() const { return create_table_ddl; }
    bool isOptimized() const { return optimized; }

    ASTPtr getOrderBy();
    void updateOrderBy(const ASTPtr & order_by);

    ASTPtr getClusterByKey();
    void updateClusterByKey(const ASTPtr & cluster_by_key);

    WorkloadTableStats & getStats() { return stats; }

private:
    const StoragePtr storage;
    ASTPtr create_table_ddl;
    WorkloadTableStats stats;

    bool optimized{false};
};

}
