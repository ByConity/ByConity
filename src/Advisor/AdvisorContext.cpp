#include <Advisor/AdvisorContext.h>

#include <Advisor/ColumnUsage.h>
#include <Advisor/SignatureUsage.h>
#include <Advisor/WorkloadQuery.h>
#include <Advisor/WorkloadTable.h>
#include <Analyzers/QualifiedColumnName.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Common/ThreadPool.h>

namespace DB
{
AdvisorContext AdvisorContext::buildFrom(ContextMutablePtr session_context, WorkloadTables & tables, WorkloadQueries & queries, ThreadPool & query_thread_pool)
{
    ColumnUsages column_usages = buildColumnUsages(queries);
    SignatureUsages signature_usages = buildSignatureUsages(queries, session_context);

    return AdvisorContext(
        session_context,
        tables,
        queries,
        query_thread_pool,
        std::move(column_usages),
        std::move(signature_usages));
}

DataTypePtr AdvisorContext::getColumnType(const QualifiedColumnName & column)
{
    if (auto it = column_types.find(column); it != column_types.end())
        return it->second;

    if (StoragePtr storage = DatabaseCatalog::instance().tryGetTable(StorageID{column.database, column.table}, session_context))
    {
        if (auto meta = storage->getInMemoryMetadataPtr())
        {
            column_types[column] = meta->getColumns().get(column.column).type;
            return column_types[column];
        }
    }

    column_types[column] = nullptr;
    return nullptr;
}

}
