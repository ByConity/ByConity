#pragma once

#include <Advisor/ColumnUsage.h>
#include <Advisor/SignatureUsage.h>
#include <Advisor/WorkloadQuery.h>
#include <Advisor/WorkloadTable.h>
#include <Analyzers/QualifiedColumnName.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ThreadPool.h>

#include <unordered_map>

namespace DB
{
class AdvisorContext
{
public:
    ContextMutablePtr session_context;
    WorkloadTables & tables;
    WorkloadQueries & queries;
    std::unordered_map<String, WorkloadQueryPtr> query_id_to_query;
    ThreadPool & query_thread_pool;
    const ColumnUsages column_usages;
    const SignatureUsages signature_usages;

    static AdvisorContext buildFrom(ContextMutablePtr session_context,
                                    WorkloadTables & tables,
                                    WorkloadQueries & queries,
                                    ThreadPool & query_thread_pool);

    DataTypePtr getColumnType(const QualifiedColumnName & column);

private:
    AdvisorContext(
        ContextMutablePtr _session_context,
        WorkloadTables & _tables,
        WorkloadQueries & _queries,
        std::unordered_map<String, WorkloadQueryPtr> _query_id_to_query,
        ThreadPool & _query_thread_pool,
        ColumnUsages _column_usages,
        SignatureUsages _signature_usages)
        : session_context(_session_context)
        , tables(_tables)
        , queries(_queries)
        , query_id_to_query(std::move(_query_id_to_query))
        , query_thread_pool(_query_thread_pool)
        , column_usages(std::move(_column_usages))
        , signature_usages(std::move(_signature_usages))
    {
    }
    std::unordered_map<QualifiedColumnName, DataTypePtr, QualifiedColumnNameHash> column_types;
};
}
