#include <Core/SettingsEnums.h>
#include <Statistics/SubqueryHelper.h>

namespace DB::Statistics
{

ContextMutablePtr SubqueryHelper::createQueryContext(ContextPtr context)
{
    auto query_context = createContextForSubQuery(context, "_create_stats_internal_");

    SettingsChanges changes;
    changes.emplace_back("dialect_type", "CLICKHOUSE");
    changes.emplace_back("database_atomic_wait_for_drop_and_detach_synchronously", true);
    changes.emplace_back("enable_deterministic_sample_by_range", true);
    changes.emplace_back("uniform_sample_by_range", true);
    changes.emplace_back("insert_distributed_sync", true);
    query_context->applySettingsChanges(changes);

    return query_context;
}

}
