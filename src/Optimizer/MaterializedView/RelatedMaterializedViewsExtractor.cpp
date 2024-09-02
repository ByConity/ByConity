#include <algorithm>
#include <Optimizer/MaterializedView/RelatedMaterializedViewsExtractor.h>
#include "Interpreters/StorageID.h"

namespace DB
{

RelatedMaterializedViews RelatedMaterializedViewsExtractor::extract(QueryPlan & plan, ContextMutablePtr context_)
{
    Void c;
    RelatedMaterializedViewsExtractor finder{context_, plan.getCTEInfo()};
    VisitorUtil::accept(plan.getPlanNode(), finder, c);
    std::sort(finder.result.materialized_views.begin(), finder.result.materialized_views.end(), [](const auto & left, const auto & right){
        return left.getFullTableName() < right.getFullTableName();
    });
    std::sort(
        finder.result.local_materialized_views.begin(),
        finder.result.local_materialized_views.end(),
        [](const auto & left, const auto & right) { return left.getFullTableName() < right.getFullTableName(); });
    return std::move(finder.result);
}


Void RelatedMaterializedViewsExtractor::visitTableScanNode(TableScanNode & node, Void &)
{
    auto table_scan = node.getStep();
    auto catalog_client = context->tryGetCnchCatalog();
    if (catalog_client)
    {
        auto start_time = context->getTimestamp();
        auto views = catalog_client->getAllViewsOn(*context, table_scan->getStorage(), start_time);
        for (const auto & view : views)
            if (visited_materialized_views.emplace(view->getStorageID()).second)
                result.materialized_views.emplace_back(view->getStorageID());
    }
    else
    {
        auto dependencies = DatabaseCatalog::instance().getDependencies(table_scan->getStorageID());
        for (const auto & item : dependencies)
            if (visited_materialized_views.emplace(item).second)
                result.materialized_views.emplace_back(item);
    }
    return Void{};
}
}
