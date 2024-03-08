#include <Optimizer/MaterializedView/RelatedMaterializedViewsExtractor.h>

namespace DB
{

RelatedMaterializedViews RelatedMaterializedViewsExtractor::extract(QueryPlan & plan, ContextMutablePtr context_)
{
    Void c;
    RelatedMaterializedViewsExtractor finder{context_, plan.getCTEInfo()};
    VisitorUtil::accept(plan.getPlanNode(), finder, c);
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
            result.materialized_views.emplace(view->getStorageID());
    }
    else
    {
        auto dependencies = DatabaseCatalog::instance().getDependencies(table_scan->getStorageID());
        for (const auto & item : dependencies)
            result.materialized_views.emplace(item);
    }
    return Void{};
}
}
