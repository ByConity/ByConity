#pragma once

#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SimplePlanVisitor.h>

namespace DB
{

struct RelatedMaterializedViews
{
    std::set<StorageID> materialized_views;
    std::set<StorageID> local_materialized_views;
    std::map<String, StorageID> local_table_to_distributed_table;
};

/**
 * Extract materialized views related into the query tables.
 */
class RelatedMaterializedViewsExtractor : public SimplePlanVisitor<Void>
{
public:
    static RelatedMaterializedViews extract(QueryPlan & plan, ContextMutablePtr context_);

protected:
    RelatedMaterializedViewsExtractor(ContextMutablePtr context_, CTEInfo & cte_info_) : SimplePlanVisitor(cte_info_), context(context_) { }

    Void visitTableScanNode(TableScanNode & node, Void &) override;

private:
    ContextMutablePtr context;
    RelatedMaterializedViews result;
};

}
