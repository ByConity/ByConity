#pragma once

#include <unordered_set>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SimplePlanVisitor.h>
#include "Interpreters/StorageID.h"

namespace DB
{

struct RelatedMaterializedViews
{
    std::vector<StorageID> materialized_views;
    std::vector<StorageID> local_materialized_views;
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
    std::unordered_set<StorageID> visited_materialized_views;
};

}
