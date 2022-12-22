#pragma once

#include <Analyzers/Analysis.h>
#include <QueryPlan/PlanBuilder.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/planning_models.h>
#include <QueryPlan/TranslationMap.h>

namespace DB
{

class QueryPlanner
{
public:
    /**
     * Entry method of planning phase.
     *
     */
    static QueryPlanPtr plan(ASTPtr & query, Analysis & analysis, ContextMutablePtr context);

    /**
     * Entry method of planning an cross-scoped ASTSelectWithUnionQuery/ASTSelectQuery.
     *
     */
    static RelationPlan planQuery(
        ASTPtr query, TranslationMapPtr outer_query_context, Analysis & analysis, ContextMutablePtr context, CTERelationPlans & cte_info);
};

}
