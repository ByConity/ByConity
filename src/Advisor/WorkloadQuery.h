#pragma once

#include <Common/ThreadPool.h>
#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/Property/Property.h>
#include <QueryPlan/QueryPlan.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <set>
#include <vector>

namespace DB
{
class WorkloadQuery;
using WorkloadQueryPtr = std::unique_ptr<WorkloadQuery>;
using WorkloadQueries = std::vector<WorkloadQueryPtr>;

class WorkloadQuery : private boost::noncopyable
{
public:
    // private actually, for the ease of shared_ptr
    WorkloadQuery(
        ContextMutablePtr context_,
        std::string sql_,
        QueryPlanPtr plan_,
        QueryPlanPtr cascades_plan_,
        std::shared_ptr<CascadesContext> cascades_context_,
        GroupExprPtr root_group_,
        std::set<QualifiedTableName> query_tables_,
        PlanCostMap costs_)
        : query_context(context_)
        , sql(std::move(sql_))
        , plan(std::move(plan_))
        , cascades_plan(std::move(cascades_plan_))
        , cascades_context(std::move(cascades_context_))
        , root_group(std::move(root_group_))
        , query_tables(std::move(query_tables_))
        , costs(std::move(costs_))
    {
    }

    const std::string & getSQL() const { return sql; }
    const QueryPlanPtr & getPlan() const { return plan; }
    const PlanCostMap & getCosts() const { return costs; }

    /*
     * obtain the optimal cost under the given table layout
     */
    double getOptimalCost(const TableLayout & table_layout);

    static WorkloadQueryPtr build(const std::string & query, const ContextPtr & from_context);
    static WorkloadQueries build(const std::vector<std::string> & queries, const ContextPtr & from_context, ThreadPool & query_thread_pool);

private:
    ContextMutablePtr query_context;
    std::string sql;
    QueryPlanPtr plan;
    QueryPlanPtr cascades_plan;
    std::shared_ptr<CascadesContext> cascades_context;
    GroupExprPtr root_group;

    std::set<QualifiedTableName> query_tables;

    PlanCostMap costs;
};

}
