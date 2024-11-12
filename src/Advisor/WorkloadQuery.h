#pragma once

#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <QueryPlan/QueryPlan.h>
#include <Common/ThreadPool.h>

#include <memory>
#include <set>
#include <vector>
#include <boost/noncopyable.hpp>

namespace DB
{
class WorkloadQuery;
using WorkloadQueryPtr = std::shared_ptr<WorkloadQuery>;
using WorkloadQueries = std::vector<WorkloadQueryPtr>;

class WorkloadQuery : private boost::noncopyable
{
public:
    // private actually, for the ease of shared_ptr
    WorkloadQuery(
        ContextMutablePtr context_,
        std::string query_id_,
        std::string sql_,
        QueryPlanPtr plan_,
        QueryPlanPtr plan_before_cascades_,
        std::set<QualifiedTableName> query_tables_,
        PlanCostMap costs_)
        : query_context(context_)
        , query_id(std::move(query_id_))
        , sql(std::move(sql_))
        , plan(std::move(plan_))
        , plan_before_cascades(std::move(plan_before_cascades_))
        , query_tables(std::move(query_tables_))
        , costs(std::move(costs_))
    {
    }

    const std::string & getQueryId() const
    {
        return query_id;
    }
    const std::string & getSQL() const { return sql; }
    const QueryPlanPtr & getPlan() const { return plan; }
    const QueryPlanPtr & getPlanBeforeCascades() const { return plan_before_cascades; }
    const PlanCostMap & getCosts() const { return costs; }

    /*
     * obtain the optimal cost under the given table layout
     */
    double getOptimalCost(const TableLayout & table_layout);

    static WorkloadQueryPtr build(const std::string & query_id, const std::string & query, const ContextPtr & from_context);

    static WorkloadQueries build(const std::vector<std::string> & queries, const ContextPtr & from_context, ThreadPool & query_thread_pool);

private:
    ContextMutablePtr query_context;
    std::string query_id;
    std::string sql;
    QueryPlanPtr plan;
    QueryPlanPtr plan_before_cascades;
    std::set<QualifiedTableName> query_tables;
    PlanCostMap costs;

    // lazy init
    std::shared_ptr<CascadesContext> cascades_context;
    GroupExprPtr root_group;
};

}
