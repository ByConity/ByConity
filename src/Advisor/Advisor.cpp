#include <Advisor/Advisor.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/ColumnUsageAdvise.h>
#include <Advisor/Rules/DataTypeAdvise.h>
#include <Advisor/Rules/MaterializedViewAdvise.h>
#include <Advisor/Rules/OrderByKeyAdvise.h>
#include <Advisor/Rules/PartitionKeyAdvise.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/WorkloadQuery.h>
#include <Advisor/WorkloadTable.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>

#include <algorithm>
#include <chrono>
#include <vector>

namespace DB
{

WorkloadAdvisors Advisor::getAdvisors(ASTAdviseQuery::AdvisorType type)
{
    switch (type)
    {
        case ASTAdviseQuery::AdvisorType::ALL:
            return {
                std::make_shared<OrderByKeyAdvisor>(),
                std::make_shared<PartitionKeyAdvisor>(),
                std::make_shared<DataTypeAdvisor>(),
                std::make_shared<MaterializedViewAdvisor>(MaterializedViewAdvisor::OutputType::PROJECTION, true, true),
                std::make_shared<MaterializedViewAdvisor>(MaterializedViewAdvisor::OutputType::MATERIALIZED_VIEW, true, true)};

        case ASTAdviseQuery::AdvisorType::ORDER_BY:
            return {std::make_shared<OrderByKeyAdvisor>()};
        case ASTAdviseQuery::AdvisorType::CLUSTER_BY:
            return {std::make_shared<PartitionKeyAdvisor>()};
        case ASTAdviseQuery::AdvisorType::DATA_TYPE:
            return {std::make_shared<DataTypeAdvisor>()};
        case ASTAdviseQuery::AdvisorType::MATERIALIZED_VIEW:
            return {std::make_shared<MaterializedViewAdvisor>(MaterializedViewAdvisor::OutputType::MATERIALIZED_VIEW, true, true)};
        case ASTAdviseQuery::AdvisorType::PROJECTION:
            return {std::make_shared<MaterializedViewAdvisor>(MaterializedViewAdvisor::OutputType::PROJECTION, true, true)};
        case ASTAdviseQuery::AdvisorType::COLUMN_USAGE:
            return {std::make_shared<ColumnUsageAdvisor>()};
    }
}

WorkloadAdvises Advisor::analyze(const std::vector<String> & queries_, ContextPtr context_)
{
    auto context = Context::createCopy(context_);

    ThreadPool query_thread_pool{std::min(static_cast<size_t>(context->getSettingsRef().max_threads), queries_.size())};

    Stopwatch stop_watch;
    stop_watch.start();
    WorkloadTables tables{context};
    WorkloadQueries queries = WorkloadQuery::build(queries_, context, query_thread_pool);
    LOG_DEBUG(log, "Build workload queries time: {} ms", stop_watch.elapsedMillisecondsAsDouble());

    stop_watch.restart();
    AdvisorContext advisor_context = AdvisorContext::buildFrom(context, tables, queries, query_thread_pool);
    LOG_DEBUG(log, "Build advisor context time: {} ms", stop_watch.elapsedMillisecondsAsDouble());

    WorkloadAdvises res;
    for (const auto & advisor : getAdvisors(type))
    {
        stop_watch.restart();
        auto advises = advisor->analyze(advisor_context);
        res.insert(res.end(), advises.begin(), advises.end());
        LOG_DEBUG(log, "{} time: {} ms", advisor->getName(), stop_watch.elapsedMillisecondsAsDouble());
    }
    return res;
}
}
