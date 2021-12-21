#include <gtest/gtest.h>
#include <Processors/QueryPlan/PlanSerDerHelper.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/FinishSortingStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergeSortingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/PartialSortingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_context.h>
#include <Parsers/ParserQuery.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/InterpreterDistributedStages.h>

namespace UnitTest
{

using namespace DB;

ASTPtr parseTestQuery(const String & query)
{
    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end);
    return parseQuery(parser, begin, end, "", 0, 0);
}

void checkPlan(PlanSegment * lhs, PlanSegment * rhs)
{
    auto lhs_str = lhs->toString();
    auto rhs_str = rhs->toString();

    std::cout<<" <<< lhs:\n" << lhs_str << std::endl;
    std::cout<<" <<< rhs:\n" << rhs_str << std::endl;

    EXPECT_EQ(lhs_str, rhs_str);
}

void executeTestQuery(const ASTPtr & query)
{
    auto context = getContext().context;
    context->setQueryContext(context);


    auto interpreter = std::make_shared<InterpreterDistributedStages>(query, context);

    PlanSegmentTree * plan_segment_tree = interpreter->getPlanSegmentTree();

    /**
     * serialize to buffer
     */
    WriteBufferFromOwnString write_buffer;
    writeBinary(plan_segment_tree->getNodes().size(), write_buffer);
    for (auto & node : plan_segment_tree->getNodes())
        node.plan_segment->serialize(write_buffer);

    ReadBufferFromString read_buffer(write_buffer.str());
    size_t plan_size;
    readBinary(plan_size, read_buffer);
    std::vector<std::shared_ptr<PlanSegment>> plansegments;

    for (size_t i = 0; i < plan_size; ++i)
    {
        auto plan = std::make_shared<PlanSegment>(context);
        plan->deserialize(read_buffer);
        plansegments.push_back(plan);
    }

    /**
     * check results
     */
    std::vector<PlanSegment *> old_plans;
    for (auto & node : plan_segment_tree->getNodes())
        old_plans.push_back(node.plan_segment.get());
    
    for (size_t i = 0; i < plan_size; ++i)
    {
        auto lhs = old_plans[i];
        auto rhs = plansegments[i].get();
        checkPlan(lhs, rhs);
    }
}

TEST(TestQueryPlan, TestQueryPlanExecution)
{
    String query = "select sum(number) from (select 1 as number) settings enable_distributed_stages = 1";
    auto query_ast = parseTestQuery(query);
    //executeTestQuery(query_ast);
}

}
