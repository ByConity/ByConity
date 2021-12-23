#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

PlanSegmentPtr createPlanSegment()
{
    PlanSegmentPtr plan_segment = std::make_unique<PlanSegment>();

    PlanSegmentInputPtr left = std::make_shared<PlanSegmentInput>(PlanSegmentType::EXCHANGE);
    PlanSegmentInputPtr right = std::make_shared<PlanSegmentInput>(PlanSegmentType::EXCHANGE);
    PlanSegmentOutputPtr output = std::make_shared<PlanSegmentOutput>(PlanSegmentType::OUTPUT);

    plan_segment->appendPlanSegmentInput(left);
    plan_segment->appendPlanSegmentInput(right);
    plan_segment->setPlanSegmentOutput(output);

    QueryPlan query_plan;
    plan_segment->setQueryPlan(std::move(query_plan));

    return plan_segment;
}

TEST(PlanSegmentTest, PlanSegmentSerDer)
{
    PlanSegmentPtr plan_segment = createPlanSegment();

    /**
     * serialize to buffer
     */
    WriteBufferFromOwnString write_buffer;
    plan_segment->serialize(write_buffer);

    /**
     * deserialize from buffer
     */
    ReadBufferFromString read_buffer(write_buffer.str());
    PlanSegmentPtr new_plan_segment = std::make_unique<PlanSegment>();
    new_plan_segment->deserialize(read_buffer);
}
