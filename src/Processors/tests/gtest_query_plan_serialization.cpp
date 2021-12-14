#include <gtest/gtest.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_context.h>


using namespace DB;


QueryPlanStepPtr createAggregatingStep()
{
    DataStream input_stream{.header = Block()};

    ColumnNumbers keys;
    AggregateDescriptions aggregates;

    Aggregator::Params params(
        Block(),
        keys,
        aggregates,
        false,
        1,
        OverflowMode::ANY,
        2,
        3,
        4,
        true,
        nullptr,
        5,
        6,
        false,
        7,
        Block()
    );

    SortDescription group_by_sort_description;

    return make_unique<AggregatingStep>(
        input_stream,
        params,
        true,
        8,
        9,
        10,
        true,
        nullptr,
        std::move(group_by_sort_description)
    );
}

QueryPlanStepPtr serializeQueryPlanStep(QueryPlanStepPtr & step)
{
    /**
     * serialize to buffer
     */
    WriteBufferFromOwnString write_buffer;
    serializePlanStep(step, write_buffer);

    /**
     * deserialize from buffer
     */
    const auto & context = getContext().context;

    ReadBufferFromString read_buffer(write_buffer.str());
    return deserializePlanStep(read_buffer, context);
}

TEST(QueryPlanTest, QueryPlanSerialization)
{
    auto agg_step = createAggregatingStep();
    auto new_agg_step = serializeQueryPlanStep(agg_step);
    std::cout << new_agg_step->getName() << std::endl;
    EXPECT_EQ(agg_step->getName(), new_agg_step->getName());
    EXPECT_EQ(dynamic_cast<AggregatingStep *>(agg_step.get())->getParams().src_header.dumpStructure(), 
              dynamic_cast<AggregatingStep *>(new_agg_step.get())->getParams().src_header.dumpStructure());
}
