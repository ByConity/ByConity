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
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_context.h>


using namespace DB;


Block createBlock()
{
    return Block();
}

DataStream createDataStream()
{
    return DataStream();
}

SortDescription createSortDescription()
{
    SortDescription sort_desc;

    Names keys{"key1", "key2", "key3", "key4"};
    for (const auto & key_name : keys)
        sort_desc.emplace_back(SortColumnDescription(key_name, 1, 1));

    return sort_desc;
}

SizeLimits createSizeLimits()
{
    return SizeLimits();
}

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

void TestSingleSimpleStep(QueryPlanStepPtr step)
{
    auto new_step = serializeQueryPlanStep(step);
    std::cout << new_step->getName() << std::endl;
    EXPECT_EQ(step->getName(), new_step->getName());
}

QueryPlanStepPtr createReadNothingStep()
{
    Block block = createBlock();
    return std::make_unique<ReadNothingStep>(block);
}

QueryPlanStepPtr createPartialSortingStep()
{
    DataStream stream = createDataStream();
    SortDescription desc = createSortDescription();
    SizeLimits limits = createSizeLimits();
    return std::make_unique<PartialSortingStep>(stream, desc, 0, limits);
}

QueryPlanStepPtr createOffsetStep()
{
    DataStream stream = createDataStream();
    return std::make_unique<OffsetStep>(stream, 0);
}

QueryPlanStepPtr createMergingSortedStep()
{
    DataStream stream = createDataStream();
    SortDescription desc = createSortDescription();
    return std::make_unique<MergingSortedStep>(stream, desc, 0, 0);
}

QueryPlanStepPtr createMergeSortingStep()
{
    DataStream stream = createDataStream();
    SortDescription desc = createSortDescription();
    return std::make_unique<MergeSortingStep>(stream, desc, 0, 0, 0, 0, 0, nullptr, 0);
}

QueryPlanStepPtr createLimitStep()
{
    DataStream stream = createDataStream();
    return std::make_unique<LimitStep>(stream, 0, 0);
}

QueryPlanStepPtr createLimitByStep()
{
    DataStream stream = createDataStream();
    Names columns;
    return std::make_unique<LimitByStep>(stream, 0, 0, columns);
}

QueryPlanStepPtr createFinishSortingStep()
{
    DataStream stream = createDataStream();
    SortDescription desc1 = createSortDescription();
    SortDescription desc2 = createSortDescription();
    return std::make_unique<FinishSortingStep>(stream, desc1, desc2, 0, 0);
}

QueryPlanStepPtr createFillingStep()
{
    DataStream stream = createDataStream();
    stream.has_single_port = true;
    SortDescription desc = createSortDescription();
    return std::make_unique<FillingStep>(stream, desc);
}

QueryPlanStepPtr createExtremesStep()
{
    DataStream stream = createDataStream();
    return std::make_unique<ExtremesStep>(stream);
}

QueryPlanStepPtr createDistinctStep()
{
    DataStream stream = createDataStream();
    SizeLimits limits = createSizeLimits();
    Names columns;
    return std::make_unique<DistinctStep>(stream, limits, 0, columns, false);
}

TEST(QueryPlanTest, SimpleStepTest)
{
    TestSingleSimpleStep(createReadNothingStep());
    TestSingleSimpleStep(createPartialSortingStep());
    TestSingleSimpleStep(createOffsetStep());
    TestSingleSimpleStep(createMergeSortingStep());
    TestSingleSimpleStep(createMergingSortedStep());
    TestSingleSimpleStep(createLimitStep());
    TestSingleSimpleStep(createLimitByStep());
    TestSingleSimpleStep(createLimitByStep());
    TestSingleSimpleStep(createFinishSortingStep());
    TestSingleSimpleStep(createFillingStep());
    TestSingleSimpleStep(createExtremesStep());
    TestSingleSimpleStep(createDistinctStep());
}
