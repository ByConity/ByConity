#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

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

#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/InterpreterSelectQuery.h>


using namespace DB;


Block createBlock()
{
    ColumnWithTypeAndName column;
    column.name = "RES";

    DataTypePtr type = DataTypeFactory::instance().get("UInt8");
    column.column = type->createColumnConst(1, Field("RES COLUMN"));
    column.type = type;

    ColumnsWithTypeAndName columns;
    columns.push_back(column);

    return Block(columns);
}

DataStream createDataStream()
{
    return DataStream{.header = createBlock()};
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

Aggregator::Params createAggregatorParams()
{
    ColumnNumbers keys;
    AggregateDescriptions aggregates;

    return Aggregator::Params(
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
}

QueryPlanStepPtr createAggregatingStep()
{
    DataStream input_stream{.header = Block()};

    Aggregator::Params params = createAggregatorParams();

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

QueryPlanStepPtr createUnionStep()
{
    DataStreams streams;
    streams.push_back(createDataStream());
    streams.push_back(createDataStream());
    return std::make_unique<UnionStep>(streams, 0);
}

QueryPlanStepPtr createMergingAggregatedStep()
{
    DataStream stream = createDataStream();
    AggregatingTransformParamsPtr params = std::make_shared<AggregatingTransformParams>(createAggregatorParams(), true);
    return std::make_unique<MergingAggregatedStep>(stream, params, false, 0, 0);
}

QueryPlanStepPtr createCubeStep()
{
    DataStream stream = createDataStream();
    AggregatingTransformParamsPtr params = std::make_shared<AggregatingTransformParams>(createAggregatorParams(), true);
    return std::make_unique<CubeStep>(stream, params);
}

QueryPlanStepPtr createRollupStep()
{
    DataStream stream = createDataStream();
    AggregatingTransformParamsPtr params = std::make_shared<AggregatingTransformParams>(createAggregatorParams(), true);
    return std::make_unique<RollupStep>(stream, params);
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
    TestSingleSimpleStep(createUnionStep());

    TestSingleSimpleStep(createMergingAggregatedStep());
    TestSingleSimpleStep(createCubeStep());
    TestSingleSimpleStep(createRollupStep());
}

ActionsDAGPtr createActionsDAG()
{
    auto actions_dag = std::make_shared<ActionsDAG>();
    const auto & context = getContext().context;

    tryRegisterFunctions();
    auto & factory = FunctionFactory::instance();
    auto function_builder = factory.get("lower", context);

    ColumnWithTypeAndName column;
    column.name = "TEST";

    DataTypePtr type = DataTypeFactory::instance().get("String");
    column.column = type->createColumnConst(1, Field("TEST CONSTANT"));
    column.type = type;

    actions_dag->addColumn(column);

    ActionsDAG::NodeRawConstPtrs children;
    children.push_back(&actions_dag->getNodes().back());
    actions_dag->addFunction(function_builder, std::move(children), "lower()");

    return actions_dag;
}

void TestSingleActionsStep(QueryPlanStepPtr step)
{
    auto new_step = serializeQueryPlanStep(step);
    std::cout << new_step->getName() << std::endl;

    EXPECT_EQ(step->getName(), new_step->getName());

    // todo test for others
}

QueryPlanStepPtr createExpressionStep()
{
    DataStream stream = createDataStream();
    ActionsDAGPtr actions = createActionsDAG();

    return std::make_unique<ExpressionStep>(stream, std::move(actions));
}

QueryPlanStepPtr createFilterStep()
{
    DataStream stream = createDataStream();
    ActionsDAGPtr actions = createActionsDAG();

    return std::make_unique<FilterStep>(stream, std::move(actions), "RES", false);
}

QueryPlanStepPtr createTotalsHavingStep()
{
    DataStream stream = createDataStream();
    ActionsDAGPtr actions = createActionsDAG();

    return std::make_unique<TotalsHavingStep>(
        stream,
        false,
        std::move(actions),
        "TEST",
        TotalsMode::AFTER_HAVING_AUTO,
        1.0,
        false
        );
}

TEST(QueryPlanTest, ActionsStepTest)
{
    TestSingleActionsStep(createExpressionStep());
    TestSingleActionsStep(createFilterStep());
    TestSingleActionsStep(createTotalsHavingStep());
}
