#include <random>
#include <Core/tests/gtest_protobuf_common.h>
#include <Protos/plan_node.pb.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ApplyStep.h>
#include <QueryPlan/ArrayJoinStep.h>
#include <QueryPlan/AssignUniqueIdStep.h>
#include <QueryPlan/BufferStep.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/CreatingSetsStep.h>
#include <QueryPlan/CubeStep.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/EnforceSingleRowStep.h>
#include <QueryPlan/ExceptStep.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/ExplainAnalyzeStep.h>
#include <QueryPlan/ExpressionStep.h>
#include <QueryPlan/ExtremesStep.h>
#include <QueryPlan/FillingStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/FinalSampleStep.h>
#include <QueryPlan/FinishSortingStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/IntersectOrExceptStep.h>
#include <QueryPlan/IntersectStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/LimitByStep.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingAggregatedStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/OffsetStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <QueryPlan/PartitionTopNStep.h>
#include <QueryPlan/PlanSegmentSourceStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/ReadFromMergeTree.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <QueryPlan/ReadNothingStep.h>
#include <QueryPlan/RemoteExchangeSourceStep.h>
#include <QueryPlan/RollupStep.h>
#include <QueryPlan/SettingQuotaAndLimitsStep.h>
#include <QueryPlan/SortingStep.h>
#include <QueryPlan/TableFinishStep.h>
#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/TableWriteStep.h>
#include <QueryPlan/TopNFilteringStep.h>
#include <QueryPlan/TotalsHavingStep.h>
#include <QueryPlan/UnionStep.h>
#include <QueryPlan/ValuesStep.h>
#include <QueryPlan/WindowStep.h>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <Common/ClickHouseRevision.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;
using namespace DB::UnitTest;

TEST_F(ProtobufTest, AssignUniqueIdStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto unique_id = fmt::format("text{}", eng() % 100);
        auto result = std::make_shared<AssignUniqueIdStep>(base_input_stream, unique_id);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::AssignUniqueIdStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = AssignUniqueIdStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AssignUniqueIdStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, EnforceSingleRowStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto result = std::make_shared<EnforceSingleRowStep>(base_input_stream);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::EnforceSingleRowStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = EnforceSingleRowStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::EnforceSingleRowStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ExtremesStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto result = std::make_shared<ExtremesStep>(base_input_stream);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::ExtremesStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ExtremesStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ExtremesStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FillingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        base_input_stream.has_single_port = true; // make ctor happy
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto result = std::make_shared<FillingStep>(base_input_stream, sort_description);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::FillingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = FillingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FillingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FinishSortingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription prefix_description;
        for (int i = 0; i < 2; ++i)
            prefix_description.emplace_back(generateSortColumnDescription(eng));
        SortDescription result_description;
        for (int i = 0; i < 2; ++i)
            result_description.emplace_back(generateSortColumnDescription(eng));
        auto max_block_size = eng() % 1000;
        auto limit = eng() % 1000;
        auto result = std::make_shared<FinishSortingStep>(base_input_stream, prefix_description, result_description, max_block_size, limit);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::FinishSortingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = FinishSortingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FinishSortingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, LimitByStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto group_length = eng() % 1000;
        auto group_offset = eng() % 1000;
        Names columns;
        for (int i = 0; i < 10; ++i)
            columns.emplace_back(fmt::format("text{}", eng() % 100));
        auto result = std::make_shared<LimitByStep>(base_input_stream, group_length, group_offset, columns);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::LimitByStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = LimitByStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::LimitByStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, LimitStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto limit = eng() % 1000;
        auto offset = eng() % 1000;
        auto always_read_till_end = eng() % 2 == 1;
        auto with_ties = eng() % 2 == 1;
        SortDescription description;
        for (int i = 0; i < 2; ++i)
            description.emplace_back(generateSortColumnDescription(eng));
        auto partial = eng() % 2 == 1;
        auto result = std::make_shared<LimitStep>(base_input_stream, limit, offset, always_read_till_end, with_ties, description, partial);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::LimitStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = LimitStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::LimitStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, MarkDistinctStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto marker_symbol = fmt::format("text{}", eng() % 100);
        std::vector<String> distinct_symbols;
        for (int i = 0; i < 10; ++i)
            distinct_symbols.emplace_back(fmt::format("text{}", eng() % 100));
        auto result = std::make_shared<MarkDistinctStep>(base_input_stream, marker_symbol, distinct_symbols);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::MarkDistinctStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = MarkDistinctStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MarkDistinctStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, MergingSortedStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto max_block_size = eng() % 1000;
        auto limit = eng() % 1000;
        auto result = std::make_shared<MergingSortedStep>(base_input_stream, sort_description, max_block_size, limit);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::MergingSortedStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = MergingSortedStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MergingSortedStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, OffsetStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto offset = eng() % 1000;
        auto result = std::make_shared<OffsetStep>(base_input_stream, offset);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::OffsetStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = OffsetStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::OffsetStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, SortingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription result_description;
        for (int i = 0; i < 2; ++i)
            result_description.emplace_back(generateSortColumnDescription(eng));
        auto limit = eng() % 1000;
        SortDescription prefix_description;
        for (int i = 0; i < 2; ++i)
            prefix_description.emplace_back(generateSortColumnDescription(eng));
        auto result = std::make_shared<SortingStep>(base_input_stream, result_description, limit, SortingStep::Stage::FULL, prefix_description);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::SortingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = SortingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::SortingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ValuesStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto base_output_header = generateBlock(eng);
        Fields fields;
        for (int i = 0; i < 2; ++i)
            fields.emplace_back(generateField(eng));
        auto rows = eng() % 1000;
        auto result = std::make_shared<ValuesStep>(base_output_header, fields, rows);

        return result;
    }();

    // serialize to protobuf
    Protos::ValuesStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ValuesStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ValuesStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ExceptStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto [base_input_streams, base_output_stream, output_to_inputs] = generateSetOperationStep(eng);
        auto distinct = eng() % 2 == 1;
        auto result = std::make_shared<ExceptStep>(base_input_streams, base_output_stream, output_to_inputs, distinct);

        return result;
    }();

    // serialize to protobuf
    Protos::ExceptStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ExceptStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ExceptStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, IntersectStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto [base_input_streams, base_output_stream, output_to_inputs] = generateSetOperationStep(eng);
        auto distinct = eng() % 2 == 1;
        auto result = std::make_shared<IntersectStep>(base_input_streams, base_output_stream, output_to_inputs, distinct);

        return result;
    }();

    // serialize to protobuf
    Protos::IntersectStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = IntersectStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::IntersectStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, UnionStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto [base_input_streams, base_output_stream, output_to_inputs] = generateSetOperationStep(eng);
        auto max_threads = eng() % 1000;
        auto local = eng() % 2 == 1;
        auto result = std::make_shared<UnionStep>(base_input_streams, base_output_stream, output_to_inputs, max_threads, local);

        return result;
    }();

    // serialize to protobuf
    Protos::UnionStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = UnionStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::UnionStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ExchangeStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        DataStreams input_streams;
        for (int i = 0; i < 2; ++i)
            input_streams.emplace_back(generateDataStream(eng));
        auto exchange_type = static_cast<ExchangeMode>(eng() % 3);
        auto schema = generatePartitioning(eng);
        auto keep_order = eng() % 2 == 1;
        auto result = std::make_shared<ExchangeStep>(input_streams, exchange_type, schema, keep_order);

        return result;
    }();

    // serialize to protobuf
    Protos::ExchangeStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ExchangeStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ExchangeStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, CTERefStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto base_output_header = generateBlock(eng);
        auto id = eng() % 1000;
        std::unordered_map<String, String> output_columns;
        for (int i = 0; i < 10; ++i)
            output_columns[fmt::format("text{}", eng() % 100)] = fmt::format("text{}", eng() % 100);
        auto has_filter = eng() % 2 == 1;
        auto result = std::make_shared<CTERefStep>(base_output_header, id, output_columns, has_filter);

        return result;
    }();

    // serialize to protobuf
    Protos::CTERefStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = CTERefStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::CTERefStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, DistinctStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto set_size_limits = generateSizeLimits(eng);
        auto limit_hint = eng() % 1000;
        Names columns;
        for (int i = 0; i < 10; ++i)
            columns.emplace_back(fmt::format("text{}", eng() % 100));
        auto pre_distinct = eng() % 2 == 1;
        auto result = std::make_shared<DistinctStep>(base_input_stream, set_size_limits, limit_hint, columns, pre_distinct);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::DistinctStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = DistinctStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::DistinctStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, PartialSortingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto limit = eng() % 1000;
        auto size_limits = generateSizeLimits(eng);
        auto result = std::make_shared<PartialSortingStep>(base_input_stream, sort_description, limit, size_limits);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::PartialSortingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = PartialSortingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::PartialSortingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, PartitionTopNStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        Names partition;
        for (int i = 0; i < 10; ++i)
            partition.emplace_back(fmt::format("text{}", eng() % 100));
        Names order_by;
        for (int i = 0; i < 10; ++i)
            order_by.emplace_back(fmt::format("text{}", eng() % 100));
        auto limit = eng() % 1000;
        auto model = static_cast<TopNModel>(eng() % 3);
        auto result = std::make_shared<PartitionTopNStep>(base_input_stream, partition, order_by, limit, model);
        result->setStepDescription(step_description);
        return result;
    }();

    // serialize to protobuf
    Protos::PartitionTopNStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = PartitionTopNStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::PartitionTopNStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ReadNothingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&] {
        auto base_output_header = generateBlock(eng);
        auto result = std::make_shared<ReadNothingStep>(base_output_header);

        return result;
    }();

    // serialize to protobuf
    Protos::ReadNothingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ReadNothingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ReadNothingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, TopNFilteringStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription sort_description;
        for (int i = 0; i < 2; ++i)
            sort_description.emplace_back(generateSortColumnDescription(eng));
        auto size = eng() % 1000;
        auto model = static_cast<TopNModel>(eng() % 3);
        auto res = std::make_shared<TopNFilteringStep>(base_input_stream, sort_description, size, model);
        res->setStepDescription(step_description);
        return res;
    }();

    // serialize to protobuf
    Protos::TopNFilteringStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = TopNFilteringStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::TopNFilteringStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, TableWriteStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto target = generateTableWriteStepInsertTarget(eng);
        auto s = std::make_shared<TableWriteStep>(base_input_stream, target, false);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::TableWriteStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = TableWriteStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::TableWriteStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, TableFinishStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto target = generateTableWriteStepInsertTarget(eng);
        auto output_affected_row_count_symbol = fmt::format("text{}", eng() % 100);
        auto s = std::make_shared<TableFinishStep>(base_input_stream, target, output_affected_row_count_symbol, nullptr, false);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::TableFinishStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = TableFinishStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::TableFinishStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FilterStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto filter = generateAST(eng);
        auto remove_filter_column = eng() % 2 == 1;
        auto s = std::make_shared<FilterStep>(base_input_stream, filter, remove_filter_column);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::FilterStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = FilterStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FilterStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ProjectionStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto assignments = generateAssignments(eng);
        NameToType name_to_type;
        for (const auto & [k, v] : assignments)
        {
            name_to_type[k] = test_data_types[eng() % 3];
            (void)v;
        }
        auto final_project = eng() % 2 == 1;
        auto s = std::make_shared<ProjectionStep>(base_input_stream, assignments, name_to_type, final_project);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::ProjectionStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ProjectionStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ProjectionStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, JoinStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        DataStreams input_streams;
        for (int i = 0; i < 2; ++i)
            input_streams.emplace_back(generateDataStream(eng));
        DataStream output_stream;
        output_stream = generateDataStream(eng);
        auto step_description = fmt::format("text{}", eng() % 100);
        auto kind = static_cast<ASTTableJoin::Kind>(eng() % 3);
        auto strictness = static_cast<ASTTableJoin::Strictness>(eng() % 3);
        auto max_streams = eng() % 1000;
        auto keep_left_read_in_order = eng() % 2 == 1;
        Names left_keys;
        for (int i = 0; i < 10; ++i)
            left_keys.emplace_back(fmt::format("text{}", eng() % 100));
        Names right_keys;
        for (int i = 0; i < 10; ++i)
            right_keys.emplace_back(fmt::format("text{}", eng() % 100));
        std::vector<bool> key_ids_null_safe;
        for (size_t i = 0; i < left_keys.size(); ++i)
            key_ids_null_safe.emplace_back(eng() % 2 == 0);
        auto filter = generateAST(eng);
        auto has_using = eng() % 2 == 1;
        std::optional<std::vector<bool>> require_right_keys;
        if (eng() % 2 == 0)
            require_right_keys = std::vector<bool>({true, false});
        auto asof_inequality = static_cast<ASOF::Inequality>(eng() % 3);
        auto distribution_type = static_cast<DistributionType>(eng() % 3);
        auto join_algorithm = static_cast<JoinAlgorithm>(eng() % 3);
        auto is_magic = eng() % 2 == 1;
        auto is_ordered = eng() % 2 == 1;
        auto simple_reordered = false;
        LinkedHashMap<String, RuntimeFilterBuildInfos> runtime_filter_builders;
        runtime_filter_builders.emplace("a", generateRuntimeFilterBuildInfos(eng));
        runtime_filter_builders.emplace("b", generateRuntimeFilterBuildInfos(eng));
        auto s = std::make_shared<JoinStep>(
            input_streams,
            output_stream,
            kind,
            strictness,
            max_streams,
            keep_left_read_in_order,
            left_keys,
            right_keys,
            key_ids_null_safe,
            filter,
            has_using,
            require_right_keys,
            asof_inequality,
            distribution_type,
            join_algorithm,
            is_magic,
            is_ordered,
            simple_reordered);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::JoinStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = JoinStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::JoinStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, MergeSortingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        SortDescription description;
        for (int i = 0; i < 2; ++i)
            description.emplace_back(generateSortColumnDescription(eng));
        auto max_merged_block_size = eng() % 1000;
        auto limit = eng() % 1000;
        auto max_bytes_before_remerge = eng() % 1000;
        auto remerge_lowered_memory_bytes_ratio = eng() % 100 * 0.01;
        auto max_bytes_before_external_sort = eng() % 1000;
        auto tmp_volume = context ? context->getTemporaryVolume() : nullptr;
        auto min_free_disk_space = eng() % 1000;
        auto enable_auto_spill = eng() % 2;
        auto s = std::make_shared<MergeSortingStep>(
            base_input_stream,
            description,
            max_merged_block_size,
            limit,
            max_bytes_before_remerge,
            remerge_lowered_memory_bytes_ratio,
            max_bytes_before_external_sort,
            tmp_volume,
            min_free_disk_space,
            enable_auto_spill);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::MergeSortingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = MergeSortingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MergeSortingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, MergingAggregatedStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        NameSet distinct_keys;
        for (int i = 0; i < 10; ++i)
            distinct_keys.emplace(fmt::format("text{}", eng() % 100));
        Names keys{distinct_keys.begin(), distinct_keys.end()};
        GroupingSetsParamsList grouping_sets_params;
        for (int i = 0; i < 2; ++i)
            grouping_sets_params.emplace_back(generateGroupingSetsParams(eng));
        GroupingDescriptions groupings;
        for (int i = 0; i < 2; ++i)
            groupings.emplace_back(generateGroupingDescription(eng));
        auto params = generateAggregatingTransformParams(eng);
        auto memory_efficient_aggregation = eng() % 2 == 1;
        auto max_threads = eng() % 1000;
        auto memory_efficient_merge_threads = eng() % 1000;
        auto s = std::make_shared<MergingAggregatedStep>(
            base_input_stream,
            keys,
            grouping_sets_params,
            groupings,
            params,
            memory_efficient_aggregation,
            max_threads,
            memory_efficient_merge_threads);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::MergingAggregatedStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = MergingAggregatedStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::MergingAggregatedStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, AggregatingStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        NameSet distinct_keys;
        for (int i = 0; i < 10; ++i)
            distinct_keys.emplace(fmt::format("text{}", eng() % 100));
        Names keys{distinct_keys.begin(), distinct_keys.end()};
        NameSet keys_not_hashed;
        for (int i = 0; i < 10; ++i)
            keys_not_hashed.emplace(fmt::format("text{}", eng() % 100));
        auto params = generateAggregatorParams(eng);
        GroupingSetsParamsList grouping_sets_params;
        for (int i = 0; i < 2; ++i)
            grouping_sets_params.emplace_back(generateGroupingSetsParams(eng));
        auto final = eng() % 2 == 1;
        auto max_block_size = eng() % 1000;
        auto merge_threads = eng() % 1000;
        auto temporary_data_merge_threads = eng() % 1000;
        auto storage_has_evenly_distributed_read = eng() % 2 == 1;
        auto group_by_info = generateInputOrderInfo(eng);
        SortDescription group_by_sort_description;
        for (int i = 0; i < 2; ++i)
            group_by_sort_description.emplace_back(generateSortColumnDescription(eng));
        GroupingDescriptions groupings;
        for (int i = 0; i < 2; ++i)
            groupings.emplace_back(generateGroupingDescription(eng));
        auto should_produce_results_in_order_of_bucket_number = eng() % 2 == 1;
        auto s = std::make_shared<AggregatingStep>(
            base_input_stream,
            keys,
            keys_not_hashed,
            params,
            grouping_sets_params,
            final,
            max_block_size,
            merge_threads,
            temporary_data_merge_threads,
            storage_has_evenly_distributed_read,
            group_by_info,
            group_by_sort_description,
            groupings,
            false,
            should_produce_results_in_order_of_bucket_number);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::AggregatingStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = AggregatingStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AggregatingStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ArrayJoinStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng, true);
        auto array_join = generateArrayJoinAction(eng);
        auto s = std::make_shared<ArrayJoinStep>(base_input_stream, array_join);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::ArrayJoinStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ArrayJoinStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ArrayJoinStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, TableScanStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        auto storage_id = test_storage_ids[eng() % 3];
        NamesWithAliases column_alias = {{"a", "aaa"}, {"b", "bbb"}};
        auto query_info = generateSelectQueryInfo(eng);
        auto max_block_size = eng() % 1000;
        std::shared_ptr<AggregatingStep> pushdown_aggregation = nullptr;
        std::shared_ptr<ProjectionStep> pushdown_projection = nullptr;
        std::shared_ptr<ProjectionStep> pushdown_index_projection = nullptr;
        std::shared_ptr<FilterStep> pushdown_filter = nullptr;

        auto s = std::make_shared<TableScanStep>(
            context,
            storage_id,
            column_alias,
            query_info,
            max_block_size,
            String{} /*alias*/,
            false,
            PlanHints{},
            Assignments{},
            pushdown_aggregation,
            pushdown_projection,
            pushdown_filter);

        return s;
    }();

    // serialize to protobuf
    Protos::TableScanStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = TableScanStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::TableScanStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, RemoteExchangeSourceStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        auto input_stream = generateDataStream(eng);
        auto step_description = fmt::format("text{}", eng() % 100);
        auto inputs = std::vector{generatePlanSegmentInput(eng)};
        auto s = std::make_shared<RemoteExchangeSourceStep>(inputs, input_stream, false, false);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::RemoteExchangeSourceStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = RemoteExchangeSourceStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::RemoteExchangeSourceStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, FinalSampleStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto sample_size = eng() % 1000;
        auto max_chunk_size = eng() % 1000;
        auto s = std::make_shared<FinalSampleStep>(base_input_stream, sample_size, max_chunk_size);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::FinalSampleStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = FinalSampleStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::FinalSampleStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, ReadStorageRowCountStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        auto base_output_header = generateBlock(eng);
        auto storage_id = test_storage_ids[eng() % 3];
        auto query = generateAST(eng);
        auto agg_desc = generateAggregateDescription(eng, 0);
        auto num_rows = eng() % 1000;
        auto is_final_agg = false;
        auto s = std::make_shared<ReadStorageRowCountStep>(base_output_header, query, agg_desc, num_rows, is_final_agg);

        return s;
    }();

    // serialize to protobuf
    Protos::ReadStorageRowCountStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ReadStorageRowCountStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ReadStorageRowCountStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, WindowStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        WindowDescription desc;
        Names originalColumns = {
            "a",
            "b",
            "c",
        };
        Names countOutputs = {
            "c",
        };
        Names markers = {
            "d",
        };
        String row_number_symbol("e");
        for (const auto & column : originalColumns)
        {
            desc.partition_by.push_back(SortColumnDescription(column, 1 /* direction */, 1 /* nulls_direction */));
        }
        for (const auto & column : originalColumns)
        {
            desc.full_sort_description.push_back(SortColumnDescription(column, 1 /* direction */, 1 /* nulls_direction */));
        }
        WindowFrame default_frame{
            true,
            WindowFrame::FrameType::Range,
            WindowFrame::BoundaryType::Unbounded,
            0,
            true,
            WindowFrame::BoundaryType::Current,
            0,
            false};
        desc.frame = default_frame;
        auto output_stream = generateDataStream(eng);

        std::vector<WindowFunctionDescription> functions;
        for (size_t i = 0; i < markers.size(); i++)
        {
            String output = countOutputs.at(i);

            Names argument_names = {markers[i]};
            DataTypes types{std::make_shared<DataTypeUInt8>()};
            Array params;
            AggregateFunctionProperties properties;
            AggregateFunctionPtr aggregate_function = AggregateFunctionFactory::instance().get("sum", types, params, properties);
            WindowFunctionDescription function{output, nullptr, aggregate_function, params, types, argument_names};
            functions.emplace_back(function);
        }
        {
            Names argument_names;
            DataTypes types;
            Array params;
            AggregateFunctionProperties properties;

            AggregateFunctionPtr aggregate_function = AggregateFunctionFactory::instance().get("row_number", types, params, properties);
            WindowFunctionDescription function{row_number_symbol, nullptr, aggregate_function, params, types, argument_names};
            functions.emplace_back(function);
        }

        auto s = std::make_shared<WindowStep>(output_stream, desc, functions, true);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::WindowStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = WindowStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::WindowStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, IntersectOrExceptStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        DataStreams input_streams;
        for (int i = 0; i < 2; ++i)
            input_streams.emplace_back(generateDataStream(eng));
        auto current_operator = static_cast<IntersectOrExceptStep::Operator>(eng() % 3);
        auto max_threads = eng() % 1000;
        auto s = std::make_shared<IntersectOrExceptStep>(input_streams, current_operator, max_threads);
        return s;
    }();

    // serialize to protobuf
    Protos::IntersectOrExceptStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = IntersectOrExceptStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::IntersectOrExceptStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}

TEST_F(ProtobufTest, BufferStep)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = [&eng] {
        std::string step_description = fmt::format("description {}", eng() % 100);
        auto base_input_stream = generateDataStream(eng);
        auto s = std::make_shared<BufferStep>(base_input_stream);
        s->setStepDescription(step_description);
        return s;
    }();

    // serialize to protobuf
    Protos::BufferStep pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = BufferStep::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::BufferStep pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
    compareStep(step, step2);
}
