#include <Core/tests/gtest_protobuf_common.h>
#include "Interpreters/DistributedStages/PlanSegment.h"


using namespace DB;
using namespace DB::UnitTest;

ContextMutablePtr ProtobufTest::session_context;
ContextMutablePtr ProtobufTest::context;
DataTypes ProtobufTest::test_data_types;
std::vector<NameAndTypePair> ProtobufTest::test_name_and_type_pairs;
std::vector<StorageID> ProtobufTest::test_storage_ids;

TEST_F(ProtobufTest, Field)
{
    std::default_random_engine eng(42);
    for (int i = 0; i < 10; ++i)
    {
        auto obj = generateField(eng);
        Protos::Field pb;
        obj.toProto(pb);
        Field obj2;
        obj2.fillFromProto(pb);
        ASSERT_EQ(obj, obj2);
        Protos::Field pb2;
        obj2.toProto(pb2);
        compareProto(pb, pb2);
    }
}

TEST_F(ProtobufTest, FillColumnDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateFillColumnDescription(eng);
    // serialize to protobuf
    Protos::FillColumnDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    FillColumnDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::FillColumnDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, SortColumnDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateSortColumnDescription(eng);
    // serialize to protobuf
    Protos::SortColumnDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    SortColumnDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::SortColumnDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, Block)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateBlock(eng);
    // serialize to protobuf
    Protos::Block pb;
    serializeHeaderToProto(obj, pb);
    // deserialize from protobuf
    Block obj2 = deserializeHeaderFromProto(pb);
    // re-serialize to protobuf
    Protos::Block pb2;
    serializeHeaderToProto(obj2, pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, DataStream)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateDataStream(eng);
    // serialize to protobuf
    Protos::DataStream pb;
    obj.toProto(pb);
    // deserialize from protobuf
    DataStream obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::DataStream pb2;
    obj2.toProto(pb2);
    std::string str, str2;
    pb.SerializeToString(&str);
    pb2.SerializeToString(&str2);
    std::string dbg_str;
    std::string dbg_str2;
    google::protobuf::TextFormat::PrintToString(pb, &dbg_str);
    google::protobuf::TextFormat::PrintToString(pb2, &dbg_str2);
    ASSERT_EQ(dbg_str, dbg_str2);
    ASSERT_EQ(str, str2);
}

TEST_F(ProtobufTest, SizeLimits)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateSizeLimits(eng);
    // serialize to protobuf
    Protos::SizeLimits pb;
    obj.toProto(pb);
    // deserialize from protobuf
    SizeLimits obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::SizeLimits pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, Partitioning)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generatePartitioning(eng);
    // serialize to protobuf
    Protos::Partitioning pb;
    obj.toProto(pb);
    // deserialize from protobuf
    Partitioning obj2 = Partitioning::fromProto(pb);
    // re-serialize to protobuf
    Protos::Partitioning pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, NameAndTypePair)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateNameAndTypePair(eng);
    // serialize to protobuf
    Protos::NameAndTypePair pb;
    obj.toProto(pb);
    // deserialize from protobuf
    NameAndTypePair obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::NameAndTypePair pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}


TEST_F(ProtobufTest, GroupingSetsParams)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateGroupingSetsParams(eng);
    // serialize to protobuf
    Protos::GroupingSetsParams pb;
    obj.toProto(pb);
    // deserialize from protobuf
    GroupingSetsParams obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::GroupingSetsParams pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, GroupingDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateGroupingDescription(eng);
    // serialize to protobuf
    Protos::GroupingDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    GroupingDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::GroupingDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AggregateDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateAggregateDescription(eng);
    // serialize to protobuf
    Protos::AggregateDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    AggregateDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::AggregateDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}
TEST_F(ProtobufTest, AggregatorParams)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateAggregatorParams(eng);

    // serialize to protobuf
    Protos::AggregatorParams pb;
    step.toProto(pb);
    // deserialize from protobuf
    auto step2 = Aggregator::Params::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AggregatorParams pb2;
    step2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AggregatingTransformParams)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateAggregatingTransformParams(eng);

    // serialize to protobuf
    Protos::AggregatingTransformParams pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = AggregatingTransformParams::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::AggregatingTransformParams pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, ArrayJoinAction)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateArrayJoinAction(eng);

    // serialize to protobuf
    Protos::ArrayJoinAction pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = ArrayJoinAction::fromProto(pb, context);
    // re-serialize to protobuf
    Protos::ArrayJoinAction pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, SelectQueryInfo)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateSelectQueryInfo(eng);
    // serialize to protobuf
    Protos::SelectQueryInfo pb;
    obj.toProto(pb);
    // deserialize from protobuf
    SelectQueryInfo obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::SelectQueryInfo pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, AddressInfo)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateAddressInfo(eng);
    // serialize to protobuf
    Protos::AddressInfo pb;
    obj.toProto(pb);
    // deserialize from protobuf
    AddressInfo obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::AddressInfo pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, PlanSegmentInput)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generatePlanSegmentInput(eng);
    // serialize to protobuf
    Protos::PlanSegmentInput pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = std::make_shared<PlanSegmentInput>();
    step2->fillFromProto(pb, context);
    // re-serialize to protobuf
    Protos::PlanSegmentInput pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, InputOrderInfo)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateInputOrderInfo(eng);

    // serialize to protobuf
    Protos::InputOrderInfo pb;
    step->toProto(pb);
    // deserialize from protobuf
    auto step2 = InputOrderInfo::fromProto(pb);
    // re-serialize to protobuf
    Protos::InputOrderInfo pb2;
    step2->toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, TableWriteStepInsertTarget)
{
    std::default_random_engine eng(42);
    // construct valid step
    auto step = generateTableWriteStepInsertTarget(eng);

    // serialize to protobuf
    Protos::TableWriteStep::InsertTarget pb;
    step->toProtoImpl(pb);
    // deserialize from protobuf
    auto step2 = TableWriteStep::InsertTarget::createFromProtoImpl(pb, context);
    // re-serialize to protobuf
    Protos::TableWriteStep::InsertTarget pb2;
    step2->toProtoImpl(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, WindowFunctionDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateWindowFunctionDescription(eng);
    // serialize to protobuf
    Protos::WindowFunctionDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    WindowFunctionDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::WindowFunctionDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, WindowDescription)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateWindowDescription(eng);
    // serialize to protobuf
    Protos::WindowDescription pb;
    obj.toProto(pb);
    // deserialize from protobuf
    WindowDescription obj2;
    obj2.fillFromProto(pb);
    // re-serialize to protobuf
    Protos::WindowDescription pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}

TEST_F(ProtobufTest, RuntimeFilterBuildInfos)
{
    std::default_random_engine eng(42);
    // construct valid object
    auto obj = generateRuntimeFilterBuildInfos(eng);
    // serialize to protobuf
    Protos::RuntimeFilterBuildInfos pb;
    obj.toProto(pb);
    // deserialize from protobuf
    auto obj2 = RuntimeFilterBuildInfos::fromProto(pb);
    // re-serialize to protobuf
    Protos::RuntimeFilterBuildInfos pb2;
    obj2.toProto(pb2);
    compareProto(pb, pb2);
}
