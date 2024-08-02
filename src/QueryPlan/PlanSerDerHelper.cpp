/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <QueryPlan/PlanSerDerHelper.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataTypes/DataTypeHelper.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/TableJoin.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/queryToString.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Protos/ReadWriteProtobuf.h>
#include <Protos/plan_node.pb.h>
#include <Protos/plan_node_utils.pb.h>
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
#include <QueryPlan/LocalExchangeStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingAggregatedStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/MultiJoinStep.h>
#include <QueryPlan/ExpandStep.h>
#include <QueryPlan/OffsetStep.h>
#include <QueryPlan/OutfileWriteStep.h>
#include <QueryPlan/OutfileFinishStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <QueryPlan/PartitionTopNStep.h>
#include <QueryPlan/PlanSegmentSourceStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/ReadFromMergeTree.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <QueryPlan/ReadNothingStep.h>
#include <QueryPlan/ReadStorageRowCountStep.h>
#include <QueryPlan/RemoteExchangeSourceStep.h>
#include <QueryPlan/IntermediateResultCacheStep.h>
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
#include <google/protobuf/util/message_differencer.h>
#include "Common/SipHash.h"
#include <Common/ClickHouseRevision.h>
#include "Core/NamesAndTypes.h"
#include "IO/ReadBufferFromString.h"
#include "IO/WriteBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "QueryPlan/Assignment.h"

namespace DB
{
void serializeColumn(const ColumnPtr & column, const DataTypePtr & data_type, WriteBuffer & buf)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */

    writeBinary(isColumnConst(*column), buf);

    ColumnPtr full_column = column->convertToFullColumnIfConst();

    serializeDataType(data_type, buf);
    writeBinary(full_column->size(), buf);

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&buf](ISerialization::SubstreamPath) -> WriteBuffer * { return &buf; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    auto serialization = data_type->getDefaultSerialization();

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*full_column, 0, 0, settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
}

ColumnPtr deserializeColumn(ReadBuffer & buf)
{
    bool is_const_column;
    readBinary(is_const_column, buf);

    auto data_type = deserializeDataType(buf);
    size_t rows;
    readBinary(rows, buf);

    ColumnPtr column = data_type->createColumn();

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &buf; };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    auto serialization = data_type->getDefaultSerialization();

    serialization->deserializeBinaryBulkStatePrefix(settings, state);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(
            "Cannot read all data when deserialize column. Rows read: " + toString(column->size()) + ". Rows expected: " + toString(rows)
                + ".",
            ErrorCodes::CANNOT_READ_ALL_DATA);

    if (is_const_column)
        column = ColumnConst::create(column, rows);

    return column;
}

void serializeBlock(const Block & block, WriteBuffer & buf)
{
    BlockOutputStreamPtr block_out
        = std::make_shared<NativeBlockOutputStream>(buf, ClickHouseRevision::getVersionRevision(), block);
    block_out->write(block);
}

void serializeBlockWithData(const Block & block, WriteBuffer & buf)
{
    BlockOutputStreamPtr block_out
        = std::make_shared<NativeBlockOutputStream>(buf, ClickHouseRevision::getVersionRevision(), block.cloneEmpty());
    block_out->write(block);
}

Block deserializeBlock(ReadBuffer & buf)
{
    BlockInputStreamPtr block_in = std::make_shared<NativeBlockInputStream>(buf, ClickHouseRevision::getVersionRevision());
    return block_in->read();
}

void serializeHeaderToProto(const Block & block, Protos::Block & proto)
{
    // we only handle header
    for (const auto & pair : block.getNamesAndTypes())
    {
        pair.toProto(*proto.add_names_and_types());
    }
}

Block deserializeHeaderFromProto(const Protos::Block & proto)
{
    std::vector<NameAndTypePair> pairs;
    for (const auto & pair_pb : proto.names_and_types())
    {
        NameAndTypePair pair;
        pair.fillFromProto(pair_pb);
        pairs.emplace_back(std::move(pair));
    }
    return Block(std::move(pairs));
}


QueryPlanStepPtr deserializePlanStep(ReadBuffer & buf, ContextPtr context)
{
    String blob;
    readBinary(blob, buf);
    Protos::QueryPlanStep proto;
    proto.ParseFromString(blob);
    auto step = deserializeQueryPlanStepFromProto(proto, context);
    return step;
}

void serializePlanStep(const QueryPlanStepPtr & step, WriteBuffer & buf)
{
    Protos::QueryPlanStep proto;
    serializeQueryPlanStepToProto(step, proto);
    String blob;
    proto.SerializeToString(&blob);
    writeBinary(blob, buf);
}


void serializeAssignmentsToProto(const Assignments & assignments, Protos::Assignments & proto)
{
    for (const auto & [k, v] : assignments)
    {
        auto pair = proto.add_pairs();
        pair->set_key(k);
        serializeASTToProto(v, *pair->mutable_value());
    }
}

Assignments deserializeAssignmentsFromProto(const Protos::Assignments & proto)
{
    Assignments res;
    for (const auto & pair : proto.pairs())
    {
        auto k = pair.key();
        auto v = deserializeASTFromProto(pair.value());
        res.emplace_back(k, v);
    }
    return res;
}
void serializeAggregateFunctionToProto(
    AggregateFunctionPtr function, const Array & parameters, const DataTypes & arg_types, Protos::AggregateFunction & proto)
{
    proto.set_func_name(function->getName());
    for (const auto & arg_type : arg_types)
        serializeDataTypeToProto(arg_type, *proto.add_arg_types());
    serializeFieldVectorToProto(parameters, *proto.mutable_parameters());
}

void serializeAggregateFunctionToProto(AggregateFunctionPtr function, const Array & parameters, Protos::AggregateFunction & proto)
{
    // use arg type in agg_func
    // removal of low_card info is possible
    serializeAggregateFunctionToProto(function, parameters, function->getArgumentTypes(), proto);
}

std::tuple<AggregateFunctionPtr, Array, DataTypes> deserializeAggregateFunctionFromProto(const Protos::AggregateFunction & proto)
{
    auto func_name = proto.func_name();
    DataTypes arg_types;
    for (auto & proto_element : proto.arg_types())
    {
        auto element = deserializeDataTypeFromProto(proto_element);
        arg_types.emplace_back(element);
    }

    auto parameters = deserializeFieldVectorFromProto<Array>(proto.parameters());

    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get(func_name, arg_types, parameters, properties);
    return {std::move(function), std::move(parameters), std::move(arg_types)};
}

template <typename Step, typename ProtoType>
inline void serializeQueryPlanStepToProtoImpl(const QueryPlanStepPtr & origin_step, ProtoType & proto)
{
    auto step = std::dynamic_pointer_cast<Step>(origin_step);
    if (!step)
    {
        auto err_msg = fmt::format("step type unmatched");
        throw Exception(err_msg, ErrorCodes::PROTOBUF_BAD_CAST);
    }
    step->toProto(proto);
}

void serializeQueryPlanStepToProto(const QueryPlanStepPtr & step, Protos::QueryPlanStep & proto)
{
    switch (step->getType())
    {
#define CASE_DEF(TYPE, VAR_NAME) \
    case IQueryPlanStep::Type::TYPE: { \
        serializeQueryPlanStepToProtoImpl<TYPE##Step, Protos::TYPE##Step>(step, *proto.mutable_##VAR_NAME##_step()); \
        return; \
    }

        APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(CASE_DEF)
#undef CASE_DEF

        default: {
            auto err_msg = fmt::format(FMT_STRING("not implemented step: {}"), static_cast<int>(step->getType()));
            throw Exception(err_msg, ErrorCodes::PROTOBUF_BAD_CAST);
        }
    }
}

template <typename Step, typename ProtoType>
inline QueryPlanStepPtr deserializeQueryPlanStepFromProtoImpl(const ProtoType & proto, ContextPtr context)
{
    auto step = Step::fromProto(proto, context);
    return step;
}

QueryPlanStepPtr deserializeQueryPlanStepFromProto(const Protos::QueryPlanStep & proto, ContextPtr context)
{
    switch (proto.step_case())
    {
#define CASE_DEF(TYPE, VAR_NAME) \
    case Protos::QueryPlanStep::StepCase::k##TYPE##Step: { \
        return deserializeQueryPlanStepFromProtoImpl<TYPE##Step, Protos::TYPE##Step>(proto.VAR_NAME##_step(), context); \
    }
        APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(CASE_DEF)
#undef CASE_DEF

        default: {
            auto err_msg = fmt::format(FMT_STRING("not implemented protobuf step: {}"), static_cast<int>(proto.step_case()));
            throw Exception(err_msg, ErrorCodes::PROTOBUF_BAD_CAST);
        }
    }
}

template <typename StepType, typename ProtoType>
bool isPlanStepEqualImpl(const IQueryPlanStep & a, const IQueryPlanStep & b)
{
    const auto & sa = reinterpret_cast<const StepType &>(a);
    const auto & sb = reinterpret_cast<const StepType &>(b);
    ProtoType pb_a;
    ProtoType pb_b;
    sa.toProto(pb_a, true);
    sb.toProto(pb_b, true);

    auto is_equal = google::protobuf::util::MessageDifferencer::Equals(pb_a, pb_b);
    return is_equal;
}

bool isPlanStepEqual(const IQueryPlanStep & a, const IQueryPlanStep & b)
{
    if (a.getType() != b.getType())
        return false;

    switch (a.getType())
    {
#define CASE_DEF(TYPE, VAR_NAME) \
    case IQueryPlanStep::Type::TYPE: { \
        return isPlanStepEqualImpl<TYPE##Step, Protos::TYPE##Step>(a, b); \
    }

        APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(CASE_DEF)

        default:
            throw Exception("unsupported step " + a.getName(), ErrorCodes::PROTOBUF_BAD_CAST);
#undef CASE_DEF
    }
}

template <typename StepType, typename ProtoType>
UInt64 hashPlanStepImpl(const IQueryPlanStep & raw_step, bool ignore_output_stream)
{
    const auto & step = reinterpret_cast<const StepType &>(raw_step);
    ProtoType proto;
    step.toProto(proto, ignore_output_stream);

    auto res = sipHash64Protobuf(proto);
    return res;
}

UInt64 hashPlanStep(const IQueryPlanStep & step, bool ignore_output_stream)
{
    switch (step.getType())
    {
#define CASE_DEF(TYPE, VAR_NAME) \
    case IQueryPlanStep::Type::TYPE: { \
        return hashPlanStepImpl<TYPE##Step, Protos::TYPE##Step>(step, ignore_output_stream); \
    }

        APPLY_STEP_PROTOBUF_TYPES_AND_NAMES(CASE_DEF)

        default:
            throw Exception("unsupported step", ErrorCodes::PROTOBUF_BAD_CAST);
#undef CASE_DEF
    }
}
}
