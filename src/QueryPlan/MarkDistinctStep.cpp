#include <QueryPlan/MarkDistinctStep.h>

#include <DataTypes/DataTypeHelper.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterConsumer.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/MarkDistinctTransform.h>

namespace DB
{
MarkDistinctStep::MarkDistinctStep(const DataStream & input_stream_, String marker_symbol_, std::vector<String> distinct_symbols_)
    : ITransformingStep(input_stream_, MarkDistinctTransform::transformHeader(input_stream_.header, marker_symbol_), {}, true), marker_symbol(marker_symbol_), distinct_symbols(std::move(distinct_symbols_))
{
}

void MarkDistinctStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream = input_streams[0];
    output_stream->header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeUInt8>(), marker_symbol});
}

void MarkDistinctStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    // TODO Add Local Exchange
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<MarkDistinctTransform>(header, marker_symbol, distinct_symbols); });
}

std::shared_ptr<MarkDistinctStep> MarkDistinctStep::fromProto(const Protos::MarkDistinctStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto marker_symbol = proto.marker_symbol();
    std::vector<String> distinct_symbols;
    for (const auto & element : proto.distinct_symbols())
        distinct_symbols.emplace_back(element);
    auto step = std::make_shared<MarkDistinctStep>(base_input_stream, marker_symbol, distinct_symbols);
    step->setStepDescription(step_description);
    return step;
}

void MarkDistinctStep::toProto(Protos::MarkDistinctStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_marker_symbol(marker_symbol);
    for (const auto & element : distinct_symbols)
        proto.add_distinct_symbols(element);
}

std::shared_ptr<IQueryPlanStep> MarkDistinctStep::copy(ContextPtr) const
{
    return std::make_shared<MarkDistinctStep>(input_streams[0], marker_symbol, distinct_symbols);
}

}
