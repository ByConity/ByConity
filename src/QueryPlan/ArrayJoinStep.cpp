#include <QueryPlan/ArrayJoinStep.h>
#include <Processors/Transforms/ArrayJoinTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ArrayJoinStep::ArrayJoinStep(const DataStream & input_stream_, ArrayJoinActionPtr array_join_)
    : ITransformingStep(
        input_stream_,
        ArrayJoinTransform::transformHeader(input_stream_.header, array_join_),
        getTraits())
    , array_join(std::move(array_join_))
{
}

void ArrayJoinStep::updateInputStream(DataStream input_stream, Block result_header)
{
    output_stream = createOutputStream(
            input_stream,
            ArrayJoinTransform::transformHeader(input_stream.header, array_join),
            getDataStreamTraits());

    input_streams.clear();
    input_streams.emplace_back(std::move(input_stream));
    res_header = std::move(result_header);
}

void ArrayJoinStep::setInputStreams(const DataStreams & input_streams_)
{
    updateInputStream(input_streams_[0], res_header);
}

void ArrayJoinStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<ArrayJoinTransform>(header, array_join, on_totals);
    });

    if (res_header && !blocksHaveEqualStructure(res_header, output_stream->header))
    {
        auto actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                res_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

        auto actions = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, actions);
        });
    }
}

void ArrayJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    settings.out << prefix << (array_join->is_left ? "LEFT " : "") << "ARRAY JOIN ";
    for (const auto & column : array_join->columns)
    {
        if (!first)
            settings.out << ", ";
        first = false;


        settings.out << column;
    }
    settings.out << '\n';
}

void ArrayJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Left", array_join->is_left);

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : array_join->columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

void ArrayJoinStep::toProto(Protos::ArrayJoinStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());

    if (!array_join)
        throw Exception("ArrayJoin cannot be nullptr", ErrorCodes::LOGICAL_ERROR);
    array_join->toProto(*proto.mutable_array_join());
}

std::shared_ptr<ArrayJoinStep> ArrayJoinStep::fromProto(const Protos::ArrayJoinStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto array_join = ArrayJoinAction::fromProto(proto.array_join(), context);
    auto step = std::make_shared<ArrayJoinStep>(base_input_stream, array_join);
    step->setStepDescription(step_description);
    return step;
}

std::shared_ptr<IQueryPlanStep> ArrayJoinStep::copy(ContextPtr) const
{
    return std::make_shared<ArrayJoinStep>(input_streams[0], array_join);
}

}
