#include <QueryPlan/TopNFilteringStep.h>
#include <IO/Operators.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/TopNFilteringTransform.h>

namespace DB
{

TopNFilteringStep::TopNFilteringStep(const DataStream & input_stream_,
                                     SortDescription sort_description_,
                                     UInt64 size_,
                                     TopNModel model_)
    : ITransformingStep(input_stream_, input_stream_.header, {}),
    sort_description(std::move(sort_description_)),
    size(size_),
    model(model_)
{}

void TopNFilteringStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void TopNFilteringStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform(
        [&](const Block & header) { return std::make_shared<TopNFilteringTransform>(header, sort_description, size, model); });
}

void TopNFilteringStep::describeActions(FormatSettings &) const
{
}

void TopNFilteringStep::describeActions(JSONBuilder::JSONMap &) const
{
}

std::shared_ptr<TopNFilteringStep> TopNFilteringStep::fromProto(const Protos::TopNFilteringStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription sort_description;
    for (const auto & proto_element : proto.sort_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        sort_description.emplace_back(std::move(element));
    }
    auto size = proto.size();
    auto model = TopNModelConverter::fromProto(proto.model());
    auto step = std::make_shared<TopNFilteringStep>(base_input_stream, sort_description, size, model);
    step->setStepDescription(step_description);
    return step;
}

void TopNFilteringStep::toProto(Protos::TopNFilteringStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    for (const auto & element : sort_description)
        element.toProto(*proto.add_sort_description());
    proto.set_size(size);
    proto.set_model(TopNModelConverter::toProto(model));
}

std::shared_ptr<IQueryPlanStep> TopNFilteringStep::copy(ContextPtr) const
{
    return std::make_shared<TopNFilteringStep>(input_streams[0], sort_description, size, model);
}

}
