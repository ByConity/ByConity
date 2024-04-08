#include <IO/OutfileCommon.h>
#include <QueryPlan/OutfileWriteStep.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/OutfileWriteTransform.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {.preserves_distinct_columns = true,
         .returns_single_stream = false,
         .preserves_number_of_streams = true,
         .preserves_sorting = true},
        {.preserves_number_of_rows = true}};
}

OutfileWriteStep::OutfileWriteStep(const DataStream & input_stream_, OutfileTargetPtr outfile_target_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits()), outfile_target(outfile_target_)
{
}

void OutfileWriteStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream = DataStream{.header = std::move((input_streams_[0].header))};
}

void OutfileWriteStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_context)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([](const Block & header) { return std::make_shared<MaterializingTransform>(header); });

    std::shared_ptr<WriteBuffer> out_buf = outfile_target->getOutfileBuffer();
    OutputFormatPtr output_format = FormatFactory::instance().getOutputFormatParallelIfPossible(
        outfile_target->getFormat(), *out_buf, pipeline.getHeader(), build_context.context, outfile_target->outToMultiFile(), {});
    output_format->setOutFileTarget(outfile_target);
    output_format->setAutoFlush();

    pipeline.addTransform(std::make_shared<OutfileWriteTransform>(
        std::move(output_format), output_stream->header, build_context.context));
}

std::shared_ptr<IQueryPlanStep> OutfileWriteStep::copy(ContextPtr) const
{
    return std::make_shared<OutfileWriteStep>(input_streams[0], outfile_target);
}

void OutfileWriteStep::toProto(Protos::OutfileWriteStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    if (!outfile_target)
        throw Exception("outfile target cannot be nullptr", ErrorCodes::LOGICAL_ERROR);
    outfile_target->toProto(*proto.mutable_outfile_target());
}

std::shared_ptr<OutfileWriteStep> OutfileWriteStep::fromProto(const Protos::OutfileWriteStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto outfile_target = OutfileTarget::fromProto(proto.outfile_target(), context);
    return std::make_shared<OutfileWriteStep>(base_input_stream, outfile_target);
}

}
