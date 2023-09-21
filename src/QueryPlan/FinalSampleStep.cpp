#include <QueryPlan/FinalSampleStep.h>

namespace DB
{
void FinalSampleStep::toProto(Protos::FinalSampleStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    proto.set_sample_size(sample_size);
    proto.set_max_chunk_size(max_chunk_size);
}

std::shared_ptr<FinalSampleStep> FinalSampleStep::fromProto(const Protos::FinalSampleStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto sample_size = proto.sample_size();
    auto max_chunk_size = proto.max_chunk_size();
    auto step = std::make_shared<FinalSampleStep>(base_input_stream, sample_size, max_chunk_size);
    step->setStepDescription(step_description);
    return step;
}
}