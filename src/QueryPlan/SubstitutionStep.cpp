
#include <QueryPlan/SubstitutionStep.h>
#include <DataStreams/materializeBlock.h>
#include <Processors/Transforms/SubstitutionTransform.h>
#include <Processors/QueryPipeline.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

SubstitutionStep::SubstitutionStep(
    const DataStream & input_stream_,
    const std::unordered_map<String, String> & name_substitution_info_):
    ITransformingStep(input_stream_,
                      SubstitutionTransform::transformHeader(input_stream_.header, name_substitution_info_), ITransformingStep::Traits()),
    name_substitution_info(name_substitution_info_) {}

void SubstitutionStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = SubstitutionTransform::transformHeader(input_streams_[0].header, name_substitution_info);
}

void SubstitutionStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType /*stream_type*/)
    {
        return std::make_shared<SubstitutionTransform>(header, name_substitution_info);
    });
}

void SubstitutionStep::describeActions(JSONBuilder::JSONMap & map) const
{
    String substitution_name;
    for (const auto & substitution: name_substitution_info)
        substitution_name += substitution.first + " : " + substitution.second + ",";
    if (!substitution_name.empty())
        substitution_name.pop_back();
    map.add("Substitution Name Mapping", substitution_name);
}

void SubstitutionStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "substitute with column map: " << '\n';
    for (const auto & substitution : name_substitution_info)
        settings.out << prefix << substitution.first << " : " << substitution.second << "\n";
}

std::shared_ptr<IQueryPlanStep> SubstitutionStep::copy(ContextPtr) const
{
    throw Exception("SubstitutionStep can not copy", ErrorCodes::NOT_IMPLEMENTED);
}


}
