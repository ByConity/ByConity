#include <QueryPlan/ExplainAnalyzeStep.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExplainAnalyzeTransform.h>

#include <type_traits>
#include <utility>

namespace DB
{


ExplainAnalyzeStep::ExplainAnalyzeStep(
    const DataStream & input_stream_,
    const String & output_name_,
    ASTExplainQuery::ExplainKind kind_,
    ContextMutablePtr context_,
    std::shared_ptr<QueryPlan> query_plan_ptr_,
    QueryPlanSettings settings_)
    : ITransformingStep(std::move(input_stream_), {{std::make_shared<DataTypeString>(), output_name_}}, {})
    , kind(kind_)
    , context(context_)
    , query_plan_ptr(query_plan_ptr_)
    , settings(settings_)
{
}

void ExplainAnalyzeStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & )
{
    if (!query_plan_ptr)
        throw Exception("QueryPlan is not set", ErrorCodes::LOGICAL_ERROR);
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) {
        return std::make_shared<ExplainAnalyzeTransform>(
            header, output_stream->header, kind, query_plan_ptr, context, segment_descriptions, settings);
    });
}

void ExplainAnalyzeStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
}

std::shared_ptr<IQueryPlanStep> ExplainAnalyzeStep::copy(ContextPtr) const
{
    return std::make_shared<ExplainAnalyzeStep>(input_streams[0], getOutputName(), kind, context, query_plan_ptr, settings);
}

}
