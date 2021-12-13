#include <Processors/QueryPlan/ExchangeStep.h>


namespace DB
{


ExchangeStep::ExchangeStep(DataStreams input_streams_, const ExchangeMode & mode_, Partitioning schema_, bool keep_order_)
    : exchange_type(mode_)
    , schema(std::move(schema_))
    , keep_order(keep_order_)
{
    input_streams = std::move(input_streams_);
    output_stream = DataStream{.header = input_streams[0].header};
}

QueryPipelinePtr ExchangeStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &)
{
    return std::move(pipelines[0]);
}

void ExchangeStep::serialize(WriteBuffer &) const
{
    throw Exception("ExchangeStep should be rewritten into RemoteSourceStep", ErrorCodes::NOT_IMPLEMENTED);
}

QueryPlanStepPtr ExchangeStep::deserialize(ReadBuffer &, ContextPtr &)
{
    throw Exception("ExchangeStep should be rewritten into RemoteSourceStep", ErrorCodes::NOT_IMPLEMENTED);
}

}
