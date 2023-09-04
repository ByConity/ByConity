#include <QueryPlan/IntersectOrExceptStep.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/IntersectOrExceptTransform.h>
#include <Processors/ResizeProcessor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static Block checkHeaders(const DataStreams & input_streams_)
{
    if (input_streams_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot perform intersect/except on empty set of query plan steps");

    Block res = input_streams_.front().header;
    for (const auto & stream : input_streams_)
        assertBlocksHaveEqualStructure(stream.header, res, "IntersectOrExceptStep");

    return res;
}

IntersectOrExceptStep::IntersectOrExceptStep(
    DataStreams input_streams_ , Operator operator_ , size_t max_threads_)
    : header(checkHeaders(input_streams_))
    , current_operator(operator_)
    , max_threads(max_threads_)
{
    input_streams = std::move(input_streams_);
    output_stream = DataStream{.header = header};
}

QueryPipelinePtr IntersectOrExceptStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &)
{
    auto pipeline = std::make_unique<QueryPipeline>();
    QueryPipelineProcessorsCollector collector(*pipeline, this);

    if (pipelines.empty())
    {
        pipeline->init(Pipe(std::make_shared<NullSource>(output_stream->header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    for (auto & cur_pipeline : pipelines)
    {
        /// Just in case.
        if (!isCompatibleHeader(cur_pipeline->getHeader(), getOutputStream().header))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputStream().header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform([&](const Block & cur_header)
            {
                return std::make_shared<ExpressionTransform>(cur_header, converting_actions);
            });
        }

        /// For the case of union.
        cur_pipeline->addTransform(std::make_shared<ResizeProcessor>(header, cur_pipeline->getNumStreams(), 1));
    }

    *pipeline = QueryPipeline::unitePipelines(std::move(pipelines), max_threads);
    pipeline->addTransform(std::make_shared<IntersectOrExceptTransform>(header, current_operator));

    processors = collector.detachProcessors();
    return pipeline;
}

void IntersectOrExceptStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void IntersectOrExceptStep::serialize(WriteBuffer & buf) const
{
    writeBinary(input_streams.size(), buf);
    for (const auto & input_stream : input_streams)
        serializeDataStream(input_stream, buf);
    
    serializeEnum(current_operator, buf);
    
}

QueryPlanStepPtr IntersectOrExceptStep::deserialize(ReadBuffer & buf, ContextPtr)
{
    size_t size;
    readBinary(size, buf);
    
    DataStreams input_streams(size);
    for (size_t i = 0; i < size; ++i)
        input_streams[i] = deserializeDataStream(buf);

    Operator current_operator;
    deserializeEnum(current_operator, buf);
 
    return std::make_shared<IntersectOrExceptStep>(input_streams, current_operator);
}

String IntersectOrExceptStep::getOperator() const 
{
    if (current_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL) {
        return "INTERSECT_ALL";
    }
    if (current_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT) {
        return "INTERSECT_DISTINCT";
    }
    if (current_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL) {
        return "EXCEPT_ALL";
    }
    if (current_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT) {
        return "EXCEPT_DISTINCT";
    }
    if (current_operator == ASTSelectIntersectExceptQuery::Operator::UNKNOWN) {
        return "UNKNOWN";
    }
    return "UNKNOWN";
}
}
