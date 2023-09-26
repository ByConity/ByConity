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

String IntersectOrExceptStep::getOperatorStr() const
{
    const auto & name = ASTSelectIntersectExceptQuery::OperatorConverter::toString(current_operator);
    if (name.empty())
        return "UNKNOWN";
    return name;
}

ASTSelectIntersectExceptQuery::Operator IntersectOrExceptStep::getOperator() const
{
    return current_operator;
}

void IntersectOrExceptStep::toProto(Protos::IntersectOrExceptStep & proto, bool) const
{
    for (const auto & element : input_streams)
        element.toProto(*proto.add_input_streams());
    proto.set_current_operator(OperatorConverter::toProto(current_operator));
    proto.set_max_threads(max_threads);
}

std::shared_ptr<IntersectOrExceptStep> IntersectOrExceptStep::fromProto(const Protos::IntersectOrExceptStep & proto, ContextPtr)
{
    DataStreams input_streams;
    for (const auto & proto_element : proto.input_streams())
    {
        DataStream element;
        element.fillFromProto(proto_element);
        input_streams.emplace_back(std::move(element));
    }
    auto current_operator = OperatorConverter::fromProto(proto.current_operator());
    auto max_threads = proto.max_threads();
    auto step = std::make_shared<IntersectOrExceptStep>(input_streams, current_operator, max_threads);
    return step;
}
}
