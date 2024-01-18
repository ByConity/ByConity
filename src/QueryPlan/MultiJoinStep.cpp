#include <QueryPlan/MultiJoinStep.h>

#include <Optimizer/Cascades/Memo.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/Signature/ExpressionReorderNormalizer.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/CTEInfo.h>

namespace DB
{
QueryPipelinePtr MultiJoinStep::updatePipeline(QueryPipelines, const BuildQueryPipelineSettings &)
{
    throw Exception("UNREACHABLE MultiJoinStep::updatePipeline()", ErrorCodes::NOT_IMPLEMENTED);
}

void MultiJoinStep::toProto(Protos::MultiJoinStep & proto, bool) const
{
    graph.toProto(*proto.mutable_graph());
}

QueryPlanStepPtr MultiJoinStep::deserialize(ReadBuffer &, ContextPtr)
{
    throw Exception("UNREACHABLE MultiJoinStep::deserialize()!", ErrorCodes::NOT_IMPLEMENTED);
}

std::shared_ptr<IQueryPlanStep> MultiJoinStep::copy(ContextPtr) const
{
    return std::make_shared<MultiJoinStep>(output_stream.value(), graph);
}

void MultiJoinStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
}

}
