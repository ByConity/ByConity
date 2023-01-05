#include <QueryPlan/AssignUniqueIdStep.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AssignUniqueIdTransform.h>


namespace DB
{
AssignUniqueIdStep::AssignUniqueIdStep(const DataStream & input_stream_, String unique_id_)
    : ITransformingStep(input_stream_, AssignUniqueIdTransform::transformHeader(input_stream_.header, unique_id_), {})
    , unique_id(std::move(unique_id_))
{
}

void AssignUniqueIdStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream = input_streams[0];
    output_stream->header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), unique_id});
}

void AssignUniqueIdStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<AssignUniqueIdTransform>(header, unique_id); });
}

void AssignUniqueIdStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);
    writeStringBinary(unique_id, buf);
}

QueryPlanStepPtr AssignUniqueIdStep::deserialize(ReadBuffer & buf, ContextPtr)
{
    String step_description;
    readBinary(step_description, buf);

    DataStream input_stream = deserializeDataStream(buf);
    String unique_id;
    readStringBinary(unique_id, buf);
    return std::make_unique<AssignUniqueIdStep>(input_stream, unique_id);
}

std::shared_ptr<IQueryPlanStep> AssignUniqueIdStep::copy(ContextPtr) const
{
    return std::make_unique<AssignUniqueIdStep>(input_streams[0], unique_id);
}

}
