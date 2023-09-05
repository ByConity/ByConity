#include <IO/VarInt.h>
#include <Interpreters/Aggregator.h>
#include <QueryPlan/TableFinishStep.h>
#include <Storages/IStorage.h>
#include "Processors/Transforms/TableFinishTransform.h"

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

TableFinishStep::TableFinishStep(
    const DataStream & input_stream_, TableWriteStep::TargetPtr target_, String output_affected_row_count_symbol_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , target(std::move(target_))
    , output_affected_row_count_symbol(std::move(output_affected_row_count_symbol_))
    , log(&Poco::Logger::get("TableFinishStep"))
{
}

std::shared_ptr<IQueryPlanStep> TableFinishStep::copy(ContextPtr) const
{
    return std::make_shared<TableFinishStep>(input_streams[0], target, output_affected_row_count_symbol);
}

void TableFinishStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<TableFinishTransform>(getInputStreams()[0].header, target->getStorage(), settings.context));
}

void TableFinishStep::serialize(WriteBuffer & buffer) const
{
    IQueryPlanStep::serializeImpl(buffer);
    target->serialize(buffer);
    writeStringBinary(output_affected_row_count_symbol, buffer);
}

QueryPlanStepPtr TableFinishStep::deserialize(ReadBuffer & buffer, ContextPtr & context)
{
    String step_description;
    readBinary(step_description, buffer);
    DataStream input_stream = deserializeDataStream(buffer);
    TableWriteStep::TargetPtr target = TableWriteStep::Target::deserialize(buffer, context);

    String output_affected_row_count_symbol;
    readStringBinary(output_affected_row_count_symbol, buffer);
    return std::make_shared<TableFinishStep>(input_stream, target, output_affected_row_count_symbol);
}
}
