#include <IO/VarInt.h>
#include <Interpreters/Aggregator.h>
#include <Parsers/ASTSerDerHelper.h>
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
    const DataStream & input_stream_, TableWriteStep::TargetPtr target_,
    String output_affected_row_count_symbol_, ASTPtr query_, bool insert_select_with_profiles_)
    : ITransformingStep(input_stream_, {}, getTraits())
    , target(std::move(target_))
    , output_affected_row_count_symbol(std::move(output_affected_row_count_symbol_))
    , query(query_)
    , insert_select_with_profiles(insert_select_with_profiles_)
    , log(&Poco::Logger::get("TableFinishStep"))
{
    if (insert_select_with_profiles)
    {
        Block new_header = {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "inserted_rows")};
        output_stream = DataStream{.header = std::move(new_header)};
    }
    else
        output_stream = {input_stream_.header};
}

std::shared_ptr<IQueryPlanStep> TableFinishStep::copy(ContextPtr) const
{
    return std::make_shared<TableFinishStep>(input_streams[0], target, output_affected_row_count_symbol, query, insert_select_with_profiles);
}

void TableFinishStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);
    pipeline.addTransform(std::make_shared<TableFinishTransform>(
        getInputStreams()[0].header, target->getStorage(), settings.context, query, insert_select_with_profiles));
}

void TableFinishStep::toProto(Protos::TableFinishStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    if (!target)
        throw Exception("Target cannot be nullptr", ErrorCodes::LOGICAL_ERROR);
    target->toProto(*proto.mutable_target());
    proto.set_output_affected_row_count_symbol(output_affected_row_count_symbol);
    serializeASTToProto(query, *proto.mutable_query());
}

std::shared_ptr<TableFinishStep> TableFinishStep::fromProto(const Protos::TableFinishStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto target = TableWriteStep::Target::fromProto(proto.target(), context);
    auto output_affected_row_count_symbol = proto.output_affected_row_count_symbol();
    auto step = std::make_shared<TableFinishStep>(base_input_stream, target, output_affected_row_count_symbol, proto.has_query() ? deserializeASTFromProto(proto.query()) : nullptr);
    step->setStepDescription(step_description);
    return step;
}
}
