#include <IO/VarInt.h>
#include <Interpreters/Aggregator.h>
#include <Parsers/ASTSerDerHelper.h>
#include <QueryPlan/TableFinishStep.h>
#include <Storages/IStorage.h>
#include <Common/Stopwatch.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Transforms/TableFinishTransform.h>
#include <Storages/RemoteFile/StorageCnchHDFS.h>
#include <Storages/RemoteFile/StorageCnchS3.h>

namespace ProfileEvents
{
    extern const int TableFinishStepPreClearHDFSTableMicroseconds;
    extern const int TableFinishStepPreClearS3TableMicroseconds;
}

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
    , log(getLogger("TableFinishStep"))
{
    if (insert_select_with_profiles)
    {
        Block new_header
            = {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), output_affected_row_count_symbol)};
        output_stream = DataStream{.header = std::move(new_header)};
    }
    else
        output_stream = {input_stream_.header};
}


void TableFinishStep::preExecute(ContextMutablePtr query_context)
{
    // FIXME(jiashuo): not thred-safe, don't `insert overwrite` same table or different table with same path in parallel
    if (auto * hdfs_table = dynamic_cast<StorageCnchHDFS *>(target->getStorage().get()))
    {
        if (auto * insert = dynamic_cast<ASTInsertQuery *>(query.get()); insert->is_overwrite)
        {
            Stopwatch watch;
            query_context->setSetting("prefer_cnch_catalog", hdfs_table->settings.prefer_cnch_catalog.value);
            hdfs_table->clear(query_context);
            ProfileEvents::increment(ProfileEvents::TableFinishStepPreClearHDFSTableMicroseconds, watch.elapsedMicroseconds());
        }
    }
    else if (auto * s3_table = dynamic_cast<StorageCnchS3 *>(target->getStorage().get()))
    {
        if (auto * insert = dynamic_cast<ASTInsertQuery *>(query.get()); insert->is_overwrite)
        {
            Stopwatch watch;
            query_context->setSetting("prefer_cnch_catalog", s3_table->settings.prefer_cnch_catalog.value);
            s3_table->clear(query_context);
            ProfileEvents::increment(ProfileEvents::TableFinishStepPreClearS3TableMicroseconds, watch.elapsedMicroseconds());
        }
    }
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
    proto.set_insert_select_with_profiles(insert_select_with_profiles);
    serializeASTToProto(query, *proto.mutable_query());
}

std::shared_ptr<TableFinishStep> TableFinishStep::fromProto(const Protos::TableFinishStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto target = TableWriteStep::Target::fromProto(proto.target(), context);
    auto output_affected_row_count_symbol = proto.output_affected_row_count_symbol();
    bool insert_select_with_profiles = proto.has_insert_select_with_profiles() ? proto.insert_select_with_profiles()
                                                                               : context->getSettingsRef().insert_select_with_profiles;
    auto step = std::make_shared<TableFinishStep>(
        base_input_stream, target, output_affected_row_count_symbol, proto.has_query() ? deserializeASTFromProto(proto.query()) : nullptr, insert_select_with_profiles);
    step->setStepDescription(step_description);
    return step;
}
}
