#include <QueryPlan/TableWriteStep.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Processors/Sources/SinkToOutputStream.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ProcessorToOutputStream.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Transforms/TableWriteTransform.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <Common/RpcClientPool.h>

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

TableWriteStep::TableWriteStep(
    const DataStream & input_stream_, TargetPtr target_, bool insert_select_with_profiles_, String output_affected_row_count_symbol_)
    : ITransformingStep(input_stream_, {}, getTraits())
    , target(target_)
    , insert_select_with_profiles(insert_select_with_profiles_)
    , output_affected_row_count_symbol(output_affected_row_count_symbol_)
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

Block TableWriteStep::getHeader(const NamesAndTypes & input_columns)
{
    Block sample_block;
    for (const auto & input_column : input_columns)
        sample_block.insert(ColumnWithTypeAndName(input_column.type, input_column.name));
    return sample_block;
}

BlockOutputStreams TableWriteStep::createOutputStream(
    StoragePtr target_table,
    const BuildQueryPipelineSettings & settings,
    Block & header,
    size_t max_threads,
    bool no_destination,
    ASTPtr query)
{
    BlockOutputStreams out_streams;
    size_t out_streams_size = 1;
    auto query_settings = settings.context->getSettingsRef();
    if (target_table->supportsParallelInsert(settings.context) && query_settings.max_insert_threads > 1)
    {
        LOG_INFO(getLogger("TableWriteStep"),
                 fmt::format("createOutputStream support parallel insert, max threads:{}, max insert threads.size:{}", max_threads, query_settings.max_insert_threads));
        out_streams_size = std::min(size_t(query_settings.max_insert_threads), max_threads);
    }

    for (size_t i = 0; i < out_streams_size; ++i)
    {
        /// We create a pipeline of several streams, into which we will write data.
        BlockOutputStreamPtr out;

        auto metadata_snapshot = target_table->getInMemoryMetadataPtr();
        /// NOTE: we explicitly ignore bound materialized views when inserting into Kafka Storage.
        ///       Otherwise we'll get duplicates when MV reads same rows again from Kafka.
        if (target_table->noPushingToViews() && !no_destination)
            out = target_table->write(query, metadata_snapshot, settings.context);
        else
            out = std::make_shared<PushingToViewsBlockOutputStream>(
                target_table, metadata_snapshot, settings.context, query, no_destination);

        /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.

        /// Checking constraints. It must be done after calculation of all defaults, so we can check them on calculated columns.
        if (const auto & constraints = metadata_snapshot->getConstraints(); !constraints.empty())
            out = std::make_shared<CheckConstraintsBlockOutputStream>(
                target_table->getStorageID(), out, out->getHeader(), metadata_snapshot->getConstraints(), settings.context);

        bool null_as_default = query_settings.insert_null_as_default;
        if (null_as_default)
        {
            const auto & input_columns = input_streams[0].header.getColumnsWithTypeAndName();
            const auto & query_columns = header.getColumnsWithTypeAndName();
            const auto & output_columns = metadata_snapshot->getColumns();

            if (input_columns.size() == query_columns.size())
            {
                for (size_t col_idx = 0; col_idx < query_columns.size(); ++col_idx)
                {
                    /// Change query sample block columns to Nullable to allow inserting nullable columns, where NULL values will be substituted with
                    /// default column values (in AddingDefaultsTransform), so all values will be cast correctly.
                    if (isNullableOrLowCardinalityNullable(input_columns[col_idx].type) && !isNullableOrLowCardinalityNullable(query_columns[col_idx].type) && output_columns.has(query_columns[col_idx].name))
                        header.setColumn(col_idx, ColumnWithTypeAndName(makeNullableOrLowCardinalityNullable(query_columns[col_idx].column), makeNullableOrLowCardinalityNullable(query_columns[col_idx].type), query_columns[col_idx].name));
                }
            }
        }

        /// Actually we don't know structure of input blocks from query/table,
        /// because some clients break insertion protocol (columns != header)
        out = std::make_shared<AddingDefaultBlockOutputStream>(
            out, header, metadata_snapshot->getColumns(), settings.context, null_as_default);

        auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
        out_wrapper->setProcessListElement(settings.context->getProcessListElement());
        out_streams.emplace_back(std::move(out_wrapper));
    }

    return out_streams;
}

void TableWriteStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    switch (target->getTargetType())
    {
        case TargetType::INSERT: {
            auto * insert_target = dynamic_cast<TableWriteStep::InsertTarget *>(target.get());
            auto target_storage = DatabaseCatalog::instance().getTable(insert_target->getStorageID(), settings.context);

            auto insert_target_header = getHeader(insert_target->getColumns());
            auto out_streams = createOutputStream(
                target_storage, settings, insert_target_header, settings.context->getSettingsRef().max_threads, false, insert_target->getQuery());

            if (out_streams.empty())
                throw Exception("No output stream when transform TableWriteStep", ErrorCodes::LOGICAL_ERROR);

            const auto & header = out_streams[0]->getHeader();
            auto actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position);
            auto actions = std::make_shared<ExpressionActions>(
                actions_dag, ExpressionActionsSettings::fromContext(settings.context, CompileExpressions::yes));

            pipeline.addSimpleTransform(
                [&](const Block & in_header) -> ProcessorPtr { return std::make_shared<ExpressionTransform>(in_header, actions); });

            size_t min_insert_block_size_rows = settings.context->getSettingsRef().min_insert_block_size_rows;
            size_t min_insert_block_size_bytes = settings.context->getSettingsRef().min_insert_block_size_bytes;
            /// It's important to squash blocks as early as possible (before other transforms),
            ///  because other transforms may work inefficient if block size is small.

            /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
            /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
            if (!(settings.context->getSettingsRef().insert_distributed_sync && target_storage->isRemote()) && settings.context->getSettingsRef().enable_insert_squashing)
            {
                pipeline.addSimpleTransform(
                    [&](const Block & current_header) -> ProcessorPtr {
                        return std::make_shared<SimpleSquashingChunksTransform>(current_header, min_insert_block_size_rows, min_insert_block_size_bytes);}
                );
                LOG_INFO(getLogger("TableWriteStep"), fmt::format("squash min insert block size rows:{}, min insert block size bytes:{}", min_insert_block_size_rows, min_insert_block_size_bytes));
            }
            //LOG_DEBUG(getLogger("TableWriteStep"), fmt::format("output header: {}", stream->getHeader().dumpStructure()));
            pipeline.resize(out_streams.size());
            LOG_INFO(getLogger("TableWriteStep"), fmt::format("pipeline size: {}, out streams size {}", pipeline.getNumStreams(), out_streams.size()));

            if (insert_select_with_profiles)
            {
                pipeline.addSimpleTransform([&](const Block &, QueryPipeline::StreamType type) -> ProcessorPtr
                {
                    if (type != QueryPipeline::StreamType::Main)
                        return nullptr;

                    auto stream = std::move(out_streams.back());
                    out_streams.pop_back();

                    return std::make_shared<ProcessorToOutputStream>(std::move(stream), output_affected_row_count_symbol);
                });
            }
            else
            {
                pipeline.addSimpleTransform(
                    [&]([[maybe_unused]] const Block & in_header) -> ProcessorPtr {
                        auto stream = std::move(out_streams.back());
                        out_streams.pop_back();
                        return std::make_shared<TableWriteTransform>(stream, insert_target_header, insert_target->getStorage(), settings.context);}
                    );
                break;
            }
        }
    }
}

void TableWriteStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    if (insert_select_with_profiles)
    {
        Block new_header
            = {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), output_affected_row_count_symbol)};
        output_stream = DataStream{.header = std::move(new_header)};
    }
    else
        output_stream = DataStream{.header = std::move((input_streams_[0].header))};
}

std::shared_ptr<IQueryPlanStep> TableWriteStep::copy(ContextPtr) const
{
    return std::make_shared<TableWriteStep>(input_streams[0], target, insert_select_with_profiles, output_affected_row_count_symbol);
}

void TableWriteStep::toProto(Protos::TableWriteStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());

    if (!target)
        throw Exception("Target cannot be nullptr", ErrorCodes::LOGICAL_ERROR);
    target->toProto(*proto.mutable_target());
    proto.set_insert_select_with_profiles(insert_select_with_profiles);
    proto.set_output_affected_row_count_symbol(output_affected_row_count_symbol);
}

std::shared_ptr<TableWriteStep> TableWriteStep::fromProto(const Protos::TableWriteStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto target = TableWriteStep::Target::fromProto(proto.target(), context);
    bool insert_select_with_profiles = proto.has_insert_select_with_profiles() ? proto.insert_select_with_profiles()
                                                                               : context->getSettingsRef().insert_select_with_profiles;
    String output_affected_row_count_symbol
        = proto.has_output_affected_row_count_symbol() ? proto.output_affected_row_count_symbol() : "inserted_rows";
    auto step = std::make_shared<TableWriteStep>(base_input_stream, target, insert_select_with_profiles, output_affected_row_count_symbol);
    step->setStepDescription(step_description);
    return step;
}

void TableWriteStep::allocate(const ContextPtr & context)
{
    auto storage_id = target->getStorage()->prepareTableWrite(context);
    if (auto * input_target = dynamic_cast<InsertTarget *>(target.get()))
        target = std::make_shared<InsertTarget>(input_target->getStorage(), storage_id, input_target->getColumns(), input_target->getQuery());
    else
        throw Exception("unknown TableWrite::Target", ErrorCodes::LOGICAL_ERROR);
}

void TableWriteStep::Target::toProto(Protos::TableWriteStep::Target & proto) const
{
    switch (getTargetType())
    {
        case TargetType::INSERT: {
            const auto * ptr = dynamic_cast<const TableWriteStep::InsertTarget *>(this);
            ptr->toProtoImpl(*proto.mutable_insert_target());
            return;
        }

        default:
            throw Exception(fmt::format("unknown TableWrite::Target {}", static_cast<int>(getTargetType())), ErrorCodes::LOGICAL_ERROR);
    }
}

TableWriteStep::TargetPtr TableWriteStep::Target::fromProto(const Protos::TableWriteStep::Target & proto, ContextPtr context)
{
    switch (proto.target_case())
    {
        case Protos::TableWriteStep::Target::TargetCase::kInsertTarget:
            return TableWriteStep::InsertTarget::createFromProtoImpl(proto.insert_target(), context);
        default:
            throw Exception(fmt::format("unknown TableWrite::Target {}", static_cast<int>(proto.target_case())), ErrorCodes::LOGICAL_ERROR);
    }
}

void TableWriteStep::InsertTarget::toProtoImpl(Protos::TableWriteStep::InsertTarget & proto) const
{
    storage_id.toProto(*proto.mutable_storage_id());
    for (const auto & element : columns)
        element.toProto(*proto.add_columns());
    serializeASTToProto(query, *proto.mutable_query());
}

std::shared_ptr<TableWriteStep::InsertTarget>
TableWriteStep::InsertTarget::createFromProtoImpl(const Protos::TableWriteStep::InsertTarget & proto, ContextPtr context)
{
    auto storage_id = StorageID::fromProto(proto.storage_id(), context);
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    NamesAndTypes columns;
    for (const auto & proto_element : proto.columns())
    {
        NameAndTypePair element;
        element.fillFromProto(proto_element);
        columns.emplace_back(std::move(element));
    }
    ASTPtr query;
    if (proto.has_query())
        query = deserializeASTFromProto(proto.query());

    auto step = std::make_shared<TableWriteStep::InsertTarget>(storage, storage_id, columns, query);

    return step;
}

String TableWriteStep::InsertTarget::toString(const String & remove_tenant_id) const
{
    auto tmp_id = storage_id;
    tmp_id.database_name = getOriginalDatabaseName(tmp_id.database_name, remove_tenant_id);
    return "Insert " + tmp_id.getNameForLogs();
}

NameToNameMap TableWriteStep::InsertTarget::getTableColumnToInputColumnMap(const Names & input_columns) const
{
    NameToNameMap name_to_name_map;
    if (columns.size() != input_columns.size())
        throw Exception("Number of columns in insert target doesn't match number of columns in input", ErrorCodes::LOGICAL_ERROR);
    for (size_t i = 0; i < input_columns.size(); ++i)
        name_to_name_map.emplace(columns[i].name, input_columns[i]);
    return name_to_name_map;
}
}
