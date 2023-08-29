#include <QueryPlan/PlanSerDerHelper.h>
#include <Processors/QueryPipeline.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <IO/WriteHelpers.h>
#include <QueryPlan/ReadStorageRowCountStep.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Interpreters/JoinedTables.h>
#include <common/types.h>
#include <common/scope_guard_safe.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>


namespace DB
{

ReadStorageRowCountStep::ReadStorageRowCountStep(Block output_header, StorageID storage_id_, ASTPtr query_, AggregateDescription agg_desc_, UInt64 num_rows_)
    : ISourceStep(DataStream{.header = output_header})
    , storage_id(storage_id_)
    , query(query_)
    , agg_desc(agg_desc_)
    , num_rows(num_rows_)
{
}

std::shared_ptr<IQueryPlanStep> ReadStorageRowCountStep::copy(ContextPtr ) const
{
    return std::make_shared<ReadStorageRowCountStep>(output_stream->header, storage_id, query, agg_desc, num_rows);
}
void ReadStorageRowCountStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & context)
{
    const auto & func = agg_desc.function;

    const AggregateFunctionCount & agg_count = static_cast<const AggregateFunctionCount &>(*func);

    std::vector<char> state(agg_count.sizeOfData());
    AggregateDataPtr place = state.data();

    agg_count.create(place);
    SCOPE_EXIT_MEMORY_SAFE(agg_count.destroy(place));

    agg_count.set(place, num_rows);
    auto column = ColumnAggregateFunction::create(func);
    column->insertFrom(place);

    size_t arguments_size = agg_desc.argument_names.size();
    DataTypes argument_types(arguments_size);
    for (size_t j = 0; j < arguments_size; ++j)
        argument_types[j] = std::make_shared<DataTypeInt64>();

    Block block_with_count{
        {std::move(column), std::make_shared<DataTypeAggregateFunction>(func, argument_types, agg_desc.parameters), agg_desc.column_name}};

    auto istream = std::make_shared<OneBlockInputStream>(block_with_count);
    auto pipe = Pipe(std::make_shared<SourceFromInputStream>(istream));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    if (context.context)
        pipeline.addInterpreterContext(context.context);
}

void ReadStorageRowCountStep::serialize(WriteBuffer & buffer) const
{
    serializeBlock(output_stream->header, buffer);
    storage_id.serialize(buffer);
    serializeAST(query, buffer);
    agg_desc.serialize(buffer);
    writeBinary(num_rows, buffer);
}

QueryPlanStepPtr ReadStorageRowCountStep::deserialize(ReadBuffer & buffer, ContextPtr context_)
{
    Block output_header = deserializeBlock(buffer);
    StorageID storage_id = StorageID::deserialize(buffer, context_);
    ASTPtr query = deserializeAST(buffer);
    AggregateDescription desc;
    desc.deserialize(buffer);
    UInt64 num_rows;
    readBinary(num_rows, buffer);

    return std::make_unique<ReadStorageRowCountStep>( output_header, storage_id, query, desc, num_rows);
}

}
