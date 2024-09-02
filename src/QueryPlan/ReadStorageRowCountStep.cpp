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
#include <Optimizer/SymbolsExtractor.h>


namespace DB
{

ReadStorageRowCountStep::ReadStorageRowCountStep(Block output_header, ASTPtr query_, AggregateDescription agg_desc_, UInt64 num_rows_, bool is_final_agg_, DatabaseAndTableName database_and_table_)
    : ISourceStep(DataStream{.header = output_header})
    , query(query_)
    , agg_desc(agg_desc_)
    , num_rows(num_rows_)
    , is_final_agg(is_final_agg_)
    , database_and_table(database_and_table_)
{
}

std::shared_ptr<IQueryPlanStep> ReadStorageRowCountStep::copy(ContextPtr ) const
{
    return std::make_shared<ReadStorageRowCountStep>(output_stream->header, query, agg_desc, num_rows, is_final_agg, database_and_table);
}
void ReadStorageRowCountStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & context)
{
    const auto & func = agg_desc.function;
    const AggregateFunctionCount & agg_count = static_cast<const AggregateFunctionCount &>(*func);
    Block output_header;
    if (is_final_agg)
    {
        auto count_column = ColumnVector<UInt64>::create();
        count_column->insertValue(num_rows);
        output_header.insert({count_column->getPtr(), agg_count.getReturnType(), agg_desc.column_name});
    }
    else
    {
        std::vector<char> state(agg_count.sizeOfData());
        AggregateDataPtr place = state.data();

        agg_count.create(place);
        SCOPE_EXIT_MEMORY_SAFE(agg_count.destroy(place));

        agg_count.set(place, num_rows);
        auto column = ColumnAggregateFunction::create(func);
        column->insertFrom(place);

        // AggregateFunction's argument type must keep same. 
        output_header.insert(
            {std::move(column), std::make_shared<DataTypeAggregateFunction>(func, func->getArgumentTypes(), agg_desc.parameters), agg_desc.column_name});
    }

    auto istream = std::make_shared<OneBlockInputStream>(output_header);
    auto pipe = Pipe(std::make_shared<SourceFromInputStream>(istream));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    if (context.context)
        pipeline.addInterpreterContext(context.context);
}

void ReadStorageRowCountStep::toProto(Protos::ReadStorageRowCountStep & proto, bool) const
{
    ISourceStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    serializeASTToProto(query, *proto.mutable_query());
    agg_desc.toProto(*proto.mutable_agg_desc());
    proto.set_num_rows(num_rows);
    proto.set_is_final_agg(is_final_agg);
}

std::shared_ptr<ReadStorageRowCountStep>
ReadStorageRowCountStep::fromProto(const Protos::ReadStorageRowCountStep & proto, ContextPtr)
{
    auto base_output_header = ISourceStep::deserializeFromProtoBase(proto.query_plan_base());
    auto query = deserializeASTFromProto(proto.query());
    AggregateDescription agg_desc;
    agg_desc.fillFromProto(proto.agg_desc());
    auto num_rows = proto.num_rows();
    bool is_final = proto.is_final_agg();
    auto step = std::make_shared<ReadStorageRowCountStep>(base_output_header, query, agg_desc, num_rows, is_final);

    return step;
}
}
