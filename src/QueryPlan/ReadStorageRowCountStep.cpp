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
#include <Interpreters/StorageID.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/executeSubQuery.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Optimizer/SymbolsExtractor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

ReadStorageRowCountStep::ReadStorageRowCountStep(Block output_header, ASTPtr query_, AggregateDescription agg_desc_, bool is_final_agg_, StorageID storage_id_)
    : ISourceStep(DataStream{.header = output_header})
    , query(query_)
    , agg_desc(agg_desc_)
    , is_final_agg(is_final_agg_)
    , storage_id(storage_id_)
{
}

std::shared_ptr<IQueryPlanStep> ReadStorageRowCountStep::copy(ContextPtr ) const
{
    auto step = std::make_shared<ReadStorageRowCountStep>(output_stream->header, query, agg_desc, is_final_agg, storage_id);
    step->setNumRows(num_rows);
    return step;
}

void ReadStorageRowCountStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & context)
{
    if (storage_id)
    {
        std::optional<UInt64> rows_cnt{};
        // get storage
        StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context.context);

        // get row number
        auto & select_query = query->as<ASTSelectQuery &>();
        if (!select_query.where() && !select_query.prewhere())
        {
            rows_cnt = storage->totalRows(context.context);
        }
        else // It's possible to optimize count() given only partition predicates
        {
            auto interpreter = std::make_shared<InterpreterSelectQuery>(query->clone(), context.context, SelectQueryOptions());
            SelectQueryInfo temp_query_info;
            temp_query_info.query = interpreter->getQuery();
            temp_query_info.syntax_analyzer_result = interpreter->getSyntaxAnalyzerResult();
            temp_query_info.sets = interpreter->getQueryAnalyzer()->getPreparedSets();
            rows_cnt = storage->totalRowsByPartitionPredicate(temp_query_info, context.context);
        }

        if (!rows_cnt)
        {
            try
            {
                auto select_list = std::make_shared<ASTExpressionList>();
                auto count_func = makeASTFunction("count");
                select_query.refSelect() = std::make_shared<ASTExpressionList>();
                select_query.refSelect()->children.emplace_back(count_func);
                DataTypes types;
                auto pre_execute
                    = [&types](InterpreterSelectQueryUseOptimizer & interpreter) { types = interpreter.getSampleBlock().getDataTypes(); };

                auto query_context = createContextForSubQuery(context.context);
                SettingsChanges changes;
                changes.emplace_back("max_result_rows", 1);
                changes.emplace_back("result_overflow_mode", "throw");
                changes.emplace_back("extremes", false);
                changes.emplace_back("optimize_trivial_count_query", false);
                query_context->applySettingsChanges(changes);
                auto block = executeSubPipelineWithOneRow(query, query_context, pre_execute);

                if (block.rows() != 1 || block.columns() != 1)
                    throw Exception(
                        ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                        "Trivial count query returned error data: {}",
                        select_query.formatForErrorMessage());

                block = materializeBlock(block);
                auto columns = block.getColumns();
                num_rows = columns[0]->getUInt(0);
            }
            catch (...)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Trivial count query execution failed. Please set optimize_trivial_count_query = 0 , and try again.",
                    select_query.formatForErrorMessage());
            }
        }
        else
            num_rows = rows_cnt.value();
    }

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
    if (storage_id)
        storage_id.toProto(*proto.mutable_storage_id());
}

std::shared_ptr<ReadStorageRowCountStep>
ReadStorageRowCountStep::fromProto(const Protos::ReadStorageRowCountStep & proto, ContextPtr context)
{
    auto base_output_header = ISourceStep::deserializeFromProtoBase(proto.query_plan_base());
    auto query = deserializeASTFromProto(proto.query());
    AggregateDescription agg_desc;
    agg_desc.fillFromProto(proto.agg_desc());
    auto num_rows = proto.num_rows();
    bool is_final = proto.is_final_agg();
    StorageID storage_id = StorageID::createEmpty();
    if (proto.has_storage_id())
        storage_id = StorageID::fromProto(proto.storage_id(), context);
    auto step = std::make_shared<ReadStorageRowCountStep>(base_output_header, query, agg_desc, is_final, storage_id);
    step->setNumRows(num_rows);
    return step;
}
}
