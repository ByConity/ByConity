#include <QueryPlan/ReadFromPreparedSource.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <Processors/QueryPipeline.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <IO/WriteHelpers.h>
#include <Parsers/queryToString.h>


namespace DB
{

ReadFromPreparedSource::ReadFromPreparedSource(Pipe pipe_, std::shared_ptr<const Context> context_)
    : ISourceStep(DataStream{.header = pipe_.getHeader()})
    , pipe(std::move(pipe_))
    , context(std::move(context_))
{
}

void ReadFromPreparedSource::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));

    if (context)
        pipeline.addInterpreterContext(std::move(context));
}

std::shared_ptr<IQueryPlanStep> ReadFromPreparedSource::copy(ContextPtr) const
{
    throw Exception("ReadFromPreparedSource can not copy", ErrorCodes::NOT_IMPLEMENTED);
}

std::shared_ptr<IQueryPlanStep> ReadFromStorageStep::copy(ContextPtr) const
{
    throw Exception("ReadFromStorageStep can not copy", ErrorCodes::NOT_IMPLEMENTED);
}

}
