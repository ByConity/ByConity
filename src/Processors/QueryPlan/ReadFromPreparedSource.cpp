#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/PlanSerDerHelper.h>
#include <Processors/QueryPipeline.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>


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

void ReadFromStorageStep::serialize(WriteBuffer & buffer) const
{
    writeBinary(step_description, buffer);
    storage_id.serialize(buffer);
    query_info.serialize(buffer);
    serializeStrings(column_names, buffer);
    writeBinary(UInt8(processed_stage), buffer);
    writeBinary(max_block_size, buffer);
    writeBinary(num_streams, buffer);
}

QueryPlanStepPtr ReadFromStorageStep::deserialize(ReadBuffer & buffer, ContextPtr context)
{
    String step_description;
    readBinary(step_description, buffer);

    StorageID storage_id = StorageID::deserialize(buffer);

    SelectQueryInfo query_info;
    query_info.deserialize(buffer);

    Names column_names = deserializeStrings(buffer);

    UInt8 binary_stage;
    readBinary(binary_stage, buffer);
    auto processed_stage = QueryProcessingStage::Enum(binary_stage);

    size_t max_block_size;
    readBinary(max_block_size, buffer);

    unsigned num_streams;
    readBinary(num_streams, buffer);

    StoragePtr storage = DatabaseCatalog::instance().getTable(storage_id, context);

    auto pipe = storage->read(column_names, 
                              storage->getInMemoryMetadataPtr(), 
                              query_info, 
                              context, 
                              processed_stage,
                              max_block_size,
                              num_streams);

    return std::make_unique<ReadFromStorageStep>(std::move(pipe), step_description);
}

}
