#include <Processors/QueryPlan/ReadFromSourceStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/PlanSerDerHelper.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <IO/WriteHelpers.h>
#include <Parsers/queryToString.h>


namespace DB
{

ReadFromSourceStep::ReadFromSourceStep(Block header_,
                                       StorageID storage_id_, 
                                       const SelectQueryInfo & query_info_,
                                       const Names & column_names_,
                                       QueryProcessingStage::Enum processed_stage_,
                                       size_t max_block_size_,
                                       unsigned num_streams_,
                                       ContextPtr context_)
    : ISourceStep(DataStream{.header = header_})
    , storage_id(storage_id_)
    , query_info(query_info_)
    , column_names(column_names_)
    , processed_stage(processed_stage_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , context(std::move(context_))
{
}

void ReadFromSourceStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto step = generateStep();
    if (auto * source = dynamic_cast<ISourceStep *>(step.get()))
        source->initializePipeline(pipeline, settings);
}

QueryPlanStepPtr ReadFromSourceStep::generateStep()
{
    StoragePtr storage = DatabaseCatalog::instance().getTable({storage_id.database_name, storage_id.table_name}, context);

    auto pipe = storage->read(column_names, 
                              storage->getInMemoryMetadataPtr(), 
                              query_info, 
                              context, 
                              processed_stage,
                              max_block_size,
                              num_streams);

    if (pipe.empty())
    {
        auto header = storage->getInMemoryMetadataPtr()->getSampleBlockForColumns(column_names, storage->getVirtuals(), storage_id);
        auto null_pipe = InterpreterSelectQuery::generateNullSourcePipe(header, query_info);
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(null_pipe));
        read_from_pipe->setStepDescription("Read from NullSource");
        return read_from_pipe;
    }
    else
        return std::make_unique<ReadFromStorageStep>(std::move(pipe), step_description);
}

void ReadFromSourceStep::serialize(WriteBuffer & buffer) const
{
    writeBinary(step_description, buffer);
    serializeBlock(output_stream->header, buffer);

    storage_id.serialize(buffer);
    query_info.serialize(buffer);
    serializeStrings(column_names, buffer);
    writeBinary(UInt8(processed_stage), buffer);
    writeBinary(max_block_size, buffer);
    writeBinary(num_streams, buffer);
}

QueryPlanStepPtr ReadFromSourceStep::deserialize(ReadBuffer & buffer, ContextPtr context)
{
    String step_description;
    readBinary(step_description, buffer);

    auto header = deserializeBlock(buffer);

    StorageID storage_id = StorageID::deserialize(buffer, context);

    SelectQueryInfo query_info;
    query_info.deserialize(buffer);

    std::cout<<" << ReadFromSource: " << queryToString(query_info.query) << std::endl;

    /**
     * reconstuct query level info based on query
     */
    SelectQueryOptions options;
    auto interpreter = std::make_shared<InterpreterSelectQuery>(query_info.query, context, options.distributedStages());
    query_info = interpreter->getQueryInfo();

    Names column_names = deserializeStrings(buffer);

    UInt8 binary_stage;
    readBinary(binary_stage, buffer);
    auto processed_stage = QueryProcessingStage::Enum(binary_stage);

    size_t max_block_size;
    readBinary(max_block_size, buffer);

    unsigned num_streams;
    readBinary(num_streams, buffer);

    auto source_step = std::make_unique<ReadFromSourceStep>(header,
                                                            storage_id,
                                                            query_info,
                                                            column_names,
                                                            processed_stage,
                                                            max_block_size,
                                                            num_streams,
                                                            context);

    return source_step->generateStep();
}

}
