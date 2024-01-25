/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <QueryPlan/PlanSegmentSourceStep.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <QueryPlan/PlanSerDerHelper.h>
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

PlanSegmentSourceStep::PlanSegmentSourceStep(Block header_,
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
    StoragePtr storage = DatabaseCatalog::instance().getTable({storage_id.database_name, storage_id.table_name}, context);
    storage_id.uuid = storage->getStorageUUID();
    // std::cout<<" PlanSegmentSourceStep header: " << header_.dumpStructure() << std::endl;
    // std::cout<<" PlanSegmentSourceStep processed_stage: " << QueryProcessingStage::toString(processed_stage) << std::endl;
}

void PlanSegmentSourceStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto step = generateStep();
    if (auto * source = dynamic_cast<ISourceStep *>(step.get()))
        source->initializePipeline(pipeline, settings);
}

QueryPlanStepPtr PlanSegmentSourceStep::generateStep()
{
    StoragePtr storage = DatabaseCatalog::instance().getTable({storage_id.database_name, storage_id.table_name}, context);
    auto storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);

    auto pipe = storage->read(column_names,
                              storage_snapshot,
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

std::shared_ptr<IQueryPlanStep> PlanSegmentSourceStep::copy(ContextPtr) const
{
    throw Exception("PlanSegmentSourceStep can not copy", ErrorCodes::NOT_IMPLEMENTED);
}

}
