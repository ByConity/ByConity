#include <Storages/StorageCloudMergeTree.h>

#include <Common/Exception.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/CloudMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MutationCommands.h>
#include <WorkerTasks/CloudMergeTreeMutateTask.h>
#include <WorkerTasks/CloudMergeTreeMergeTask.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/ManipulationType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

StorageCloudMergeTree::StorageCloudMergeTree(
    const StorageID & table_id_,
    String cnch_database_name_,
    String cnch_table_name_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeMetaBase::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : MergeTreeCloudData( // NOLINT
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_))
    , cnch_database_name(std::move(cnch_database_name_))
    , cnch_table_name(std::move(cnch_table_name_))
{
}

StorageCloudMergeTree::~StorageCloudMergeTree()
{
}

void StorageCloudMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    if (auto plan = MergeTreeDataSelectExecutor(*this).read(
            column_names, metadata_snapshot, query_info, local_context, max_block_size, num_streams, processed_stage))
        query_plan = std::move(*plan);
}

Pipe StorageCloudMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context), BuildQueryPipelineSettings::fromContext(local_context));
}

BlockOutputStreamPtr StorageCloudMergeTree::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    return std::make_shared<CloudMergeTreeBlockOutputStream>(*this, metadata_snapshot, std::move(local_context));
}

ManipulationTaskPtr StorageCloudMergeTree::manipulate(const ManipulationTaskParams & input_params, ContextPtr task_context)
{
    ManipulationTaskPtr task;
    switch (input_params.type)
    {
        case ManipulationType::Merge:
            task = std::make_shared<CloudMergeTreeMutateTask>(*this, input_params, task_context);
            break;
        case ManipulationType::Mutate:
            task = std::make_shared<CloudMergeTreeMutateTask>(*this, input_params, task_context);
            break;
        default:
            throw Exception("Unsupported manipulation task: " + String(typeToString(input_params.type)), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// task->execute();

    /// LOG_DEBUG(log, "Finished manipulate task {}", input_params.task_id);
    return task;
}

MutationCommands StorageCloudMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & /*part*/) const
{
    return {};
}

}
