#include <Storages/StorageCloudMergeTree.h>

#include <Common/Exception.h>
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
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool /*attach*/,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeData::MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool /* has_force_restore_data_flag */)
    : MergeTreeMetaBase(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::move(settings_),
        false /* FIXME */)
{}

void StorageCloudMergeTree::loadDataParts(const MutableDataPartsVector & /*data_parts*/)
{
    // TODO
}


void StorageCloudMergeTree::manipulate(const ManipulationTaskParams & input_params, ContextPtr task_context)
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

    task->execute();

    LOG_DEBUG(log, "Finished manipulate task {}", input_params.task_id);
}

MutationCommands StorageCloudMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & /*part*/) const
{
    return {};
}

}
