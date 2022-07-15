#include <WorkerTasks/CloudMergeTreeMutateTask.h>

#include <CloudServices/CnchPartsHelper.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <WorkerTasks/MergeTreeDataMutator.h>

namespace DB
{

CloudMergeTreeMutateTask::CloudMergeTreeMutateTask(
    StorageCloudMergeTree & storage_,
    ManipulationTaskParams params_,
    ContextPtr context_)
    : ManipulationTask(std::move(params_), std::move(context_))
    , storage(storage_)
{
}

void CloudMergeTreeMutateTask::executeImpl()
{
    auto lock_holder = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);
    auto visible_parts = CnchPartsHelper::calcVisibleParts(params.all_parts, false);

    MergeTreeDataMutator mutate_executor(storage, getContext()->getSettingsRef().background_pool_size);
    auto temp_parts = mutate_executor.mutatePartsToTemporaryParts(params, *manipulation_entry, getContext(), lock_holder);

    // dumpAndCommitCnchParts(storage, ManipulationType::Mutate, temp_parts, context, params.task_id);

    /// TODO: write part log
    // Stopwatch stopwatch;

    // auto write_part_log = [&] (const ExecutionStatus & execution_status)
    // {
    //     writePartLog(
    //         PartLogElement::MUTATE_PART,
    //         execution_status,
    //         stopwatch.elapsed(),
    //         {},
    //         new_part,
    //         {},
    //         list_entry.get());
    // };

    // write_part_log({});
}

}
