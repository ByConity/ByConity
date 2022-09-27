#include <WorkerTasks/CloudMergeTreeMutateTask.h>

#include <CloudServices/commitCnchParts.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCloudMergeTree.h>
#include <WorkerTasks/MergeTreeDataMutator.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

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

    MergeTreeDataMutator mutate_executor(storage, getContext()->getSettingsRef().background_pool_size);
    auto data_parts = mutate_executor.mutatePartsToTemporaryParts(params, *manipulation_entry, getContext(), lock_holder);

    if (isCancelled())
        throw Exception("Merge task " + params.task_id + " is cancelled", ErrorCodes::ABORTED);

    CnchDataWriter cnch_writer(storage, *getContext(), ManipulationType::Mutate, params.task_id);
    cnch_writer.dumpAndCommitCnchParts(data_parts);
}

}
