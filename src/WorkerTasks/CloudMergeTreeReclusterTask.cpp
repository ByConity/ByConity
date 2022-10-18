#include <WorkerTasks/CloudMergeTreeReclusterTask.h>
#include <WorkerTasks/MergeTreeDataReclusterMutator.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCloudMergeTree.h>
#include <CloudServices/commitCnchParts.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

CloudMergeTreeReclusterTask::CloudMergeTreeReclusterTask(
    StorageCloudMergeTree & storage_,
    ManipulationTaskParams params_,
    ContextPtr context_)
    : ManipulationTask(std::move(params_), std::move(context_))
    , storage(storage_)
{
}

void CloudMergeTreeReclusterTask::executeImpl()
{
    auto lock_holder = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

    MergeTreeDataReclusterMutator mutator(storage);

    auto clustered_tmp_parts =  mutator.executeClusterTask(params, *manipulation_entry, getContext());

    if (isCancelled())
        throw Exception("Recluster task " + params.task_id + " is cancelled", ErrorCodes::ABORTED);

    MergeTreeMutableDataPartsVector parts_to_commit;
    std::vector<ReservationPtr> reservations;
    for (auto & part : params.source_data_parts)
    {
        MergeTreePartInfo drop_part_info(
        part->info.partition_id, part->info.min_block, part->info.max_block, part->info.level + 1, part->info.mutation, 0);
        reservations.emplace_back(storage.reserveSpace(0));
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part->name, reservations.back()->getDisk(), 0);
        
        auto drop_part = std::make_shared<MergeTreeDataPartCNCH>(
            storage, drop_part_info.getPartName(), drop_part_info, single_disk_volume, std::nullopt);
        drop_part->partition.assign(part->partition);
        drop_part->deleted = true;
        /// rows_count and bytes_on_disk is required for parts info statistics.
        // drop_part->covered_parts_rows = part->rows_count;
        // drop_part->covered_parts_size = part->bytes_on_disk;
        parts_to_commit.push_back(std::move(drop_part));
    }

    parts_to_commit.insert(parts_to_commit.end(), clustered_tmp_parts.begin(), clustered_tmp_parts.end());

    CnchDataWriter cnch_writer(storage, getContext(), ManipulationType::Clustering, params.task_id);
    cnch_writer.dumpAndCommitCnchParts(parts_to_commit);

}

}
