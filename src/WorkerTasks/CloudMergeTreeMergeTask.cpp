#include <WorkerTasks/CloudMergeTreeMergeTask.h>

#include <Interpreters/Context.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <WorkerTasks/MergeTreeDataMerger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

CloudMergeTreeMergeTask::CloudMergeTreeMergeTask(
    StorageCloudMergeTree & storage_,
    ManipulationTaskParams params_,
    ContextPtr context_)
    : ManipulationTask(std::move(params_), std::move(context_))
    , storage(storage_)
{
}

void CloudMergeTreeMergeTask::executeImpl()
{
    auto lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

    auto & cloud_table = dynamic_cast<StorageCloudMergeTree &>(*params.storage.get());
    MergeTreeDataMerger merger(cloud_table, params, getContext(), manipulation_entry->get(), [&]() {
        if (isCancelled())
            return true;

        auto last_touch_time = getManipulationListElement()->last_touch_time.load(std::memory_order_relaxed);

        /// TODO: add settings
        if (time(nullptr) - last_touch_time > 600)
            setCancelled();

        return isCancelled();
    });

    auto merged_part = merger.mergePartsToTemporaryPart();

    IMutableMergeTreeDataPartsVector temp_parts;
    std::vector<ReservationPtr> reserved_spaces; // hold space

    for (auto & part : params.source_data_parts)
    {
        /// TODO: Double check, set drop part's mutation to current txnid and hint_mutation to corresponding part's mutation.
        if (part->info.level == MergeTreePartInfo::MAX_LEVEL)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Drop part info level is MAX_LEVEL");

        MergeTreePartInfo drop_part_info(
            part->info.partition_id,
            part->info.min_block,
            part->info.max_block,
            part->info.level + 1,
            0, // TODO: set mutation
            0 /* must be zero for drop part */);

        reserved_spaces.emplace_back(cloud_table.reserveSpace(part->bytes_on_disk));
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part->name, reserved_spaces.back()->getDisk(), 0);

        auto drop_part = std::make_shared<MergeTreeDataPartCNCH>(
            storage, drop_part_info.getPartName(), drop_part_info, single_disk_volume, std::nullopt);

        drop_part->partition.assign(part->partition);
        drop_part->bucket_number = part->bucket_number;
        drop_part->deleted = true;
        temp_parts.push_back(std::move(drop_part));
    }

    temp_parts.push_back(std::move(merged_part));

    if (isCancelled())
        throw Exception("Merge task " + params.task_id + " is cancelled", ErrorCodes::ABORTED);

    // auto dumped_data = dumpAndCommitCnchParts(storage, ManipulationType::Merge, temp_parts, context, params.task_id);
    // tryPreloadChecksumsAndPrimaryIndex(storage, dumped_data.parts, ManipulationType::Merge, context);
}

}
