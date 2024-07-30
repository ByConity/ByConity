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

#include <WorkerTasks/CloudMergeTreeMergeTask.h>

#include <CloudServices/CnchDataWriter.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Transaction/Actions/MergeMutateAction.h>
#include <Transaction/ICnchTransaction.h>
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
    if (/*params.source_parts.empty() && */params.source_data_parts.empty())
        throw Exception("Expected non-empty source parts in ManipulationTaskParams", ErrorCodes::BAD_ARGUMENTS);

    if (params.new_part_names.empty())
        throw Exception("Expected non-empty new part names in ManipulationTaskParams", ErrorCodes::BAD_ARGUMENTS);
}

void CloudMergeTreeMergeTask::executeImpl()
{
    auto heartbeat_timeout = getContext()->getSettingsRef().cloud_task_auto_stop_timeout.value;
    auto lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

    auto & cloud_table = dynamic_cast<StorageCloudMergeTree &>(*params.storage.get());
    MergeTreeDataMerger merger(cloud_table, params, getContext(), manipulation_entry->get(), [&]() {
        return isCancelled(heartbeat_timeout);
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
            getContext()->getCurrentTransactionID().toUInt64(),
            0 /* must be zero for drop part */);

        reserved_spaces.emplace_back(cloud_table.reserveSpace(0)); /// Drop part is empty part.
        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part->name, reserved_spaces.back()->getDisk(), 0);

        auto drop_part = std::make_shared<MergeTreeDataPartCNCH>(
            storage, drop_part_info.getPartName(), drop_part_info, single_disk_volume, std::nullopt);

        drop_part->partition.assign(part->partition);
        drop_part->bucket_number = part->bucket_number;
        drop_part->deleted = true;
        /// rows_count and bytes_on_disk is required for parts info statistics.
        drop_part->covered_parts_rows = part->rows_count;
        drop_part->covered_parts_size = part->bytes_on_disk;
        drop_part->last_modification_time = part->last_modification_time? part->last_modification_time : part->commit_time;
        temp_parts.push_back(std::move(drop_part));
    }

    /// 0 rows part may come from unique table or DELETE mutation, and we can safely mark it as deleted.
    if (merged_part->rows_count == 0)
        merged_part->deleted = true;

    temp_parts.push_back(std::move(merged_part));

    if (isCancelled(heartbeat_timeout))
        throw Exception("Merge task " + params.task_id + " is cancelled", ErrorCodes::ABORTED);

    UInt64 peak_memory_usage = 0;

    ManipulationListElement * manipulation_list_element = getManipulationListElement();
    if (manipulation_list_element)
    {
        peak_memory_usage = manipulation_list_element->getMemoryTracker().getPeak();
    }

    CnchDataWriter cnch_writer(storage, getContext(), ManipulationType::Merge, params.task_id,
        /*consumer_group_*/ {}, /*tpl_*/ {}, /*binlog*/ {}, peak_memory_usage);
    DumpedData data = cnch_writer.dumpAndCommitCnchParts(temp_parts);
    auto commit_time = getContext()->getCurrentTransaction()->commitV2();
    for (const auto & part : data.parts)
    {
        MergeMutateAction::updatePartData(part, commit_time);
        part->relative_path = part->info.getPartNameWithHintMutation();
    }
    if (params.parts_preload_level)
        cnch_writer.preload(data.parts);
}

}
