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

#pragma once

#include <memory>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/Actions/S3AttachMetaAction.h>
#include <Transaction/TxnTimestamp.h>
#include <WorkerTasks/ManipulationType.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition_list.h>
#include <CloudServices/CnchDedupHelper.h>

namespace DB
{
class MergeTreeMetaBase;

struct DumpedData
{
    MutableMergeTreeDataPartsCNCHVector parts;
    DeleteBitmapMetaPtrVector bitmaps;
    MutableMergeTreeDataPartsCNCHVector staged_parts;
    CnchDedupHelper::DedupMode dedup_mode = CnchDedupHelper::DedupMode::APPEND;

    bool isEmpty() const;
    void extend(DumpedData && data);
};

// TODO: proper writer
class CnchDataWriter : private boost::noncopyable
{
public:
    CnchDataWriter(
        MergeTreeMetaBase & storage_,
        ContextPtr context_,
        ManipulationType type_,
        String task_id_ = {},
        String consumer_group_ = {},
        const cppkafka::TopicPartitionList & tpl_ = {},
        const MySQLBinLogInfo & binlog_ = {},
        UInt64 peak_memory_usage_ = 0);

    ~CnchDataWriter();

    DumpedData dumpAndCommitCnchParts(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps = {},
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    void initialize(size_t max_threads);

    void schedule(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps,
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    void finalize();

    // server side only
    void commitPreparedCnchParts(const DumpedData & data, const std::unique_ptr<S3AttachPartsInfo>& s3_parts_info = nullptr);

    /// Convert staged parts to visible parts along with the given delete bitmaps.
    void publishStagedParts(const MergeTreeDataPartsCNCHVector & staged_parts, const LocalDeleteBitmaps & bitmaps_to_dump);

    DumpedData dumpCnchParts(
        const IMutableMergeTreeDataPartsVector & temp_parts,
        const LocalDeleteBitmaps & temp_bitmaps = {},
        const IMutableMergeTreeDataPartsVector & temp_staged_parts = {});

    void commitDumpedParts(const DumpedData & dumped_data);

    void preload(const MutableMergeTreeDataPartsCNCHVector & dumped_parts);

    void setPeakMemoryUsage(UInt64 peak_memory_usage_) { peak_memory_usage = peak_memory_usage_; }

    void setDedupMode(CnchDedupHelper::DedupMode dedup_mode_)
    {
        dedup_mode = dedup_mode_;
        res.dedup_mode = dedup_mode;
    }

    CnchDedupHelper::DedupMode getDedupMode() const
    {
        return dedup_mode;
    }

    bool isNeedDedupStage() const { return dedup_mode != CnchDedupHelper::DedupMode::APPEND; }

    void setFromAttach() { from_attach = true; }

    DumpedData res;

    static UUID newPartID(const MergeTreePartInfo& part_info, UInt64 txn_timestamp)
    {
        UUID random_id = UUIDHelpers::generateV4();
        UInt64& random_id_low = UUIDHelpers::getHighBytes(random_id);
        UInt64& random_id_high = UUIDHelpers::getLowBytes(random_id);
        boost::hash_combine(random_id_low, part_info.min_block);
        boost::hash_combine(random_id_high, part_info.max_block);
        boost::hash_combine(random_id_low, part_info.mutation);
        boost::hash_combine(random_id_high, txn_timestamp);
        return random_id;
    }

private:
    MergeTreeMetaBase & storage;
    ContextPtr context;
    ManipulationType type;
    String task_id;

    /// ATTACH path should be distingushed from INSERT;
    /// but we now record all new parts in INSERT, here we add this field as a mark
    bool from_attach{false};

    String consumer_group;
    cppkafka::TopicPartitionList tpl;
    MySQLBinLogInfo binlog;

    /// dump with thread pool
    std::unique_ptr<ThreadPool> thread_pool;
    ExceptionHandler handler;
    std::atomic_bool cancelled{false};
    mutable std::mutex write_mutex;

    PlanSegmentInstanceId instance_id{};

    UInt64 peak_memory_usage;

    CnchDedupHelper::DedupMode dedup_mode = CnchDedupHelper::DedupMode::APPEND;
};

}
