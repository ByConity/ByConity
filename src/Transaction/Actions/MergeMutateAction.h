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

#include <Common/Logger.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/TransactionCommon.h>
#include <WorkerTasks/ManipulationType.h>

namespace DB
{

class MergeMutateAction : public IAction
{
public:
    MergeMutateAction(
        const ContextPtr & query_context_,
        const TxnTimestamp & txn_id_,
        TransactionRecord record,
        ManipulationType type_,
        const StoragePtr table_,
        const Strings & source_part_names_,
        UInt64 manipulation_submit_time_ns_ = 0,
        UInt64 peak_memory_usage_ = 0)
        : IAction(query_context_, txn_id_)
        , txn_record(std::move(record))
        , type(type_)
        , table(table_)
        , source_part_names(source_part_names_)
        , manipulation_submit_time_ns(manipulation_submit_time_ns_)
        , peak_memory_usage(peak_memory_usage_)
        , log(getLogger("MergeMutationAction"))
    {
    }

    void appendPart(MutableMergeTreeDataPartCNCHPtr part);
    void setDeleteBitmaps(const DeleteBitmapMetaPtrVector & delete_bitmaps_) { delete_bitmaps = delete_bitmaps_; }

    /// V1 APIs
    void executeV1(TxnTimestamp commit_time) override;

    /// V2 APIs
    void executeV2() override;
    void postCommit(TxnTimestamp commit_time) override;
    void abort() override;

    static void updatePartData(MutableMergeTreeDataPartCNCHPtr part, TxnTimestamp commit_time);

    UInt32 getSize() const override { return parts.size() + delete_bitmaps.size(); }

private:
    ServerPartLogElement::Type getPartLogType() const
    {
        return type == ManipulationType::Merge ? ServerPartLogElement::MERGE_PARTS : ServerPartLogElement::MUTATE_PART;
    }

    MutableMergeTreeDataPartsCNCHVector getMergedPart() const;

    TransactionRecord txn_record;
    ManipulationType type;
    const StoragePtr table;
    Strings source_part_names;

    UInt64 manipulation_submit_time_ns;
    UInt64 peak_memory_usage;

    LoggerPtr log;

    MutableMergeTreeDataPartsCNCHVector parts;
    DeleteBitmapMetaPtrVector delete_bitmaps;
    bool executed{false};
};

}
