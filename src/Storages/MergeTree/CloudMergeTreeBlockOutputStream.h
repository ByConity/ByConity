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
#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/CnchDataWriter.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <common/logger_useful.h>
#include <Common/SimpleIncrement.h>
#include "WorkerTasks/ManipulationType.h"

namespace DB
{
class Block;
class Context;
class MergeTreeMetaBase;

class CloudMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    CloudMergeTreeBlockOutputStream(
        MergeTreeMetaBase & storage_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_,
        ASTPtr overwrite_partition_ = nullptr);

    Block getHeader() const override;

    void writePrefix() override;
    void write(const Block & block) override;
    MergeTreeMutableDataPartsVector convertBlockIntoDataParts(const Block & block, bool use_inner_block_id = false);
    void writeSuffix() override;
    void writeSuffixImpl();

    void disableTransactionCommit() { disable_transaction_commit = true; }

private:
    using FilterInfo = CnchDedupHelper::FilterInfo;
    FilterInfo dedupWithUniqueKey(const Block & block);

    void writeSuffixForInsert();
    void writeSuffixForUpsert();

    bool shouldDedupInWriteSuffixStage();

    void initOverwritePartitionPruner();

    void checkAndInit();

    MergeTreeMetaBase & storage;
    LoggerPtr log;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;

    MergeTreeDataWriter writer;
    CnchDataWriter cnch_writer;

    // if we want to do batch preload indexing in write suffix
    MutableMergeTreeDataPartsCNCHVector preload_parts;

    bool disable_transaction_commit{false};
    SimpleIncrement increment;

    time_t last_check_parts_time = 0;

    ASTPtr overwrite_partition;
    NameSet overwrite_partition_ids;

    struct DedupParameters
    {
        bool enable_staging_area = false;
        bool enable_append_mode = false; /// If it's true, we'll not dedup with existing parts, but it will dedup in block self.
        bool enable_partial_update = false; /// If it's true, the write will be considered as a partial update one
    };
    DedupParameters dedup_parameters;
};

}
