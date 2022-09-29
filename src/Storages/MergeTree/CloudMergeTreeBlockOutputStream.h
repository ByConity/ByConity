#pragma once

#include <CloudServices/CnchDedupHelper.h>
#include <CloudServices/commitCnchParts.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
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
        const StoragePolicyPtr & local_policy_,
        const String & local_rel_path_,
        bool to_staging_area_ = false);

    Block getHeader() const override;

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

    MergeTreeMetaBase & storage;
    Poco::Logger * log;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    bool to_staging_area;

    MergeTreeDataWriter writer;
    CnchDataWriter cnch_writer;

    // if we want to do batch preload indexing in write suffix
    MutableMergeTreeDataPartsCNCHVector preload_parts;

    bool disable_transaction_commit{false};
    SimpleIncrement increment;
};

}
