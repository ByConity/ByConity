#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <common/logger_useful.h>
// #include <MergeTreeCommon/CnchDedupHelper.h>

namespace DB
{
class Block;
class Context;
class MergeTreeMetaBase;


class CloudMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    CloudMergeTreeBlockOutputStream(
        MergeTreeMetaBase & storage_, StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_, const StoragePolicyPtr& local_policy_,
        const String& local_rel_path_, bool to_staging_area_ = false)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , context(std::move(context_))
        , to_staging_area(to_staging_area_)
        , writer(storage, local_policy_, local_rel_path_)
    {
    }

    Block getHeader() const override;

    void write(const Block & block) override;
    void writeSuffix() override;
    void writeSuffixImpl();

    // IMutableMergeTreeDataPartsVector convertBlockIntoDataParts(const Block & block);

    void disableTransactionCommit() { disable_transaction_commit = true; }

private:
    // using FilterInfo = CnchDedupHelper::FilterInfo;
    // FilterInfo dedupWithUniqueKey(const Block & block);

    // void writeSuffixForInsert();
    // void writeSuffixForUpsert();

    MergeTreeMetaBase & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    [[maybe_unused]] bool to_staging_area;

    [[maybe_unused]] MergeTreeDataWriter writer;
    // MergeTreeCNCHDataDumper dumper;
    // if we want to do batch preload indexing in write suffix
    MutableMergeTreeDataPartsCNCHVector preload_parts;

    bool disable_transaction_commit{false};
};

}
