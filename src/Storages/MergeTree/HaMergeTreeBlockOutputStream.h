#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/HaMergeTreeLogEntry.h>
#include <common/types.h>


namespace Poco { class Logger; }

namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

class StorageHaMergeTree;


class HaMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    HaMergeTreeBlockOutputStream(
        StorageHaMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_);

    Block getHeader() const override;
    void writePrefix() override;
    void write(const Block & block) override;

    HaMergeTreeLogEntryVec generateLogEntriesForParts(zkutil::ZooKeeperPtr & zookeeper, MergeTreeData::MutableDataPartsVector & parts);

    /// For ATTACHing existing data on filesystem.
    void writeExistingParts(MergeTreeData::MutableDataPartsVector & parts);

    /// /// For replacing parts atomatically.
    /// void replaceExistingParts(MergeTreeData::MutableDataPartsVector & part, MergeTreePartInfo & drop_range);

    /// void writeReshardingPart(MergeTreeData::MutableDataPartPtr & part);

    /// void writeFetchedReshardingPart(MergeTreeData::MutableDataPartPtr & part);

    /// TBD: If deduplicate logic is still valid in HA MergeTree case.
    /// For proper deduplication in MaterializedViews
    bool lastBlockIsDuplicate() const
    {
        return last_block_is_duplicate;
    }

private:
    void commitParts(zkutil::ZooKeeperPtr & zookeeper, MergeTreeData::MutableDataPartsVector & parts, const HaMergeTreeLogEntryVec & log_entries);

    StorageHaMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;

    size_t quorum;
    size_t quorum_timeout_ms;
    size_t max_parts_per_block;
    bool optimize_on_insert;

    bool last_block_is_duplicate = false;

    using Logger = Poco::Logger;
    Logger * log;
};

}
