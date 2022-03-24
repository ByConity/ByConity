#include <Storages/MergeTree/HaMergeTreeBlockOutputStream.h>

#include <IO/Operators.h>
#include <Interpreters/PartLog.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Storages/MergeTree/HaMergeTreeLogManager.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>
#include <Storages/StorageHaMergeTree.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/KeeperException.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_LIVE_REPLICAS;
    extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int NO_ZOOKEEPER;
    extern const int READONLY;
    extern const int UNKNOWN_STATUS_OF_INSERT;
    extern const int INSERT_WAS_DEDUPLICATED;
    extern const int KEEPER_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
    extern const int NO_ACTIVE_REPLICAS;
}

HaMergeTreeBlockOutputStream::HaMergeTreeBlockOutputStream(
    StorageHaMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , quorum(context->getSettingsRef().insert_quorum)
    , quorum_timeout_ms(context->getSettingsRef().insert_quorum_timeout.totalMilliseconds())
    , max_parts_per_block(context->getSettingsRef().max_partitions_per_insert_block)
    , optimize_on_insert(context->getSettingsRef().optimize_on_insert)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (HaMergeTreeBlockOutputStream)"))
{
    quorum = 0; /// TODO
}

Block HaMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

/// Allow to verify that the session in ZooKeeper is still alive.
static void assertSessionIsNotExpired(zkutil::ZooKeeperPtr & zookeeper)
{
    if (!zookeeper)
        throw Exception("No ZooKeeper session.", ErrorCodes::NO_ZOOKEEPER);

    if (zookeeper->expired())
        throw Exception("ZooKeeper session has been expired.", ErrorCodes::NO_ZOOKEEPER);
}

void HaMergeTreeBlockOutputStream::writePrefix()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event);
}

void HaMergeTreeBlockOutputStream::write(const Block & block)
{
    last_block_is_duplicate = false;

    auto zookeeper = storage.getZooKeeper();
    assertSessionIsNotExpired(zookeeper);

    Stopwatch watch;
    MergeTreeData::MutableDataPartsVector parts;
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);

    BitEngineDictionaryHaManager::BitEngineLockPtr bitengine_lock;
    BitEngineDictionaryHaManager * ha_manager = storage.getBitEngineDictionaryHaManager();
    if (ha_manager)
    {
        bitengine_lock = ha_manager->tryGetLock();
        // double check the status of bitengine manager if the storage is shutdown when it was waitting for the lock
        if (ha_manager->isStopped())
            return;
    }

    for (auto & current_block : part_blocks)
    {
        parts.push_back(storage.writer.writeTempPart(current_block, metadata_snapshot, context));

        LOG_DEBUG(log, "Wrote block with {} rows", current_block.block.rows());
    }

    try
    {
        writeExistingParts(parts);

        LOG_DEBUG(log, "Commit {} parts", parts.size());
        PartLog::addNewParts(storage.getContext(), parts, watch.elapsed(), ExecutionStatus(0));
    }
    catch (...)
    {
        PartLog::addNewParts(storage.getContext(), parts, watch.elapsed(), ExecutionStatus::fromCurrentException(__PRETTY_FUNCTION__));
        throw;
    }
}

HaMergeTreeLogEntryVec
HaMergeTreeBlockOutputStream::generateLogEntriesForParts(zkutil::ZooKeeperPtr & zookeeper, MergeTreeData::MutableDataPartsVector & parts)
{
    if (parts.empty())
        return {};

    assertSessionIsNotExpired(zookeeper);

    /// Part 1: allocate block numbers and LSNs in a request
    String block_prefix = storage.zookeeper_path + "/block_numbers/block-";
    String lsn_prefix = storage.getZKLatestLSNPath() + "/lsn-";

    Coordination::Requests ops;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        ops.push_back(zkutil::makeCreateRequest(block_prefix, "", zkutil::CreateMode::EphemeralSequential));
        ops.push_back(zkutil::makeCreateRequest(lsn_prefix, "", zkutil::CreateMode::EphemeralSequential));
    }
    Coordination::Responses results = zookeeper->multi(ops);

    auto get_path = [&results](size_t i) { return dynamic_cast<Coordination::CreateResponse &>(*results[i]).path_created; };

    /// Updated latest_lsn, ignore any exception
    try
    {
        auto lastest_lsn = get_path(ops.size() - 1).substr(lsn_prefix.length());
        zookeeper->set(storage.getZKLatestLSNPath(), lastest_lsn);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    /// Part 2: commit parts
    HaMergeTreeLogEntryVec entries;
    entries.reserve(parts.size());

    for (size_t i = 0; i < parts.size(); ++i)
    {
        auto block_number = parse<Int64>(get_path(i * 2).substr(block_prefix.length()));
        auto lsn = parse<Int64>(get_path(i * 2 + 1).substr(lsn_prefix.length()));

        /// fill part info
        auto & part = parts[i];
        part->info.min_block = block_number;
        part->info.max_block = block_number;
        part->info.level = 0;
        part->name = part->getNewName(part->info);

        /// create log entry
        entries.emplace_back(std::make_shared<HaMergeTreeLogEntry>());
        auto & entry = *entries.back();
        entry.lsn = lsn;
        entry.type = HaMergeTreeLogEntry::GET_PART;
        entry.create_time = time(nullptr);
        entry.source_replica = storage.replica_name;
        entry.new_part_name = part->name;
        entry.block_id = toString(block_number);
        entry.quorum = quorum;
        entry.is_executed = true;
    }

    return entries;
}

void HaMergeTreeBlockOutputStream::writeExistingParts(MergeTreeData::MutableDataPartsVector & parts)
{
    auto zookeeper = storage.getZooKeeper(); /// check may be duplicated, but it is OK
    auto entries = generateLogEntriesForParts(zookeeper, parts);
    commitParts(zookeeper, parts, entries);
}

void HaMergeTreeBlockOutputStream::commitParts(zkutil::ZooKeeperPtr &, MergeTreeData::MutableDataPartsVector & parts, const HaMergeTreeLogEntryVec & entries)
{
    storage.queue.write(entries);

    MergeTreeData::Transaction transaction(storage);
    for (auto & part : parts)
        storage.renameTempPartAndAdd(part, nullptr, &transaction);
    transaction.commit();
}
}
