#include <Storages/MergeTree/HaUniqueMergeTreeBlockOutputStream.h>

#include <Storages/StorageHaUniqueMergeTree.h>

#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/RemoteBlockOutputStream.h>
#include <Parsers/queryToString.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPipeline.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/IndexFile/Iterator.h>
#include <Common/Endian.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
//    extern const int TOO_FEW_LIVE_REPLICAS;
//    extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE;
//    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
//    extern const int NO_ZOOKEEPER;
    extern const int READONLY;
//    extern const int UNKNOWN_STATUS_OF_INSERT;
//    extern const int INSERT_WAS_DEDUPLICATED;
//    extern const int KEEPER_EXCEPTION;
//    extern const int TIMEOUT_EXCEEDED;
   extern const int NO_ACTIVE_REPLICAS;
   extern const int REPLICA_STATUS_CHANGED;
}

namespace
{
/// Search `key' in `parts'.
/// If found, return index of the part containing the key and set rowid and version(optional) for the key.
/// Otherwise return -1.
int searchPartForKey(
    const MergeTreeData::DataPartsVector & parts,
    const UniqueKeyIndicesVector & indices,
    const String & key,
    UInt32 & rowid,
    UInt64 & version)
{
    if (parts.size() != indices.size())
        throw Exception(
            "Parts number " + toString(parts.size()) + " is not equal to indices number " + toString(indices.size()),
            ErrorCodes::LOGICAL_ERROR);
    for (int i = parts.size() - 1; i >= 0; --i)
    {
        auto & part = parts[i];
        auto & index = indices[i];
        if (part->getValueFromUniqueIndex(index, key, rowid, &version))
            return i;
    }
    return -1;
}

struct BlockUniqueKeyComparator
{
    const ColumnsWithTypeAndName & keys;
    explicit BlockUniqueKeyComparator(const ColumnsWithTypeAndName & keys_) : keys(keys_) { }

    bool operator()(size_t lhs, size_t rhs) const
    {
        for (auto & key : keys)
        {
            int cmp = key.column->compareAt(lhs, rhs, *key.column, /*nan_direction_hint=*/1);
            if (cmp < 0)
                return true;
            if (cmp > 0)
                return false;
        }
        return false;
    }
};

PaddedPODArray<UInt32> getRowidPermutation(const PaddedPODArray<UInt32> & xs)
{
    PaddedPODArray<UInt32> perm;
    perm.resize(xs.size());
    for (UInt32 i = 0; i < xs.size(); ++i)
        perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&xs](UInt32 lhs, UInt32 rhs) { return xs[lhs] < xs[rhs]; });
    return perm;
}

PaddedPODArray<UInt32> permuteRowids(const PaddedPODArray<UInt32> & rowids, const PaddedPODArray<UInt32> & perm)
{
    PaddedPODArray<UInt32> res;
    res.resize(rowids.size());
    for (size_t i = 0; i < res.size(); ++i)
    {
        if (perm[i] >= rowids.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Row id {} is out of size {}", perm[i], rowids.size());
        res[i] = rowids[perm[i]];
    }
    return res;
}

/**
 * Permutate the indexes in correct order, the rules of order are as follow:
 * 1. Sort by target index ascending.
 * 2. In the case that target indexes are same, sort by source index descending because the index is larger, the data is newer.
 * For example, replace_dst_indexes is {5, 5, 4, 5}, replace_src_indexes is {0, 2, 1, 3}
 * After correct permutation, replace_dst_indexes is {4, 5, 5, 5}, replace_src_indexes is {1, 3, 2, 0}
 */
void handleCorrectPermutation(PaddedPODArray<UInt32> & replace_dst_indexes, PaddedPODArray<UInt32> & replace_src_indexes)
{
    PaddedPODArray<UInt32> perm;
    perm.resize(replace_dst_indexes.size());
    for (UInt32 i = 0; i < replace_dst_indexes.size(); ++i)
        perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](UInt32 lhs, UInt32 rhs) {
        if (replace_dst_indexes[lhs] != replace_dst_indexes[rhs])
            return replace_dst_indexes[lhs] < replace_dst_indexes[rhs];
        else
            return replace_src_indexes[lhs] > replace_src_indexes[rhs];
    });
    replace_dst_indexes = permuteRowids(replace_dst_indexes, perm);
    replace_src_indexes = permuteRowids(replace_src_indexes, perm);
}

void mergeIndices(PaddedPODArray<UInt32> & rowids, PaddedPODArray<UInt32> & tmpids)
{
    size_t rowids_size = rowids.size();
    size_t tmpids_size = rowids.size();
    rowids.reserve(rowids_size + tmpids_size);
    for (auto val: tmpids)
        rowids.push_back(val);
}

} /// anonymous namespace

using IndexFileIteratorPtr = std::unique_ptr<IndexFile::Iterator>;

HaUniqueMergeTreeBlockOutputStream::HaUniqueMergeTreeBlockOutputStream(
    StorageHaUniqueMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    size_t max_parts_per_block_,
    bool enable_partial_update_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , max_parts_per_block(max_parts_per_block_)
    , allow_materialized(context->getSettingsRef().insert_allow_materialized_columns)
    , log(&Logger::get(storage.getLogName() + " (BlockOutputStream)"))
    , need_forward(!storage.is_leader)
    , enable_partial_update(enable_partial_update_)
{
    if (need_forward)
    {
        HaMergeTreeAddress addr;
        leader_name = storage.getCachedLeader(&addr);
        /// cached leader info may be outdated. We should never forward to ourself.
        if (leader_name.empty() || leader_name == storage.replica_name)
        {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            throw Exception("Can't forward write because leader is unknown right now", ErrorCodes::READONLY);
        }

        /// if materialized columns are not allowed in INSERT, we should exclude them in the forward request.
        /// (note that materialized columns will be computed by AddingDefaultBlockOutputStream in this case)
        ASTPtr query = RemoteBlockOutputStream::createInsertToRemoteTableQuery(
            addr.database,
            addr.table,
            allow_materialized ? metadata_snapshot_->getSampleBlock(true) : metadata_snapshot_->getSampleBlockNonMaterialized(true));
        auto query_str = queryToString(query);

        auto & client_info = context->getClientInfo();
        String user = client_info.current_user;
        String password = client_info.current_password; /// could be empty when the request comes from kafka engine
        if (auto address = storage.findClusterAddress(addr); address)
        {
            user = address->user;
            password = address->password;
        }

        remote_conn = std::make_shared<Connection>(
            addr.host, addr.queries_port, storage.getStorageID().database_name, user, password,
            "", "", "RemoteConnection" , Protocol::Compression::Enable, Protocol::Secure::Disable);
        const Settings & settings = context->getSettingsRef();
        auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
        remote_stream = std::make_shared<RemoteBlockOutputStream>(*remote_conn, timeouts, query_str, settings, client_info);
    }
    else
    {
        storage.assertNotReadonly();
        if (storage.getUniqueTableState() != UniqueTableState::NORMAL)
            throw Exception("Can't write to table of state " + toString(storage.getUniqueTableState()), ErrorCodes::REPLICA_STATUS_CHANGED);
    }
}

Block HaUniqueMergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void HaUniqueMergeTreeBlockOutputStream::writePrefix()
{
    if (remote_stream)
        remote_stream->writePrefix();
    else
        storage.delayInsertOrThrowIfNeeded();

    /// NOTE: here the current_thread is a thread local object, defined in ThreadStatus.h in which has per thread related info,
    ///  here we get the memory usage for this thread from current_thread's memory tracker before the write starts.
    saved_memory_used = current_thread->memory_tracker.get();
}

void HaUniqueMergeTreeBlockOutputStream::write(const Block & block)
{
    const auto & settings = storage.getSettings();
    if (settings->disable_block_output)
    {
        return;
    }

    if (!block)
        return;

    if (remote_stream)
    {
        /// RemoteBlockOutputStream::write requires block structure to be the same with its header.
        /// If block comes from materialized view, the order of columns could be different from the header.
        /// therefore we prepare a new block to
        /// 1. re-order columns according to the header
        /// 2. remove materialized columns when allow_materialized == false
        auto required_columns = remote_stream->getHeader().getNamesAndTypesList();
        Block new_block;
        for (auto & column : required_columns)
            new_block.insert(block.getByName(column.name));
        return remote_stream->write(new_block);
    }

    /// in order to abort the operation when we lose leadership (zk session expired),
    /// all ZK operation below should use `zookeeper' saved here
    auto zookeeper = storage.getZooKeeper();
    storage.checkSessionIsNotExpired(zookeeper);

    if (!storage.is_leader)
        throw Exception("Can't write to " + storage.getLogName() + " because replica " + storage.replicaName() + " is not leader", ErrorCodes::READONLY);

    storage.delayInsertOrThrowIfNeeded();
    metadata_snapshot->check(block, true);

    bool preprocessed = false;
    /// For table-level unique without version, need to remove duplicate keys before splitting
    /// because the original version (rowid) for each row is lost after splitting.
    if (!storage.unique_within_partition && storage.merging_params.version_column.empty())
    {
        size_t block_size = block.rows();
        IColumn::Filter filter(block_size, 1);
        size_t num_filtered = removeDupKeys(const_cast<Block &>(block), /*version_column=*/nullptr, filter);
        if (num_filtered)
        {
            ssize_t new_size_hint = block_size - num_filtered;
            for (size_t i = 0; i < block.columns(); ++i) {
                ColumnWithTypeAndName & col = const_cast<Block &>(block).getByPosition(i);
                col.column = col.column->filter(filter, new_size_hint);
            }
        }
        preprocessed = true;
    }

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);

    /// unique table doesn't support concurrent write
    auto lock = storage.uniqueWriteLock();

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;
        if (enable_partial_update)
            LOG_DEBUG(log, "Wrote block with {} rows in partial update mode", current_block.block.rows());
        else
            LOG_DEBUG(log, "Wrote block with {} rows", current_block.block.rows());

        MergeTreeData::DataPartsVector existing_parts; /// parts that could get modified
        if (storage.unique_within_partition && storage.is_offline_column.empty())
        {
            Row rowCopy = current_block.partition;
            MergeTreePartition partition(std::move(rowCopy));
            existing_parts = storage.getDataPartsVectorInPartition(MergeTreeData::DataPartState::Committed, partition.getID(storage));
        }
        else
        {
            existing_parts = storage.getDataPartsVector();
        }

        /// Write of each block consists of 4 steps
        /// 1. identify rows of old parts that were replaced by this block
        /// 2. write new part for this partition and new delete files for updated parts
        /// 3. write and commit new manifest log
        /// 4. commit new part and delete files of old parts in one transaction
        MergeTreeData::MutableDataPartPtr part = nullptr;
        try
        {
            PartsWithDeleteRows parts_with_deletes;
            DeletesOnMergingParts deletes_on_merging_parts;
            if (processPartitionBlock(current_block, preprocessed, existing_parts, parts_with_deletes, deletes_on_merging_parts))
            {
                LOG_DEBUG(log, "All rows in block are filtered, no new part will be generated.");
                if (parts_with_deletes.size() == 0)
                    continue;
            }
            else
            {
                if (current_block.block.has(StorageInMemoryMetadata::delete_flag_column_name))
                    throw Exception(
                        "HaUniqueMergeTree engine tries to write the delete flag column to disk which is just a func column and should not be written to disk.",
                        ErrorCodes::LOGICAL_ERROR);
                part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);
                UInt64 block_number = storage.allocateBlockNumberDirect(zookeeper);
                part->info.min_block = block_number;
                part->info.max_block = block_number;
                part->info.level = 0;
                part->name = part->getNewName(part->info);
            }

            /// generate new delete bitmaps
            MergeTreeData::DataPartsVector old_parts;
            for (auto & part_with_delete : parts_with_deletes)
                old_parts.push_back(part_with_delete.first);
            MergeTreeData::DataPartsDeleteSnapshot old_deletes = storage.getLatestDeleteSnapshot(old_parts);
            MergeTreeData::DataPartsDeleteSnapshot new_deletes;

            /// if part is nullptr, it means that this insert operation just deletes some rows with the help of _delete_flag_ column.
            if (part)
            {
                /// add empty delete bitmap for new part
                new_deletes.insert({part, std::make_shared<Roaring>()});
            }

            size_t deleted_row_size = 0;
            for (auto & part_with_delete : parts_with_deletes)
            {
                deleted_row_size += part_with_delete.second.cardinality();
                auto new_bitmap = std::make_unique<Roaring>();
                DeleteBitmapPtr old_bitmap = old_deletes[part_with_delete.first];
                if (old_bitmap)
                    *new_bitmap = *old_bitmap; // copy old delete bitmap
                *new_bitmap |= part_with_delete.second;

                DeleteBitmapPtr bitmap(std::move(new_bitmap));
                new_deletes.insert({part_with_delete.first, bitmap});
            }

            {
                /// make sure only one thread can allocate new lsn and write to manifest at a time,
                /// so that new log's prev_version matches latest log version
                auto manifest_lock = storage.manifest_store->writeLock();

                /// allocate LSN
                Coordination::Requests ops;
                auto [lsn, set_lsn_request] = storage.allocLSNAndMakeSetRequest(zookeeper);
                ops.emplace_back(std::move(set_lsn_request));

                /// pre-commit new part and delete files
                for (auto & new_delete : new_deletes)
                    new_delete.first->writeDeleteFileWithVersion(new_delete.second, lsn);
                MergeTreeData::SyncPartsTransaction local_txn(storage, new_deletes, lsn);
                if (part)
                    local_txn.preCommit(part);

                /// update /latest_lsn
                Coordination::Responses responses;
                auto multi_code = zookeeper->tryMultiNoThrow(ops, responses);
                if (multi_code != Coordination::Error::ZOK)
                {
                    local_txn.rollback();
                    String errmsg = "Unexpected ZK error while adding block for LSN " + toString(lsn) + " : "
                                    + Coordination::errorMessage(multi_code);
                    if (Coordination::isUserError(multi_code))
                    {
                        String failed_op_path = zkutil::KeeperMultiException(multi_code, ops, responses).getPathForFirstFailedOp();
                        throw Exception(errmsg + ", path " + failed_op_path, ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);
                    }
                    else
                    {
                        throw Exception(errmsg, ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR);
                    }
                }

                /// write and commit manifest log
                ManifestLogEntry log_entry;
                log_entry.type = ManifestLogEntry::SYNC_PARTS;
                log_entry.version = lsn;
                log_entry.prev_version = storage.manifest_store->latestVersion();
                log_entry.source_replica = storage.replica_name;
                if (part)
                    log_entry.added_parts.push_back(part->name);
                for (auto & entry : parts_with_deletes)
                    log_entry.updated_parts.push_back(entry.first->name);
                try
                {
                    storage.manifest_store->append(manifest_lock, log_entry, /*commit=*/true);
                    local_txn.commit();
                    if (part)
                        LOG_DEBUG(log, "Wrote committed part {} and updated {} rows at version {} in {} ms", part->name, deleted_row_size, lsn, watch.elapsedMilliseconds());
                    else
                        LOG_DEBUG(log, "Deleted {} rows at version {} in {} ms", deleted_row_size, lsn, watch.elapsedMilliseconds());
                    /// Only after commit, we can append cached keys to merging part's delete buffer
                    for (auto & entry : deletes_on_merging_parts)
                    {
                        auto & merge_state = entry.second.first;
                        auto & cached_keys = entry.second.second;
                        merge_state->delete_buffer.insert(cached_keys.begin(), cached_keys.end());
                        cached_keys.clear();
                    }
                    for (auto & entry : parts_with_deletes)
                        const_cast<IMergeTreeDataPart &>(*entry.first).gcUniqueIndexIfNeeded();
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    local_txn.rollback();
                    throw;
                }
            }
            if (part)
                PartLog::addNewPart(storage.getContext(), part, watch.elapsed(), ExecutionStatus(0));
        }
        catch (...)
        {
            if (part)
                PartLog::addNewPart(storage.getContext(), part, watch.elapsed(), ExecutionStatus::fromCurrentException(__PRETTY_FUNCTION__));
            throw;
        }
    }
}

void HaUniqueMergeTreeBlockOutputStream::writeSuffix()
{
    /// loading key index will contribute to added memory usage
    auto added_memory = current_thread->memory_tracker.get() - saved_memory_used;
    if (added_memory > (1 << 26))
        LOG_DEBUG(log, "Added {} memory after write, peak memory usage is {}", formatReadableSizeWithBinarySuffix(added_memory), formatReadableSizeWithBinarySuffix(current_thread->memory_tracker.getPeak()));

    if (remote_stream)
        remote_stream->writeSuffix();
}

/// Return whether the row should be inserted.
static bool processRowWithOfflineColumn(
    const MergeTreeData::DataPartsVector & parts,
    const UniqueKeyIndicesVector & indices,
    const String & key,
    UInt64 version,
    UInt8 is_offline,
    std::vector<std::pair<MergeTreeData::DataPartPtr, UInt32>> & removed_rows)
{
    if (parts.size() != indices.size())
        throw Exception(
            "Parts number " + toString(parts.size()) + " is not equal to indices number " + toString(indices.size()),
            ErrorCodes::LOGICAL_ERROR);
    if (is_offline)
    {
        /// offline row can only be inserted when there are no offline row in newer(>) partition.
        /// offline row replaces all rows in older(<=) partitions.
        /// For example,
        ///     partition   key     is_offline
        ///     2020-10-26  100     1
        ///     2020-10-27  100     0
        ///     2020-10-28  100     0
        /// The following row can't be inserted because it's covered by offline row in 2020-10-26
        ///     2020-10-25  100     1
        /// The following row replaces offline row in 2020-10-26
        ///     2020-10-26  100     1
        /// The following row replaces rows with the same key in 2020-10-26 and 2020-10-27
        ///     2020-10-27  100     1
        UInt64 skip_part_with_version = 0;
        for (int i = parts.size() - 1; i >= 0; --i)
        {
            auto & part = parts[i];
            auto & index = indices[i];
            UInt64 rhs_version = part->getVersionFromPartition();
            if (skip_part_with_version > 0 && rhs_version == skip_part_with_version)
                continue;

            UInt32 rowid;
            UInt8 rhs_is_offline = 0;
            if (part->getValueFromUniqueIndex(index, key, rowid, /*version=*/nullptr, &rhs_is_offline))
            {
                skip_part_with_version = rhs_version;
                /// offline row replace all rows with the same key in older partitions
                bool replace = version >= rhs_version;
                if (replace)
                    removed_rows.emplace_back(part, rowid);
                /// there can be at most one offline row with the same key, so search ends here
                if (rhs_is_offline)
                    return replace;
            }
        }
        return true;
    }
    else
    {
        /// online row can only be inserted when there are no offline row in newer(>=) partition.
        /// online row can only replace online row in the same partition.
        /// For example,
        ///     partition   key     is_offline
        ///     2020-10-26  100     1
        ///     2020-10-27  100     0
        ///     2020-10-28  100     0
        /// the following rows can't be inserted because they are covered by offline row in 2020-10-26
        ///     2020-10-25  100     0
        ///     2020-10-26  100     0
        /// the following row will replace existing row in 2020-10-27
        ///     2020-10-27  100     0
        UInt64 skip_part_with_version = 0;
        for (int i = parts.size() - 1; i >= 0; --i)
        {
            auto & part = parts[i];
            auto & index = indices[i];
            UInt64 rhs_version = part->getVersionFromPartition();
            if (version > rhs_version)
                break;
            if (skip_part_with_version > 0 && rhs_version == skip_part_with_version)
                continue;

            UInt32 rowid;
            UInt8 rhs_is_offline = 0;
            if (part->getValueFromUniqueIndex(index, key, rowid, /*version=*/nullptr, &rhs_is_offline))
            {
                /// do not insert because it's covered by offline row
                if (rhs_is_offline)
                    return false;
                if (version == rhs_version)
                {
                    removed_rows.emplace_back(part, rowid);
                    break;
                }
                skip_part_with_version = rhs_version;
            }
        }
        return true;
    }
}

/// If there are duplicated keys inside `block', only keep the highest version for each key. Removed rows are mark deleted using `filter'.
/// If `version_column' is nullptr, use row number as version, otherwise use value from version column.
/// Return number of rows mark-deleted by this function.
size_t HaUniqueMergeTreeBlockOutputStream::removeDupKeys(Block & block, ColumnWithTypeAndName * version_column, IColumn::Filter & filter)
{
    auto block_size = block.rows();
    size_t num_filtered = 0;
    if (block_size != filter.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filter size {} doesn't match block size {}", filter.size(), block_size);

    auto unique_key_expr = metadata_snapshot->getUniqueKeyExpression();
    auto unique_key_names = metadata_snapshot->getUniqueKeyColumns();
    unique_key_expr->execute(block);

    ColumnsWithTypeAndName keys;
    for (auto & name : unique_key_names)
        keys.emplace_back(block.getByName(name));
    BlockUniqueKeyComparator comparator(keys);
    /// first rowid of key -> rowid of the highest version of the same key
    std::map<size_t, size_t, decltype(comparator)> index(comparator);

    ColumnWithTypeAndName delete_flag_column;
    if (version_column && block.has(StorageInMemoryMetadata::delete_flag_column_name))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::delete_flag_column_name);

    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version = [&] (int rowid) {
            return delete_flag_column.column && delete_flag_column.column->getBool(rowid) && version_column
                && !version_column->column->getUInt(rowid);
        };

    /// if there are duplicated keys, only keep the highest version for each key
    for (size_t rowid = 0; rowid < block_size; ++rowid)
    {
        if (auto it = index.find(rowid); it != index.end())
        {
            /// When there is no explict version column, use rowid as version number,
            /// Otherwise use value from version column
            size_t old_pos = it->second;
            size_t new_pos = rowid;
            if (version_column && !delete_ignore_version(rowid) && version_column->column->getUInt(old_pos) > version_column->column->getUInt(new_pos))
                std::swap(old_pos, new_pos);
            filter[old_pos] = 0;
            it->second = new_pos;
            num_filtered++;
        }
        else
            index[rowid] = rowid;
    }
    return num_filtered;
}

size_t HaUniqueMergeTreeBlockOutputStream::removeDupKeysInPartialUpdateMode(
    Block & block,
    ColumnPtr version_column,
    IColumn::Filter & filter,
    PaddedPODArray<UInt32> & replace_dst_indexes,
    PaddedPODArray<UInt32> & replace_src_indexes)
{
    auto block_size = block.rows();
    size_t num_filtered = 0;
    if (block_size != filter.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filter size {} doesn't match block size {}", filter.size(), block_size);

    auto unique_key_names = metadata_snapshot->getUniqueKeyColumns();
    metadata_snapshot->getUniqueKeyExpression()->execute(block);

    ColumnsWithTypeAndName keys;
    for (auto & name : unique_key_names)
        keys.emplace_back(block.getByName(name));
    BlockUniqueKeyComparator comparator(keys);
    /// first rowid of key -> rowid of the highest version of the same key
    std::map<size_t, size_t, decltype(comparator)> index(comparator);


    ColumnWithTypeAndName delete_flag_column;
    if (block.has(StorageInMemoryMetadata::delete_flag_column_name))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::delete_flag_column_name);

    auto is_delete_row = [&](int rowid) { return delete_flag_column.column && delete_flag_column.column->getBool(rowid); };
    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version
        = [&](int rowid) { return is_delete_row(rowid) && version_column && !version_column->getUInt(rowid); };

    /// if there are duplicated keys, only keep the highest version for each key
    for (size_t rowid = 0; rowid < block_size; ++rowid)
    {
        if (auto it = index.find(rowid); it != index.end())
        {
            /// When there is no explict version column, use rowid as version number,
            /// Otherwise use value from version column
            size_t old_pos = it->second;
            size_t new_pos = rowid;
            if (version_column && !delete_ignore_version(rowid) && version_column->getUInt(old_pos) > version_column->getUInt(new_pos))
                std::swap(old_pos, new_pos);
            else
            {
                // Only when the version of this row is larger than previous row and the row is not delete row, it will apply partial update action. 
                if (!is_delete_row(new_pos) && !is_delete_row(old_pos))
                {
                    replace_dst_indexes.push_back(UInt32(new_pos));
                    replace_src_indexes.push_back(UInt32(old_pos));
                }
            }
            filter[old_pos] = 0;
            it->second = new_pos;
            num_filtered++;
        }
        else
            index[rowid] = rowid;
    }
    index.clear();

    /********************************************************************************************************
     * Find the final target row for each source row.
     * For example, block has six rows and the first column is unique key:
     * row id       data
     *    0     (1, 'a', [1, 2])
     *    1     (2, 'b', [3, 4]) 
     *    2     (1, 'c', [1, 2, 3])
     *    3     (1, 'd', [4, 5])
     *    4     (2, 'e', [4, 5])
     *    5     (1, 'f', [4, 5])
     * Currently, replace_dst_indexes is {2, 3, 4, 5}, replace_src_indexes is {0, 2, 1, 3}
     * Actually, the final target row for the row 0 is 3.
     * So, it needs to reverse the list and record the real target row.
     * For the result, replace_dst_indexes is {5, 4, 5, 5}, replace_src_indexes is {3, 1, 2, 0}
     ********************************************************************************************************/
    std::map<UInt32, UInt32> src_to_dst;
    PaddedPODArray<UInt32> tmp_replace_dst_indexes, tmp_replace_src_indexes;
    size_t size = replace_dst_indexes.size();
    tmp_replace_dst_indexes.reserve(size);
    tmp_replace_src_indexes.reserve(size);
    for (int i = size - 1; i >= 0; --i)
    {
        UInt32 real_dst = replace_dst_indexes[i];
        if (src_to_dst.count(replace_dst_indexes[i]))
            real_dst = src_to_dst[replace_dst_indexes[i]];
        tmp_replace_dst_indexes.push_back(real_dst);
        tmp_replace_src_indexes.push_back(replace_src_indexes[i]);
        src_to_dst[replace_src_indexes[i]] = real_dst;
    }
    replace_dst_indexes = std::move(tmp_replace_dst_indexes);
    replace_src_indexes = std::move(tmp_replace_src_indexes);
    handleCorrectPermutation(replace_dst_indexes, replace_src_indexes);
    return num_filtered;
}

void HaUniqueMergeTreeBlockOutputStream::readColumnsFromStorage(
    const MergeTreeData::DataPartPtr & part,
    RowidPairs & rowid_pairs,
    Block & to_block,
    PaddedPODArray<UInt32> & to_block_rowids)
{
    if (rowid_pairs.empty())
        return;

    size_t block_size_before = to_block.rows();
    Stopwatch timer;
    /// sort by part_rowid so that we can read part sequentially
    std::sort(rowid_pairs.begin(), rowid_pairs.end(), [](auto & lhs, auto & rhs) { return lhs.part_rowid < rhs.part_rowid; });

    DeleteBitmapPtr delete_bitmap(new Roaring);
    for (auto & pair : rowid_pairs)
        const_cast<Roaring &>(*delete_bitmap).add(pair.part_rowid);
    const_cast<Roaring &>(*delete_bitmap).flip(0, part->rows_count);

    Names read_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
    auto source = std::make_unique<MergeTreeSequentialSource>(
        storage,
        metadata_snapshot,
        part,
        delete_bitmap,
        read_columns,
        /*direct_io=*/false,
        /*take_column_types_from_storage=*/true,
        /*quite=*/false);
    Pipes pipes;
    pipes.emplace_back(Pipe(std::move(source)));

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.setMaxThreads(1);

    BlockInputStreamPtr input = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));
    input->readPrefix();
    while (const Block block = input->read())
    {
        if (!blocksHaveEqualStructure(to_block, block))
            throw Exception("Block structure mismatch", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        {
            const auto source_column = block.getByPosition(i).column;
            auto mutable_column = IColumn::mutate(std::move(to_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());

            to_block.getByPosition(i).column = std::move(mutable_column);
        }
    }
    input->readSuffix();

    for (auto & pair : rowid_pairs)
        to_block_rowids.push_back(pair.block_rowid);

    if (to_block.rows() - block_size_before != rowid_pairs.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Block size {} is not equal to expected size {}",
            to_block.rows() - block_size_before,
            rowid_pairs.size());

    LOG_DEBUG(
        log,
        "Query for {} rows in data part {} from storage, total row {}, cost {} ms",
        rowid_pairs.size(),
        part->name,
        part->rows_count,
        timer.elapsedMilliseconds());
}

void HaUniqueMergeTreeBlockOutputStream::readColumnsFromRowStore(
    const MergeTreeData::DataPartPtr & part,
    RowidPairs & rowid_pairs,
    Block & to_block,
    PaddedPODArray<UInt32> & to_block_rowids,
    const UniqueRowStorePtr & row_store)
{
    if (rowid_pairs.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of rowid pairs can not be zero.");

    /// Get unique row store meta
    UniqueRowStoreMetaPtr row_store_meta = part->tryGetUniqueRowStoreMeta();
    if (!row_store_meta)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Row store meta can not be null when row store exists for part {}", part->name);

    Stopwatch timer;
    size_t block_size_before = to_block.rows();
    /// Sort by part_rowid so that we can read row store sequentially
    std::sort(rowid_pairs.begin(), rowid_pairs.end(), [](auto & lhs, auto & rhs) { return lhs.part_rowid < rhs.part_rowid; });

    IndexFile::ReadOptions opts;
    opts.fill_cache = true;
    IndexFileIteratorPtr iter = row_store->new_iterator(opts);
    iter->SeekToFirst();

    /// Get mutable columns
    std::vector<IColumn::MutablePtr> mutable_columns;
    mutable_columns.resize(to_block.columns());
    for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        mutable_columns[i] = IColumn::mutate(std::move(to_block.getByPosition(i).column));
    std::vector<size_t> column_indexes;

    column_indexes.resize(row_store_meta->columns.size());
    std::vector<SerializationPtr> serializations;
    size_t id = 0;
    for (auto & name_and_type : row_store_meta->columns)
    {
        /// After drop column, adding the info into removed_columns of existing parts is an asynchronous operation, thus the removed_columns may be lagging.
        if (row_store_meta->removed_columns.count(name_and_type.name) || !to_block.has(name_and_type.name))
            column_indexes[id++] = to_block.columns();
        else
            column_indexes[id++] = to_block.getPositionByName(name_and_type.name);
        serializations.emplace_back(name_and_type.type->getDefaultSerialization());
    }

    const IndexFile::Comparator * comparator = IndexFile::BytewiseComparator();
    for (auto & pair : rowid_pairs)
    {
        size_t row = pair.part_rowid;
        row = Endian::big(row);
        Slice key(reinterpret_cast<const char *>(&row), sizeof(row));

        bool exact_match = false;
        int cmp = comparator->Compare(key, iter->key());
        if (cmp > 0)
            iter->NextUntil(key, exact_match);
        else if (cmp == 0)
            exact_match = true;

        if (!exact_match)
        {
            size_t row_id_current;
            ReadBufferFromString buffer(iter->key());
            readBinary(row_id_current, buffer);
            row_id_current = Endian::big(row_id_current);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not find row {} from row store in data part {}, current row id {}, part total rows {}",
                pair.part_rowid,
                part->name,
                row_id_current,
                part->rows_count);
        }

        Slice value = iter->value();
        ReadBufferFromMemory buffer(value.data(), value.size());
        for (size_t i = 0, size = row_store_meta->columns.size(); i < size; ++i)
        {
            if (column_indexes[i] < mutable_columns.size())
                serializations[i]->deserializeBinary(*mutable_columns[column_indexes[i]], buffer);
            else
            {
                Field val;
                serializations[i]->deserializeBinary(val, buffer);
            }
        }

        to_block_rowids.push_back(pair.block_rowid);
    }

    /// Handle missing columns
    for (size_t i = 0; i < mutable_columns.size(); ++i)
    {
        if (mutable_columns[i]->size() == block_size_before)
            /// TODO(lta): handle the case that the user specifies the default expression.
            mutable_columns[i]->insertManyDefaults(rowid_pairs.size());
    }

    for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        to_block.getByPosition(i).column = std::move(mutable_columns[i]);

    if (to_block.rows() - block_size_before != rowid_pairs.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Block size {} is not equal to expected size {}",
            to_block.rows() - block_size_before,
            rowid_pairs.size());
    LOG_DEBUG(
        log,
        "Query for {} rows in data part {} from row store, total row {}, cost {} ms",
        rowid_pairs.size(),
        part->name,
        part->rows_count,
        timer.elapsedMilliseconds());
}

bool HaUniqueMergeTreeBlockOutputStream::processPartitionBlock(
    BlockWithPartition & block_with_partition,
    bool preprocessed,
    const MergeTreeData::DataPartsVector & existing_parts,
    PartsWithDeleteRows & parts_with_deletes,
    DeletesOnMergingParts & deletes_on_merging_parts)
{
    if (enable_partial_update)
        return processPartitionBlockInPartialUpdateMode(block_with_partition, existing_parts, parts_with_deletes, deletes_on_merging_parts);

    auto & block = block_with_partition.block;

    size_t block_size = block.rows();
    IColumn::Filter filter(block_size, 1);
    size_t num_filtered = 0;

    ColumnWithTypeAndName version_column;
    if (storage.merging_params.hasExplicitVersionColumn())
        /// copy element here because removeDupKeys below may change block,
        /// which will invalidate all pointers and references to its elements.
        /// note that this won't copy the actual data of the column.
        version_column = block.getByName(storage.merging_params.version_column);

    ColumnWithTypeAndName is_offline_column;
    if (!storage.is_offline_column.empty())
        is_offline_column = block.getByName(storage.is_offline_column);

    ColumnWithTypeAndName delete_flag_column;
    if (block.has(StorageInMemoryMetadata::delete_flag_column_name))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::delete_flag_column_name);

    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version = [&] (int rowid) {
            return delete_flag_column.column && delete_flag_column.column->getBool(rowid) && version_column.column
                && !version_column.column->getUInt(rowid);
        };

    if (!preprocessed)
    {
        /// user can't specify explicit version column when use is_offline column feature
        if (is_offline_column.column)
            /// use is_offline_column as version so that offline row replaces !offline row
            num_filtered += removeDupKeys(block, &is_offline_column, filter);
        else if (version_column.column)
            num_filtered += removeDupKeys(block, &version_column, filter);
        else
            num_filtered += removeDupKeys(block, nullptr, filter);
    }

    UInt64 cur_version = 0;
    if (storage.merging_params.partitionValueAsVersion())
        cur_version = block_with_partition.partition[0].safeGet<UInt64>();

    auto unique_key_column_names = metadata_snapshot->getUniqueKeyColumns();
    UniqueKeyIndicesVector key_indices(existing_parts.size());
    for (size_t i = 0; i < existing_parts.size(); ++i)
    {
        key_indices[i] = existing_parts[i]->getUniqueKeyIndex();
    }

    for (size_t rowid = 0; rowid < block_size; ++rowid) {
        if (filter[rowid] == 0)
            continue;
        WriteBufferFromOwnString buf;
        for (auto & col_name : unique_key_column_names)
        {
            auto & col = block.getByName(col_name);
            auto serialization = col.type->getDefaultSerialization();
            serialization->serializeMemComparable(*col.column, rowid, buf);
        }
        String & key = buf.str();

        /// search key in existing parts
        std::vector<std::pair<MergeTreeData::DataPartPtr, UInt32>> removed_rows;
        if (is_offline_column.column)
        {
            UInt8 is_offline = is_offline_column.column->getBool(rowid);
            if (!processRowWithOfflineColumn(existing_parts, key_indices, key, cur_version, is_offline, removed_rows))
            {
                filter[rowid] = 0;
                num_filtered++;
                continue;
            }
        }
        else
        {
            UInt32 part_rowid = 0;
            UInt64 part_version = 0;
            int part_index = searchPartForKey(existing_parts, key_indices, key, part_rowid, part_version);
            if (part_index < 0)
            {
                /// check if it's just a delete operation
                if (delete_flag_column.column && delete_flag_column.column->getBool(rowid))
                {
                    filter[rowid] = 0;
                    num_filtered++;
                }
                continue;
            }
            if (!storage.merging_params.version_column.empty() && !delete_ignore_version(rowid))
            {
                if (version_column.column)
                    cur_version = version_column.column->getUInt(rowid);
                if (cur_version < part_version)
                {
                    filter[rowid] = 0;
                    num_filtered++;
                    continue;
                }
            }
            removed_rows.emplace_back(existing_parts[part_index], part_rowid);
        }

        /// check if it's just a delete operation
        if (delete_flag_column.column && delete_flag_column.column->getBool(rowid))
        {
            filter[rowid] = 0;
            num_filtered++;
        }

        for (auto & entry : removed_rows)
        {
            auto & part_with_key = entry.first;
            UInt32 part_rowid = entry.second;
            /// mark delete existing row with the same key
            parts_with_deletes[part_with_key].add(part_rowid);

            /// if part is under merge, need to cache deleted key temporarily so that merge task can remove them later
            if (auto merge_it = storage.running_merge_states.find(part_with_key); merge_it != storage.running_merge_states.end())
            {
                auto & merge_state = merge_it->second;
                if (merge_state->isCancelled())
                    continue;
                auto & pair = deletes_on_merging_parts[merge_state->new_part_name];
                if (merge_state->delete_buffer.size() + pair.second.size() < storage.getSettings()->max_delete_buffer_size_per_merge)
                {
                    if (pair.first == nullptr)
                        pair.first = merge_state;
                    pair.second.insert(key);
                }
                else
                {
                    LOG_INFO(log, "Cancel merge of {} because delete buffer size exceeds limit", merge_state->new_part_name);
                    merge_state->cancel();
                    deletes_on_merging_parts.erase(merge_state->new_part_name);
                }
            }
        }
    }

    if (block_size == num_filtered)
        return true;

    /// remove column not in header, including func columns
    auto header_block = getHeader();
    for (auto & col_name : block.getNames())
        if (!header_block.has(col_name))
            block.erase(col_name);

    if (num_filtered > 0)
    {
        ssize_t new_size_hint = block_size - num_filtered;
        for (size_t i = 0; i < block.columns(); ++i) {
            ColumnWithTypeAndName & col = block.getByPosition(i);
            col.column = col.column->filter(filter, new_size_hint);
        }
    }
    return false;
}

bool HaUniqueMergeTreeBlockOutputStream::processPartitionBlockInPartialUpdateMode(
    BlockWithPartition & block_with_partition,
    const MergeTreeData::DataPartsVector & existing_parts,
    PartsWithDeleteRows & parts_with_deletes,
    DeletesOnMergingParts & deletes_on_merging_parts)
{
    /********************************************************************
     * In partial update mode, we divide the process into several phases:
     * Phase 1: Remove duplicate keys in block and get replace info.
     * Phase 2: Get indexes that need to be searched in previous parts.
     * Phase 3: Query data from previous parts parallel.
     * Phase 4: Replace column and filter data parallel.
     * Phase 5: Remove column not in header.
     ********************************************************************/
    auto & block = block_with_partition.block;

    size_t block_size = block.rows();

    ColumnPtr version_column = nullptr;
    if (storage.merging_params.hasExplicitVersionColumn())
        /// copy element here because removeDupKeys below may change block,
        /// which will invalidate all pointers and references to its elements.
        /// note that this won't copy the actual data of the column.
        version_column = block.getByName(storage.merging_params.version_column).column;

    UInt64 cur_version = 0;
    if (storage.merging_params.partitionValueAsVersion())
        cur_version = block_with_partition.partition[0].safeGet<UInt64>();

    /// Phase 1: Remove duplicate keys in block and get replace info
    PaddedPODArray<UInt32> replace_dst_indexes, replace_src_indexes;
    IColumn::Filter filter(block_size, 1);
    size_t num_filtered = removeDupKeysInPartialUpdateMode(block, version_column, filter, replace_dst_indexes, replace_src_indexes);

    auto unique_key_column_names = metadata_snapshot->getUniqueKeyColumns();
    UniqueKeyIndicesVector key_indices(existing_parts.size());
    for (size_t i = 0; i < existing_parts.size(); ++i)
    {
        key_indices[i] = existing_parts[i]->getUniqueKeyIndex();
    }

    PartRowidPairs part_rowid_pairs {existing_parts.size()};

    auto process_row_deletion = [this, &parts_with_deletes, &deletes_on_merging_parts](
        const MergeTreeData::DataPartPtr & part_with_key, UInt32 part_rowid, const String & key)
    {
        /// mark delete existing row with the same key
        parts_with_deletes[part_with_key].add(part_rowid);

        /// if part is under merge, need to cache deleted key temporarily so that merge task can remove them later
        if (auto merge_it = storage.running_merge_states.find(part_with_key); merge_it != storage.running_merge_states.end())
        {
            auto & merge_state = merge_it->second;
            if (merge_state->isCancelled())
                return;
            auto & pair = deletes_on_merging_parts[merge_state->new_part_name];
            if (merge_state->delete_buffer.size() + pair.second.size() < storage.getSettings()->max_delete_buffer_size_per_merge)
            {
                if (pair.first == nullptr)
                    pair.first = merge_state;
                pair.second.insert(key);
            }
            else
            {
                LOG_INFO(log, "Cancel merge of {} because delete buffer size exceeds limit", merge_state->new_part_name);
                merge_state->cancel();
                deletes_on_merging_parts.erase(merge_state->new_part_name);
            }
        }
    };

    ColumnWithTypeAndName delete_flag_column;
    if (block.has(StorageInMemoryMetadata::delete_flag_column_name))
        delete_flag_column = block.getByName(StorageInMemoryMetadata::delete_flag_column_name);

    /// In the case that engine has been set version column, if version is set by user(not zero), the delete row will obey the rule of version.
    /// Otherwise, the delete row will ignore comparing version, just doing the deletion directly.
    auto delete_ignore_version = [&](int rowid) {
        return delete_flag_column.column && delete_flag_column.column->getBool(rowid) && version_column
            && !version_column->getUInt(rowid);
    };

    /********************************************************************************************************
     * Phase 2: Get indexes that need to be searched in previous parts.
     * It's necessary to remove those rows whose version is lower than that of previous parts.
     * For example, table has four column, the first column is unique key and second column is version:
     * CREATE TABLE test.example (id UInt8, version UInt8, int0 UInt8, String0 String) Engine=HaUniqueMergeTree...
     * Currently, it has one row (1, 4, 0, '') whose int0 and String0 are both default value.
     * The insert block has two row: (1, 3, 3, 'a') (1, 4, 4, '')
     * After removeDupKeys method, replace_dst_indexes is {1}, replace_src_indexes {0}
     * But version(3) of row 0 is lower than that(4) of previous parts, so row 0 should be discard.
     * The final result should be (1, 4, 4, '') whose String0 is still default value.
     * If version of row 0 is set to 4, the final result would be (1, 4, 4, 'a').
     ********************************************************************************************************/
    PaddedPODArray<UInt32> tmp_replace_dst_indexes, tmp_replace_src_indexes;
    size_t replace_index_id = 0;
    for (size_t rowid = 0; rowid < block_size; ++rowid) {
        if (filter[rowid] == 0)
            continue;
        WriteBufferFromOwnString buf;
        for (auto & col_name : unique_key_column_names)
        {
            auto & col = block.getByName(col_name);
            auto serialization = col.type->getDefaultSerialization();
            serialization->serializeMemComparable(*col.column, rowid, buf);
        }
        String & key = buf.str();

        /// search key in existing parts
        UInt32 part_rowid = 0;
        UInt64 part_version = 0;
        int part_index = searchPartForKey(existing_parts, key_indices, key, part_rowid, part_version);
        if (part_index < 0)
        {
            /// check if it's just a delete operation
            if (delete_flag_column.column && delete_flag_column.column->getBool(rowid))
            {
                filter[rowid] = 0;
                num_filtered++;
            }
            continue;
        }
        if (!storage.merging_params.version_column.empty() && !delete_ignore_version(rowid))
        {
            if (version_column)
                cur_version = version_column->getUInt(rowid);
            if (cur_version < part_version)
            {
                filter[rowid] = 0;
                num_filtered++;
                continue;
            }
        }

        /// check if it's just a delete operation
        if (delete_flag_column.column && delete_flag_column.column->getBool(rowid))
        {
            filter[rowid] = 0;
            num_filtered++;
        }
        else
        {
            part_rowid_pairs[part_index].push_back({part_rowid, static_cast<UInt32>(rowid + block_size)});

            /// Some rows may be just replace in block and not found in previous parts.
            while (replace_index_id < replace_dst_indexes.size() && replace_dst_indexes[replace_index_id] < rowid)
            {
                tmp_replace_dst_indexes.push_back(replace_dst_indexes[replace_index_id]);
                tmp_replace_src_indexes.push_back(replace_src_indexes[replace_index_id]);
                replace_index_id++;
            }
            /// Check whether to delete the previous one, there maybe multiple replaced column for the same
            while (replace_index_id < replace_dst_indexes.size() && replace_dst_indexes[replace_index_id] == rowid)
            {
                if (version_column)
                    cur_version = version_column->getUInt(rowid);
                if (cur_version >= part_version)
                {
                    tmp_replace_dst_indexes.push_back(replace_dst_indexes[replace_index_id]);
                    tmp_replace_src_indexes.push_back(replace_src_indexes[replace_index_id]);
                }
                replace_index_id++;
            }
        }
        process_row_deletion(existing_parts[part_index], part_rowid, key);
    }
    while (replace_index_id < replace_dst_indexes.size())
    {
        tmp_replace_dst_indexes.push_back(replace_dst_indexes[replace_index_id]);
        tmp_replace_src_indexes.push_back(replace_src_indexes[replace_index_id]);
        replace_index_id++;
    }
    replace_dst_indexes = std::move(tmp_replace_dst_indexes);
    replace_src_indexes = std::move(tmp_replace_src_indexes);

    if (block_size == num_filtered)
        return true; /// all filtered

    /// Phase 3: Query data from previous parts parallel.
    Stopwatch query_data_timer;
    size_t valid_num = 0, total_query_row = 0;
    for (size_t i = 0; i < part_rowid_pairs.size(); ++i)
    {
        if (part_rowid_pairs[i].empty())
            continue;
        valid_num++;
        total_query_row += part_rowid_pairs[i].size();
    }
    size_t thread_num = valid_num == 0 ? 1: std::min(valid_num, assert_cast<size_t>(8));
    ThreadPool read_data_pool(thread_num);
    std::vector<Block> columns_from_storage_vector(thread_num);
    std::vector<PaddedPODArray<UInt32>> block_rowids_vector(thread_num);
    for (size_t i = 0 ; i < thread_num; ++i)
    {
        columns_from_storage_vector[i] = metadata_snapshot->getSampleBlock();
        read_data_pool.scheduleOrThrowOnError([&, i]() {
            size_t j = 0, cnt = 0;
            while (j < part_rowid_pairs.size() && cnt != i)
            {
                if (!part_rowid_pairs[j].empty())
                    ++cnt;
                ++j;
            }
            cnt = 0;
            while (j < part_rowid_pairs.size())
            {
                if (!part_rowid_pairs[j].empty())
                {
                    if (cnt == 0)
                    {
                        UniqueRowStorePtr row_store = existing_parts[j]->tryGetUniqueRowStore();
                        if (row_store)
                            readColumnsFromRowStore(
                                existing_parts[j], part_rowid_pairs[j], columns_from_storage_vector[i], block_rowids_vector[i], row_store);
                        else
                            readColumnsFromStorage(
                                existing_parts[j], part_rowid_pairs[j], columns_from_storage_vector[i], block_rowids_vector[i]);
                    }
                    if (++cnt == thread_num)
                        cnt = 0;
                }
                j++;
            }
        });
    }
    read_data_pool.wait();
    for (size_t i = 0, size = columns_from_storage_vector[0].columns(); i < size; ++i)
    {
        auto mutable_column = IColumn::mutate(std::move(columns_from_storage_vector[0].getByPosition(i).column));
        for (size_t j = 1; j < thread_num; ++j)
        {
            const auto source_column = columns_from_storage_vector[j].getByPosition(i).column;
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
        }
        columns_from_storage_vector[0].getByPosition(i).column = std::move(mutable_column);
    }
    for (size_t j = 1; j < thread_num; ++j)
        block_rowids_vector[0].insert(block_rowids_vector[j].begin(), block_rowids_vector[j].end());
    Block & columns_from_storage = columns_from_storage_vector[0];
    PaddedPODArray<UInt32> & block_rowids = block_rowids_vector[0];
    if (columns_from_storage.rows() != block_rowids.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of block {} is not equal to replace rows {}", columns_from_storage.rows(), block_rowids.size());
    LOG_DEBUG(
        log,
        "Read from {} part parallel cost {}ms, total row {}",
        part_rowid_pairs.size(),
        query_data_timer.elapsedMilliseconds(),
        total_query_row);

    /// Phase 4: Replace column and filter data parallel.
    if (!block_rowids.empty())
    {
        PaddedPODArray<UInt32> tmp_src_indexes = getRowidPermutation(block_rowids);
        PaddedPODArray<UInt32> tmp_dst_indexes = permuteRowids(block_rowids, tmp_src_indexes);
        mergeIndices(replace_src_indexes, tmp_src_indexes);
        mergeIndices(replace_dst_indexes, tmp_dst_indexes);
    }

    NameSet non_updatable_columns;
    for (auto & name : metadata_snapshot->getColumnsRequiredForUniqueKey())
        non_updatable_columns.insert(name);
    for (auto & name : metadata_snapshot->getUniqueKeyColumns())
        non_updatable_columns.insert(name);

    thread_num = std::min(block.columns(), static_cast<size_t>(8));
    ThreadPool replace_column_pool(thread_num);
    for (size_t i = 0 ; i < thread_num; ++i)
    {
        replace_column_pool.scheduleOrThrowOnError([&, i]() {
            for (size_t j = i, size = block.columns(); j < size; j += thread_num)
            {
                auto & col = block.getByPosition(j);
                if (col.name == StorageInMemoryMetadata::delete_flag_column_name)
                    continue;

                if (replace_dst_indexes.empty() || non_updatable_columns.count(col.name))
                {
                    if (num_filtered > 0)
                    {
                        ssize_t new_size_hint = block_size - num_filtered;
                        col.column = col.column->filter(filter, new_size_hint);
                    }
                }
                else
                {
                    ColumnPtr is_default_col = col.column->selectDefault();
                    const IColumn::Filter & is_default_filter = assert_cast<const ColumnUInt8 &>(*is_default_col).getData();

                    /// all values are non-default, nothing to replace
                    if (filterIsAlwaysFalse(is_default_filter) && !col.type->isMap() && !num_filtered)
                        continue;

                    ColumnPtr column_from_storage = columns_from_storage.getByName(col.name).column;
                    if (!column_from_storage)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Column for storage {} is nullptr while replacing column for partial update.",
                            col.name);

                    ColumnPtr new_column = col.column->replaceFrom(
                        replace_dst_indexes,
                        *column_from_storage,
                        replace_src_indexes,
                        filterIsAlwaysTrue(is_default_filter) ? nullptr : &is_default_filter,
                        num_filtered > 0 ? &filter : nullptr);
                    col.column = std::move(new_column);
                }
            }
        });
    }
    replace_column_pool.wait();

    /// Phase 5: Remove column not in header, including func columns
    auto header_block = getHeader();
    for (auto & col_name : block.getNames())
        if (!header_block.has(col_name))
            block.erase(col_name);

    return false;
}

}
