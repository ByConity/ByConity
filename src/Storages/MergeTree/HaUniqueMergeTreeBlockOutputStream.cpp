#include <Storages/MergeTree/HaUniqueMergeTreeBlockOutputStream.h>

#include <DataStreams/RemoteBlockOutputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Block.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <Common/ThreadStatus.h> // for current_thread

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

HaUniqueMergeTreeBlockOutputStream::HaUniqueMergeTreeBlockOutputStream(
    StorageHaUniqueMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_, size_t max_parts_per_block_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_), max_parts_per_block(max_parts_per_block_),
          allow_materialized(context->getSettingsRef().insert_allow_materialized_columns),
          log(&Logger::get(storage.getLogName() + " (BlockOutputStream)")),
          need_forward(!storage.is_leader)
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

/// Search `key' in `parts'.
/// If found, return part containing the key and set rowid and version(optional) for the key.
/// Otherwise return nullptr.
static MergeTreeData::DataPartPtr searchPartForKey(
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
            return part;
    }
    return nullptr;
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

bool HaUniqueMergeTreeBlockOutputStream::processPartitionBlock(
    BlockWithPartition & block_with_partition,
    bool preprocessed,
    const MergeTreeData::DataPartsVector & existing_parts,
    PartsWithDeleteRows & parts_with_deletes,
    DeletesOnMergingParts & deletes_on_merging_parts)
{
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
            serialization->serializeBinary(*col.column, rowid, buf);
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
            MergeTreeData::DataPartPtr part_with_key = searchPartForKey(existing_parts, key_indices, key, part_rowid, part_version);
            if (part_with_key == nullptr)
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
            removed_rows.emplace_back(part_with_key, part_rowid);
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

}
