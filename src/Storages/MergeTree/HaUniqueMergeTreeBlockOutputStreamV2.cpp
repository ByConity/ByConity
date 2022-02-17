#include "HaUniqueMergeTreeBlockOutputStreamV2.h"

#include <Storages/StorageHaUniqueMergeTree.h>

// for reading rows from storage
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
    extern const int LOGICAL_ERROR;
    extern const int READONLY;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int REPLICA_STATUS_CHANGED;
}

namespace
{
bool filterIsAlwaysTrue(const IColumn::Filter & filter)
{
    /// TODO: there should be faster SIMD way to implement this
    for (size_t i = 0, size = filter.size(); i < size; ++i)
        if (filter[i] == 0)
            return false;
    return true;
}

bool filterIsAlwaysFalse(const IColumn::Filter & filter)
{
    /// TODO: there should be faster SIMD way to implement this
    for (size_t i = 0, size = filter.size(); i < size; ++i)
        if (filter[i] == 1)
            return false;
    return true;
}

size_t filterDefaultCount(const IColumn::Filter & filter)
{
    size_t count = 0;
    /// TODO: there should be faster SIMD way to implement this
    for (size_t i = 0, size = filter.size(); i < size; ++i)
        if (filter[i] == 1)
            count++;
    return count;
}

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
        res[i] = rowids[perm[i]];
    return res;
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

HaUniqueMergeTreeBlockOutputStreamV2::HaUniqueMergeTreeBlockOutputStreamV2(
    StorageHaUniqueMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_, size_t max_parts_per_block_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_), max_parts_per_block(max_parts_per_block_)
        , allow_materialized(context->getSettingsRef().insert_allow_materialized_columns)
        , log(&Logger::get(storage.getLogName() + " (BlockOutputStream)"))
        , need_forward(!storage.is_leader)
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

        /// FIXME (UNIQUE KEY): Handle the case where password may become empty
        auto & client_info = context->getClientInfo();
        String user = client_info.current_user;
        String password;
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

Block HaUniqueMergeTreeBlockOutputStreamV2::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void HaUniqueMergeTreeBlockOutputStreamV2::writePrefix()
{
    if (remote_stream)
        remote_stream->writePrefix();
    else
        storage.delayInsertOrThrowIfNeeded();
}

void HaUniqueMergeTreeBlockOutputStreamV2::write(const Block & block)
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

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);

    /// unique table doesn't support concurrent write
    /// TODO:
    /// 1. support table level unique
    /// 2. move part write out of insert lock
    auto lock = storage.uniqueWriteLock();

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;
        LOG_DEBUG(log, "Wrote block with {} rows", current_block.block.rows());

        MergeTreePartition partition(current_block.partition);
        /// parts that could get modified
        MergeTreeData::DataPartsVector existing_parts = storage.getDataPartsVectorInPartition(MergeTreeData::DataPartState::Committed, partition.getID(storage));

        MergeTreeData::MutableDataPartPtr part = nullptr;
        try
        {
            PartsWithDeleteRows parts_with_deletes;
            DeletesOnMergingParts deletes_on_merging_parts;
            Stopwatch process_partition_timer;
            total_process_block_row += current_block.block.rows();
            if (processPartitionBlock(current_block, existing_parts, parts_with_deletes, deletes_on_merging_parts))
            {
                LOG_DEBUG(log, "All rows in block are filtered, no new part will be generated.");
                if (parts_with_deletes.size() == 0)
                    continue;
            }
            else
            {
                total_process_block_cost += process_partition_timer.elapsedMilliseconds();

                if (current_block.block.has(StorageInMemoryMetadata::delete_flag_column_name))
                    throw Exception(
                        "HaUniqueMergeTree engine tries to write the delete flag column to disk which is just a func column and should not be written to disk.",
                        ErrorCodes::LOGICAL_ERROR);
                Stopwatch write_timer;
                part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);

                total_write_part_cost += write_timer.elapsedMilliseconds();
                total_write_part_row += current_block.block.rows();
                total_write_row_store_cost += storage.writer.getWriteRowStoreCost();

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
                    total_write_cost += watch.elapsedMilliseconds();
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

void HaUniqueMergeTreeBlockOutputStreamV2::writeSuffix()
{
    if (total_process_block_row > 0)
    {
        LOG_DEBUG(
            log,
            "Total Process block in {} ms, total row: {}, remove dup: {} ms, remove dup judge stage: {} ms, remove dup replace column: {} ms, "
            "remove dup default value count: {}, remove dup replace handle column count: {}, get uki in {} ms, query uki in {} ms, get&query "
            "uki in {} ms, filter cost {} ms",
            total_process_block_cost,
            total_process_block_row,
            total_remove_dedup_cost,
            total_remove_dedup_judge_cost,
            total_remove_dedup_replace_column_cost,
            total_remove_dedup_default_value_row,
            total_remove_dedup_handle_column_count,
            total_get_uki_cost,
            total_query_uki_cost,
            total_get_uki_cost + total_query_uki_cost,
            total_filter_cost);
        LOG_DEBUG(log, "Total Read from store in {} ms, total row: {}", total_read_from_store_cost, total_read_row);
        LOG_DEBUG(
            log,
            "Total Replace column in {} ms, replace default value rows: {}, replace handle column count: {}",
            total_replace_cost,
            total_replace_default_value_row,
            total_replace_handle_column_count);

        LOG_DEBUG(log, "Total Write part in {} ms, total row: {}", total_write_part_cost - total_write_row_store_cost, total_write_part_row);
        LOG_DEBUG(log, "Total Write row store in {} ms, get row store in {} ms", total_write_row_store_cost, total_get_row_store_cost);
        LOG_DEBUG(log, "Total Wrote committed part in {} ms", total_write_cost);
    }
    if (remote_stream)
        remote_stream->writeSuffix();
}

size_t HaUniqueMergeTreeBlockOutputStreamV2::removeDupKeys(
    Block & block, IColumn::Filter & filter, PaddedPODArray<UInt32> & replace_dst_indexes, PaddedPODArray<UInt32> & replace_src_indexes)
{
    auto block_size = block.rows();
    size_t num_filtered = 0;
    if (block_size != filter.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filter size {} doesn't match block size {}", filter.size(), block_size);

    auto unique_key_names = metadata_snapshot->getUniqueKeyColumns();
    metadata_snapshot->getUniqueKeyExpression()->execute(block);

    Stopwatch judge_timer;
    ColumnsWithTypeAndName keys;
    for (auto & name : unique_key_names)
        keys.emplace_back(block.getByName(name));
    BlockUniqueKeyComparator comparator(keys);
    /// first rowid of key -> rowid of the highest version of the same key
    std::map<size_t, size_t, decltype(comparator)> index(comparator);

    /// if there are duplicated keys, only keep the highest version for each key
    for (size_t rowid = 0; rowid < block_size; ++rowid)
    {
        if (auto it = index.find(rowid); it != index.end())
        {
            /// When there is no explict version column, use rowid as version number,
            /// Otherwise use value from version column
            size_t old_pos = it->second;
            replace_dst_indexes.push_back(UInt32(rowid));
            replace_src_indexes.push_back(UInt32(old_pos));
            size_t new_pos = rowid;
            filter[old_pos] = 0;
            it->second = new_pos;
            num_filtered++;
        }
        else
        {
            index[rowid] = rowid;
        }
    }
    index.clear();
    total_remove_dedup_judge_cost += judge_timer.elapsedMilliseconds();
    return num_filtered;
}

void HaUniqueMergeTreeBlockOutputStreamV2::readColumnsFromStorage(
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

    DeleteBitmapPtr delete_bitmap (new Roaring);
    for (auto & pair : rowid_pairs)
        const_cast<Roaring &>(*delete_bitmap).add(pair.part_rowid);
    const_cast<Roaring &>(*delete_bitmap).flip(0, part->rows_count);

    Names read_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
    auto source = std::make_unique<MergeTreeSequentialSource>(
            storage, metadata_snapshot, part,
            delete_bitmap,
            read_columns,
            /*direct_io=*/ false,
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block size {} is not equal to expected size {}", to_block.rows() - block_size_before, rowid_pairs.size());

    LOG_DEBUG(log, "Query for {} rows in data part {} from storage, cost {} ms", rowid_pairs.size(), part->name, timer.elapsedMilliseconds());
}

void HaUniqueMergeTreeBlockOutputStreamV2::readColumnsFromRowStore(
    const MergeTreeData::DataPartPtr & part,
    RowidPairs & rowid_pairs,
    Block & to_block,
    PaddedPODArray<UInt32> & to_block_rowids,
    const UniqueRowStorePtr & row_store)
{
    if (rowid_pairs.empty())
        return;

    Stopwatch timer;
    size_t block_size_before = to_block.rows();
    /// sort by part_rowid so that we can read row store sequentially
    std::sort(rowid_pairs.begin(), rowid_pairs.end(), [](auto & lhs, auto & rhs) { return lhs.part_rowid < rhs.part_rowid; });

    DeleteBitmapPtr delete_bitmap (new Roaring);
    for (auto & pair : rowid_pairs)
        const_cast<Roaring &>(*delete_bitmap).add(pair.part_rowid);
    const_cast<Roaring &>(*delete_bitmap).flip(0, part->rows_count);

    IndexFile::ReadOptions opts;
    opts.fill_cache = true;
    IndexFileIteratorPtr iter = row_store->new_iterator(opts);
    iter->SeekToFirst();

    /// get mutable columns
    std::vector<IColumn::MutablePtr> mutable_columns;
    mutable_columns.resize(to_block.columns());
    for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        mutable_columns[i] = IColumn::mutate(std::move(to_block.getByPosition(i).column));

    const IndexFile::Comparator * comparator = IndexFile::BytewiseComparator();
    for (auto & pair : rowid_pairs)
    {
        size_t row = pair.part_rowid;
        row = Endian::big(row);
        WriteBufferFromOwnString row_buf;
        writeBinary(row, row_buf);
        String key = row_buf.str();

        /// method 1
        // iter->Seek(key);
        // if (!iter->Valid() || comparator->Compare(key, iter->key()) != 0)
        // {
        //     throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find row {} from row store in data part {}", pair.part_rowid, part->name);
        // }

        /// method 2
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

        std::vector<SerializationPtr> serializations;
        /// TODO(lta): check serializations
        for (const auto & column: to_block.getNamesAndTypesList())
            serializations.emplace_back(column.type->getDefaultSerialization());

        /// TODO(lta): check if metadata is match
        Slice value = iter->value();
        ReadBufferFromMemory buffer(value.data(), value.size());
        for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        {
            Field val;
            serializations[i]->deserializeBinary(val, buffer);
            mutable_columns[i]->insert(val);
        }

        to_block_rowids.push_back(pair.block_rowid);
    }

    for (size_t i = 0, size = to_block.columns(); i < size; ++i)
        to_block.getByPosition(i).column = std::move(mutable_columns[i]);

    if (to_block.rows() - block_size_before != rowid_pairs.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block size {} is not equal to expected size {}", to_block.rows() - block_size_before, rowid_pairs.size());
    LOG_DEBUG(log, "Query for {} rows in data part {} from row store, cost {} ms", rowid_pairs.size(), part->name, timer.elapsedMilliseconds());
}

/// TODO:
/// - support version column
/// - support delete flag
bool HaUniqueMergeTreeBlockOutputStreamV2::processPartitionBlock(
    BlockWithPartition & block_with_partition,
    const MergeTreeData::DataPartsVector & existing_parts,
    PartsWithDeleteRows & parts_with_deletes,
    DeletesOnMergingParts & deletes_on_merging_parts)
{
    auto & block = block_with_partition.block;

    size_t block_size = block.rows();
    IColumn::Filter filter(block_size, 1);
    Stopwatch remove_dedup_timer;
    PaddedPODArray<UInt32> replace_dst_indexes, replace_src_indexes;
    size_t num_filtered = removeDupKeys(block, filter, replace_dst_indexes, replace_src_indexes);

    total_remove_dedup_cost += remove_dedup_timer.elapsedMilliseconds();

    Stopwatch get_uki_timer;
    auto unique_key_column_names = metadata_snapshot->getUniqueKeyColumns();
    UniqueKeyIndicesVector key_indices(existing_parts.size());
    for (size_t i = 0; i < existing_parts.size(); ++i)
    {
        key_indices[i] = existing_parts[i]->getUniqueKeyIndex();
    }
    total_get_uki_cost += get_uki_timer.elapsedMilliseconds();

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

    Stopwatch query_uki_timer;
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
            continue;
        }

        /// TODO(lta): optimize(skip non partial update row)
        /// Rowid should be added with block_size to be distinguished with those rows which will be replaced from column self.
        part_rowid_pairs[part_index].push_back({part_rowid, static_cast<UInt32>(rowid + block_size)});
        process_row_deletion(existing_parts[part_index], part_rowid, key);
    }
    total_query_uki_cost += query_uki_timer.elapsedMilliseconds();

    if (block_size == num_filtered)
        return true; /// all filtered

    Block columns_from_storage = metadata_snapshot->getSampleBlock();
    PaddedPODArray<UInt32> block_rowids;

    Stopwatch timer;
    size_t total_row = 0;
    for (size_t i = 0; i < part_rowid_pairs.size(); ++i)
    {
        if (part_rowid_pairs[i].empty())
            continue;
        
        Stopwatch get_row_store_timer;
        UniqueRowStorePtr row_store = existing_parts[i]->tryGetUniqueRowStore();
        total_get_row_store_cost += get_row_store_timer.elapsedMilliseconds();

        total_row += part_rowid_pairs[i].size();
        
        /// According to the result of performace test, when the query data ratio is less than 5%, query row store has a better performance than query column store
        /// For more detail, please see https://bytedance.feishu.cn/docs/doccnilaBbofUvfnQ3zBuLQKjFe#xMQQwA
        if (row_store && part_rowid_pairs[i].size() * 100 < existing_parts[i]->rows_count * 5)
            readColumnsFromRowStore(existing_parts[i], part_rowid_pairs[i], columns_from_storage, block_rowids, row_store);
        else
            readColumnsFromStorage(existing_parts[i], part_rowid_pairs[i], columns_from_storage, block_rowids);
        LOG_DEBUG(
            log,
            "Read from part {} cost {}ms, part rows {}, read rows {}",
            existing_parts[i]->name,
            get_row_store_timer.elapsedMilliseconds(),
            existing_parts[i]->rows_count,
            part_rowid_pairs[i].size());
    }
    LOG_DEBUG(log, "Read from {} part cost {}ms, total row {}", part_rowid_pairs.size(), timer.elapsedMilliseconds(), total_row);
    total_read_from_store_cost += timer.elapsedMilliseconds();
    total_read_row += total_row;

    Stopwatch replace_timer;
    if (!block_rowids.empty() || !replace_dst_indexes.empty())
    {
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

        for (auto & col : block)
        {
            if (col.name == StorageInMemoryMetadata::delete_flag_column_name)
                continue;

            if (non_updatable_columns.count(col.name))
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
                if (filterIsAlwaysFalse(is_default_filter))
                    continue;

                total_replace_default_value_row += filterDefaultCount(is_default_filter);
                total_replace_handle_column_count++;
                ColumnPtr column_from_storage = columns_from_storage.getByName(col.name).column;
                ColumnPtr new_column = col.column->replaceFrom(
                    replace_dst_indexes,
                    *column_from_storage,
                    replace_src_indexes,
                    filterIsAlwaysTrue(is_default_filter) ? nullptr : &is_default_filter,
                    num_filtered > 0 ? &filter : nullptr);
                col.column = std::move(new_column);
            }
        }

        total_replace_cost += replace_timer.elapsedMilliseconds();
    }
    else
    {
        Stopwatch filter_timer;
        if (num_filtered > 0)
        {
            ssize_t new_size_hint = block_size - num_filtered;
            for (size_t i = 0; i < block.columns(); ++i) {
                ColumnWithTypeAndName & col = block.getByPosition(i);
                col.column = col.column->filter(filter, new_size_hint);
            }
            total_filter_cost += filter_timer.elapsedMilliseconds();
        }
    }

    /// remove column not in header, including func columns
    auto header_block = getHeader();
    for (auto & col_name : block.getNames())
        if (!header_block.has(col_name))
            block.erase(col_name);

    return false;
}

} // namespace DB
