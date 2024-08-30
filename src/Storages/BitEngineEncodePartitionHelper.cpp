#include <CloudServices/CnchDataWriter.h>
#include <CloudServices/CnchPartsHelper.h>
#include <DaemonManager/BackgroundJob.h>
#include <DaemonManager/DaemonManagerClient.h>
#include <DataStreams/TransactionWrapperBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Parsers/queryToString.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Storages/BitEngine/BitEngineDictionaryManager.h>
#include <Storages/BitEngineEncodePartitionHelper.h>
#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Poco/Logger.h>
#include <Common/Stopwatch.h>
#include "DataStreams/IBlockStream_fwd.h"
#include "DataStreams/UnionBlockInputStream.h"
#include <DataStreams/SquashingBlockInputStream.h>
#include <Parsers/ASTAlterQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

/// To do mutate, reserve amount of space equals to sum size of parts times specified coefficient.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

Pipe StorageCnchMergeTree::preattachPartition(const PartitionCommand & command, const ContextPtr & local_context, const ASTPtr & query)
{
    if (!isBitEngineTable())
        return {};

    String partition_id = getPartitionIDFromQuery(command.partition, local_context);
    auto catalog = local_context->getCnchCatalog();
    auto cur_txn = local_context->getCurrentTransaction();

    /// 1. get partition lock
    LockInfoPtr partition_lock = std::make_shared<LockInfo>(cur_txn->getTransactionID());
    partition_lock->setMode(LockMode::X);
    partition_lock->setTimeout(local_context->getSettingsRef().ingest_column_memory_lock_timeout.value.totalMilliseconds()); // default 5s
    partition_lock->setUUIDAndPrefix(getStorageUUID(), LockInfo::task_domain);
    partition_lock->setPartition(partition_id);

    Stopwatch lock_watch;
    auto cnch_lock = std::make_shared<CnchLockHolder>(local_context, std::move(partition_lock));
    cnch_lock->lock();
    LOG_DEBUG(log, "Acquired lock in {} ms", lock_watch.elapsedMilliseconds());

    /// 2. stop merges of the table
    /// remove the merge mutate tasks that could cause WW conflict before get server part
    auto daemon_manager_client_ptr = local_context->getDaemonManagerClient();
    if (!daemon_manager_client_ptr)
        throw Exception("Failed to get daemon manager client", ErrorCodes::SYSTEM_ERROR);

    std::optional<DaemonManager::BGJobInfo> merge_job_info
        = daemon_manager_client_ptr->getDMBGJobInfo(getStorageUUID(), CnchBGThreadType::MergeMutate, local_context->getCurrentQueryId());
    if (!merge_job_info || merge_job_info->host_port.empty())
        LOG_DEBUG(
            log,
            "Will skip removing related merge tasks as there is no valid host server for table's merge job: {}",
            getStorageID().getNameForLogs());
    else
    {
        auto server_client_ptr = local_context->getCnchServerClient(merge_job_info->host_port);
        if (!server_client_ptr)
            throw Exception("Failed to get server client with host port " + merge_job_info->host_port, ErrorCodes::SYSTEM_ERROR);
        if (!server_client_ptr->removeMergeMutateTasksOnPartitions(getStorageID(), {partition_id}))
            throw Exception(
                "Failed to get remove MergeMutateTasks on partition_id " + partition_id + " for table " + getStorageID().getNameForLogs(),
                ErrorCodes::SYSTEM_ERROR);
    }

    /// 3. get source_parts of the partition
    ServerDataPartsVector source_parts = catalog->getServerDataPartsInPartitions(
        shared_from_this(), {partition_id}, local_context->getCurrentCnchStartTime(), local_context.get());
    ServerDataPartsVector visible_source_parts
        = CnchPartsHelper::calcVisibleParts(source_parts, false, CnchPartsHelper::LoggingOption::EnableLogging);
    LOG_DEBUG(
        log,
        "In partition_id: {}, number of server source parts: {}, visible source parts: {}",
        partition_id,
        source_parts.size(),
        visible_source_parts.size());

    /// 4. allocate dict table and parts
    auto underlying_dicts_mapping = getUnderlyDictionaryTables();
    for (auto & entry : underlying_dicts_mapping)
    {
        auto storage_underlying_dict
            = DatabaseCatalog::instance().tryGetTable(StorageID{entry.second.first, entry.second.second}, local_context);

        auto * storage_underlying_dict_cnch = dynamic_cast<StorageCnchMergeTree *>(storage_underlying_dict.get());
        if (storage_underlying_dict_cnch)
        {
            storage_underlying_dict_cnch->allocateForBitEngine(local_context, std::set<Int64>{}, WorkerEngineType::DICT);
        }
    }

    /// 5. allocate bitengine table and visible_source_parts
    String local_table_name = getCloudTableName(local_context);
    collectResource(local_context, visible_source_parts, local_table_name);

    /// 6. send Alter query to each worker, and get the Remote Stream to construct an pipe
    /// 6.1 rewrite the query to cloud worker
    auto query_send = query->clone();
    ASTAlterQuery & query_send_ref = query_send->as<ASTAlterQuery &>();
    query_send_ref.database = getStorageID().getDatabaseName();
    query_send_ref.table = local_table_name;
    String query_send_to_worker = queryToString(query_send);

    /// 6.2 collect worker group
    auto worker_group = getWorkerGroupForTable(*this, local_context);
    local_context->setCurrentWorkerGroup(worker_group);
    healthCheckForWorkerGroup(local_context, worker_group);

    /// 6.3 construct remote_stream/pipe to send query to each worker
    std::vector<BlockInputStreamPtr> remote_streams;
    for (const auto & shard_info : worker_group->getShardsInfo())
    {
        auto preattach_stream = CnchStorageCommonHelper::sendQueryPerShard(local_context, query_send_to_worker, shard_info);
        remote_streams.emplace_back(preattach_stream);
    }

    cur_txn->setMainTableUUID(getStorageUUID());
    auto union_stream = std::make_shared<UnionBlockInputStream>(remote_streams, nullptr, local_context->getSettingsRef().max_threads);
    auto transaction_stream = std::make_shared<TransactionWrapperBlockInputStream>(union_stream, std::move(cur_txn), std::move(cnch_lock));

    return Pipe{std::make_shared<SourceFromInputStream>(std::move(transaction_stream))};
}

BitEngineEncodePartitionStream::BitEngineEncodePartitionStream(
    const StorageCloudMergeTree & cloud_merge_tree, const PartitionCommand & command_, ContextPtr local_context_)
    : storage(cloud_merge_tree), command(command_), local_context(local_context_)
{
}

FutureMergedMutatedPart getFuturePart(const MergeTreeMetaBase::DataPartPtr & part, ContextPtr & local_context)
{
    auto new_part_info = part->info;
    new_part_info.level += 1;
    new_part_info.hint_mutation = new_part_info.mutation;
    new_part_info.mutation = local_context->getCurrentTransactionID().toUInt64();

    FutureMergedMutatedPart future_part;
    future_part.uuid = UUIDHelpers::generateV4();
    future_part.parts.push_back(part);
    future_part.part_info = new_part_info;
    future_part.name = new_part_info.getPartName();
    future_part.type = part->getType();
    return future_part;
}

Block BitEngineEncodePartitionStream::readImpl()
{
    Stopwatch watch;
    String partition_id = storage.getPartitionIDFromQuery(command.partition, local_context);
    auto parts_to_encode = storage.getDataPartsVectorInPartition(MergeTreeMetaBase::DataPartState::Committed, partition_id);

    /// 1. encapsulate  FutureParts for parts_to_encode
    std::vector<FutureMergedMutatedPart> future_parts;
    for (const auto & part : parts_to_encode)
    {
        future_parts.emplace_back(getFuturePart(part, local_context));
    }

    /// 2. BitEngineDictionaryManager encode parts
    auto dict_manager = storage.getBitEngineDictionaryManager();
    auto temp_parts = dict_manager->encodeParts(storage, future_parts, local_context);

    /// 3. commit encoded parts
    CnchDataWriter cnch_writer(const_cast<StorageCloudMergeTree &>(storage), local_context, ManipulationType::Insert);
    cnch_writer.dumpAndCommitCnchParts(temp_parts);

    LOG_DEBUG(
        &Poco::Logger::get("BitEngineEncodePartition"),
        "({}) BitEngine encode partition_id {} with {} parts cost {} s.",
        storage.getStorageID().getNameForLogs(),
        partition_id,
        parts_to_encode.size(),
        watch.elapsedSeconds());

    return {};
}

PartsEncoder::PartsEncoder(const StorageCloudMergeTree & storage_, BitEngineDictionaryManager & dict_manager_, ContextPtr local_context_)
    : storage(storage_), dict_manager(dict_manager_), local_context(local_context_)
{
}

MergeTreeMetaBase::MutableDataPartsVector PartsEncoder::encodeBitEngineParts(std::vector<FutureMergedMutatedPart> & future_parts)
{
    MergeTreeMetaBase::MutableDataPartsVector result_parts;
    for (auto & part : future_parts)
    {
        auto encoded_part = encodeBitEnginePart(part);
        result_parts.emplace_back(encoded_part);
    }
    return result_parts;
}

static bool needSyncPart(size_t input_rows, size_t input_bytes, const MergeTreeSettings & settings)
{
    return (
        (settings.min_rows_to_fsync_after_merge && input_rows >= settings.min_rows_to_fsync_after_merge)
        || (settings.min_compressed_bytes_to_fsync_after_merge && input_bytes >= settings.min_compressed_bytes_to_fsync_after_merge));
}

MergeTreeMetaBase::MutableDataPartPtr PartsEncoder::encodeBitEnginePart(FutureMergedMutatedPart & future_part)
{
    auto & source_part = future_part.parts.at(0);
    auto columns_to_encode = getBitEngineColumnsInPart(source_part);
    if (columns_to_encode.empty())
        return nullptr;

    bool need_sync = needSyncPart(source_part->rows_count, source_part->getBytesOnDisk(), *storage.getSettings());
    auto compression_codec = source_part->default_codec;

    if (!compression_codec)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown codec for bitengine encode part: {}", source_part->name);

    auto new_partial_part = createEmptyTempPart(future_part);

    auto input_stream = createInputStream(source_part, columns_to_encode.getNames());
    auto output_stream = createOutputStream(source_part, input_stream->getHeader(), new_partial_part, compression_codec);
    encodeTransform(*input_stream, *output_stream, new_partial_part, need_sync);

    finalizeTempPart(source_part, new_partial_part, compression_codec);

    return new_partial_part;
}

NamesAndTypesList PartsEncoder::getBitEngineColumnsInPart(const IMergeTreeDataPartPtr & part)
{
    const auto & columns = part->getColumns();
    NamesAndTypesList columns_to_encode;
    for (const auto & column : columns)
    {
        if (!isBitmap64(column.type))
            continue;

        bool bitengine_type = column.type->isBitEngineEncode();
        if (bitengine_type || storage.isBitEngineEncodeColumn(column.name))
        {
            if (!bitengine_type)
                const_cast<IDataType *>(column.type.get())->setFlags(TYPE_BITENGINE_ENCODE_FLAG);
            columns_to_encode.push_back(column);
        }
    }

    return columns_to_encode;
}

IMutableMergeTreeDataPartPtr PartsEncoder::createEmptyTempPart(FutureMergedMutatedPart & future_part)
{
    auto & part = future_part.parts.at(0);
    auto estimated_space_for_result = static_cast<size_t>(part->getBytesOnDisk() * DISK_USAGE_COEFFICIENT_TO_RESERVE);
    ReservationPtr reserved_space = storage.reserveSpace(estimated_space_for_result, IStorage::StorageLocation::AUXILITY);

    if (!reserved_space)
        throw Exception("Not enough space for encoding part '" + part->name + "' ", ErrorCodes::NOT_ENOUGH_SPACE);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, reserved_space->getDisk(), 0);

    auto new_partial_part = storage.createPart(
        future_part.name,
        MergeTreeDataPartType::WIDE,
        future_part.part_info,
        single_disk_volume,
        "tmp_enc_" + future_part.name,
        nullptr,
        IStorage::StorageLocation::AUXILITY);

    new_partial_part->uuid = future_part.uuid;
    new_partial_part->is_temp = true;
    new_partial_part->ttl_infos = part->ttl_infos;
    new_partial_part->versions = part->versions;

    new_partial_part->index_granularity_info = part->index_granularity_info;
    new_partial_part->setColumns(part->getColumns());
    new_partial_part->partition.assign(part->partition);
    new_partial_part->columns_commit_time = part->columns_commit_time;
    new_partial_part->mutation_commit_time = part->mutation_commit_time;
    if (storage.isBucketTable())
        new_partial_part->bucket_number = part->bucket_number;

    new_partial_part->checksums_ptr = std::make_shared<MergeTreeData::DataPart::Checksums>();

    auto disk = new_partial_part->volume->getDisk();
    String new_part_tmp_path = new_partial_part->getFullRelativePath();

    SyncGuardPtr sync_guard;
    if (storage.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(new_part_tmp_path);

    /// calculate which columns can be skipped in encoding
    // NameSet files_to_skip = source_part->getFileNamesWithoutChecksums();
    disk->createDirectories(new_part_tmp_path);

    return new_partial_part;
}

BlockInputStreamPtr PartsEncoder::createInputStream(const IMergeTreeDataPartPtr & part, Names column_names)
{
    auto input_source = std::make_unique<MergeTreeSequentialSource>(
        storage,
        storage.getStorageSnapshot(storage.getInMemoryMetadataPtr(), nullptr),
        part,
        column_names,
        /*read_with_direct_io*/ false,
        /*take_column_types_from_storage*/ true);

    QueryPipeline pipeline;
    pipeline.init(Pipe(std::move(input_source)));
    pipeline.setMaxThreads(1);
    BlockInputStreamPtr pipeline_input_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    pipeline_input_stream = std::make_shared<SquashingBlockInputStream>(pipeline_input_stream, 
        local_context->getSettingsRef().min_insert_block_size_rows,
        local_context->getSettingsRef().min_insert_block_size_bytes * 2U);

    return pipeline_input_stream;
}

BlockOutputStreamPtr PartsEncoder::createOutputStream(
    const IMergeTreeDataPartPtr & source_part,
    const Block & header_in,
    IMutableMergeTreeDataPartPtr & new_temp_part,
    const CompressionCodecPtr & codec)
{
    /// Calc header
    Block header_to_write;
    for (const auto & column : header_in.getColumnsWithTypeAndName())
    {
        if (isBitmap64(column.type))
        {
            header_to_write.insert(ColumnWithTypeAndName(column.column, column.type, column.name + BITENGINE_COLUMN_EXTENSION));
        }
    }

    MergeTreeWriterSettings writer_settings(
        storage.getContext()->getSettings(),
        storage.getSettings(),
        /*can_use_adaptive_granularity = */ false,
        false);

    return std::make_shared<MergedColumnOnlyOutputStream>(
        new_temp_part,
        storage.getInMemoryMetadataPtr(),
        writer_settings,
        header_to_write,
        codec,
        std::vector<MergeTreeIndexPtr>{},
        nullptr,
        source_part->index_granularity);
}

void PartsEncoder::encodeTransform(
    IBlockInputStream & in, IBlockOutputStream & out, IMutableMergeTreeDataPartPtr & new_temp_part, bool need_sync)
{
    in.readPrefix();
    out.writePrefix();

    Block block;
    while ((block = in.read()))
    {
        writeImplicitColumnForBitEngine(block, new_temp_part->bucket_number);
        out.write(block);
    }

    in.readSuffix();
    auto changed_checksums = dynamic_cast<MergedColumnOnlyOutputStream &>(out).writeSuffixAndGetChecksums(
        new_temp_part, *new_temp_part->getChecksums(), need_sync);
    new_temp_part->checksums_ptr->add(std::move(changed_checksums));
}

void PartsEncoder::writeImplicitColumnForBitEngine(Block & block, Int64 bucket_number)
{
    ColumnsWithTypeAndName encoded_columns;
    const auto & columns = block.getColumnsWithTypeAndName();

    for (const auto & column : columns)
    {
        if (!isBitmap64(column.type))
            continue;

        /// check whether the column is a legal BitEngine column in table
        if (!storage.isBitEngineEncodeColumn(column.name))
            continue;

        try
        {
            auto encoded_column = dict_manager.encodeColumn(column, column.name, bucket_number, local_context, BitEngineEncodeSettings{});

            encoded_columns.push_back(encoded_column);
        }
        catch (Exception & e)
        {
            // LOG_ERROR(&Poco::Logger::get("BitEnginePartsEncoder"), "BitEngine encode column exception: {}", e.message());
            // tryLogCurrentException(__PRETTY_FUNCTION__);
            throw Exception("BitEngine encode exception. reason: " + String(e.message()), ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (!encoded_columns.empty())
    {
        for (auto & encoded_column : encoded_columns)
            block.insertUnique(encoded_column);
    }
}

void PartsEncoder::finalizeTempPart(
    const MergeTreeDataPartPtr & source_part, const MergeTreeMutableDataPartPtr & new_partial_part, const CompressionCodecPtr & codec)
{
    auto disk = new_partial_part->volume->getDisk();
    auto new_part_checksums_ptr = new_partial_part->getChecksums();

    if (new_partial_part->uuid != UUIDHelpers::Nil)
    {
        auto out = disk->writeFile(new_partial_part->getFullRelativePath() + IMergeTreeDataPart::UUID_FILE_NAME, {.buffer_size = 4096});
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_partial_part->uuid, out_hashing);
        new_part_checksums_ptr->files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_part_checksums_ptr->files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
    }

    {
        /// Write file with checksums.
        auto out_checksums = disk->writeFile(fs::path(new_partial_part->getFullRelativePath()) / "checksums.txt", {.buffer_size = 4096});
        new_part_checksums_ptr->versions = new_partial_part->versions;
        new_part_checksums_ptr->write(*out_checksums);
    } /// close fd

    {
        auto out = disk->writeFile(
            new_partial_part->getFullRelativePath() + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, {.buffer_size = 4096});
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out);
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = disk->writeFile(fs::path(new_partial_part->getFullRelativePath()) / "columns.txt", {.buffer_size = 4096});
        new_partial_part->getColumns().writeText(*out_columns);
    } /// close fd

    new_partial_part->rows_count = source_part->rows_count;
    new_partial_part->index_granularity = source_part->index_granularity;
    new_partial_part->index = source_part->getIndex();
    new_partial_part->minmax_idx = source_part->minmax_idx;
    new_partial_part->modification_time = time(nullptr);
    new_partial_part->loadProjections(false, false);
    new_partial_part->setBytesOnDisk(
        MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_partial_part->volume->getDisk(), new_partial_part->getFullRelativePath()));
    new_partial_part->default_codec = codec;
}
}
