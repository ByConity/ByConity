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

#include <atomic>
#include <memory>
#include <CloudServices/CnchDataWriter.h>
#include <common/scope_guard_safe.h>
#include <Catalog/Catalog.h>
#include <CloudServices/CnchMergeMutateThread.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <Interpreters/Context.h>
#include <Interpreters/PartLog.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Transaction/Actions/DropRangeAction.h>
#include <Transaction/Actions/InsertAction.h>
#include <Transaction/Actions/MergeMutateAction.h>
#include <Transaction/Actions/S3AttachMetaAction.h>
#include <Transaction/Actions/S3DetachMetaAction.h>
#include <Transaction/CnchServerTransaction.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>
#include <WorkerTasks/ManipulationType.h>
#include <common/strong_typedef.h>

namespace ProfileEvents
{
    extern const Event CnchWriteDataElapsedMilliseconds;
    extern const Event PreloadSubmitTotalOps;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_TIMEOUT;
    extern const int BAD_DATA_PART_NAME;
    extern const int BUCKET_TABLE_ENGINE_MISMATCH;
}

bool DumpedData::isEmpty()
{
    return parts.empty() && bitmaps.empty() && staged_parts.empty();
}

void DumpedData::extend(DumpedData && data)
{
    if (data.isEmpty())
        return;

    auto extendImpl = [] (auto & src, auto && dst) {
        if (src.empty())
        {
            src = std::move(dst);
        }
        else
        {
            src.reserve(src.size() + dst.size());
            std::move(std::begin(dst), std::end(dst), std::back_inserter(src));
        }
    };

    extendImpl(parts, std::move(data.parts));
    extendImpl(bitmaps, std::move(data.bitmaps));
    extendImpl(staged_parts, std::move(data.staged_parts));

    if (dedup_mode != data.dedup_mode)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Dedup mode is mismatch, {}/{}", typeToString(dedup_mode), typeToString(data.dedup_mode));
}

using DumpCancelPred = std::function<bool()>;

static std::function<void()> createCnchDumpJob(
    std::function<void()> job, ExceptionHandler & handler, const ThreadGroupStatusPtr & thread_group, DumpCancelPred cancelled)
{
    return [job{std::move(job)}, &handler, thread_group, is_cancelled{std::move(cancelled)}]() {
        try
        {
            SCOPE_EXIT({
                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
            });
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);

            if (!is_cancelled())
                job();
        }
        catch (...)
        {
            handler.setException(std::current_exception());
        }
    };
}


CnchDataWriter::CnchDataWriter(
    MergeTreeMetaBase & storage_,
    ContextPtr context_,
    ManipulationType type_,
    String task_id_,
    String consumer_group_,
    const cppkafka::TopicPartitionList & tpl_,
    const MySQLBinLogInfo & binlog_,
    UInt64 peak_memory_usage_)
    : storage(storage_)
    , context(context_)
    , type(type_)
    , task_id(std::move(task_id_))
    , consumer_group(std::move(consumer_group_))
    , tpl(tpl_)
    , binlog(binlog_)
    , instance_id(context->getPlanSegmentInstanceId())
    , peak_memory_usage(peak_memory_usage_)
{
}

CnchDataWriter::~CnchDataWriter()
{
    if (thread_pool)
    {
        cancelled.store(true, std::memory_order_seq_cst);
        thread_pool->wait();
    }
}

DumpedData CnchDataWriter::dumpAndCommitCnchParts(
    const IMutableMergeTreeDataPartsVector & temp_parts,
    const LocalDeleteBitmaps & temp_bitmaps,
    const IMutableMergeTreeDataPartsVector & temp_staged_parts)
{
    if (temp_parts.empty() && temp_bitmaps.empty() && temp_staged_parts.empty())
        // Nothing to dump and commit, returns
        return {.dedup_mode = dedup_mode};

    LOG_DEBUG(
        storage.getLogger(),
        "Start dump and commit {} parts, {} bitmaps, {} staged parts",
        temp_parts.size(),
        temp_bitmaps.size(),
        temp_staged_parts.size());
    // FIXME: find the root case.
    for (const auto & part : temp_parts)
    {
        if (part->info.min_block < 0 || part->info.max_block <= 0)
            throw Exception("Attempt to submit illegal part " + part->info.getPartName(), ErrorCodes::BAD_DATA_PART_NAME);
    }

    auto data = dumpCnchParts(temp_parts, temp_bitmaps, temp_staged_parts);
    commitDumpedParts(data);
    return data;
}

DumpedData CnchDataWriter::dumpCnchParts(
    const IMutableMergeTreeDataPartsVector & temp_parts,
    const LocalDeleteBitmaps & temp_bitmaps,
    const IMutableMergeTreeDataPartsVector & temp_staged_parts)
{
    if (temp_parts.empty() && temp_bitmaps.empty() && temp_staged_parts.empty())
        // Nothing to dump, returns
        return {.dedup_mode = dedup_mode};

    Stopwatch watch;

    const auto & settings = context->getSettingsRef();

    if (settings.debug_cnch_remain_temp_part)
    {
        for (const auto & part : temp_parts)
            part->is_temp = false;
        for (const auto & part : temp_staged_parts)
            part->is_temp = false;
    }

    auto curr_txn = context->getCurrentTransaction();

    // set main table uuid in server or worker side
    curr_txn->setMainTableUUID(storage.getCnchStorageUUID());

    /// get offsets first and the parts shouldn't be dumped and committed if get offsets failed
    if (context->getServerType() == ServerType::cnch_worker)
    {
        auto kafka_table_id = curr_txn ? curr_txn->getKafkaTableID() : StorageID::createEmpty();
        if (!kafka_table_id.empty())
        {
            if (!curr_txn)
                throw Exception("No transaction set for committing Kafka transaction", ErrorCodes::LOGICAL_ERROR);

            curr_txn->getKafkaTpl(consumer_group, tpl);
            if (tpl.empty() || consumer_group.empty())
                throw Exception("No tpl got for kafka consume, and we won't dump and commit parts", ErrorCodes::LOGICAL_ERROR);
        }

        binlog = curr_txn->getBinlogInfo();
    }

    auto txn_id = curr_txn->getTransactionID();

    /// Write undo buffer first before dump to vfs
    UndoResources undo_resources;
    undo_resources.reserve(temp_parts.size() + temp_bitmaps.size() + temp_staged_parts.size());
    /// For local parts and stage parts, the remote parts can be at different disk,
    /// so we record the disk name of each part in the undo buffer.
    /// For the delete bitmap, it's always be dumped to the default disk
    std::vector<DiskPtr> part_disks;
    part_disks.reserve(temp_parts.size() + temp_staged_parts.size());
    for (const auto & part : temp_parts)
    {
        String part_name = part->info.getPartNameWithHintMutation();
        auto disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();

        // Assign part id here, since we need to record it into undo buffer before dump part to filesystem
        String relative_path = part_name;
        if (disk->getType() == DiskType::Type::ByteS3)
        {
            UUID part_id = newPartID(part->info, txn_id.toUInt64());
            part->uuid = part_id;
            relative_path = UUIDHelpers::UUIDToString(part_id);
        }

        undo_resources.emplace_back(txn_id, UndoResourceType::Part, part_name, relative_path + '/');
        undo_resources.back().setDiskName(disk->getName());
        part_disks.emplace_back(std::move(disk));
    }
    for (const auto & bitmap : temp_bitmaps)
        undo_resources.emplace_back(bitmap->getUndoResource(txn_id));
    for (const auto & staged_part : temp_staged_parts)
    {
        String part_name = staged_part->info.getPartNameWithHintMutation();
        auto disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();

        // Assign part id here, since we need to record it into undo buffer before dump part to filesystem
        String relative_path = part_name;
        if (disk->getType() == DiskType::Type::ByteS3)
        {
            UUID part_id = newPartID(staged_part->info, txn_id.toUInt64());
            staged_part->uuid = part_id;
            relative_path = UUIDHelpers::UUIDToString(part_id);
        }

        undo_resources.emplace_back(txn_id, UndoResourceType::StagedPart, part_name, relative_path + '/');
        undo_resources.back().setDiskName(disk->getName());
        part_disks.emplace_back(std::move(disk));
    }

    try
    {
        context->getCnchCatalog()->writeUndoBuffer(storage.getCnchStorageID(), txn_id, undo_resources, instance_id);
        LOG_DEBUG(storage.getLogger(), "Wrote undo buffer for {} resources in {} ms", undo_resources.size(), watch.elapsedMilliseconds());
    }
    catch (...)
    {
        LOG_ERROR(storage.getLogger(), "Fail to write undo buffer");
        throw;
    }

    /// Parallel dumping to shared storage
    DumpedData result{.dedup_mode = dedup_mode};
    S3ObjectMetadata::PartGeneratorID part_generator_id(S3ObjectMetadata::PartGeneratorID::TRANSACTION,
        curr_txn->getTransactionID().toString());
    MergeTreeCNCHDataDumper dumper(storage, part_generator_id);

    watch.restart();
    size_t pool_size = std::min(static_cast<size_t>(storage.getSettings()->cnch_parallel_dumping_threads), std::max(temp_staged_parts.size(), temp_parts.size()));
    /// make sure pool_size >= 1
    pool_size = pool_size >= 1 ? pool_size : 1;
    result.parts.resize(temp_parts.size());
    /// parallel dump delete bitmaps
    // TODO: dump all bitmaps to one file to avoid creating too many small files on vfs
    result.bitmaps = dumpDeleteBitmaps(storage, temp_bitmaps);
    result.staged_parts.resize(temp_staged_parts.size());

    auto dump_parts = [&, this](size_t i) -> void {
        for (; i < temp_parts.size(); i += pool_size)
        {
            const auto & temp_part = temp_parts[i];
            auto dumped_part = dumper.dumpTempPart(temp_part, part_disks[i]);
            LOG_TRACE(storage.getLogger(), "Dumped part {}", temp_part->name);
            result.parts[i] = std::move(dumped_part);
        }
    };

    auto dump_staged_parts = [&, this](size_t i) -> void {
        for (; i < temp_staged_parts.size(); i += pool_size)
        {
            const auto & temp_staged_part = temp_staged_parts[i];
            auto staged_part = dumper.dumpTempPart(temp_staged_part, part_disks[i + temp_parts.size()]);
            LOG_TRACE(storage.getLogger(), "Dumped staged part {}", temp_staged_part->name);
            result.staged_parts[i] = std::move(staged_part);
        }
    };

    if (pool_size > 1)
    {
        ThreadPool dump_pool(pool_size);
        for (size_t thread_id = 1; thread_id <= pool_size; thread_id++)
        {
            dump_pool.scheduleOrThrowOnError([&dump_parts, i = thread_id - 1, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE({
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                });
                if (thread_group)
                    CurrentThread::attachTo(thread_group);
                dump_parts(i);
            });
        }
        dump_pool.wait();

        for (size_t thread_id = 1; thread_id <= pool_size; thread_id++)
        {
            dump_pool.scheduleOrThrowOnError([&dump_staged_parts, i = thread_id - 1, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE({
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                });
                if (thread_group)
                    CurrentThread::attachTo(thread_group);
                dump_staged_parts(i);
            });
        }
        dump_pool.wait();
    }
    else
    {
        assert(pool_size == 1);
        dump_parts(0);
        dump_staged_parts(0);
    }

    LOG_DEBUG(
        storage.getLogger(),
        "Dumped {} parts, {} bitmaps, {} staged parts in {} ms",
        temp_parts.size(),
        temp_bitmaps.size(),
        temp_staged_parts.size(),
        watch.elapsedMilliseconds());
    return result;
}

void CnchDataWriter::commitDumpedParts(const DumpedData & dumped_data)
{
    Stopwatch watch;
    const auto & settings = context->getSettingsRef();
    const auto & dumped_parts = dumped_data.parts;
    const auto & delete_bitmaps = dumped_data.bitmaps;
    const auto & dumped_staged_parts = dumped_data.staged_parts;

    if (dumped_parts.empty() && delete_bitmaps.empty() && dumped_staged_parts.empty())
        // Nothing to commit, returns
        return;

    TxnTimestamp txn_id = context->getCurrentTransactionID();

    try
    {
        // Check if current transaction can directly be executed on current server
        if (dynamic_pointer_cast<CnchServerTransaction>(context->getCurrentTransaction()))
        {
            if (settings.debug_cnch_force_commit_parts_rpc)
            {
                auto server_client = context->getCnchServerClient("0.0.0.0", context->getRPCPort());
                server_client->commitParts(txn_id, type, storage, dumped_data, task_id, false, consumer_group, tpl, binlog, peak_memory_usage);
            }
            else
            {
                commitPreparedCnchParts(dumped_data);
            }
        }
        else
        {
            auto is_server = context->getServerType() == ServerType::cnch_server;
            CnchServerClientPtr server_client;
            if (auto worker_txn = dynamic_pointer_cast<CnchWorkerTransaction>(context->getCurrentTransaction()); worker_txn && worker_txn->tryGetServerClient())
            {
                /// case: client submits INSERTs directly to worker
                server_client = worker_txn->getServerClient();
            }
            else if (const auto & client_info = context->getClientInfo(); client_info.rpc_port)
            {
                /// case: "insert select/infile" forward to worker | manipulation task | cnch system log flush | ingestion from kafka | etc
                server_client = context->getCnchServerClient(client_info.current_address.host().toString(), client_info.rpc_port);
            }
            else
            {
                throw Exception("Server with transaction " + txn_id.toString() + " is unknown", ErrorCodes::LOGICAL_ERROR);
            }

            server_client->precommitParts(context, txn_id, type, storage, dumped_data, task_id, is_server, consumer_group, tpl, binlog, peak_memory_usage);
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(storage.getLogger(), __PRETTY_FUNCTION__);
        throw;
    }

    /// part log will be written in InsertAction::postCommit

    LOG_DEBUG(
        storage.getLogger(),
        "Committed {} parts, {} bitmaps, {} staged parts in transaction {}, elapsed {} ms, dedup mode is {}",
        dumped_parts.size(),
        delete_bitmaps.size(),
        dumped_staged_parts.size(),
        toString(UInt64(txn_id)),
        watch.elapsedMilliseconds(),
        typeToString(dumped_data.dedup_mode));
}

void CnchDataWriter::initialize(size_t max_threads)
{
    if (max_threads > 1)
    {
        thread_pool = std::make_unique<ThreadPool>(max_threads);
    }
}

void CnchDataWriter::schedule(
    const IMutableMergeTreeDataPartsVector & temp_parts,
    const LocalDeleteBitmaps & temp_bitmaps,
    const IMutableMergeTreeDataPartsVector & temp_staged_parts)
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchWriteDataElapsedMilliseconds, watch.elapsedMilliseconds(), Metrics::MetricType::Timer);});

    if (!thread_pool)
    {
        auto dumped = dumpAndCommitCnchParts(temp_parts, temp_bitmaps, temp_staged_parts);
        {
            /// just in case
            std::lock_guard lock(write_mutex);
            res.extend(std::move(dumped));
        }
        return;
    }

    /// check exception
    handler.throwIfException();

    auto cancel_pred = [this] { return handler.hasException() || cancelled.load(std::memory_order_seq_cst); };
    auto thread_group = CurrentThread::getGroup();

    /// dump temp parts
    for (const auto & temp_part : temp_parts)
    {
        LOG_TRACE(storage.getLogger(), "dump temp_part {}", temp_part->name);

        thread_pool->scheduleOrThrowOnError(createCnchDumpJob(
            [temp_part, this] {
                setThreadName("DumpPart");
                auto dumped = dumpAndCommitCnchParts({temp_part});
                {
                    std::lock_guard lock(write_mutex);
                    res.extend(std::move(dumped));
                }
            },
            handler,
            thread_group,
            cancel_pred));
    }

    /// dump temp staged parts
    for (const auto & temp_staged_part : temp_staged_parts)
    {
        thread_pool->scheduleOrThrowOnError(createCnchDumpJob(
            [temp_staged_part, this] {
                setThreadName("DumpStaged");
                auto dumped = dumpAndCommitCnchParts({}, {}, {temp_staged_part});
                {
                    std::lock_guard lock(write_mutex);
                    res.extend(std::move(dumped));
                }
            },
            handler,
            thread_group,
            cancel_pred));
    }

    /// batch dump delete bitmap
    thread_pool->scheduleOrThrowOnError(createCnchDumpJob(
        [temp_bitmaps, this] {
            setThreadName("DumpBitmap");
            auto dumped = dumpAndCommitCnchParts({}, temp_bitmaps, {});
            {
                std::lock_guard lock(write_mutex);
                res.extend(std::move(dumped));
            }
        },
        handler,
        thread_group,
        cancel_pred));

    /// check exception
    handler.throwIfException();
}

void CnchDataWriter::finalize()
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchWriteDataElapsedMilliseconds, watch.elapsedMilliseconds()); });

    if (thread_pool)
    {
        thread_pool->wait();
    }
    handler.throwIfException();
}

void CnchDataWriter::commitPreparedCnchParts(const DumpedData & dumped_data, const std::unique_ptr<S3AttachPartsInfo> & s3_parts_info)
{
    Stopwatch watch;
    namespace AutoStats = Statistics::AutoStats;
    AutoStats::ModifiedCounter modified_counter;

    if (context->getServerType() != ServerType::cnch_server)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Must be called in Server mode: {}", context->getServerType());

    auto * log = storage.getLogger();
    auto txn = context->getCurrentTransaction();
    auto txn_id = txn->getTransactionID();
    /// set main table uuid in server side
    txn->setMainTableUUID(storage.getCnchStorageUUID());
    txn->setCommitMode(storage.getSettings()->enable_publish_version_on_commit ? TransactionCommitMode::SEQUENTIAL : TransactionCommitMode::INDEPENDENT);

    auto storage_ptr = storage.shared_from_this();
    if (!storage_ptr)
        throw Exception("storage_ptr is nullptr and invalid for use", ErrorCodes::LOGICAL_ERROR);

    do
    {
        if (type == ManipulationType::Insert)
        {
            if (dumped_data.parts.empty() && dumped_data.bitmaps.empty() && dumped_data.staged_parts.empty() && consumer_group.empty())
            {
                LOG_DEBUG(log, "Nothing to commit, we skip this call.");
                break;
            }
            modified_counter.analyze(storage_ptr, dumped_data.parts);
            modified_counter.analyze(storage_ptr, dumped_data.staged_parts);

            if (!tpl.empty() && !consumer_group.empty())
                txn->setKafkaTpl(consumer_group, tpl);

            if (!binlog.binlog_file.empty())
            {
                auto database = DatabaseCatalog::instance().getDatabase(storage_ptr->getDatabaseName(), context);
                auto materialized_mysql = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database.get());
                if (!materialized_mysql)
                    throw Exception("Try to commit binlog but database '" + database->getDatabaseName() +  "' is not CnchMaterializedMySQL", ErrorCodes::LOGICAL_ERROR);

                txn->setBinlogInfo(binlog);
                txn->setBinlogName(getNameForMaterializedBinlog(materialized_mysql->getStorageID().uuid, storage_ptr->getTableName()));
            }

            // check the part is already correctly clustered for bucket table. All new inserted parts should be clustered.
            if (storage_ptr->isBucketTable())
            {
                auto table_definition_hash = storage_ptr->getTableHashForClusterBy();
                for (const auto & part : dumped_data.parts)
                {
                    // NOTE: set allow_attach_parts_with_different_table_definition_hash to false and
                    // skip_table_definition_hash_check to true if you want to force set part's TDH to table's TDH
                    if (context->getSettings().skip_table_definition_hash_check)
                        part->table_definition_hash = table_definition_hash.getDeterminHash();

                    if (context->getSettings().allow_attach_parts_with_different_table_definition_hash && !storage_ptr->getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey())
                        continue;

                    if (!part->deleted &&
                        (part->bucket_number < 0 || !table_definition_hash.match(part->table_definition_hash)))
                    {
                        throw Exception(
                            "Part " + part->name + " is not clustered or it has different table definition with storage. Part bucket number : "
                            + std::to_string(part->bucket_number) + ", part table_definition_hash : [" + std::to_string(part->table_definition_hash)
                            + "], table's table_definition_hash : [" + table_definition_hash.toString() + "]",
                            ErrorCodes::BUCKET_TABLE_ENGINE_MISMATCH);
                    }
                }
            }

            // Precommit stage. Write intermediate parts to KV
            auto action
                = txn->createAction<InsertAction>(storage_ptr, dumped_data.parts, dumped_data.bitmaps, dumped_data.staged_parts);
            action->as<InsertAction>()->checkAndSetDedupMode(dumped_data.dedup_mode);
            txn->appendAction(action);
            action->executeV2();
        }
        else if (type == ManipulationType::Drop)
        {
            if (dumped_data.parts.empty())
            {
                LOG_DEBUG(log, "No parts to commit, we skip this call.");
                break;
            }
            modified_counter.analyze(storage_ptr, dumped_data.parts);

            auto action = txn->createAction<DropRangeAction>(txn->getTransactionRecord(), storage_ptr);
            for (const auto & part : dumped_data.parts)
                action->as<DropRangeAction &>().appendPart(part);
            for (const auto & part : dumped_data.staged_parts)
                action->as<DropRangeAction &>().appendStagedPart(part);
            for (const auto & bitmap : dumped_data.bitmaps)
                action->as<DropRangeAction &>().appendDeleteBitmap(bitmap);

            txn->appendAction(action);
            action->executeV2();

            LOG_TRACE(
                log,
                "Committed {} parts in transaction {}, elapsed {} ms",
                dumped_data.parts.size(),
                txn_id.toUInt64(),
                watch.elapsedMilliseconds());
        }
        else if (type == ManipulationType::Merge || type == ManipulationType::Clustering || type == ManipulationType::Mutate)
        {
            auto bg_thread = context->getCnchBGThread(CnchBGThreadType::MergeMutate, storage.getStorageID());
            auto * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(bg_thread.get());
            if (dumped_data.parts.empty())
            {
                LOG_WARNING(log, "No parts to commit, worker may failed to merge parts, which task_id is {}", task_id);
                merge_mutate_thread->tryRemoveTask(task_id);
            }
            else
            {
                merge_mutate_thread->finishTask(task_id, [&](const Strings & source_part_names, UInt64 manipulation_submit_time_ns) {
                    auto action = txn->createAction<MergeMutateAction>(txn->getTransactionRecord(), type, storage_ptr, source_part_names,
                        manipulation_submit_time_ns, peak_memory_usage);

                    for (const auto & part : dumped_data.parts)
                        action->as<MergeMutateAction &>().appendPart(part);

                    action->as<MergeMutateAction &>().setDeleteBitmaps(dumped_data.bitmaps);
                    txn->appendAction(action);
                    action->executeV2();

                    LOG_TRACE(
                        log,
                        "Write {} parts in transaction {}, elapsed {} ms",
                        dumped_data.parts.size(),
                        txn_id.toUInt64(),
                        watch.elapsedMilliseconds());
                });
            }
        }
        else if (type == ManipulationType::Attach)
        {
            if (s3_parts_info == nullptr || s3_parts_info->former_parts.empty())
            {
                LOG_INFO(storage.getLogger(), "Nothing to commit, skip");
                return;
            }

            auto action = txn->createAction<S3AttachMetaAction>(storage_ptr, *s3_parts_info);
            txn->appendAction(action);
            action->executeV2();
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support commit type {}", typeToString(type));
        }
    } while (false);

    // only here we can make sure there is no exception
    // log modified count to AutoStatsMemoryRecord
    if (auto* server_txn = dynamic_cast<CnchServerTransaction*>(txn.get()))
    {
        server_txn->incrementModifiedCount(modified_counter);
    }
}

void CnchDataWriter::publishStagedParts(const MergeTreeDataPartsCNCHVector & staged_parts, const LocalDeleteBitmaps & bitmaps_to_dump)
{
    if (dedup_mode != CnchDedupHelper::DedupMode::APPEND)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Dedup mode is not append, but got {} when publish staged parts for table {}, it's a bug!",
            typeToString(dedup_mode),
            storage.getCnchStorageID().getNameForLogs());
    DumpedData items;
    items.dedup_mode = dedup_mode;

    TxnTimestamp txn_id = context->getCurrentTransactionID();

    for (const auto & staged_part : staged_parts)
    {
        // new part that shares the data file with the staged part
        Protos::DataModelPart new_part_model;
        fillPartModel(storage, *staged_part, new_part_model);
        new_part_model.mutable_part_info()->set_mutation(txn_id);
        new_part_model.set_txnid(txn_id);
        new_part_model.set_delete_flag(false);
        new_part_model.set_staging_txn_id(staged_part->info.mutation);
        // storage may not have part columns info (CloudMergeTree), so set columns/columns_commit_time manually
        auto new_part = createPartFromModelCommon(storage, new_part_model);
        /// Attention: commit time has been force set in createPartFromModelCommon method, we must clear commit time here. Otherwise, it will be visible event if the txn rollback.
        new_part->commit_time = IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME;
        new_part->setColumnsPtr(std::make_shared<NamesAndTypesList>(staged_part->getColumns()));
        new_part->columns_commit_time = staged_part->columns_commit_time;

        /// staged drop part
        MergeTreePartInfo drop_part_info = staged_part->info.newDropVersion(txn_id, StorageType::HDFS);
        auto drop_part = std::make_shared<MergeTreeDataPartCNCH>(
            storage, drop_part_info.getPartName(), drop_part_info, staged_part->volume, std::nullopt);
        drop_part->partition = staged_part->partition;
        drop_part->bucket_number = staged_part->bucket_number;
        drop_part->deleted = true;

        items.parts.emplace_back(std::move(new_part));
        items.staged_parts.emplace_back(std::move(drop_part));
    }

    /// prepare undo resources
    /// setMetadata() return reference, so need to cast move
    UndoResources undo_resources;
    for (auto & part : items.parts)
        undo_resources.emplace_back(
            std::move(UndoResource(txn_id, UndoResourceType::Part, part->info.getPartNameWithHintMutation()).setMetadataOnly(true)));
    for (auto & staged_part : items.staged_parts)
        undo_resources.emplace_back(std::move(
            UndoResource(txn_id, UndoResourceType::StagedPart, staged_part->info.getPartNameWithHintMutation()).setMetadataOnly(true)));
    for (const auto & bitmap : bitmaps_to_dump)
        undo_resources.emplace_back(bitmap->getUndoResource(txn_id));

    /// write undo buffer
    try
    {
        size_t size = undo_resources.size();
        Stopwatch watch;
        context->getCnchCatalog()->writeUndoBuffer(storage.getCnchStorageID(), txn_id, std::move(undo_resources));
        LOG_DEBUG(storage.getLogger(), "Wrote undo buffer for {} resources in {} ms", size, watch.elapsedMilliseconds());
    }
    catch (...)
    {
        LOG_ERROR(storage.getLogger(), "Fail to write undo buffer");
        throw;
    }

    /// dump delete bitmaps
    items.bitmaps = dumpDeleteBitmaps(storage, bitmaps_to_dump);

    commitDumpedParts(items);
}

void CnchDataWriter::preload(const MutableMergeTreeDataPartsCNCHVector & dumped_parts)
{
    const auto & settings = context->getSettingsRef();

    if (!settings.parts_preload_level || (!storage.getSettings()->parts_preload_level && !storage.getSettings()->enable_preload_parts)
        || !(storage.getSettings()->enable_local_disk_cache))
        return;

    try
    {
        Stopwatch timer;
        auto server_client = context->getCnchServerClientPool().get();
        MutableMergeTreeDataPartsCNCHVector preload_parts;
        std::copy_if(dumped_parts.begin(), dumped_parts.end(), std::back_inserter(preload_parts), [](const auto & part) {
            return !part->deleted;
        });

        if (!preload_parts.empty())
        {
            ProfileEvents::increment(ProfileEvents::PreloadSubmitTotalOps, 1, Metrics::MetricType::Rate);
            server_client->submitPreloadTask(storage, preload_parts, settings.preload_send_rpc_max_ms);
            LOG_DEBUG(
                storage.getLogger(),
                "Finish submit preload {} task for {} parts to server {}, elapsed {} ms",
                typeToString(type),
                preload_parts.size(),
                server_client->getRPCAddress(),
                timer.elapsedMilliseconds());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Fail to preload");
        if (storage.getSettings()->enable_parts_sync_preload)
            throw;
    }
}

UUID CnchDataWriter::newPartID(const MergeTreePartInfo& part_info, UInt64 txn_timestamp)
{
    UUID random_id = UUIDHelpers::generateV4();
    UInt64& random_id_low = UUIDHelpers::getHighBytes(random_id);
    UInt64& random_id_high = UUIDHelpers::getLowBytes(random_id);
    boost::hash_combine(random_id_low, part_info.min_block);
    boost::hash_combine(random_id_high, part_info.max_block);
    boost::hash_combine(random_id_low, part_info.mutation);
    boost::hash_combine(random_id_high, txn_timestamp);
    return random_id;
}

}
