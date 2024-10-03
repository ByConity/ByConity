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
#include <Transaction/TransactionCleaner.h>

#include <Disks/DiskByteS3.h>
#include <Catalog/Catalog.h>
#include <Common/serverLocality.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Interpreters/Context.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/Actions/S3AttachMetaFileAction.h>
#include <Transaction/Actions/S3AttachMetaAction.h>
#include <Transaction/Actions/S3DetachMetaAction.h>

namespace DB
{
namespace ErrorCodes
{
    // extern const int BAD_CAST;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int CNCH_TOPOLOGY_NOT_MATCH_ERROR;
}

TransactionCleaner::~TransactionCleaner()
{
    try
    {
        finalize();
    }
    catch(...)
    {
        tryLogCurrentException(log);
    }
}

void TransactionCleaner::cleanTransaction(const TransactionCnchPtr & txn)
{
    auto txn_record = txn->getTransactionRecord();
    if (txn_record.read_only)
        return;

    if (!txn_record.ended())
    {
        try
        {
            txn->abort();
        }
        catch (...)
        {
            /// if abort transaction failed (e.g., bytekv is not stable at this moment),
            /// we are not 100% sure how is the status going, we let dm to make the correct decision.
            txn->force_clean_by_dm = true;
            throw;
        }
    }
    if (!txn->async_post_commit)
    {
        TxnCleanTask task(txn->getTransactionID(), CleanTaskPriority::HIGH, txn_record.status());
        txn->clean(task);
        return;
    }

    scheduleTask(
        [this, txn] {
            TxnCleanTask & task = getCleanTask(txn->getTransactionID());
            txn->clean(task);
        },
        CleanTaskPriority::HIGH,
        txn_record.txnID(),
        txn_record.status());
}

void TransactionCleaner::cleanTransaction(const TransactionRecord & txn_record)
{
    if (txn_record.status() == CnchTransactionStatus::Finished)
    {
        cleanCommittedTxn(txn_record);
    }
    else
    {
        cleanAbortedTxn(txn_record);
    }
}

void TransactionCleaner::cleanCommittedTxn(const TransactionRecord & txn_record)
{
    scheduleTask([this, txn_record, &global_context = *getContext()] {
        bool clean_fs_lock_by_scan = global_context.getConfigRef().getBool("cnch_txn_clean_fs_lock_by_scan", true);

        LOG_DEBUG(log, "Start to clean the committed transaction {}\n", txn_record.txnID().toUInt64());
        TxnCleanTask & task = getCleanTask(txn_record.txnID());
        auto catalog = global_context.getCnchCatalog();
        catalog->clearZombieIntent(txn_record.txnID());

        std::vector<String> deleted_keys;
        bool dispatched = false;
        task.undo_size.fetch_add(cleanUndoBuffersWithDispatch(txn_record, false, clean_fs_lock_by_scan, deleted_keys, dispatched));

        /// Clean fs lock by txn id must be after undo resource clean, otherwise undo resource
        /// clean may clean other user's fs lock
        if (clean_fs_lock_by_scan)
        {
            // first clear any filesys lock if hold
            catalog->clearFilesysLock(txn_record.txnID());
        }

        if (dispatched) {
            catalog->clearUndoBuffersByKeys(txn_record.txnID(), deleted_keys);

            /// Normally, all UB will be deleted before this line.
            auto undo_buffer = catalog->getUndoBuffer(txn_record.txnID());
            if (!undo_buffer.empty()) {
                LOG_WARNING(log, "Undo buffer not cleaned {}", undo_buffer.begin()->first);
                return;
            }
        } else {
            /// In undispatched mode (same as old logic), there is no distribute execution on other server.
            /// Thus it's unargubaly that we could remove all undo buffer as well as the transaction.
            catalog->clearUndoBuffer(txn_record.txnID());
        }

        UInt64 ttl = global_context.getConfigRef().getUInt64("cnch_txn_safe_remove_seconds", 5 * 60);
        catalog->setTransactionRecordCleanTime(txn_record, global_context.getTimestamp(), ttl);
        LOG_DEBUG(log, "Finish cleaning the committed transaction {}\n", txn_record.txnID().toUInt64());
    }, CleanTaskPriority::LOW, txn_record.txnID(), CnchTransactionStatus::Finished);
}

UInt64 TransactionCleaner::cleanUndoBuffersWithDispatch(
    const TransactionRecord & txn_record,
    const bool callee,
    bool & clean_fs_lock_by_scan,
    std::vector<String> & deleted_keys,
    bool & dispatched)
{
    const auto & global_context = *getContext();
    auto catalog = global_context.getCnchCatalog();
    auto undo_buffer = catalog->getUndoBuffersWithKeys(txn_record.txnID());
    size_t task_cleaned_size = 0;

    /// Only need one RPC call for each servers.
    std::set<String> visit;

    cppkafka::TopicPartitionList undo_tpl;
    String consumer_group;

    for (const auto & [uuid, resource_kv] : undo_buffer)
    {
        auto [keys, resources] = resource_kv;
        LOG_DEBUG(log, "Get undo buffer of the table {}\n", uuid);

        StoragePtr table = catalog->tryGetTableByUUID(global_context, uuid, TxnTimestamp::maxTS(), true);
        if (!table)
            continue;

        auto host_port = global_context.getCnchTopologyMaster()->getTargetServer(uuid, table->getServerVwName(), false);
        auto rpc_address = host_port.getRPCAddress();


        if (!isLocalServer(rpc_address, std::to_string(global_context.getRPCPort())))
        {
            /// Skip dispatch a rpc request if we are callee or called this server before.
            if (callee || visit.contains(rpc_address))
                continue;
            LOG_DEBUG(log, "Forward clean task for txn {} to server {} table_uuid: {}", txn_record.txnID().toUInt64(), rpc_address, uuid);
            global_context.getCnchServerClientPool().get(rpc_address)->cleanUndoBuffers(txn_record);
            /// We have dispatched a rpc call.
            dispatched = true;
            /// Mark this server visited.
            visit.insert(rpc_address);
            /// A caller, after dispatched a call, can skip the rest of the procedure.
            continue;
        }

        UndoResourceNames names = integrateResources(resources);

        /// Collect extra parts to update commit time
        /// We don't want to add it into integrateResources since when clean aborted
        /// transaction, we need some extra logic rather than just delete it from catalog
        S3AttachMetaAction::collectUndoResourcesForCommit(resources, names);
        /// Clean detach parts for s3 committed
        S3DetachMetaAction::commitByUndoBuffer(global_context, table, resources);
        /// Clean s3 meta file
        S3AttachMetaFileAction::commitByUndoBuffer(global_context, resources);

        /// Release directory lock if any
        if (!names.kvfs_lock_keys.empty())
        {
            catalog->clearFilesysLocks({names.kvfs_lock_keys.begin(), names.kvfs_lock_keys.end()}, std::nullopt);
            clean_fs_lock_by_scan = false;
        }

        auto intermediate_parts = catalog->getDataPartsByNames(names.parts, table, 0);
        auto undo_bitmaps = catalog->getDeleteBitmapByKeys(table, names.bitmaps);
        auto staged_parts = catalog->getStagedDataPartsByNames(names.staged_parts, table, 0);

        catalog->setCommitTime(
            table, Catalog::CommitItems{intermediate_parts, undo_bitmaps, staged_parts}, txn_record.commitTs(), txn_record.txnID());

        // clean vfs if necessary
        for (const auto & resource : resources)
        {
            resource.commit(global_context);
        }

        // These keys need to be deleted on current host.
        deleted_keys.insert(deleted_keys.end(), keys.begin(), keys.end());

    }

    return task_cleaned_size;
}

void TransactionCleaner::cleanUndoBuffers(const TransactionRecord & txn_record)
{
    scheduleTask(
        [this, txn_record, &global_context = *getContext()] {
            LOG_DEBUG(log, "Start to clean the committed transaction (as callee) {}\n", txn_record.txnID().toUInt64());
            auto catalog = global_context.getCnchCatalog();
            bool clean_fs_lock_by_scan = global_context.getConfigRef().getBool("cnch_txn_clean_fs_lock_by_scan", true);

            std::vector<String> deleted_keys;
            bool dispatched = false;
            TxnCleanTask & task = getCleanTask(txn_record.txnID());
            task.undo_size.fetch_add(cleanUndoBuffersWithDispatch(txn_record, true, clean_fs_lock_by_scan, deleted_keys, dispatched));

            /// A callee will never dispatch calls.
            assert(dispatched == false);

            catalog->clearUndoBuffersByKeys(txn_record.txnID(), deleted_keys);

            LOG_DEBUG(log, "Finish cleaning the committed transaction (as callee) {}\n", txn_record.txnID().toUInt64());
        },
        CleanTaskPriority::HIGH,
        txn_record.txnID(),
        CnchTransactionStatus::Finished);
}

void TransactionCleaner::cleanAbortedTxn(const TransactionRecord & txn_record)
{
    scheduleTask([this, txn_record, &global_context = *getContext()]() {
        bool clean_fs_lock_by_scan = global_context.getConfigRef().getBool("cnch_txn_clean_fs_lock_by_scan", true);

        LOG_DEBUG(log, "Start to clean the aborted transaction {}\n", txn_record.txnID().toUInt64());

        // abort transaction if it is running
        if (!txn_record.ended())
        {
            LOG_DEBUG(log, "Abort the running transaction {}\n", txn_record.txnID().toUInt64());
            if (global_context.getCnchTransactionCoordinator().isActiveTransaction(txn_record.txnID()))
            {
                LOG_WARNING(log, "Transaction {} is still running.\n", txn_record.txnID().toUInt64());
                return;
            }

            auto commit_ts = global_context.getTimestamp();
            TransactionRecord target_record = txn_record;
            target_record.setStatus(CnchTransactionStatus::Aborted);
            target_record.commitTs() = commit_ts;
            bool success = global_context.getCnchCatalog()->setTransactionRecord(txn_record, target_record);
            if (!success)
            {
                LOG_WARNING(log, "Transaction has been committed or aborted, current status " + String(txnStatusToString(target_record.status())));
                return;
            }
        }

        TxnCleanTask & task = getCleanTask(txn_record.txnID());
        auto catalog = global_context.getCnchCatalog();
        catalog->clearZombieIntent(txn_record.txnID());
        auto undo_buffer = catalog->getUndoBuffer(txn_record.txnID());

        cppkafka::TopicPartitionList undo_tpl;
        String consumer_group;
        for (const auto & [uuid, resources] : undo_buffer)
        {
            LOG_DEBUG(log, "Get undo buffer of the table ", uuid);
            StoragePtr table = catalog->tryGetTableByUUID(global_context, uuid, TxnTimestamp::maxTS(), true);
            if (!table)
                continue;
            auto undo_size = catalog->applyUndos(txn_record, table, resources, clean_fs_lock_by_scan);
            task.undo_size.store(undo_size, std::memory_order_relaxed);
        }
        if (clean_fs_lock_by_scan)
        {
            // remove directory lock if there's one, this is tricky, because we don't know the directory name
            // current solution: scan all filesys lock and match the transaction id
            catalog->clearFilesysLock(txn_record.txnID());
        }

        catalog->clearUndoBuffer(txn_record.txnID());
        catalog->removeTransactionRecord(txn_record);
        LOG_DEBUG(log, "Finish cleaning aborted transaction {}\n", txn_record.txnID().toUInt64());
    }, CleanTaskPriority::LOW, txn_record.txnID(), CnchTransactionStatus::Aborted);
}

TxnCleanTask & TransactionCleaner::getCleanTask(const TxnTimestamp & txn_id)
{
    std::lock_guard lock(mutex);
    if (auto it = clean_tasks.find(txn_id.toUInt64()); it != clean_tasks.end())
    {
        return it->second;
    }
    else {
        throw Exception("Clean task was not registered, txn_id: " + txn_id.toString(), ErrorCodes::LOGICAL_ERROR);
    }
}

void TransactionCleaner::finalize()
{
    {
        std::lock_guard lock(mutex);
        shutdown = true;
    }

    high_priority_pool->wait();
    low_priority_pool->wait();
}

void TransactionCleaner::removeTask(const TxnTimestamp & txn_id)
{
    std::lock_guard lock(mutex);
    clean_tasks.erase(txn_id.toUInt64());
}

}
