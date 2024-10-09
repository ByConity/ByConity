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

#include <Transaction/CnchServerTransaction.h>
#include <Transaction/GlobalTxnCommitter.h>
#include <atomic>
#include <mutex>
#include <Catalog/Catalog.h>
#include <IO/WriteBuffer.h>
#include <Transaction/Actions/InsertAction.h>
#include <Transaction/TransactionCleaner.h>
#include <Transaction/TransactionCommon.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <common/scope_guard_safe.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <Storages/StorageCnchMergeTree.h>
#include <CloudServices/CnchDedupHelper.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Interpreters/StorageID.h>
#include <CloudServices/CnchPartsHelper.h>

namespace ProfileEvents
{
    extern const int CnchTxnCommitted;
    extern const int CnchTxnAborted;
    extern const int CnchTxnCommitV1Failed;
    extern const int CnchTxnCommitV2Failed;
    extern const int CnchTxnCommitV1ElapsedMilliseconds;
    extern const int CnchTxnCommitV2ElapsedMilliseconds;
    extern const int CnchTxnPrecommitElapsedMilliseconds;
    extern const int CnchTxnCommitKVElapsedMilliseconds;
    extern const int CnchTxnCleanFailed;
    extern const int CnchTxnCleanElapsedMilliseconds;
    extern const int CnchTxnFinishedTransactionRecord;
}

namespace CurrentMetrics
{
    extern const Metric CnchTxnActiveTransactions;
    extern const Metric CnchTxnTransactionRecords;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CNCH_TRANSACTION_COMMIT_TIMEOUT;
    extern const int CNCH_TRANSACTION_COMMIT_ERROR;
    extern const int CNCH_TRANSACTION_ABORT_ERROR;
    extern const int INSERTION_LABEL_ALREADY_EXISTS;
    extern const int FAILED_TO_PUT_INSERTION_LABEL;
    extern const int BRPC_TIMEOUT;
    extern const int ABORTED;
}

CnchServerTransaction::CnchServerTransaction(const ContextPtr & context_, TransactionRecord txn_record_)
    : ICnchTransaction(context_, std::move(txn_record_))
    , active_txn_increment{CurrentMetrics::CnchTxnActiveTransactions}
{
    if (!isReadOnly())
    {
        global_context->getCnchCatalog()->createTransactionRecord(getTransactionRecord());
        CurrentMetrics::add(CurrentMetrics::CnchTxnTransactionRecords);
    }
}

void CnchServerTransaction::appendAction(ActionPtr act)
{
    auto lock = getLock();
    actions.push_back(std::move(act));
}

std::vector<ActionPtr> & CnchServerTransaction::getPendingActions()
{
    auto lock = getLock();
    return actions;
}

static void commitModifiedCount(Statistics::AutoStats::ModifiedCounter& counter)
{
    if (counter.empty()) return;

    auto& instance = Statistics::AutoStats::AutoStatisticsMemoryRecord::instance();
    instance.append(counter);
}


TxnTimestamp CnchServerTransaction::commitV1()
{
    LOG_DEBUG(log, "Transaction {} starts commit (v1 api)\n", txn_record.txnID().toUInt64());

    if (isReadOnly())
        throw Exception("Invalid commit operation for read only transaction", ErrorCodes::LOGICAL_ERROR);

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({
        ProfileEvents::increment((getStatus() == CnchTransactionStatus::Finished ? ProfileEvents::CnchTxnCommitted : ProfileEvents::CnchTxnCommitV1Failed));
        ProfileEvents::increment(ProfileEvents::CnchTxnCommitV1ElapsedMilliseconds, watch.elapsedMilliseconds());
    });

    auto commit_ts = global_context->getTimestamp();

    try
    {
        auto lock = getLock();
        for (const auto & action : actions)
        {
            action->executeV1(commit_ts);
        }

        setStatus(CnchTransactionStatus::Finished);
        setCommitTime(commit_ts);
        LOG_DEBUG(log, "Successfully committed transaction (v1 api): {}\n", txn_record.txnID().toUInt64());
        commitModifiedCount(this->modified_counter);
        return commit_ts;
    }
    catch (...)
    {
        rollbackV1(commit_ts);
        throw;
    }
}

void CnchServerTransaction::rollbackV1(const TxnTimestamp & ts)
{
    LOG_DEBUG(log, "Transaction {} failed, start rollback (v1 api).\n", txn_record.txnID().toUInt64());
    auto lock = getLock();
    setStatus(CnchTransactionStatus::Aborted);
    setCommitTime(ts);
}

TxnTimestamp CnchServerTransaction::commitV2()
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchTxnCommitV2ElapsedMilliseconds, watch.elapsedMilliseconds()); });

    try
    {
        precommit();
        /// XXX: If a topo switch occurs during the commit phase, it may lead to parallel lock holding.
        /// While this problem is difficult to solve because committed transactions are not supported to be rolled back. Temporarily use the time window of topo switching to avoid this problem
        return commit();
    }
    catch (const Exception & e)
    {
        if (!(getContext()->getSettings().ignore_duplicate_insertion_label && e.code() == ErrorCodes::INSERTION_LABEL_ALREADY_EXISTS))
            tryLogCurrentException(log, __PRETTY_FUNCTION__);

        try
        {
            auto commit_ts = abort();
            /// the txn has been sucessfully committed for the timeout case
            if (txn_record.status() == CnchTransactionStatus::Finished)
                return commit_ts;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }

        throw;
    }
    catch (...)
    {
        LOG_DEBUG(log, "CommitV2 failed for transaction {}\n", txn_record.txnID());
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        rollback();
        throw;
    }
}

void CnchServerTransaction::precommit()
{
    LOG_DEBUG(log, "Transaction {} starts pre commit\n", txn_record.txnID().toUInt64());
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchTxnPrecommitElapsedMilliseconds, watch.elapsedMilliseconds()); });

    {
        auto lock = getLock();
        if (auto status = getStatus(); status != CnchTransactionStatus::Running)
            throw Exception("Transaction is not in running status, but in " + String(txnStatusToString(status)), ErrorCodes::LOGICAL_ERROR);

        for (auto & action : actions)
            action->executeV2();

        txn_record.prepared = true;
        action_size_before_dedup = actions.size();
    }

    auto retry_time = getContext()->getSettingsRef().max_dedup_retry_time.value;
    do
    {
        try
        {
            executeDedupStage();
            assertLockAcquired();
        }
        catch  (...)
        {
            if (retry_time == 0)
                throw;
            else if (action_size_before_dedup < actions.size())
            {
                /// TODO: Impl retry in this case, especially handle undo buffer
                LOG_WARNING(
                    log,
                    "Dedup stage failed, but result is not empty({}/{}), unable to retry, retry time: {}",
                    actions.size(),
                    action_size_before_dedup,
                    retry_time);
                throw;
            }
            else
            {
                LOG_WARNING(log, "Dedup stage failed, retry time: {}, reason: {}", retry_time, getCurrentExceptionMessage(false));
                retry_time--;
                dedup_stage_flag = false;
                continue;
            }
        }
        break;
    } while (true);
}

void CnchServerTransaction::executeDedupStage()
{
    auto local_dedup_impl_version = getDedupImplVersion(getContext());
    LOG_TRACE(log, "Dedup impl version: {}, txn id: {}", local_dedup_impl_version, getTransactionID().toUInt64());
    if (static_cast<DedupImplVersion>(local_dedup_impl_version) != DedupImplVersion::DEDUP_IN_TXN_COMMIT)
        return;

    auto expected_value = false;
    if (!dedup_stage_flag.compare_exchange_strong(expected_value, true))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Execute dedup stage concurrently which may lead to dirty dedup result, it's bug for current impl.");

    /// Currently, lock holder only serve for dedup stage for unique table, we can just clear it in case of retry.
    lock_holders.clear();
    std::unordered_map<StorageID, CnchDedupHelper::DedupTaskPtr> dedup_task_map;
    for (size_t i = 0 ; i < action_size_before_dedup; ++i)
    {
        auto & action = actions[i];
        if (auto * insert_action = dynamic_cast<InsertAction *>(action.get()))
        {
            auto dedup_task = insert_action->getDedupTask();
            if (dedup_task)
            {
                if (dedup_task_map.count(dedup_task->storage_id))
                {
                    auto & final_dedup_info = dedup_task_map[dedup_task->storage_id];
                    final_dedup_info->new_parts.insert(
                        final_dedup_info->new_parts.end(), dedup_task->new_parts.begin(), dedup_task->new_parts.end());
                    final_dedup_info->delete_bitmaps_for_new_parts.insert(
                        final_dedup_info->delete_bitmaps_for_new_parts.end(),
                        dedup_task->delete_bitmaps_for_new_parts.begin(),
                        dedup_task->delete_bitmaps_for_new_parts.end());
                }
                else
                    dedup_task_map[dedup_task->storage_id] = std::move(dedup_task);
            }
        }
    }

    if (dedup_task_map.empty())
        return;

    Stopwatch watch;
    auto txn_id = getTransactionID();
    if (dedup_task_map.size() > 1)
        LOG_TRACE(log, "Start handle dedup stage for {} tables, txn id: {}", dedup_task_map.size(), txn_id.toUInt64());

    auto handler = std::make_shared<ExceptionHandler>();
    std::vector<brpc::CallId> call_ids;
    for (auto & it : dedup_task_map)
    {
        Stopwatch task_watch;
        const auto & storage_id = it.first;
        auto & dedup_task = it.second;
        if (!dedup_task)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dedup task for table {} is nullptr, this is a bug!", storage_id.getNameForLogs());

        if (dedup_task->new_parts.empty())
            continue;

        LOG_TRACE(
            log,
            "Start handle dedup stage for table {}, part size: {}, delete bitmap size: {}, txn: {}",
            storage_id.getNameForLogs(),
            dedup_task->new_parts.size(),
            dedup_task->delete_bitmaps_for_new_parts.size(),
            txn_id.toUInt64());

        auto catalog = getContext()->getCnchCatalog();
        TxnTimestamp ts = getContext()->getTimestamp();
        auto table = catalog->tryGetTableByUUID(*getContext(), UUIDHelpers::UUIDToString(dedup_task->storage_id.uuid), ts);
        if (!table)
            throw Exception(ErrorCodes::ABORTED, "Table {} has been dropped", dedup_task->storage_id.getNameForLogs());
        auto cnch_table = dynamic_pointer_cast<StorageCnchMergeTree>(table);
        if (!cnch_table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not cnch merge tree", dedup_task->storage_id.getNameForLogs());

        /// 1. Acquire lock and fill task
        CnchDedupHelper::acquireLockAndFillDedupTask(*cnch_table, *dedup_task, *this, getContext());
        /// 2. Pick worker to execute dedup task
        auto vw_handle = getContext()->getVirtualWarehousePool().get(cnch_table->getSettings()->cnch_vw_write);
        auto sub_dedup_tasks = CnchDedupHelper::pickWorkerForDedup(*cnch_table, dedup_task, vw_handle);

        /// 3. Execute dedup task and wait result
        dedup_task->statistics.other_cost = task_watch.elapsedMilliseconds() - dedup_task->statistics.acquire_lock_cost - dedup_task->statistics.get_metadata_cost;
        for (const auto & task_pair : sub_dedup_tasks)
        {
            const auto & worker_client = task_pair.first;
            const auto & sub_dedup_task = task_pair.second;
            LOG_DEBUG(
                log,
                "Choose worker: {} to execute sub dedup task for txn {}, scope: {}",
                worker_client->getHostWithPorts().toDebugString(),
                txn_id.toUInt64(),
                sub_dedup_task->dedup_scope.toString());

            Stopwatch inner_watch;
            auto funcOnCallback
                = [&, worker_client, dedup_task, sub_dedup_task, inner_watch, pre_cost = task_watch.elapsedMilliseconds()](bool success) {
                      sub_dedup_task->statistics.execute_task_cost = inner_watch.elapsedMilliseconds();
                      LOG_DEBUG(
                          log,
                          "Worker {} {} handle sub dedup task for table {}, txn id: {}, {}",
                          worker_client->getHostWithPorts().toDebugString(),
                          success ? "success" : "failed",
                          dedup_task->storage_id.getNameForLogs(),
                          txn_id.toUInt64(),
                          sub_dedup_task->toString());

                      if (!success)
                          dedup_task->failed_task_num++;
                      if (++dedup_task->finished_task_num == sub_dedup_tasks.size())
                      {
                          dedup_task->statistics.execute_task_cost = inner_watch.elapsedMilliseconds();
                          dedup_task->statistics.total_cost = dedup_task->statistics.execute_task_cost + pre_cost;
                          LOG_DEBUG(
                              log,
                              "All sub dedup tasks finish for table {}, txn_id: {}, {}",
                              dedup_task->storage_id.getNameForLogs(),
                              txn_id.toUInt64(),
                              dedup_task->toString());
                      }
                  };
            auto call_id = worker_client->executeDedupTask(
                getContext(), txn_id, getContext()->getRPCPort(), *cnch_table, *sub_dedup_task, handler, funcOnCallback);
            call_ids.emplace_back(call_id);
        }
    }

    /// 4. Wait result
    for (auto & call_id : call_ids)
        brpc::Join(call_id);

    handler->throwIfException();

    if (dedup_task_map.size() > 1)
        LOG_TRACE(log, "Finish handle dedup stage for {} tables, total cost {} ms, txn id: {}", dedup_task_map.size(), watch.elapsedMilliseconds(), txn_id.toUInt64());
}

TxnTimestamp CnchServerTransaction::commit()
{
    LOG_DEBUG(log, "Transaction {} starts commit", txn_record.txnID().toUInt64());
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    auto lock = getLock();
    if (isReadOnly() || !txn_record.isPrepared())
        throw Exception("Invalid commit operation", ErrorCodes::LOGICAL_ERROR);

    TxnTimestamp commit_ts = global_context->getTimestamp();
    int retry = MAX_RETRY;
    do
    {
        try
        {
            if (TransactionCommitMode::SEQUENTIAL == getCommitMode())
            {
                commit_time = commit_ts;
                bool success = getContext()->getGlobalTxnCommitter()->commit(shared_from_this());
                if (success)
                {
                    TransactionRecord updated_record = getTransactionRecord();
                    updated_record.setStatus(CnchTransactionStatus::Finished)
                                .setCommitTs(commit_ts)
                                .setMainTableUUID(getMainTableUUID());
                    txn_record = std::move(updated_record);
                    ProfileEvents::increment(ProfileEvents::CnchTxnCommitted);
                    ProfileEvents::increment(ProfileEvents::CnchTxnFinishedTransactionRecord);

                    return commit_ts;
                }
                else
                {
                    setStatus(CnchTransactionStatus::Aborted);
                    throw Exception("Fail to commit txn " + txn_record.txnID().toString() + " by using GlobalTxnCommitter.", ErrorCodes::CNCH_TRANSACTION_COMMIT_ERROR);
                }
            }

            if (isPrimary() && !consumer_group.empty()) /// Kafka transaction is always primary
            {
                if (tpl.empty())
                    throw Exception("No tpl found for committing Kafka transaction", ErrorCodes::LOGICAL_ERROR);

                // CAS operation
                TransactionRecord target_record = getTransactionRecord();
                target_record.setStatus(CnchTransactionStatus::Finished)
                             .setCommitTs(commit_ts)
                             .setMainTableUUID(getMainTableUUID());
                Stopwatch stop_watch;
                auto success = global_context->getCnchCatalog()->setTransactionRecordStatusWithOffsets(txn_record, target_record, consumer_group, tpl);

                txn_record = std::move(target_record);
                if (success)
                {
                    ProfileEvents::increment(ProfileEvents::CnchTxnCommitted);
                    ProfileEvents::increment(ProfileEvents::CnchTxnFinishedTransactionRecord);
                    LOG_DEBUG(log, "Successfully committed Kafka transaction {} at {} with {} offsets number, elapsed {} ms",
                                     txn_record.txnID().toUInt64(), commit_ts, tpl.size(), stop_watch.elapsedMilliseconds());
                    commitModifiedCount(this->modified_counter);
                    return commit_ts;
                }
                else
                {
                    LOG_DEBUG(log, "Failed to commit Kafka transaction: {}, abort it directly", txn_record.txnID().toUInt64());
                    setStatus(CnchTransactionStatus::Aborted);
                    retry = 0;
                    throw Exception(
                        "Kafka transaction " + txn_record.txnID().toString()
                            + " commit failed because txn record has been changed by other transactions",
                        ErrorCodes::CNCH_TRANSACTION_COMMIT_ERROR);
                }
            }
            else if (isPrimary() && !binlog.binlog_file.empty())
            {
                if (binlog_name.empty())
                    throw Exception("Binlog name should not be empty while try to commit binlog", ErrorCodes::LOGICAL_ERROR);

                auto binlog_metadata = global_context->getCnchCatalog()->getMaterializedMySQLBinlogMetadata(binlog_name);
                if (!binlog_metadata)
                    throw Exception("Cannot get metadata for binlog #" + binlog_name, ErrorCodes::LOGICAL_ERROR);

                binlog_metadata->set_binlog_file(binlog.binlog_file);
                binlog_metadata->set_binlog_position(binlog.binlog_position);
                binlog_metadata->set_executed_gtid_set(binlog.executed_gtid_set);
                binlog_metadata->set_meta_version(binlog.meta_version);

                // CAS operation
                TransactionRecord target_record = getTransactionRecord();
                target_record.setStatus(CnchTransactionStatus::Finished)
                    .setCommitTs(commit_ts)
                    .setMainTableUUID(getMainTableUUID());
                Stopwatch stop_watch;
                auto success = global_context->getCnchCatalog()->setTransactionRecordStatusWithBinlog(txn_record, target_record, binlog_name, binlog_metadata);

                txn_record = std::move(target_record);
                if (success)
                {
                    ProfileEvents::increment(ProfileEvents::CnchTxnCommitted);
                    ProfileEvents::increment(ProfileEvents::CnchTxnFinishedTransactionRecord);
                    LOG_DEBUG(log, "Successfully committed MaterializedMySQL transaction {} at {} with binlog: {}, elapsed {} ms.", txn_record.txnID(), commit_ts, binlog_name, stop_watch.elapsedMilliseconds());
                    return commit_ts;
                }
                else
                {
                    LOG_DEBUG(log, "Failed to commit MaterializedMySQL transaction: {}, abort it directly", txn_record.txnID());
                    setStatus(CnchTransactionStatus::Aborted);
                    retry = 0;
                    throw Exception(
                        "MaterializedMySQL transaction " + txn_record.txnID().toString()
                            + " commit failed because txn record has been changed by other transactions",
                        ErrorCodes::CNCH_TRANSACTION_COMMIT_ERROR);
                }
            }
            else
            {
                Catalog::BatchCommitRequest requests(true, true);
                Catalog::BatchCommitResponse response;

                String label_key;
                String label_value;
                // if (insertion_label)
                // {
                //     /// Pack the operation creating a new label into CAS operations
                //     insertion_label->commit();
                //     label_key = global_context->getCnchCatalog()->getInsertionLabelKey(insertion_label);
                //     label_value = insertion_label->serializeValue();
                //     Catalog::SinglePutRequest put_req(label_key, label_value, true);
                //     put_req.callback = [label = insertion_label](int code, const std::string & msg) {
                //         if (code == Catalog::CAS_FAILED)
                //             throw Exception(
                //                 "Insertion label " + label->name + " already exists: " + msg, ErrorCodes::INSERTION_LABEL_ALREADY_EXISTS);
                //         else if (code != Catalog::OK)
                //             throw Exception(
                //                 "Failed to put insertion label " + label->name + ": " + msg, ErrorCodes::FAILED_TO_PUT_INSERTION_LABEL);
                //     };
                //     requests.AddPut(put_req);
                // }

                // CAS operation
                TransactionRecord target_record = getTransactionRecord();
                target_record.setStatus(CnchTransactionStatus::Finished)
                             .setCommitTs(commit_ts)
                             .setMainTableUUID(getMainTableUUID());

                bool success = false;
                if (!extern_commit_functions.empty())
                {
                    for (auto & txn_f : extern_commit_functions)
                    {
                        auto extern_requests = txn_f.commit_func(getContext());
                        for (auto & req : extern_requests.puts)
                            requests.AddPut(req);
                        for (auto & req : extern_requests.deletes)
                            requests.AddDelete(req.key);
                    }
                    success = getContext()->getCnchCatalog()->setTransactionRecordWithRequests(txn_record, target_record, requests, response);
                }
                else
                    success = global_context->getCnchCatalog()->setTransactionRecordWithRequests(txn_record, target_record, requests, response);

                txn_record = std::move(target_record);

                if (success)
                {
                    ProfileEvents::increment(ProfileEvents::CnchTxnCommitted);
                    ProfileEvents::increment(ProfileEvents::CnchTxnFinishedTransactionRecord);
                    LOG_DEBUG(log, "Successfully committed transaction {} at {}\n", txn_record.txnID().toUInt64(), commit_ts);
                    commitModifiedCount(this->modified_counter);

                    /// Clean merging_mutating_parts after txn succeed.
                    tryCleanMergeTagger();
                    return commit_ts;
                }
                else // CAS failed
                {
                    // Because of retry logic, txn may has been committed, we treat success for this case.
                    if (txn_record.status() == CnchTransactionStatus::Finished)
                    {
                        ProfileEvents::increment(ProfileEvents::CnchTxnCommitted);
                        ProfileEvents::increment(ProfileEvents::CnchTxnFinishedTransactionRecord);
                        LOG_DEBUG(log, "Transaction {} has been successfully committed in previous trials.\n", txn_record.txnID().toUInt64());
                        commitModifiedCount(this->modified_counter);
                        return txn_record.commitTs();
                    }
                    else
                    {
                        // Except for above case, treat all other cas failed cases failed.
                        throw Exception("Transaction " + txn_record.txnID().toString() + " commit failed because txn record has been changed by other transactions", ErrorCodes::CNCH_TRANSACTION_COMMIT_ERROR);
                    }
                }
            }
        }
        catch (const Exception &)
        {
            ProfileEvents::increment(ProfileEvents::CnchTxnCommitV2Failed);
            throw;
        }

        LOG_WARNING(log, "Catch bytekv request timeout exception. Will try to update transaction record again, number of retries remains: {}\n", retry);
        // slightly increase waiting time
        std::this_thread::sleep_for(std::chrono::milliseconds(200 * (MAX_RETRY - retry)));
    } while (retry-- > 0);

    throw Exception("Transaction" + txn_record.txnID().toString() + " commit timeout", ErrorCodes::CNCH_TRANSACTION_COMMIT_TIMEOUT);
}

TxnTimestamp CnchServerTransaction::rollback()
{
    LOG_DEBUG(log, "Transaction {} failed, start rollback\n", txn_record.txnID().toUInt64());
    auto lock = getLock();
    if (isReadOnly())
        throw Exception("Invalid commit operation", ErrorCodes::LOGICAL_ERROR);

    removeIntermediateData();

    // Set in memory transaction status first, if rollback kv failed, the in-memory status will be used to reset the conflict.
    setStatus(CnchTransactionStatus::Aborted);
    ProfileEvents::increment(ProfileEvents::CnchTxnAborted);
    TxnTimestamp ts;
    try
    {
        ts = global_context->getTimestamp();
        setCommitTime(ts);
        global_context->getCnchCatalog()->rollbackTransaction(txn_record);
        LOG_DEBUG(log, "Successfully rollback transaction: {}\n", txn_record.txnID().toUInt64());
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to rollback transaction: {}", txn_record.txnID().toUInt64());
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    tryCleanMergeTagger();
    return ts;
}

TxnTimestamp CnchServerTransaction::abort()
{
    LOG_DEBUG(log, "Start abort transaction {}\n", txn_record.txnID().toUInt64());

    SCOPE_EXIT_SAFE({ tryCleanMergeTagger(); });

    auto lock = getLock();
    // Abort successful primary txn shouldn't be allowed unless we mean to do so with rollback.
    if (isReadOnly() || (txn_record.isPrimary() && txn_record.status() == CnchTransactionStatus::Finished))
        throw Exception("Invalid commit operation", ErrorCodes::LOGICAL_ERROR);

    TransactionRecord target_record = getTransactionRecord();
    target_record.setStatus(CnchTransactionStatus::Aborted)
                 .setCommitTs(global_context->getTimestamp())
                 .setMainTableUUID(getMainTableUUID());
    bool success = false;
    Catalog::BatchCommitRequest requests(true, true);
    Catalog::BatchCommitResponse response;
    if (!extern_commit_functions.empty())
    {
        for (auto & txn_f : extern_commit_functions)
        {
            auto extern_requests = txn_f.abort_func(getContext());
            for (auto & req : extern_requests.puts)
                requests.AddDelete(req.key);
            for (auto & req : extern_requests.deletes)
                requests.AddPut(req);
        }
        success = getContext()->getCnchCatalog()->setTransactionRecordWithRequests(txn_record, target_record, requests, response);
    }
    else
        success = global_context->getCnchCatalog()->setTransactionRecord(txn_record, target_record);
    txn_record = std::move(target_record);

    if (success)
    {
        ProfileEvents::increment(ProfileEvents::CnchTxnAborted);
        LOG_DEBUG(log, "Successfully abort transaction: {}\n", txn_record.txnID().toUInt64());
    }
    else // CAS failed
    {
        // Don't abort committed txn, treat committed
        if (txn_record.status() == CnchTransactionStatus::Finished)
        {
            LOG_WARNING(log, "Transaction {} has been committed\n", txn_record.toString());
        }
        else if (txn_record.status() == CnchTransactionStatus::Aborted)
        {
            LOG_WARNING(log, "Transaction {} has been aborted\n", txn_record.toString());
        }
        else
        {
            // Abort failed, throw exception
            throw Exception("Abort transaction " + txn_record.txnID().toString() + " failed (CAS failure).", ErrorCodes::CNCH_TRANSACTION_ABORT_ERROR);
        }
    }

    return txn_record.commitTs();
}

void CnchServerTransaction::clean(TxnCleanTask & task)
{
    if (force_clean_by_dm)
    {
        LOG_DEBUG(log, "Force clean transaction {} from DM", task.txn_id.toUInt64());
        return;
    }
    LOG_DEBUG(log, "Start clean transaction: {}\n", task.txn_id.toUInt64());
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::CnchTxnCleanElapsedMilliseconds, watch.elapsedMilliseconds()); });

    bool clean_fs_lock_by_scan = global_context->getConfigRef().getBool("cnch_txn_clean_fs_lock_by_scan", true);

    try
    {
        // operations are done in sequence, if any operation failed, the remaining will not continue. The background scan thread will finish the remaining job.
        auto lock = getLock();
        auto catalog = global_context->getCnchCatalog();
        auto txn_id = getTransactionID();
        // releaseIntentLocks();

        if (getStatus() == CnchTransactionStatus::Finished)
        {
            /// Transaction is succeed, nothing to undo, and transaction status is still in memory,
            /// user should be responsible to release acquired resource(i.e. attach should release kvfs lock it self)
            UInt32 undo_size = 0;
            for (auto & action : actions)
            {
                undo_size += action->getSize();
            }

            task.undo_size.store(undo_size, std::memory_order_relaxed);

            for (auto & action : actions)
                action->postCommit(getCommitTime());

            catalog->clearUndoBuffer(txn_id);
            /// set clean time in kv, txn_record will be remove by daemon manager
            UInt64 ttl = global_context->getConfigRef().getUInt64("cnch_txn_safe_remove_seconds", 5 * 60);
            catalog->setTransactionRecordCleanTime(txn_record, global_context->getTimestamp(), ttl);
            // TODO: move to dm when metrics ready
            CurrentMetrics::sub(CurrentMetrics::CnchTxnTransactionRecords);
            LOG_DEBUG(log, "Successfully clean a finished transaction: {}\n", txn_record.txnID().toUInt64());
        }
        else
        {
            // clear intermediate parts first since metadata is the source
            for (auto & action : actions)
                action->abort();

            auto undo_buffer = catalog->getUndoBuffer(txn_id);
            UInt32 undo_size = 0;
            for (const auto & buffer : undo_buffer)
                undo_size += buffer.second.size();

            task.undo_size.store(undo_size, std::memory_order_relaxed);

            std::set<String> kvfs_lock_keys;
            for (const auto & [uuid, resources] : undo_buffer)
            {
                StoragePtr table = catalog->getTableByUUID(*global_context, uuid, txn_id, true);
                auto * storage = dynamic_cast<MergeTreeMetaBase *>(table.get());
                if (!storage)
                    throw Exception("Table is not of MergeTree class", ErrorCodes::LOGICAL_ERROR);

                for (const auto & resource : resources)
                {
                    if (resource.type() == UndoResourceType::KVFSLockKey)
                        kvfs_lock_keys.insert(resource.placeholders(0));
                    resource.clean(*catalog, storage);
                }
            }

            if (!kvfs_lock_keys.empty())
            {
                catalog->clearFilesysLocks({kvfs_lock_keys.begin(), kvfs_lock_keys.end()}, txn_id);
            }

            catalog->clearUndoBuffer(txn_id);

            if (kvfs_lock_keys.empty() && clean_fs_lock_by_scan)
            {
                /// to this point, can safely clear any lock if hold
                catalog->clearFilesysLock(txn_id);
            }

            // remove transaction record, if remove transaction record is not done, will still be handled by background scan thread.
            catalog->removeTransactionRecord(txn_record);
            CurrentMetrics::sub(CurrentMetrics::CnchTxnTransactionRecords);
            LOG_DEBUG(log, "Successfully clean a failed transaction: {}\n", txn_record.txnID().toUInt64());
        }
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::CnchTxnCleanFailed);
        LOG_WARNING(log, "Clean txn {" + txn_record.toString() + "} failed.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void CnchServerTransaction::removeIntermediateData()
{
    /// for seconday transaction, if commit fails, must clear all written data in kv imediately.
    /// otherwise, next dml within the transaction can see the trash data, or worse, if user try
    /// to commit the transaction again, the junk data will become visible.
    if (isPrimary()) return;
    LOG_DEBUG(log, "Secondary transaction failed, will remove all intermediate data during rollback");
    std::for_each(actions.begin(), actions.end(), [](auto & action) { action->abort(); });
    actions.clear();
}

void CnchServerTransaction::incrementModifiedCount(const Statistics::AutoStats::ModifiedCounter& new_counts)
{
    auto lock = getLock();
    modified_counter.merge(new_counts);
}

UInt32 CnchServerTransaction::getDedupImplVersion(ContextPtr local_context)
{
    auto lock = getLock();
    if (dedup_impl_version != 0)
        return dedup_impl_version;

    if (main_table_uuid != UUIDHelpers::Nil)
    {
        auto catalog = getContext()->getCnchCatalog();
        TxnTimestamp ts = getContext()->getTimestamp();
        auto table = catalog->tryGetTableByUUID(*local_context, UUIDHelpers::UUIDToString(main_table_uuid), ts);
        if (!table)
            throw Exception(ErrorCodes::ABORTED, "Table {} has been dropped", UUIDHelpers::UUIDToString(main_table_uuid));
        auto cnch_table = dynamic_pointer_cast<StorageCnchMergeTree>(table);
        if (!cnch_table)
            dedup_impl_version = 1; /// DEDUP_IN_WRITE_SUFFIX
        else
            dedup_impl_version = static_cast<UInt32>(cnch_table->getSettings()->dedup_impl_version.value);
    }
    else
        dedup_impl_version = 1;
    return dedup_impl_version;
}

}
