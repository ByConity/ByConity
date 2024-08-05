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

#include <DaemonManager/DaemonJobTxnGC.h>
#include <Catalog/Catalog.h>
#include <DaemonManager/DaemonFactory.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchServerClientPool.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Transaction/TransactionCommon.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNFINISHED;
    extern const int BAD_ARGUMENTS;
}
namespace DB::DaemonManager
{

#define DEFAULT_CLEAN_UNDOBUFFER_INTERVAL_MINUTES 360

bool DaemonJobTxnGC::executeImpl()
{

    const Context & context = *getContext();
    String last_start_key = start_key;
    auto txn_records = context.getCnchCatalog()->getTransactionRecordsForGC(
        start_key, context.getConfigRef().getInt("cnch_txn_clean_batch_size", 200000));
    LOG_DEBUG(log, "start_key changed from: {} to {}", last_start_key, start_key);
    if (!txn_records.empty())
    {
        cleanTxnRecords(txn_records);
    }

    if (triggerCleanUndoBuffers())
    {
        cleanUndoBuffers(txn_records);
        lastCleanUBtime = std::chrono::system_clock::now();
    }

    return true;
}

void DaemonJobTxnGC::cleanTxnRecords(const TransactionRecords & txn_records)
{
    const Context & context = *getContext();
    LOG_DEBUG(log, "txn_id from {} to {}",
        txn_records.front().txnID().toUInt64(),
        txn_records.back().txnID().toUInt64());

    TxnGCLog summary(log);
    summary.total = txn_records.size();
    TxnTimestamp current_time = context.getTimestamp();

    size_t num_threads = std::min(txn_records.size(), size_t(context.getConfigRef().getUInt("cnch_txn_gc_parallel", 16)));
    if (num_threads == 0)
        throw Exception("the number of thread config for cnch_txn_gc_parallel is 0", ErrorCodes::LOGICAL_ERROR);

    ThreadPool thread_pool(num_threads);
    ExceptionHandler exception_handler;

    const size_t chunk_size = txn_records.size() / num_threads;
    size_t bonus = txn_records.size() - chunk_size * num_threads;
    size_t chunk_begin = 0;
    size_t chunk_end = 0;
    for (chunk_begin = 0, chunk_end = chunk_size;
        chunk_begin < txn_records.size(); chunk_begin = chunk_end, chunk_end += chunk_size)
    {
        if (bonus) {
            ++chunk_end;
            --bonus;
        }

        thread_pool.scheduleOrThrow(
            createExceptionHandledJob(
            [this, &txn_records, current_time, &summary, chunk_begin, chunk_end, & context] {
                std::vector<TxnTimestamp> cleanTxnIds;
                cleanTxnIds.reserve(chunk_end - chunk_begin);

                for (size_t j = chunk_begin; j < chunk_end; j++)
                {
                    cleanTxnRecord(txn_records[j], current_time, cleanTxnIds, summary);
                }

                context.getCnchCatalog()->removeTransactionRecords(cleanTxnIds);
                summary.cleaned += cleanTxnIds.size();
            }, exception_handler));
    }

    thread_pool.wait();
    exception_handler.throwIfException();
}

void DaemonJobTxnGC::cleanTxnRecord(
    const TransactionRecord & txn_record, TxnTimestamp current_time, std::vector<TxnTimestamp> & cleanTxnIds, TxnGCLog & summary)
{
    const Context & context = *getContext();

    try
    {
        auto & server_pool = context.getCnchServerClientPool();
        auto client = server_pool.tryGetByRPCAddress(txn_record.location());
        const UInt64 safe_remove_interval = context.getConfigRef().getInt("cnch_txn_safe_remove_seconds", 5 * 60); // default 5 min

        bool server_exists = static_cast<bool>(client);
        if (!client)
            client = server_pool.get();

        switch (txn_record.status())
        {
            case CnchTransactionStatus::Aborted: {
                summary.aborted++;
                client->cleanTransaction(txn_record);
                break;
            }
            case CnchTransactionStatus::Finished: {
                summary.committed++;
                if (!txn_record.cleanTs())
                {
                    if (txn_record.hasMainTableUUID())
                    {
                        auto host_port = context.getCnchTopologyMaster()->getTargetServer(
                            UUIDHelpers::UUIDToString(txn_record.mainTableUUID()), DEFAULT_SERVER_VW_NAME, false);
                        client = server_pool.get(host_port);
                    }
                    client->cleanTransaction(txn_record);
                }
                else if (current_time.toSecond() - txn_record.cleanTs().toSecond() > safe_remove_interval)
                {
                    cleanTxnIds.push_back(txn_record.txnID());
                }
                break;
            }
            case CnchTransactionStatus::Running: {
                summary.running++;
                if (!server_exists)
                {
                    summary.reschedule++;
                    client->cleanTransaction(txn_record);
                }
                else if (client->getTransactionStatus(txn_record.txnID()) == CnchTransactionStatus::Inactive)
                {
                    summary.inactive++;
                    client->cleanTransaction(txn_record);
                }
                break;
            }
            default:
                throw Exception("Invalid status received", ErrorCodes::LOGICAL_ERROR);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

bool DaemonJobTxnGC::triggerCleanUndoBuffers()
{
    const int clean_undobuffer_interval_minutes = getContext()->getConfigRef().getInt("clean_undobuffer_interval_minutes", DEFAULT_CLEAN_UNDOBUFFER_INTERVAL_MINUTES); // default 6 hour
    if (clean_undobuffer_interval_minutes == 0)
        return false;

    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::minutes>(now - lastCleanUBtime).count();
    return duration >= clean_undobuffer_interval_minutes;
}

void DaemonJobTxnGC::cleanUndoBuffers(const TransactionRecords & txn_records)
{
    const Context & context = *getContext();
    auto current_ts = context.tryGetTimestamp();
    const int clean_undobuffer_interval_minutes = getContext()->getConfigRef().getInt("clean_undobuffer_interval_minutes", DEFAULT_CLEAN_UNDOBUFFER_INTERVAL_MINUTES); // default 6 hour
    if (clean_undobuffer_interval_minutes == 0 || current_ts == 0)
        return;

    LOG_DEBUG(log, "cleanUndoBuffer starts");

    /// No need to check existing transaction ids
    std::unordered_set<UInt64> existing_txn_id_set;
    std::for_each(txn_records.begin(), txn_records.end(), [&existing_txn_id_set](const auto & txn_record) {
        existing_txn_id_set.insert(txn_record.txnID());
    });

    /// Only handle transactions old than now() - clean_undobuffer_interval_minutes
    auto start_clean_ts = ((current_ts >> 18) - clean_undobuffer_interval_minutes * 60 * 1000) << 18;

    auto & server_pool = context.getCnchServerClientPool();
    auto catalog = context.getCnchCatalog();

    auto txn_undobuffers_iter = catalog->getUndoBufferIterator();
    const size_t max_missing_ids_set_size = getContext()->getConfigRef().getUInt("clean_undobuffer_ids_set_size", 100000);
    std::vector<TxnTimestamp> missing_ids_set;
    try
    {
        while(txn_undobuffers_iter.next())
        {
            const UndoResource & undo_resource = txn_undobuffers_iter.getUndoResource();
            const auto & txn_id = undo_resource.txn_id;
            if (!existing_txn_id_set.contains(txn_id) && txn_id < start_clean_ts)
                missing_ids_set.push_back(txn_id);

            if (missing_ids_set.size() > max_missing_ids_set_size)
                break;
        }
    }
    catch (...)
    {
        LOG_INFO(log, "Got exception while iterating undo buffer iterator: " + getCurrentExceptionMessage(false));
    }

    std::vector<TxnTimestamp> missing_ids(missing_ids_set.begin(), missing_ids_set.end());
    size_t count = 0;
    const size_t batch_size = getContext()->getConfigRef().getUInt("clean_undobuffer_batch_size", 1000);
    while(!missing_ids.empty())
    {
        std::vector<TxnTimestamp> missing_id_small_batch = extractLastElements(missing_ids, batch_size);
        auto missing_records = catalog->getTransactionRecords(missing_id_small_batch);
        for (auto & record : missing_records)
        {
            if (record.status() == CnchTransactionStatus::Inactive || record.status() == CnchTransactionStatus::Unknown)
            {
                // clean process for Inactive and UNKNOWN is the same as ABORTED
                count++;
                record.setStatus(CnchTransactionStatus::Aborted);

                try
                {
                    auto client = server_pool.get();
                    client->cleanTransaction(record);
                }
                catch (...)
                {
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);
                }
            }
        }
    }

    LOG_DEBUG(log, "Clean undo buffer for {} deleted txn_records", count);
}

void registerTxnGCDaemon(DaemonFactory & factory)
{
    factory.registerLocalDaemonJob<DaemonJobTxnGC>("TXN_GC");
}

std::vector<TxnTimestamp> extractLastElements(std::vector<TxnTimestamp> & from, size_t n)
{
    std::vector<TxnTimestamp> res;
    if (from.size() <= n)
    {
        res.swap(from);
        return res;
    }

    std::copy(from.end() - n, from.end(), std::back_inserter(res));
    from.resize(from.size() - n);
    return res;
}

}
