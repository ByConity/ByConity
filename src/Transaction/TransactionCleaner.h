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

#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <limits>
#include <mutex>

namespace DB
{

/// Server transaction clean tasks should have higher execution priority
/// than DM triggered clean tasks.
enum class CleanTaskPriority : int
{
    LOW = 0,
    HIGH = 1,
};

struct TxnCleanTask
{
    TxnCleanTask() = default;

    TxnCleanTask(const TxnTimestamp & txn_id_, CleanTaskPriority priority_, CnchTransactionStatus status)
        : txn_id(txn_id_), priority(priority_), txn_status(status)
    {
    }

    UInt64 elapsed() const
    {
        return watch.elapsedMilliseconds();
    }

    TxnTimestamp txn_id;
    CleanTaskPriority priority;
    CnchTransactionStatus txn_status;
    std::atomic_uint32_t undo_size{0};
    Stopwatch watch;
};

using TxnCleanTasks = std::vector<TxnCleanTask>;

/// Run transaction clean task in the background
class TransactionCleaner : WithContext
{
public:
    TransactionCleaner(
        const ContextPtr & global_context, size_t server_max_threads, size_t server_queue_size, size_t dm_max_threads, size_t dm_queue_size)
        : WithContext(global_context)
        , high_priority_pool(std::make_unique<ThreadPool>(server_max_threads, server_max_threads, server_queue_size))
        , low_priority_pool(std::make_unique<ThreadPool>(dm_max_threads, dm_max_threads, dm_queue_size))
    {
    }

    ~TransactionCleaner();

    TransactionCleaner(const TransactionCleaner &) = delete;
    TransactionCleaner & operator=(const TransactionCleaner &) = delete;

    void cleanTransaction(const TransactionCnchPtr & txn);
    void cleanTransaction(const TransactionRecord & txn_record);
    /**
     * @brief Clean undo buffers on current server (will not dispatch another RPC).
     * This Method must be execute synchronously since we want to
     * guarantee undo buffers are cleaned before txn record is cleaned.
     */
    void cleanUndoBuffers(const TransactionRecord & txn_record);

    using TxnCleanTasksMap = std::unordered_map<UInt64, TxnCleanTask>;
    const TxnCleanTasksMap & getAllTasksUnLocked() const {return clean_tasks;}
    std::unique_lock<std::mutex> getLock() const { return std::unique_lock(mutex); }

    TxnCleanTask & getCleanTask(const TxnTimestamp & txn_id);

    void finalize();

private:
    /// brpc client default timeout 3 seconds.
    /// NOTE: don't wait a long time, it will block ServerService's rpc pool.
    static constexpr uint64_t DEFAULT_WAIT_US = 20000; // 20 ms

    template <typename... Args>
    bool tryRegisterTask(const TxnTimestamp & txn_id, Args &&... args)
    {
        std::lock_guard lock(mutex);
        return !shutdown && clean_tasks.try_emplace(txn_id.toUInt64(), txn_id, std::forward<Args>(args)...).second;
    }

    /// ┌────┐                      ┌───────────────────────────────────────┐
    /// │ DM ├─────────────────────►│ Server1(cleanUndoBuffersWithDispatch) │
    /// └─▲──┘ 1. CleanCommittedTxn └───┬───────────────────────────────────┘
    ///   │                             │ 2. CleanUndoBuffers (optional)
    /// ┌─┴──┐                      ┌───▼───────────────────────────────────┐
    /// │ KV │                      │ Server2(cleanUndoBuffersWithDispatch) │
    /// └────┘                      └───────────────────────────────────────┘
    ///
    /// 1. DM will scan KV to get txns that need to be cleaned.
    /// 2. In most cases, a single server can delete all undo buffer for the txn. (like server 1)
    /// 3. If a txn involves multiple tables, each table need to set commit time for parts (in cache).
    ///   Thus server will dispatch additional RPC call to target server. (like server 2)
    ///
    /// - Dispatches from Servers are async to avoid occuping the GRPC threads.

    /**
     * @brief Clean committed transaction.
     */
    void cleanCommittedTxn(const TransactionRecord & txn_record);
    /**
     * @brief Inner call to clean undo buffers.
     * This function serves as both in non-dispatch mode or dispatch mode, called by `cleanCommittedTxn`.
     *
     * @param callee If true, then it will ignore the non-host table, otherwise, it will send RPC calls to target servers.
     * @param clean_fs_lock_by_scan If fs lock needs to be cleaned.
     * @param deleted_keys If `dispatched` is `true`, then `deleted_keys` contains the undo buffer keys to be deleted.
     * @param dispatched If any dispatch happened in this call. `false` means all undo buffers belong to this server.
     * Then caller can safely remove them via prefix.
     * @return cleaned size.
     */
    UInt64 cleanUndoBuffersWithDispatch(
        const TransactionRecord & txn_record,
        bool callee,
        bool & clean_fs_lock_by_scan,
        std::vector<String> & deleted_keys,
        bool & dispatched);
    void cleanAbortedTxn(const TransactionRecord & txn_record);

    void removeTask(const TxnTimestamp & txn_id);

    /// Older txn gets higher priority.
    static inline int getPriority(TxnTimestamp txn_id)
    {
        return std::numeric_limits<int>::max() - static_cast<int>(txn_id.toUInt64() >> 32);
    }

    template <typename F, typename... Args>
    void scheduleTask(F && f, CleanTaskPriority priority, TxnTimestamp txn_id, Args &&... args)
    {
        LOG_DEBUG(log, "start schedule clean task for transaction {}\n", txn_id.toUInt64());
        if (!tryRegisterTask(txn_id, priority, std::forward<Args>(args)...))
        {
            LOG_DEBUG(log, "The clean task of txn " + txn_id.toString() + " is already running.");
            return;
        }

        auto & thread_pool = (priority == CleanTaskPriority::HIGH) ? *high_priority_pool : *low_priority_pool;

        bool res = thread_pool.trySchedule(
            [this, f = std::forward<F>(f), txn_id] {
                try
                {
                    f();
                }
                catch (...)
                {
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);
                }
                removeTask(txn_id);
            }, /*priority*/getPriority(txn_id), /*wait_microseconds*/DEFAULT_WAIT_US);

        if (!res)
        {
            removeTask(txn_id);
            LOG_WARNING(log, "TransactionCleaner queue is full. Clean task of transaction {} will be rescheduled by dm\n", txn_id.toUInt64());
        }
        else
            LOG_DEBUG(log, "Successfully schedule clean task in cleaner queue for transaction {}\n", txn_id.toUInt64());
    }

private:
    std::unique_ptr<ThreadPool> high_priority_pool;
    std::unique_ptr<ThreadPool> low_priority_pool;

    mutable std::mutex mutex;
    TxnCleanTasksMap clean_tasks;
    bool shutdown{false};
    LoggerPtr log = getLogger("TransactionCleaner");
};

using TransactionCleanerPtr = std::unique_ptr<TransactionCleaner>;
}
