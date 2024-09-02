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

#include <Transaction/TransactionCommon.h>
#include <Catalog/Catalog.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Transaction/TransactionRecordCache.h>


#include <vector>
#include <unordered_map>

namespace DB::detail
{

template <typename T, typename Operation>
bool isCommitted(const T & element, std::unordered_map<UInt64, TransactionRecordLite> & transactions)
{
    UInt64 commit_time = Operation::getCommitTime(element);
    UInt64 txn_id = Operation::getTxnID(element);

    if (commit_time != IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
    {
        return true;
    }
    else
    {
        const auto & txn_record = transactions[txn_id];
        // for non finished txn, the commit time is still NOT_INITIALIZED_COMMIT_TIME (0) after setCommitTime is called.
        Operation::setCommitTime(element, txn_record.commit_ts);
        return txn_record.status == CnchTransactionStatus::Finished;
    }
}


/// Give a vector whose elements might be in the intermediate state (commit time == 0).
/// Convert elements in the intermediate state into the final state (set commit time for them) if the transaction has been committed
/// and remove intermediate state elements if its corresponding transaction is running.
/// The elements with commit time larger than ts will be ignored.
template <typename T, typename Operation, bool test_end_ts>
TransactionRecords getImpl(std::vector<T> & elements, const TxnTimestamp & ts, Catalog::Catalog * catalog, TransactionRecords * input_records, TransactionRecordCache * finished_or_failed_txn_cache)
{
    std::unordered_map<UInt64, TransactionRecordLite> transactions; // cache for fetched transactions
    std::unordered_set<UInt64> unfinished_transactions; // record for unfinished transactions
    std::set<TxnTimestamp> txn_ids;
    for (const auto & element : elements)
    {
        UInt64 commit_time = Operation::getCommitTime(element);
        UInt64 txn_id = Operation::getTxnID(element);

        if (commit_time != IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
        {
            transactions.try_emplace(txn_id, commit_time, CnchTransactionStatus::Finished);
        }
        else
        {
            auto cached = finished_or_failed_txn_cache ? finished_or_failed_txn_cache->get(txn_id) : nullptr;
            if (cached)
            {
                transactions.try_emplace(txn_id, cached->commit_ts, cached->status);
            }
            else 
            {
                transactions.try_emplace(txn_id, 0, CnchTransactionStatus::Inactive);
                txn_ids.insert(txn_id); 
            }
        }
    }

    TransactionRecords records;
    if (input_records)
        records = *input_records;
    else
    {
        // get txn records status in batch
        records = catalog->getTransactionRecords(std::vector<TxnTimestamp>(txn_ids.begin(), txn_ids.end()), 100000);
    }

    for (const auto & record : records)
    {
        if (record.status() == CnchTransactionStatus::Finished)
        {
            transactions[record.txnID()] = {record.commitTs(), record.status()};
        }
        else if (input_records)
            unfinished_transactions.emplace(record.txnID());

        // only cached txn record with finished or aborted status since these 2 status are final status
        if (finished_or_failed_txn_cache && (record.status() == CnchTransactionStatus::Finished || record.status() == CnchTransactionStatus::Aborted))
        {
            finished_or_failed_txn_cache->insert(record.txnID(), std::make_shared<TransactionRecordLite>(record.commitTs(), record.status()));
        }
    }

    /// We need to ignore those elements whose have been set commit time already but status of the txn is unfinished in order to make sure the data consistency.
    std::erase_if(elements, [&](const T & element) {
        bool is_uncommitted = unfinished_transactions.count(Operation::getTxnID(element)) || !isCommitted<T, Operation>(element, transactions)
            || transactions[Operation::getTxnID(element)].commit_ts > ts;
        if constexpr (test_end_ts)
            return is_uncommitted || (Operation::getEndTime(element) && Operation::getEndTime(element) <= ts);
        else
            return is_uncommitted;

    });

    return records;
}


template <typename T, typename Operation>
bool isAborted(const T & element, Catalog::Catalog * catalog, std::unordered_map<UInt64, TransactionRecordLite> & transactions)
{
    UInt64 commit_time = Operation::getCommitTime(element);
    UInt64 txn_id = Operation::getTxnID(element);

    if (commit_time != IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
    {
        transactions.try_emplace(txn_id, commit_time, CnchTransactionStatus::Finished);
        return false;
    }

    // If the txn not exists, need to check from kv to know its status.
    // If the txn exists in this map but with CnchTransactionStatus::Inactive status, it means the txn not exist.
    if (transactions.find(txn_id) == transactions.end())
    {
        auto record = catalog->tryGetTransactionRecord(TxnTimestamp(txn_id));
        if (!record)
            transactions.try_emplace(txn_id, 0, CnchTransactionStatus::Inactive);
        else
            transactions.try_emplace(txn_id, record->commitTs(), record->status());
    }

    auto & txn_record = transactions[txn_id];
    return txn_record.status == CnchTransactionStatus::Aborted || txn_record.status == CnchTransactionStatus::Inactive;
}

struct ServerDataPartOperation
{
    static UInt64 getCommitTime(const ServerDataPartPtr & server_part_ptr)
    {
        return server_part_ptr->getCommitTime();
    }

    static UInt64 getEndTime(const ServerDataPartPtr & server_part_ptr)
    {
        return server_part_ptr->getEndTime();
    }

    static UInt64 getTxnID(const ServerDataPartPtr & server_part_ptr)
    {
        return server_part_ptr->txnID();
    }

    static void setCommitTime(const ServerDataPartPtr & server_part_ptr, const TxnTimestamp & ts)
    {
        server_part_ptr->setCommitTime(ts);
    }
};

struct BitmapOperation
{
    static UInt64 getCommitTime(const DeleteBitmapMetaPtr & bitmap)
    {
        return bitmap->getCommitTime();
    }

    static UInt64 getEndTime(const DeleteBitmapMetaPtr & bitmap)
    {
        return bitmap->getEndTime();
    }

    static UInt64 getTxnID(const DeleteBitmapMetaPtr & bitmap)
    {
        return bitmap->getTxnId();
    }

    static void setCommitTime(const DeleteBitmapMetaPtr & bitmap, const TxnTimestamp & ts)
    {
        bitmap->updateCommitTime(ts);
    }
};

} // namespace DB::detail

namespace DB
{
constexpr auto isAbortedServerDataPart = detail::isAborted<ServerDataPartPtr, detail::ServerDataPartOperation>;
constexpr auto getCommittedServerDataParts = detail::getImpl<ServerDataPartPtr, detail::ServerDataPartOperation, false>;
constexpr auto getVisibleServerDataParts = detail::getImpl<ServerDataPartPtr, detail::ServerDataPartOperation, true>;
constexpr auto getCommittedBitmaps = detail::getImpl<DeleteBitmapMetaPtr, detail::BitmapOperation, false>;
constexpr auto getVisibleBitmaps = detail::getImpl<DeleteBitmapMetaPtr, detail::BitmapOperation, true>;

}
