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
#include <Core/Types.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/IntentLock.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/CurrentMetrics.h>
#include <atomic>
#include <memory>

#include <Statistics/AutoStatisticsMemoryRecord.h>

namespace DB
{
class Context;
class TransactionCoordinatorRcCnch;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// A transaction running on server; can either be implicit or secondary transaction
class CnchServerTransaction : public ICnchTransaction
{
    friend class TransactionCoordinatorRcCnch;
public:
    // ctor for server transaction
    // Use TransactionCnchRcCnch::createTransaction() to create server transaction
    // read_only transaction will not write transaction record to kv
    CnchServerTransaction(const ContextPtr & context_, TransactionRecord txn_record_);

    ~CnchServerTransaction() override = default;
    String getTxnType() const override { return "CnchServerTransaction"; }

    void appendAction(ActionPtr act) override;

    std::vector<ActionPtr> & getPendingActions() override;

    TxnTimestamp commitV1() override;
    void rollbackV1(const TxnTimestamp & ts) override;

    TxnTimestamp commitV2() override;
    void precommit() override;
    TxnTimestamp commit() override;
    TxnTimestamp rollback() override;
    TxnTimestamp abort() override;

    void clean(TxnCleanTask & task) override;

    void removeIntermediateData() override;

    void incrementModifiedCount(const Statistics::AutoStats::ModifiedCounter& new_counts);

    LoggerPtr getLogger() { return log; }

    UInt32 getDedupImplVersion(ContextPtr local_context) override;

protected:
    static constexpr size_t MAX_RETRY = 3;
    std::vector<ActionPtr> actions;
    CurrentMetrics::Increment active_txn_increment;
    Statistics::AutoStats::ModifiedCounter modified_counter;

private:

    LoggerPtr log {::getLogger("CnchServerTransaction")};

    /// Unique table related
    std::atomic_bool dedup_stage_flag{false};
    size_t action_size_before_dedup = 0;

    void executeDedupStage();

};

using CnchServerTransactionPtr = std::shared_ptr<CnchServerTransaction>;
}
