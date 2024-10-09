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
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Core/Types.h>
#include <MergeTreeCommon/InsertionLabel.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Transaction/Actions/IAction.h>
#include <bthread/mutex.h>
#include <cppkafka/cppkafka.h>
#include <Transaction/IntentLock.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/InsertionLabel.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>
#include <Databases/IDatabase.h>
#include <cppkafka/topic_partition_list.h>
#include <Common/TypePromotion.h>
#include <Common/serverLocality.h>
#include <common/logger_useful.h>
#include <Transaction/LockRequest.h>
#include <bthread/recursive_mutex.h>
#include <Catalog/MetastoreCommon.h>
#include <Protos/data_models.pb.h>

#if USE_MYSQL
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#endif

#include <memory>
#include <string>

namespace DB
{
struct TxnCleanTask;
class CnchLockHolder;
using CnchLockHolderPtr = std::shared_ptr<CnchLockHolder>;
using CnchLockHolderPtrs = std::vector<CnchLockHolderPtr>;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool isReadOnlyTransaction(const DB::IAST * ast);

class ICnchTransaction : public std::enable_shared_from_this<ICnchTransaction>, public TypePromotion<ICnchTransaction>, public WithContext
{
public:
    friend class Catalog::Catalog;
    friend class CnchLockHolder;
    // insert action.
    struct TransFunction
    {
        std::function<Catalog::BatchCommitRequest(ContextPtr context)> commit_func;
        std::function<Catalog::BatchCommitRequest(ContextPtr context)> abort_func;
    };

    explicit ICnchTransaction(const ContextPtr & context_) : WithContext(context_), global_context(context_->getGlobalContext()) { }
    explicit ICnchTransaction(const ContextPtr & context_, TransactionRecord record)
        : WithContext(context_), global_context(context_->getGlobalContext()), txn_record(std::move(record))
    {
    }

    virtual ~ICnchTransaction() = default;

    ICnchTransaction(const ICnchTransaction &) = delete;
    ICnchTransaction & operator=(const ICnchTransaction &) = delete;

    TxnTimestamp getTransactionID() const { return txn_record.txnID(); }
    TxnTimestamp getPrimaryTransactionID() const { return txn_record.primaryTxnID(); }
    TxnTimestamp getStartTime() const { return txn_record.txnID(); }
    TxnTimestamp getCommitTime() const { return txn_record.commitTs(); }
    void setCommitTime(const TxnTimestamp & commit_ts) { txn_record.setCommitTs(commit_ts); }
    TransactionRecord getTransactionRecord() const { return txn_record; }

    std::unique_lock<bthread::RecursiveMutex> getLock() const { return std::unique_lock(mutex); }

    String getInitiator() const { return txn_record.initiator(); }

    CnchTransactionStatus getStatus() const;

    void setCommitMode(const TransactionCommitMode & mode) { commit_mode = mode; }
    TransactionCommitMode getCommitMode() const { return commit_mode;}

    void setCommitTs(const TxnTimestamp & commit_time_) { commit_time = commit_time_; }
    TxnTimestamp getCommitTs() const { return commit_time; }

    bool isReadOnly() const { return txn_record.isReadOnly(); }
    void setReadOnly(bool read_only) { txn_record.read_only = read_only; }

    bool isPrepared() { return txn_record.isPrepared(); }

    bool isPrimary() { return txn_record.isPrimary(); }

    bool isSecondary() { return txn_record.isSecondary(); }

    void setMainTableUUID(const UUID & uuid);

    UUID getMainTableUUID() const;

    void setKafkaTpl(const String & consumer_group, const cppkafka::TopicPartitionList & tpl);
    void getKafkaTpl(String & consumer_group, cppkafka::TopicPartitionList & tpl) const;

    template <typename TAction, typename... Args>
    ActionPtr createAction(Args &&... args) const
    {
        return std::make_shared<TAction>(global_context, txn_record.txnID(), std::forward<Args>(args)...);
    }

    template <typename TAction, typename... Args>
    ActionPtr createActionWithLocalContext(const ContextPtr & local_context, Args &&... args) const
    {
        return std::make_shared<TAction>(local_context, txn_record.txnID(), std::forward<Args>(args)...);
    }

    template <typename... Args>
    IntentLockPtr createIntentLock(const String & lock_prefix, Args &&... args) const
    {
        String intent = fmt::format("{}", fmt::join(Strings{std::forward<Args>(args)...}, "-"));
        return std::make_unique<IntentLock>(global_context, getTransactionRecord(), lock_prefix, Strings{intent});
    }

    // IntentLockPtr createIntentLock(const LockEntity & entity, const Strings & intent_names = {});

    // If transaction is initiated by worker, record the worker's host and port
    void setCreator(String creator_) { creator = std::move(creator_); }
    const String & getCreator() const { return creator; }

    virtual String getTxnType() const = 0;

    virtual void appendAction(ActionPtr)
    {
        throw Exception("appendAction is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual std::vector<ActionPtr> & getPendingActions()
    {
        throw Exception("getPendingActions is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }


    virtual void setKafkaStorageID(StorageID)
    {
        throw Exception("setKafkaStorageID is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual StorageID getKafkaTableID() const
    {
        throw Exception("getKafkaTableID is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void setKafkaConsumerIndex(size_t)
    {
        throw Exception("setKafkaConsumerIndex is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }
    virtual size_t getKafkaConsumerIndex() const
    {
        throw Exception("getKafkaConsumerIndex is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual UInt32 getDedupImplVersion(ContextPtr /*local_context*/)
    {
        throw Exception("getDedupImplVersion is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    // void setInsertionLabel(InsertionLabelPtr label) { insertion_label = std::move(label); }
    // const InsertionLabelPtr & getInsertionLabel() const { return insertion_label; }

#if USE_MYSQL
    void setBinlogName(String binlog_name_) { binlog_name = std::move(binlog_name_); }
    void setBinlogInfo(MySQLBinLogInfo binlog_info) { binlog = std::move(binlog_info); }
    const MySQLBinLogInfo & getBinlogInfo() const { return binlog; }
#endif

public:
    // Commit API for 2PC, internally calls precommit() and commit()
    // Returns commit_ts on success.
    // throws exceptions and rollback the transaction if commitV2 fails
    virtual TxnTimestamp commitV2() = 0;

    // Precommit transaction, which is the first phase of 2PC
    virtual void precommit() = 0;

    // Commit phase of 2PC
    virtual TxnTimestamp commit() = 0;

    // Rollback transaction, discard all writes made by transaction. Set transaction status to ABORTED
    virtual TxnTimestamp rollback() = 0;

    // Abort transaction, CAS operation.
    virtual TxnTimestamp abort() = 0;

    // Commit API for one-phase commit
    // DOES NOT support rollback if fails
    // DDL statements only supports commitV1 now
    virtual TxnTimestamp commitV1() { throw Exception("commitV1 is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED); }

    // Set transaction status to aborted
    // WILL NOT rollback
    virtual void rollbackV1(const TxnTimestamp & /*ts*/)
    {
        throw Exception("commitV1 is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    // clean intermediate parts, locks and undobuffer
    virtual void clean(TxnCleanTask &) { }

    // Clean intermediate parts synchronously
    virtual void removeIntermediateData() { }

    void appendLockHolder(CnchLockHolderPtr & lock_holder);

    bool force_clean_by_dm = false;

    DatabasePtr tryGetDatabaseViaCache(const String & database_name);
    void addDatabaseIntoCache(DatabasePtr db);

    void addCommitAbortFunc(std::function<Catalog::BatchCommitRequest (ContextPtr context)> commit_f, std::function<Catalog::BatchCommitRequest (ContextPtr context)> abort_f)
    {
        extern_commit_functions.push_back({commit_f, abort_f});
    }

    // serialize the transaction for remote commit (in sequential mode).
    void serialize(Protos::TransactionMetadata & txn_meta) const;

    bool async_post_commit = false;
protected:
    void setStatus(CnchTransactionStatus status);
    void setTransactionRecord(TransactionRecord record);
    void assertLockAcquired() const;

    /// Clean CurrentlyMergingPartsTagger for merge txn after the txn finished.
    void tryCleanMergeTagger();

    /// Transaction still needs global context because the query context will expired after query is finished, but
    /// the transaction still running even query is finished.
    ContextPtr global_context;
    TransactionRecord txn_record;
    UUID main_table_uuid{UUIDHelpers::Nil};

    // independent mode by default.
    TransactionCommitMode commit_mode = TransactionCommitMode::INDEPENDENT;
    // runtime variable set when committing the transaction.
    TxnTimestamp commit_time {0};

    /// for committing offsets
    String consumer_group;
    cppkafka::TopicPartitionList tpl;

#if USE_MYSQL
    MySQLBinLogInfo binlog;
    String binlog_name;
#endif

    InsertionLabelPtr insertion_label;
    CnchLockHolderPtrs lock_holders; /// Currently it only serve for unique dedup stage

    std::vector<TransFunction> extern_commit_functions;

    /// Unique table related
    UInt32 dedup_impl_version = 0;

private:
    String creator;
    mutable bthread::RecursiveMutex mutex;

    LoggerPtr log{getLogger("ICnchTransaction")};
    mutable std::mutex database_cache_mutex;
    std::map<String, DatabasePtr> database_cache;
};

using TransactionCnchPtr = std::shared_ptr<ICnchTransaction>;

class TransactionCnchHolder
{
public:
    explicit TransactionCnchHolder(TransactionCnchPtr txn_ = nullptr) : txn(txn_) { }

    TransactionCnchHolder(TransactionCnchHolder &&) = default;
    TransactionCnchHolder & operator=(TransactionCnchHolder &&) = default;

    void release() { }

    ~TransactionCnchHolder() { release(); }

private:
    TransactionCnchPtr txn;
};

}
