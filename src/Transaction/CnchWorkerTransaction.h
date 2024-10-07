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
#include <Interpreters/StorageID.h>
#include <CloudServices/CnchServerClient.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>

#include <memory>

namespace DB
{
class Context;

class CnchWorkerTransaction : public ICnchTransaction
{
public:
    // ctor for worker initiated tranasction
    // Create a server transaction on cnch server using `client`
    CnchWorkerTransaction(const ContextPtr & context_, CnchServerClientPtr client);

    // ctor for kafka initiated transaction
    // Create a server transaction on server, and maintain kafka_table_id in the transaction
    CnchWorkerTransaction(const ContextPtr & context_, CnchServerClientPtr client, StorageID kafka_table_id, size_t consumer_index);

    // ctor for server initiated transaction
    // Server created a transaction and continue to execute on worker
    CnchWorkerTransaction(const ContextPtr & context_, const TxnTimestamp & txn_id, const TxnTimestamp & primary_txn_id = 0);

    // ctor for server initiated transaction
    // Server created a transaction, continue to execute on worker and support commit from worker txn
    CnchWorkerTransaction(const ContextPtr & context_, const TxnTimestamp & txn_id, CnchServerClientPtr client, const TxnTimestamp & primary_txn_id = 0);

    // TODO: remove this
    // create a fake worker transaction to transfer kafka storage-id
    CnchWorkerTransaction(const ContextPtr & context_, StorageID kafka_table_id_);

    // If it is a worker/kafka initiated transaction, will finish the server
    // transaction when the worker transaction gets destroyed.
    ~CnchWorkerTransaction() override;

    String getTxnType() const override { return "CnchWorkerTransaction"; }

    // Return nullptr if server client is not set.
    CnchServerClientPtr tryGetServerClient() const { return server_client; }
    // Throws if server client is not set.
    CnchServerClientPtr getServerClient() const;
    void setServerClient(CnchServerClientPtr client);

    void setKafkaStorageID(StorageID storage_id) override { kafka_table_id = std::move(storage_id); }
    StorageID getKafkaTableID() const override { return kafka_table_id; }
    void setKafkaConsumerIndex(size_t index) override { kafka_consumer_index = index; }
    size_t getKafkaConsumerIndex() const override { return kafka_consumer_index; }

    void enableExplicitCommit() { enable_explicit_commit = true; }
    bool hasEnableExplicitCommit() const { return enable_explicit_commit; }
    void setExplicitCommitStorageID(StorageID storage_id) {explicit_commit_table_id = std::move(storage_id);}
    StorageID getExplicitCommitStorageID() const {return explicit_commit_table_id;}

    // Commit API for 2PC
    // Check status in Catalog if commit on server times out.
    TxnTimestamp commitV2() override;

    void precommit() override;
    TxnTimestamp commit() override;
    TxnTimestamp rollback() override;
    TxnTimestamp abort() override
    {
        throw Exception("abort is not supported for " + getTxnType(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void setIsInitiator(bool is_initiator_) { is_initiator = is_initiator_; }

    UInt32 getDedupImplVersion(ContextPtr local_context) override;

    /// This method will be called in commit parts stage for unique table
    void setDedupImplVersion(const UInt32 & dedup_impl_version_);

private:

    /// It will be called only for the case that dedup impl version is not set in commit parts stage.
    UInt32 getDedupImplVersionFromServer(ContextPtr local_context);

    void checkServerClient() const;

    CnchServerClientPtr server_client;
    StorageID kafka_table_id{StorageID::createEmpty()};
    size_t kafka_consumer_index{SIZE_MAX};
    LoggerPtr log {getLogger("CnchWorkerTransaction")};

    /// Transaction should only be committed explicitly
    bool enable_explicit_commit{false};
    StorageID explicit_commit_table_id {StorageID::createEmpty()};
    // Transaction is initiated by us or get from somewhere else
    bool is_initiator {false};
};

using CnchWorkerTransactionPtr = std::shared_ptr<CnchWorkerTransaction>;
}
