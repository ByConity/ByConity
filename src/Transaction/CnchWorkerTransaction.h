#pragma once

#include <Interpreters/StorageID.h>
// #include <MergeTreeCommon/CnchServerClient.h>
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
    // CnchWorkerTransaction(Context & context_, CnchServerClientPtr client);

    // ctor for kafka initiated transaction
    // Create a server transaction on server, and maintain kafka_table_id in the transaction
    // CnchWorkerTransaction(Context & context_, CnchServerClientPtr client, StorageID kafka_table_id, size_t consumer_index);

    // ctor for server initiated transaction
    // Server created a transaction and continue to execute on worker
    CnchWorkerTransaction(Context & context_, const TxnTimestamp & txn_id, const TxnTimestamp & primary_txn_id = 0);

    // TODO: remove this
    // create a fake worker transaction to transfer kafka storage-id
    CnchWorkerTransaction(Context & context_, StorageID kafka_table_id_);

    // If it is a worker/kafka initiated transaction, will finish the server
    // transaction when the worker transaction gets destroyed.
    ~CnchWorkerTransaction() override;

    String getTxnType() const override { return "CnchWorkerTransaction"; }

    // Return nullptr if server client is not set.
    // CnchServerClientPtr tryGetServerClient() const { return server_client; }
    // Throws if server client is not set.
    // CnchServerClientPtr getServerClient() const;
    // void setServerClient(CnchServerClientPtr client);

    void setKafkaStorageID(StorageID storage_id) override { kafka_table_id = std::move(storage_id); }
    StorageID getKafkaTableID() const override { return kafka_table_id; }
    void setKafkaConsumerIndex(size_t index) override { kafka_consumer_index = index; }
    size_t getKafkaConsumerIndex() const override { return kafka_consumer_index; }

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

private:
    void checkServerClient() const;

private:
    // CnchServerClientPtr server_client;
    StorageID kafka_table_id{StorageID::createEmpty()};
    size_t kafka_consumer_index{SIZE_MAX};
    Poco::Logger * log {&Poco::Logger::get("CnchWorkerTransaction")};
    

    // Transaction is initiated by us or get from somewhere else
    bool isInitiator {false};
};

using CnchWorkerTransactionPtr = std::shared_ptr<CnchWorkerTransaction>;
}
