#pragma once

#include <CloudServices/RpcClientBase.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include "Transaction/ICnchTransaction.h"

namespace DB
{
namespace Protos
{
    class CnchServerService_Stub;
}

class ICnchTransaction;

class CnchServerClient : public RpcClientBase
{
public:
    static String getName() { return "CnchServerClient"; }

    explicit CnchServerClient(String host_port_);
    explicit CnchServerClient(HostWithPorts host_ports_);

    ~CnchServerClient() override;

    /// Transaction RPCs related. TODO @canh: add implement when baseline rpc implementation is merged
    std::pair<TxnTimestamp, TxnTimestamp> createTransaction(const TxnTimestamp & primary_txn_id = {0});
    std::pair<TxnTimestamp, TxnTimestamp> createTransactionForKafka(const StorageID & storage_id, const size_t consumer_index);
    TxnTimestamp commitTransaction(
        const ICnchTransaction & txn, const StorageID & kafka_storage_id = StorageID::createEmpty(), const size_t consumer_index = 0);
    void precommitTransaction(const TxnTimestamp & txn_id, const UUID & uuid = UUIDHelpers::Nil);
    TxnTimestamp rollbackTransaction(const TxnTimestamp & txn_id);
    void finishTransaction(const TxnTimestamp & txn_id);

private:
    std::unique_ptr<Protos::CnchServerService_Stub> stub;
};

using CnchServerClientPtr = std::shared_ptr<CnchServerClient>;

}
