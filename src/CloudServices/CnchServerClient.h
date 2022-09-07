#pragma once

#include <CloudServices/RpcClientBase.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <Transaction/ICnchTransaction.h>
#include <WorkerTasks/ManipulationType.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Catalog/CatalogUtils.h>

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

    CnchTransactionStatus getTransactionStatus(const TxnTimestamp & txn_id, bool need_search_catalog = false);

    void removeIntermediateData(const TxnTimestamp & txn_id);

    ServerDataPartsVector fetchDataParts(const String & remote_host, const StoragePtr & table, const Strings & partition_list, const TxnTimestamp & ts);

    void redirectCommitParts(
        const StoragePtr & table,
        const Catalog::CommitItems & commit_data,
        const TxnTimestamp & txnID,
        const bool is_merged_parts,
        const bool preallocate_mode);

    void redirectSetCommitTime(
        const StoragePtr & table,
        const Catalog::CommitItems & commit_data,
        const TxnTimestamp & commitTs,
        const UInt64 txn_id);

    TxnTimestamp commitParts(
        const TxnTimestamp & txn_id,
        ManipulationType type,
        MergeTreeMetaBase & storage,
        const MutableMergeTreeDataPartsCNCHVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const MutableMergeTreeDataPartsCNCHVector & staged_parts,
        const String & task_id = {},
        const bool from_server = false,
        const String & consumer_group = {},
        const cppkafka::TopicPartitionList & tpl = {},
        const String & from_buffer_uuid = {});

    TxnTimestamp precommitParts(
        const Context & context,
        const TxnTimestamp & txn_id,
        ManipulationType type,
        MergeTreeMetaBase & storage,
        const MutableMergeTreeDataPartsCNCHVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const MutableMergeTreeDataPartsCNCHVector & staged_parts,
        const String & task_id = {},
        const bool from_server = false,
        const String & consumer_group = {},
        const cppkafka::TopicPartitionList & tpl = {},
        const String & from_buffer_uuid = {});

    google::protobuf::RepeatedPtrField<DB::Protos::DataModelTableInfo>
    getTableInfo(const std::vector<std::shared_ptr<Protos::TableIdentifier>> & tables);
    void controlCnchBGThread(const StorageID & storage_id, CnchBGThreadType type, CnchBGThreadAction action);
    void cleanTransaction(const TransactionRecord & txn_record);
    std::set<UUID> getDeletingTablesInGlobalGC();

    void acquireLock(const LockInfoPtr & info);
    void releaseLock(const LockInfoPtr & info);
    void reportCnchLockHeartBeat(const TxnTimestamp & txn_id, UInt64 expire_time = 0);

    UInt64 getServerStartTime();
    bool scheduleGlobalGC(const std::vector<Protos::DataModelTable> & tables);
    size_t getNumOfTablesCanSendForGlobalGC();
    google::protobuf::RepeatedPtrField<DB::Protos::BackgroundThreadStatus>
    getBackGroundStatus(const CnchBGThreadType & type);

private:
    std::unique_ptr<Protos::CnchServerService_Stub> stub;
};

using CnchServerClientPtr = std::shared_ptr<CnchServerClient>;

}
