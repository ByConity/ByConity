#include <CloudServices/CnchServerClient.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/cnch_server_rpc.pb.h>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageCloudMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BRPC_CANNOT_INIT_CHANNEL;
    extern const int NOT_IMPLEMENTED;
}

CnchServerClient::CnchServerClient(String host_port_)
    : RpcClientBase(getName(), std::move(host_port_)), stub(std::make_unique<Protos::CnchServerService_Stub>(&getChannel()))
{
}

CnchServerClient::CnchServerClient(HostWithPorts host_ports_)
    : RpcClientBase(getName(), std::move(host_ports_)), stub(std::make_unique<Protos::CnchServerService_Stub>(&getChannel()))
{
}

CnchServerClient::~CnchServerClient() = default;

std::pair<TxnTimestamp, TxnTimestamp> CnchServerClient::createTransaction(const TxnTimestamp & primary_txn_id)
{
    brpc::Controller cntl;
    Protos::CreateTransactionReq request;
    Protos::CreateTransactionResp response;
    if (primary_txn_id)
        request.set_primary_txn_id(primary_txn_id.toUInt64());
    stub->createTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return {response.txn_id(), response.start_time()};
}

TxnTimestamp
CnchServerClient::commitTransaction(const ICnchTransaction & txn, const StorageID & kafka_storage_id, const size_t consumer_index)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(10 * 1000);

    Protos::CommitTransactionReq request;
    Protos::CommitTransactionResp response;

    request.set_txn_id(txn.getTransactionID());
    if (const auto & label = txn.getInsertionLabel())
        request.set_insertion_label(label->name);

    if (!kafka_storage_id.empty())
    {
        RPCHelpers::fillStorageID(kafka_storage_id, *request.mutable_kafka_storage_id());
        request.set_kafka_consumer_index(consumer_index);
    }
    stub->commitTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
    return response.commit_ts();
}

void CnchServerClient::precommitTransaction(const TxnTimestamp & txn_id, const UUID & uuid)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(10 * 1000);

    Protos::PrecommitTransactionReq request;
    Protos::PrecommitTransactionResp response;

    request.set_txn_id(txn_id);
    RPCHelpers::fillUUID(uuid, *request.mutable_main_table_uuid());
    stub->precommitTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

TxnTimestamp CnchServerClient::rollbackTransaction(const TxnTimestamp & txn_id)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(10 * 1000);

    Protos::RollbackTransactionReq request;
    Protos::RollbackTransactionResp response;

    request.set_txn_id(txn_id);
    stub->rollbackTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
    return response.commit_ts();
}

void CnchServerClient::finishTransaction(const TxnTimestamp & txn_id)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(20 * 1000); /// TODO: from config

    Protos::FinishTransactionReq request;
    Protos::FinishTransactionResp response;

    request.set_txn_id(txn_id);

    stub->finishTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

std::pair<TxnTimestamp, TxnTimestamp> CnchServerClient::createTransactionForKafka(const StorageID & storage_id, const size_t consumer_index)
{
    brpc::Controller cntl;
    Protos::CreateKafkaTransactionReq request;
    Protos::CreateKafkaTransactionResp response;

    RPCHelpers::fillUUID(storage_id.uuid, *request.mutable_uuid());
    request.set_consumer_index(consumer_index);
    request.set_table_name(storage_id.table_name);

    stub->createTransactionForKafka(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return {response.txn_id(), response.start_time()};
}

ServerDataPartsVector CnchServerClient::fetchDataParts(const String & remote_host, const StoragePtr & table, const Strings & partition_list, const TxnTimestamp & ts)
{
    brpc::Controller cntl;
    Protos::FetchDataPartsReq request;
    Protos::FetchDataPartsResp response;

    request.set_remote_host(remote_host);
    request.set_database(table->getDatabaseName());
    request.set_table(table->getTableName());
    request.set_table_commit_time(table->commit_time);
    request.set_timestamp(ts.toUInt64());

    for (auto & partition_id : partition_list)
        request.add_partitions(partition_id);

    stub->fetchDataParts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    auto & storage = dynamic_cast<MergeTreeMetaBase &>(*table);
    return createServerPartsFromModels(storage, response.parts());
}

void buildRedirectCommitRequestBase(
    const StoragePtr & table,
    const Catalog::CommitItems & commit_data,
    Protos::RedirectCommitPartsReq & request)
{
    request.set_database(table->getDatabaseName());
    request.set_table(table->getTableName());
    RPCHelpers::fillUUID(table->getStorageUUID(), *request.mutable_uuid());

    for (auto & part : commit_data.data_parts)
    {
        fillPartModel(*table, *part, *request.add_parts());
    }

    for (auto & staged_part : commit_data.staged_parts)
    {
        fillPartModel(*table, *staged_part, *request.add_staged_parts());
    }

    for (auto & delete_bitmap : commit_data.delete_bitmaps)
    {
        auto * new_bitmap = request.add_delete_bitmaps();
        new_bitmap->CopyFrom(*(delete_bitmap->getModel()));
    }
}

void CnchServerClient::redirectCommitParts(
    const StoragePtr & table,
    const Catalog::CommitItems & commit_data,
    const TxnTimestamp & txnID,
    const bool is_merged_parts,
    const bool preallocate_mode)
{
    brpc::Controller cntl;
    Protos::RedirectCommitPartsReq request;
    Protos::RedirectCommitPartsResp response;

    buildRedirectCommitRequestBase(table, commit_data, request);

    request.set_txn_id(txnID.toUInt64());
    request.set_from_merge_task(is_merged_parts);
    request.set_preallocate_mode(preallocate_mode);

    stub->redirectCommitParts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::redirectSetCommitTime(
    const StoragePtr & table,
    const Catalog::CommitItems & commit_data,
    const TxnTimestamp & commitTs,
    const UInt64 txn_id)
{
    brpc::Controller cntl;
    Protos::RedirectCommitPartsReq request;
    Protos::RedirectCommitPartsResp response;

    buildRedirectCommitRequestBase(table, commit_data, request);

    request.set_txn_id(txn_id);
    request.set_commit_ts(commitTs.toUInt64());

    stub->redirectSetCommitTime(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

TxnTimestamp CnchServerClient::commitParts(
    const TxnTimestamp & txn_id,
    ManipulationType type,
    MergeTreeMetaBase & storage,
    const MutableMergeTreeDataPartsCNCHVector & parts,
    const DeleteBitmapMetaPtrVector & delete_bitmaps,
    const MutableMergeTreeDataPartsCNCHVector & staged_parts,
    const String & task_id,
    const bool from_server,
    const String & consumer_group,
    const cppkafka::TopicPartitionList & tpl,
    const String & from_buffer_uuid)
{
    /// TODO: check txn_id & start_ts

    brpc::Controller cntl;
    cntl.set_timeout_ms(10 * 1000); /// TODO: from config
    Protos::CommitPartsReq request;
    Protos::CommitPartsResp response;

    if (from_server)
    {
        StorageCnchMergeTree & cnch_storage = dynamic_cast<StorageCnchMergeTree &>(storage);
        request.set_database(cnch_storage.getDatabaseName());
        request.set_table(cnch_storage.getTableName());
        RPCHelpers::fillUUID(cnch_storage.getStorageUUID(), *request.mutable_uuid());
    }
    else
    {
        StorageCloudMergeTree & cloud_storage = dynamic_cast<StorageCloudMergeTree &>(storage);
        // request.set_database(cloud_storage.getCnchDatabase());
        // request.set_table(cloud_storage.getCnchTable());
        RPCHelpers::fillUUID(cloud_storage.getStorageUUID(), *request.mutable_uuid());
    }

    request.set_type(UInt32(type));
    for (const auto & part : parts)
    {
        auto * new_part = request.add_parts();
        fillPartModel(storage, *part, *new_part);
        request.add_paths()->assign(part->relative_path);
    }
    for (const auto & staged_part : staged_parts)
    {
        fillPartModel(storage, *staged_part, *request.add_staged_parts());
        request.add_staged_parts_paths()->assign(staged_part->relative_path);
    }

    request.set_txn_id(UInt64(txn_id));
    if (!task_id.empty())
        request.set_task_id(task_id);

    if (!from_buffer_uuid.empty())
        request.set_from_buffer_uuid(from_buffer_uuid);

    /// add tpl for kafka commit
    if (!consumer_group.empty()) {
        if (tpl.empty())
            throw Exception("No tpl get while committing kafka data", ErrorCodes::LOGICAL_ERROR);
        request.set_consumer_group(consumer_group);
        for (auto & tp : tpl)
        {
            auto * cur_tp = request.add_tpl();
            cur_tp->set_topic(tp.get_topic());
            cur_tp->set_partition(tp.get_partition());
            cur_tp->set_offset(tp.get_offset());
        }
    }

    /// add delete bitmaps for table with unique key
    for (const auto & delete_bitmap : delete_bitmaps)
    {
        auto * new_bitmap = request.add_delete_bitmaps();
        new_bitmap->CopyFrom(*(delete_bitmap->getModel()));
    }

    stub->commitParts(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.commit_timestamp();
}

TxnTimestamp CnchServerClient::precommitParts(
    const Context & context,
    const TxnTimestamp & txn_id,
    ManipulationType type,
    MergeTreeMetaBase & storage,
    const MutableMergeTreeDataPartsCNCHVector & parts,
    const DeleteBitmapMetaPtrVector & delete_bitmaps,
    const MutableMergeTreeDataPartsCNCHVector & staged_parts,
    const String & task_id,
    const bool from_server,
    const String & consumer_group,
    const cppkafka::TopicPartitionList & tpl,
    const String & from_buffer_uuid)
{
    // TODO: this method only apply to ManipulationType which supports 2pc
    if (type != ManipulationType::Insert)
    {
        // fallback to 1pc
        return commitParts(txn_id, type, storage, parts, delete_bitmaps, staged_parts, task_id, from_server, consumer_group, tpl, from_buffer_uuid);
    }

    const UInt64 batch_size = context.getSettingsRef().catalog_max_commit_size;

    // Precommit parts in batches {batch_begin, batch_end}
    const size_t max_size = std::max({parts.size(), delete_bitmaps.size(), staged_parts.size()});
    for (size_t batch_begin = 0; batch_begin < max_size; batch_begin += batch_size)
    {
        size_t batch_end = batch_begin + batch_size;

        size_t part_batch_begin = std::min(batch_begin, parts.size());
        size_t part_batch_end = std::min(batch_end, parts.size());
        size_t bitmap_batch_begin = std::min(batch_begin, delete_bitmaps.size());
        size_t bitmap_batch_end = std::min(batch_end, delete_bitmaps.size());
        size_t staged_part_batch_begin = std::min(batch_begin, staged_parts.size());
        size_t staged_part_batch_end = std::min(batch_end, staged_parts.size());

        Poco::Logger * log = &Poco::Logger::get(__func__);
        LOG_DEBUG(
            log,
            "Precommit: parts in batch: [{} ~  {}] of total:  {}; delete_bitmaps in batch [{} ~ {}] of total {}; staged parts in batch [{} "
            "~ {}] of total {}.",
            part_batch_begin,
            part_batch_end,
            parts.size(),
            bitmap_batch_begin,
            bitmap_batch_end,
            delete_bitmaps.size(),
            staged_part_batch_begin,
            staged_part_batch_end,
            staged_parts.size());

        commitParts(
            txn_id,
            type,
            storage,
            {parts.begin() + part_batch_begin, parts.begin() + part_batch_end},
            {delete_bitmaps.begin() + bitmap_batch_begin, delete_bitmaps.begin() + bitmap_batch_end},
            {staged_parts.begin() + staged_part_batch_begin, staged_parts.begin() + staged_part_batch_end},
            task_id,
            from_server,
            consumer_group,
            tpl,
            from_buffer_uuid);
    }

    // no commit_time for precommit
    return {};
}

google::protobuf::RepeatedPtrField<DB::Protos::DataModelTableInfo>
CnchServerClient::getTableInfo(const std::vector<std::shared_ptr<Protos::TableIdentifier>> & tables)
{
    brpc::Controller cntl;
    Protos::GetTableInfoReq request;
    Protos::GetTableInfoResp response;

    for (auto & table : tables)
    {
        DB::Protos::TableIdentifier * table_id = request.add_table_ids();
        table_id->CopyFrom(*table);
    }

    stub->getTableInfo(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.table_infos();
}

}
