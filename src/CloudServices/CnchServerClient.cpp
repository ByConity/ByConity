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

#include <CloudServices/CnchServerClient.h>
#include <Common/ProfileEventsTimer.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Parsers/ASTSerDerHelper.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Protos/auto_statistics.pb.h>
#include <common/types.h>
#include <Storages/MergeTree/MarkRange.h>
#include <CloudServices/CnchDataWriter.h>
#include <Optimizer/PredicateUtils.h>


namespace ProfileEvents
{
    extern const int ServerRpcRequest;
    extern const int ServerRpcElaspsedMicroseconds;
}

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

static std::optional<Protos::TraceInfo> setTraceInfo()
{
    Protos::TraceInfo trace_info;
    if (auto query_id = CurrentThread::getQueryId(); !query_id.empty())
        trace_info.set_query_id(query_id.toString());
    if (auto txn_id = CurrentThread::getTransactionId(); txn_id)
        trace_info.set_txn_id(txn_id);
    return (trace_info.has_txn_id() || trace_info.has_query_id()) ? std::optional<Protos::TraceInfo>(trace_info) : std::nullopt;
}

std::pair<TxnTimestamp, TxnTimestamp> CnchServerClient::createTransaction(
    const TxnTimestamp & primary_txn_id, bool read_only)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::CreateTransactionReq request;
    Protos::CreateTransactionResp response;
    if (primary_txn_id)
        request.set_primary_txn_id(primary_txn_id.toUInt64());
    request.set_read_only(read_only);
    stub->createTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return {response.txn_id(), response.start_time()};
}

TxnTimestamp
CnchServerClient::commitTransaction(const ICnchTransaction & txn, const StorageID & kafka_storage_id, const size_t consumer_index)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    cntl.set_timeout_ms(10 * 1000);

    Protos::CommitTransactionReq request;
    Protos::CommitTransactionResp response;

    request.set_txn_id(txn.getTransactionID());
    // if (const auto & label = txn.getInsertionLabel())
    //     request.set_insertion_label(label->name);

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

void CnchServerClient::precommitTransaction(const ContextPtr & context, const TxnTimestamp & txn_id, const UUID & uuid)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    cntl.set_timeout_ms(context->getSettingsRef().max_dedup_execution_time.totalMilliseconds());

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
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
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
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    cntl.set_timeout_ms(20 * 1000); /// TODO: from config

    Protos::FinishTransactionReq request;
    Protos::FinishTransactionResp response;

    request.set_txn_id(txn_id);

    stub->finishTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::commitTransactionViaGlobalCommitter(const TransactionCnchPtr & txn)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    cntl.set_timeout_ms(10 * 1000);  /// make it configurable later

    Protos::RedirectCommitTransactionReq request;
    Protos::RedirectCommitTransactionResp response;

    txn->serialize(*(request.mutable_txn_meta()));

    stub->redirectCommitTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

std::pair<TxnTimestamp, TxnTimestamp> CnchServerClient::createTransactionForKafka(const StorageID & storage_id, const size_t consumer_index)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
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

ServerDataPartsVector CnchServerClient::fetchDataParts(const String & remote_host, const ConstStoragePtr & table, const Strings & partition_list, const TxnTimestamp & ts, const std::set<Int64> & bucket_numbers)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    if (const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(table.get()))
        cntl.set_timeout_ms(storage->getSettings()->cnch_meta_rpc_timeout_ms);
    else
        cntl.set_timeout_ms(8 * 1000);
    Protos::FetchDataPartsReq request;
    Protos::FetchDataPartsResp response;

    request.set_remote_host(remote_host);
    request.set_database(table->getDatabaseName());
    request.set_table(table->getTableName());
    request.set_table_commit_time(table->commit_time);
    request.set_timestamp(ts.toUInt64());

    auto trace_info = setTraceInfo();
    if (trace_info)
    {
        *request.mutable_trace_info() = *trace_info;
    }

    for (const auto & partition_id : partition_list)
        request.add_partitions(partition_id);

    for (auto & bucket_number : bucket_numbers)
        request.add_bucket_numbers(bucket_number);

    stub->fetchDataParts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    const auto & storage = dynamic_cast<const MergeTreeMetaBase &>(*table);
    return createServerPartsFromModels(storage, response.parts());
}

DeleteBitmapMetaPtrVector CnchServerClient::fetchDeleteBitmaps(
    const String & remote_host,
    const ConstStoragePtr & table,
    const Strings & partition_list,
    const TxnTimestamp & ts,
    const std::set<Int64> & bucket_numbers)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    if (const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(table.get()))
        cntl.set_timeout_ms(storage->getSettings()->cnch_meta_rpc_timeout_ms);
    else
        cntl.set_timeout_ms(8 * 1000);
    Protos::FetchDeleteBitmapsReq request;
    Protos::FetchDeleteBitmapsResp response;

    request.set_remote_host(remote_host);
    request.set_database(table->getDatabaseName());
    request.set_table(table->getTableName());
    request.set_table_commit_time(table->commit_time);
    request.set_timestamp(ts.toUInt64());
    auto trace_info = setTraceInfo();
    if (trace_info)
        *request.mutable_trace_info() = *trace_info;

    for (const auto & bucket_number : bucket_numbers)
        request.add_bucket_numbers(bucket_number);

    for (const auto & partition_id : partition_list)
        request.add_partitions(partition_id);

    stub->fetchDeleteBitmaps(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    const auto & storage = dynamic_cast<const MergeTreeMetaBase &>(*table);
    DeleteBitmapMetaPtrVector ret;
    ret.reserve(response.delete_bitmaps().size());
    for (const auto & delete_bitmap : response.delete_bitmaps())
    {
        ret.emplace_back(std::make_shared<DeleteBitmapMeta>(storage, std::make_shared<Protos::DataModelDeleteBitmap>(delete_bitmap)));
    }
    return ret;
}

PrunedPartitions CnchServerClient::fetchPartitions(
    const String & remote_host,
    const ConstStoragePtr & table,
    const SelectQueryInfo & query_info,
    const Names & column_names,
    const TxnTimestamp & txn_id,
    const bool & ignore_ttl)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    if (const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(table.get()))
        cntl.set_timeout_ms(storage->getSettings()->cnch_meta_rpc_timeout_ms);
    else
        cntl.set_timeout_ms(8 * 1000);

    Protos::FetchPartitionsReq request;
    Protos::FetchPartitionsResp response;

    request.set_remote_host(remote_host);
    request.set_database(table->getDatabaseName());
    request.set_table(table->getTableName());
    WriteBufferFromOwnString buff;
    ASTs conjuncts;
    if (auto where = query_info.getSelectQuery()->where())
        conjuncts.emplace_back(where);
    if (query_info.partition_filter)
        conjuncts.emplace_back(query_info.partition_filter);
    auto cloned_select = query_info.getSelectQuery()->clone();
    cloned_select->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts<>(conjuncts));
    serializeAST(cloned_select, buff);
    request.set_predicate(buff.str());

    for (const auto & name : column_names)
        request.add_column_name_filter(name);

    request.set_txnid(txn_id.toUInt64());
    request.set_ignore_ttl(ignore_ttl);

    stub->fetchPartitions(&cntl, &request, & response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    auto trace_info = setTraceInfo();
    if (trace_info)
        *request.mutable_trace_info() = *trace_info;

    UInt64 total_size = response.total_size();
    Strings fetched_partitions;
    for (const auto & partition : response.partitions())
        fetched_partitions.push_back(partition);
    return PrunedPartitions{fetched_partitions, total_size};
}

template <class Request>
void buildRedirectRequest(const StoragePtr & table, const Catalog::CommitItems & commit_data, Request & request)
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
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    if (const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(table.get()))
        cntl.set_timeout_ms(storage->getSettings()->cnch_meta_rpc_timeout_ms);
    Protos::RedirectCommitPartsReq request;
    Protos::RedirectCommitPartsResp response;

    buildRedirectRequest(table, commit_data, request);

    request.set_txn_id(txnID.toUInt64());
    request.set_from_merge_task(is_merged_parts);
    request.set_preallocate_mode(preallocate_mode);

    stub->redirectCommitParts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::redirectClearParts(const StoragePtr & table, const Catalog::CommitItems & commit_data)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RedirectClearPartsReq request;
    Protos::RedirectClearPartsResp response;
    buildRedirectRequest(table, commit_data, request);

    stub->redirectClearParts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::redirectSetCommitTime(
    const StoragePtr & table,
    const Catalog::CommitItems & commit_data,
    const TxnTimestamp & commitTs,
    const UInt64 txn_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RedirectCommitPartsReq request;
    Protos::RedirectCommitPartsResp response;

    buildRedirectRequest(table, commit_data, request);

    request.set_txn_id(txn_id);
    request.set_commit_ts(commitTs.toUInt64());

    stub->redirectSetCommitTime(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::redirectAttachDetachedS3Parts(
    const StoragePtr & to_table,
    const UUID & from_table_uuid,
    const UUID & to_table_uuid,
    const IMergeTreeDataPartsVector & commit_parts,
    const IMergeTreeDataPartsVector & commit_staged_parts,
    const Strings & detached_part_names,
    size_t detached_visible_part_size,
    size_t detached_staged_part_size,
    const Strings & detached_bitmap_names,
    const DeleteBitmapMetaPtrVector & detached_bitmaps,
    const DeleteBitmapMetaPtrVector & bitmaps,
    const DB::Protos::DetachAttachType & type,
    const UInt64 & txn_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RedirectAttachDetachedS3PartsReq request;
    Protos::RedirectAttachDetachedS3PartsResp response;

    if (UUIDHelpers::Nil != from_table_uuid)
        RPCHelpers::fillUUID(from_table_uuid, *request.mutable_from_table_uuid());

    RPCHelpers::fillUUID(to_table_uuid, *request.mutable_to_table_uuid());
    if (txn_id > 0)
        request.set_txn_id(txn_id);

    if (to_table)
    {
        for (auto & part : commit_parts)
            fillPartModel(*to_table, *part, *request.add_commit_parts());

        for (auto & part : commit_staged_parts)
            fillPartModel(*to_table, *part, *request.add_commit_staged_parts());
    }

    for (auto & part_name : detached_part_names)
        request.add_detached_part_names(part_name);

    for (auto & bitmap_name : detached_bitmap_names)
        request.add_detached_bitmap_names(bitmap_name);

    for (auto & bitmap_meta: detached_bitmaps)
    {
        auto * new_bitmap = request.add_detached_bitmaps();
        new_bitmap->CopyFrom(*(bitmap_meta->getModel()));
    }

    for (auto & bitmap_meta: bitmaps)
    {
        auto * new_bitmap = request.add_bitmaps();
        new_bitmap->CopyFrom(*(bitmap_meta->getModel()));
    }

    request.set_type(type);
    request.set_detached_visible_part_size(detached_visible_part_size);
    request.set_detached_staged_part_size(detached_staged_part_size);

    stub->redirectAttachDetachedS3Parts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}


void CnchServerClient::redirectDetachAttachedS3Parts(
    const StoragePtr & to_table,
    const UUID & from_table_uuid,
    const UUID & to_table_uuid,
    const IMergeTreeDataPartsVector & attached_parts,
    const IMergeTreeDataPartsVector & attached_staged_parts,
    const IMergeTreeDataPartsVector & commit_parts,
    const Strings & attached_part_names,
    const Strings & attached_bitmap_names,
    const DeleteBitmapMetaPtrVector & attached_bitmaps,
    const DeleteBitmapMetaPtrVector & bitmaps,
    const std::vector<std::pair<String, String>> & detached_part_metas,
    const std::vector<std::pair<String, String>> & detached_bitmap_metas,
    const DB::Protos::DetachAttachType & type,
    const UInt64 & txn_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RedirectDetachAttachedS3PartsReq request;
    Protos::RedirectDetachAttachedS3PartsResp response;

    if (UUIDHelpers::Nil != from_table_uuid)
        RPCHelpers::fillUUID(from_table_uuid, *request.mutable_from_table_uuid());

    RPCHelpers::fillUUID(to_table_uuid, *request.mutable_to_table_uuid());
    if (txn_id > 0)
        request.set_txn_id(txn_id);

    if (to_table)
    {
        for (auto & part : commit_parts)
        {
            // add empty part model if part is nullptr
            auto new_added_part = request.add_commit_parts();
            if (part)
            {
                fillPartModel(*to_table, *part, *new_added_part);
            }
        }

        for (auto & part : attached_parts)
            fillPartModel(*to_table, *part, *request.add_attached_parts());

        for (auto & part : attached_staged_parts)
            fillPartModel(*to_table, *part, *request.add_attached_staged_parts());
    }

    for (auto & part_name : attached_part_names)
        request.add_attached_part_names(part_name);
    for (auto & bitmap_name : attached_bitmap_names)
        request.add_attached_bitmap_names(bitmap_name);


    for (auto & bitmap_meta: attached_bitmaps)
    {
        auto * new_bitmap = request.add_attached_bitmaps();
        new_bitmap->CopyFrom(*(bitmap_meta->getModel()));
    }

    for (auto & bitmap_meta: bitmaps)
    {
        auto * new_bitmap = request.add_bitmaps();
        new_bitmap->CopyFrom(*(bitmap_meta->getModel()));
    }

    for (auto & [name, meta] : detached_part_metas)
    {
        Protos::DetachMeta * meta_model = request.add_detached_part_metas();
        meta_model->set_name(name);
        meta_model->set_meta(meta);
    }

    for (auto & [name, meta] : detached_bitmap_metas)
    {
        Protos::DetachMeta * meta_model = request.add_detached_bitmap_metas();
        meta_model->set_name(name);
        meta_model->set_meta(meta);
    }

    request.set_type(type);

    stub->redirectDetachAttachedS3Parts(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

UInt32 CnchServerClient::commitParts(
    const TxnTimestamp & txn_id,
    ManipulationType type,
    MergeTreeMetaBase & storage,
    const DumpedData & dumped_data,
    const String & task_id,
    const bool from_server,
    const String & consumer_group,
    const cppkafka::TopicPartitionList & tpl,
    const MySQLBinLogInfo & binlog,
    const UInt64 peak_memory_usage)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    /// TODO: check txn_id & start_ts

    const auto & parts = dumped_data.parts;
    const auto & delete_bitmaps = dumped_data.bitmaps;
    const auto & staged_parts = dumped_data.staged_parts;

    brpc::Controller cntl;
    cntl.set_timeout_ms(storage.getSettings()->cnch_meta_rpc_timeout_ms);
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
        if (auto * cnch_storage = dynamic_cast<StorageCnchMergeTree *>(&storage))
        {
            request.set_database(cnch_storage->getDatabaseName());
            request.set_table(cnch_storage->getTableName());
            RPCHelpers::fillUUID(cnch_storage->getStorageUUID(), *request.mutable_uuid());
        }
        else if (auto * cloud_storage = dynamic_cast<StorageCloudMergeTree *>(&storage))
        {
            request.set_database(cloud_storage->getCnchDatabase());
            request.set_table(cloud_storage->getCnchTable());
            RPCHelpers::fillUUID(cloud_storage->getStorageUUID(), *request.mutable_uuid());
        }
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

    /// add tpl for kafka commit
    if (!consumer_group.empty())
    {
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

    /// add binlog for MaterializedMySQL
    if (!binlog.binlog_file.empty())
    {
        auto * binlog_req = request.mutable_binlog();
        binlog_req->set_binlog_file(binlog.binlog_file);
        binlog_req->set_binlog_position(binlog.binlog_position);
        binlog_req->set_executed_gtid_set(binlog.executed_gtid_set);
        binlog_req->set_meta_version(binlog.meta_version);
    }

    request.set_peak_memory_usage(peak_memory_usage);

    /// add delete bitmaps for table with unique key
    for (const auto & delete_bitmap : delete_bitmaps)
    {
        auto * new_bitmap = request.add_delete_bitmaps();
        new_bitmap->CopyFrom(*(delete_bitmap->getModel()));
    }

    request.set_dedup_mode(static_cast<UInt32>(dumped_data.dedup_mode));

    stub->commitParts(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
    return response.has_dedup_impl_version() ? response.dedup_impl_version(): 1;
}

/* This method commits from worker side, it split the commit parts in multiple batches to avoid rpc timeout for too many parts.
   Note, it only applys to ManipulationType which supports 2pc, now we already separate txn commit from part commit */
UInt32 CnchServerClient::precommitParts(
    ContextPtr context,
    const TxnTimestamp & txn_id,
    ManipulationType type,
    MergeTreeMetaBase & storage,
    const DumpedData & dumped_data,
    const String & task_id,
    const bool from_server,
    const String & consumer_group,
    const cppkafka::TopicPartitionList & tpl,
    const MySQLBinLogInfo & binlog,
    const UInt64 peak_memory_usage)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    const UInt64 batch_size = context->getSettingsRef().catalog_max_commit_size;
    const auto & parts = dumped_data.parts;
    const auto & delete_bitmaps = dumped_data.bitmaps;
    const auto & staged_parts = dumped_data.staged_parts;

    // Precommit parts in batches {batch_begin, batch_end}
    const size_t max_size = std::max({parts.size(), delete_bitmaps.size(), staged_parts.size()});
    UInt32 dedup_impl_version = 1;
    for (size_t batch_begin = 0; batch_begin < max_size; batch_begin += batch_size)
    {
        size_t batch_end = batch_begin + batch_size;

        size_t part_batch_begin = std::min(batch_begin, parts.size());
        size_t part_batch_end = std::min(batch_end, parts.size());
        size_t bitmap_batch_begin = std::min(batch_begin, delete_bitmaps.size());
        size_t bitmap_batch_end = std::min(batch_end, delete_bitmaps.size());
        size_t staged_part_batch_begin = std::min(batch_begin, staged_parts.size());
        size_t staged_part_batch_end = std::min(batch_end, staged_parts.size());

        LoggerPtr log = getLogger(__func__);
        LOG_DEBUG(
            log,
            "Precommit: parts in batch: [{} ~  {}] of total:  {}; delete_bitmaps in batch [{} ~ {}] of total {}; staged parts in batch [{} "
            "~ {}] of total {}; dedup mode is {}",
            part_batch_begin,
            part_batch_end,
            parts.size(),
            bitmap_batch_begin,
            bitmap_batch_end,
            delete_bitmaps.size(),
            staged_part_batch_begin,
            staged_part_batch_end,
            staged_parts.size(),
            typeToString(dumped_data.dedup_mode));

        DumpedData new_dumped_data;
        new_dumped_data.parts = {parts.begin() + part_batch_begin, parts.begin() + part_batch_end};
        new_dumped_data.bitmaps = {delete_bitmaps.begin() + bitmap_batch_begin, delete_bitmaps.begin() + bitmap_batch_end};
        new_dumped_data.staged_parts = {staged_parts.begin() + staged_part_batch_begin, staged_parts.begin() + staged_part_batch_end};
        new_dumped_data.dedup_mode = dumped_data.dedup_mode;

        dedup_impl_version = commitParts(
            txn_id,
            type,
            storage,
            new_dumped_data,
            task_id,
            from_server,
            consumer_group,
            tpl,
            binlog,
            peak_memory_usage);
    }
    return dedup_impl_version;
}

google::protobuf::RepeatedPtrField<DB::Protos::DataModelTableInfo>
CnchServerClient::getTableInfo(const std::vector<std::shared_ptr<Protos::TableIdentifier>> & tables)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetTableInfoReq request;
    Protos::GetTableInfoResp response;

    for (const auto & table : tables)
    {
        DB::Protos::TableIdentifier * table_id = request.add_table_ids();
        table_id->CopyFrom(*table);
    }

    stub->getTableInfo(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.table_infos();
}

CnchTransactionStatus CnchServerClient::getTransactionStatus(const TxnTimestamp & txn_id, const bool need_search_catalog)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetTransactionStatusReq request;
    Protos::GetTransactionStatusResp response;

    request.set_txn_id(txn_id.toUInt64());
    request.set_need_search_catalog(need_search_catalog);

    stub->getTransactionStatus(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.status();
}
void CnchServerClient::removeIntermediateData(const TxnTimestamp & txn_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RollbackTransactionReq request;
    Protos::RollbackTransactionResp response;

    request.set_txn_id(txn_id);
    request.set_only_clean_data(true);
    stub->rollbackTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::controlCnchBGThread(const StorageID & storage_id, CnchBGThreadType type, CnchBGThreadAction action)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ControlCnchBGThreadReq request;
    Protos::ControlCnchBGThreadResp response;
    if (storage_id.empty())
        cntl.set_timeout_ms(360 * 1000);

    RPCHelpers::fillStorageID(storage_id, *request.mutable_storage_id());
    request.set_type(uint32_t(type));
    request.set_action(uint32_t(action));

    stub->controlCnchBGThread(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::cleanTransaction(const TransactionRecord & txn_record)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::CleanTransactionReq request;
    Protos::CleanTransactionResp response;

    LOG_DEBUG(getLogger(__func__), "clean txn: [{}] on server: {}", txn_record.toString(), getRPCAddress());

    request.mutable_txn_record()->CopyFrom(txn_record.pb_model);
    stub->cleanTransaction(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::cleanUndoBuffers(const TransactionRecord & txn_record)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::CleanUndoBuffersReq request;
    Protos::CleanUndoBuffersResp response;

    LOG_DEBUG(getLogger(__func__), "clean undo buffers for txn: [{}] on server: {}", txn_record.toString(), getRPCAddress());

    request.mutable_txn_record()->CopyFrom(txn_record.pb_model);
    stub->cleanUndoBuffers(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::acquireLock(const LockInfoPtr & lock)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::AcquireLockReq request;
    Protos::AcquireLockResp response;
    // TODO: set a big enough waiting time
    cntl.set_timeout_ms(3000 + (10 * lock->timeout));
    cntl.set_max_retry(0);

    fillLockInfoModel(*lock, *request.mutable_lock());
    stub->acquireLock(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
    lock->status = static_cast<LockStatus>(response.lock_status());
}

void CnchServerClient::releaseLock(const LockInfoPtr & lock)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ReleaseLockReq request;
    Protos::ReleaseLockResp response;

    fillLockInfoModel(*lock, *request.mutable_lock());
    stub->releaseLock(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::assertLockAcquired(const LockInfoPtr & lock)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::AssertLockReq request;
    Protos::AssertLockResp response;

    request.set_txn_id(lock->txn_id);
    request.set_lock_id(lock->lock_id);
    stub->assertLockAcquired(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::reportCnchLockHeartBeat(const TxnTimestamp & txn_id, UInt64 expire_time)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ReportCnchLockHeartBeatReq request;
    Protos::ReportCnchLockHeartBeatResp response;

    request.set_txn_id(txn_id);
    request.set_expire_time(expire_time);

    stub->reportCnchLockHeartBeat(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

std::set<UUID> CnchServerClient::getDeletingTablesInGlobalGC()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetDeletingTablesInGlobalGCReq request;
    Protos::GetDeletingTablesInGlobalGCResp response;

    stub->getDeletingTablesInGlobalGC(&cntl, &request, &response, nullptr);

    assertController(cntl);

    std::set<UUID> res;
    for (auto & uuid : response.uuids())
        res.insert(RPCHelpers::createUUID(uuid));
    return res;
}

bool CnchServerClient::removeMergeMutateTasksOnPartitions(const StorageID & storage_id, const std::unordered_set<String> & partitions)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RemoveMergeMutateTasksOnPartitionsReq request;
    RPCHelpers::fillStorageID(storage_id, *request.mutable_storage_id());
    for (const auto & p : partitions)
        request.add_partitions(p);
    Protos::RemoveMergeMutateTasksOnPartitionsResp response;

    stub->removeMergeMutateTasksOnPartitions(&cntl, &request, &response, nullptr);

    assertController(cntl);
    return response.ret();
}

std::optional<TxnTimestamp> CnchServerClient::getMinActiveTimestamp(const StorageID & storage_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetMinActiveTimestampReq request;
    Protos::GetMinActiveTimestampResp response;

    RPCHelpers::fillStorageID(storage_id, *request.mutable_storage_id());

    stub->getMinActiveTimestamp(&cntl, &request, &response, nullptr);
    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.has_timestamp() ? std::optional(response.timestamp()) : std::nullopt;
}

UInt64 CnchServerClient::getServerStartTime()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetServerStartTimeReq request;
    Protos::GetServerStartTimeResp response;

    stub->getServerStartTime(&cntl, &request, &response, nullptr);

    assertController(cntl);
    return response.server_start_time();
}

UInt32 CnchServerClient::getDedupImplVersion(const TxnTimestamp & txn_id, const UUID & uuid)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetDedupImplVersionReq request;
    Protos::GetDedupImplVersionResp response;
    request.set_txn_id(txn_id);
    RPCHelpers::fillUUID(uuid, *request.mutable_uuid());

    stub->getDedupImplVersion(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
    return response.version();
}

bool CnchServerClient::scheduleGlobalGC(const std::vector<Protos::DataModelTable> & tables)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ScheduleGlobalGCReq request;

    for (auto & table : tables)
    {
        DB::Protos::DataModelTable * temp_table = request.add_tables();
        temp_table->CopyFrom(table);
    }
    Protos::ScheduleGlobalGCResp response;

    stub->scheduleGlobalGC(&cntl, &request, &response, nullptr);

    assertController(cntl);
    return response.ret();
}

std::unordered_map<UUID, UInt64> CnchServerClient::queryUdiCounter()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::QueryUdiCounterReq request;
    Protos::QueryUdiCounterResp response;
    stub->queryUdiCounter(&cntl, &request, &response, nullptr);
    RPCHelpers::checkResponse(response);
    assertController(cntl);
    std::unordered_map<UUID, UInt64> result;
    assert(response.tables_size() == response.udi_counts_size());
    for (int i = 0; i < response.tables_size(); ++i)
    {
        auto uuid = RPCHelpers::createUUID(response.tables(i));
        auto count = response.udi_counts(i);
        result[uuid] = count;
    }

    return result;
}

void CnchServerClient::redirectUdiCounter(const std::unordered_map<UUID, UInt64> & data)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RedirectUdiCounterReq request;
    for (auto & [k, v] : data)
    {
        auto uuid_ptr = request.add_tables();
        RPCHelpers::fillUUID(k, *uuid_ptr);
        request.add_udi_counts(v);
    }
    Protos::RedirectUdiCounterResp response;
    stub->redirectUdiCounter(&cntl, &request, &response, nullptr);
    RPCHelpers::checkResponse(response);
    assertController(cntl);
}

void CnchServerClient::scheduleDistributeUdiCount()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ScheduleDistributeUdiCountReq request;
    Protos::ScheduleDistributeUdiCountResp response;

    stub->scheduleDistributeUdiCount(&cntl, &request, &response, nullptr);
    RPCHelpers::checkResponse(response);
    assertController(cntl);
}

void CnchServerClient::scheduleAutoStatsCollect()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ScheduleAutoStatsCollectReq request;
    Protos::ScheduleAutoStatsCollectResp response;

    stub->scheduleAutoStatsCollect(&cntl, &request, &response, nullptr);
    RPCHelpers::checkResponse(response);
    assertController(cntl);
}
void CnchServerClient::redirectAsyncStatsTasks(google::protobuf::RepeatedPtrField<Protos::AutoStats::TaskInfoCore> tasks)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RedirectAsyncStatsTasksReq request;
    *request.mutable_tasks() = std::move(tasks);
    Protos::RedirectAsyncStatsTasksResp response;

    stub->redirectAsyncStatsTasks(&cntl, &request, &response, nullptr);
    RPCHelpers::checkResponse(response);
    assertController(cntl);
}

UInt64 CnchServerClient::getNumOfTablesCanSendForGlobalGC()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetNumOfTablesCanSendForGlobalGCReq request;
    Protos::GetNumOfTablesCanSendForGlobalGCResp response;

    stub->getNumOfTablesCanSendForGlobalGC(&cntl, &request, &response, nullptr);

    assertController(cntl);
    return response.num_of_tables_can_send();
}

google::protobuf::RepeatedPtrField<DB::Protos::BackgroundThreadStatus>
CnchServerClient::getBackGroundStatus(const CnchBGThreadType & type)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::BackgroundThreadStatusReq request;
    Protos::BackgroundThreadStatusResp response;
    request.set_type(type);

    stub->getBackgroundThreadStatus(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.status();
}

brpc::CallId CnchServerClient::submitPreloadTask(const MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & parts, UInt64 timeout_ms)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    auto * cntl = new brpc::Controller();
    auto call_id = cntl->call_id();
    if (parts.empty())
        return call_id;

    Protos::SubmitPreloadTaskReq request;
    request.set_ts(time(nullptr));

    auto response = new Protos::SubmitPreloadTaskResp();
    if (timeout_ms)
        cntl->set_timeout_ms(timeout_ms);

    /// prefer to get cnch table uuid from settings as multiple CloudMergeTrees cannot share a same uuid,
    /// thus most CloudMergeTrees have no uuids on the worker side
    String uuid_str = storage.getSettings()->cnch_table_uuid.value;
    if (uuid_str.empty())
        uuid_str = UUIDHelpers::UUIDToString(storage.getStorageUUID());
    RPCHelpers::fillUUID(UUIDHelpers::toUUID(uuid_str), *request.mutable_uuid());

    for (const auto & part : parts)
    {
        auto new_part = request.add_parts();
        fillPartModel(storage, *part, *new_part);
    }

    stub->submitPreloadTask(cntl, &request, response, brpc::NewCallback(RPCHelpers::onAsyncCallDone, response, cntl, std::make_shared<ExceptionHandler>()));
    return call_id;
}

UInt32 CnchServerClient::reportDeduperHeartbeat(const StorageID & cnch_storage_id, const String & worker_table_name)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ReportDeduperHeartbeatReq request;
    Protos::ReportDeduperHeartbeatResp response;

    RPCHelpers::fillStorageID(cnch_storage_id, *request.mutable_cnch_storage_id());
    request.set_worker_table_name(worker_table_name);

    stub->reportDeduperHeartbeat(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);

    return response.code();
}

void CnchServerClient::executeOptimize(const StorageID & storage_id, const String & partition_id, bool enable_try, bool mutations_sync, UInt64 timeout_ms)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ExecuteOptimizeQueryReq request;
    Protos::ExecuteOptimizeQueryResp response;

    RPCHelpers::fillStorageID(storage_id, *request.mutable_storage_id());
    request.set_partition_id(partition_id);
    request.set_enable_try(enable_try);
    request.set_mutations_sync(mutations_sync);

    if (mutations_sync && timeout_ms)
    {
        cntl.set_timeout_ms(timeout_ms);
        request.set_timeout_ms(timeout_ms);
    }

    stub->executeOptimize(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

brpc::CallId CnchServerClient::submitBackupTask(const String & backup_id, const String & backup_command)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    auto * cntl = new brpc::Controller();
    Protos::SubmitBackupTaskReq request;
    auto * response = new Protos::SubmitBackupTaskResp();

    request.set_id(backup_id);
    request.set_command(backup_command);

    stub->submitBackupTask(cntl, &request, response, brpc::NewCallback(RPCHelpers::onAsyncCallDone, response, cntl, std::make_shared<ExceptionHandler>()));

    return cntl->call_id();
}

std::optional<String> CnchServerClient::getRunningBackupTask()
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::GetRunningBackupTaskReq request;
    Protos::GetRunningBackupTaskResp response;

    stub->getRunningBackupTask(&cntl, &request, &response, nullptr);

    try
    {
        // If rpc error, just return empty string
        assertController(cntl);
        RPCHelpers::checkResponse(response);
    }
    catch (...)
    {
        return std::nullopt;
    }

    return response.has_ret() ? response.ret() : "";
}

void CnchServerClient::removeRunningBackupTask(const String & backup_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::RemoveRunningBackupTaskReq request;
    Protos::RemoveRunningBackupTaskResp response;

    request.set_id(backup_id);

    stub->removeRunningBackupTask(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::notifyAccessEntityChange(IAccessEntity::Type type, const String & name, const UUID & uuid)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::notifyAccessEntityChangeReq request;
    Protos::notifyAccessEntityChangeResp response;

    request.set_type(toString(type));
    request.set_name(name);
    RPCHelpers::fillUUID(uuid, *request.mutable_uuid());
    stub->notifyAccessEntityChange(&cntl, &request, &response, nullptr);
}

#if USE_MYSQL
void CnchServerClient::submitMaterializedMySQLDDLQuery(
    const String & database_name, const String & sync_thread, const String & query, const MySQLBinLogInfo & binlog)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::SubmitMaterializedMySQLDDLQueryReq request;
    Protos::SubmitMaterializedMySQLDDLQueryResp response;

    request.set_database_name(database_name);
    request.set_thread_key(sync_thread);
    request.set_ddl_query(query);

    request.set_binlog_file(binlog.binlog_file);
    request.set_binlog_position(binlog.binlog_position);
    request.set_executed_gtid_set(binlog.executed_gtid_set);
    request.set_meta_version(binlog.meta_version);

    stub->submitMaterializedMySQLDDLQuery(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::reportHeartBeatForSyncThread(const String & database_name, const String & sync_thread)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ReportHeartbeatForSyncThreadReq request;
    Protos::ReportHeartbeatForSyncThreadResp response;

    request.set_database_name(database_name);
    request.set_thread_key(sync_thread);

    stub->reportHeartbeatForSyncThread(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::reportSyncFailedForSyncThread(const String & database_name, const String & sync_thread)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ReportSyncFailedForSyncThreadReq request;
    Protos::ReportSyncFailedForSyncThreadResp response;

    request.set_database_name(database_name);
    request.set_thread_key(sync_thread);

    stub->reportSyncFailedForSyncThread(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}
#endif

void CnchServerClient::handleRefreshTaskOnFinish(StorageID & mv_storage_id, String task_id, Int64 txn_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::handleRefreshTaskOnFinishReq request;
    Protos::handleRefreshTaskOnFinishResp response;

    RPCHelpers::fillStorageID(mv_storage_id, *request.mutable_mv_storage_id());
    request.set_task_id(task_id);
    request.set_txn_id(txn_id);

    stub->handleRefreshTaskOnFinish(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

void CnchServerClient::forceRecalculateMetrics(const StorageID & storage_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::ForceRecalculateMetricsReq request;
    Protos::ForceRecalculateMetricsResp response;

    RPCHelpers::fillStorageID(storage_id, *request.mutable_storage_id());

    stub->forceRecalculateMetrics(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
}

std::vector<Protos::LastModificationTimeHint> CnchServerClient::getLastModificationTimeHints(const StorageID & storage_id)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::getLastModificationTimeHintsReq request;
    Protos::getLastModificationTimeHintsResp response;

    RPCHelpers::fillStorageID(storage_id, *request.mutable_storage_id());

    stub->getLastModificationTimeHints(&cntl, &request, &response, nullptr);

    assertController(cntl);
    std::vector<Protos::LastModificationTimeHint> ret;
    RPCHelpers::checkResponse(response);
    for (const auto & last_modification_time_hint : response.last_modification_time_hints())
    {
        ret.push_back(last_modification_time_hint);
    }

    return ret;
}

void CnchServerClient::notifyTableCreated(const UUID & uuid, const int64_t cnch_notify_table_created_rpc_timeout_ms)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    cntl.set_timeout_ms(cnch_notify_table_created_rpc_timeout_ms);

    Protos::notifyTableCreatedReq req;
    Protos::notifyTableCreatedResp resp;

    RPCHelpers::fillUUID(uuid, *req.mutable_storage_uuid());

    stub->notifyTableCreated(&cntl, &req, &resp, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);
}


MergeTreeDataPartsCNCHVector CnchServerClient::fetchCloudTableMeta(
    const StorageCloudMergeTree & storage, const TxnTimestamp & ts, const std::unordered_set<Int64> & bucket_numbers)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    cntl.set_timeout_ms(storage.getSettings()->cnch_meta_rpc_timeout_ms);
    Protos::FetchCloudTableMetaReq request;
    Protos::FetchCloudTableMetaResp response;

    RPCHelpers::fillStorageID(storage.getCnchStorageID(), *request.mutable_storage_id());

    request.set_timestamp(ts.toUInt64());

    for (const auto & bucket_number : bucket_numbers)
        request.add_bucket_numbers(bucket_number);

    stub->fetchCloudTableMeta(&cntl, &request, &response, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(response);
    return createPartVectorFromModelsForSend<MergeTreeDataPartCNCHPtr>(storage, response.parts());
}

void CnchServerClient::checkDelayInsertOrThrowIfNeeded(UUID storage_uuid)
{
    auto timer = ProfileEventsTimer(ProfileEvents::ServerRpcRequest, ProfileEvents::ServerRpcElaspsedMicroseconds);
    brpc::Controller cntl;
    Protos::checkDelayInsertOrThrowIfNeededReq req;
    Protos::checkDelayInsertOrThrowIfNeededResp resp;

    RPCHelpers::fillUUID(storage_uuid, *req.mutable_storage_uuid());
    stub->checkDelayInsertOrThrowIfNeeded(&cntl, &req, &resp, nullptr);

    assertController(cntl);
    RPCHelpers::checkResponse(resp);
}
}
