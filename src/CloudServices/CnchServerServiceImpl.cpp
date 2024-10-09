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

#include <atomic>
#include <CloudServices/CnchServerServiceImpl.h>

#include <Catalog/Catalog.h>
#include <Catalog/CatalogUtils.h>
#include <CloudServices/CnchDataWriter.h>
#include <CloudServices/CnchMergeMutateThread.h>
#include <CloudServices/DedupWorkerManager.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Protos/RPCHelpers.h>
#include <Transaction/LockManager.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>
#include <Transaction/GlobalTxnCommitter.h>
#include <WorkerTasks/ManipulationType.h>
#include <Common/RWLock.h>
#include <Common/tests/gtest_global_context.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsRpcUtils.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Common/Exception.h>
#include <Parsers/ParserBackupQuery.h>
#include <Parsers/parseQuery.h>
#include <Backups/BackupsWorker.h>
#include <CloudServices/CnchDedupHelper.h>
#include <DataTypes/ObjectUtils.h>
#include <Parsers/ASTSerDerHelper.h>
#include <IO/ReadBufferFromString.h>
#include <Optimizer/SelectQueryInfoHelper.h>
#include <Interpreters/DatabaseCatalog.h>
#include <CloudServices/CnchMergeMutateThread.h>
#include <CloudServices/CnchRefreshMaterializedViewThread.h>
#include <CloudServices/CnchDataWriter.h>
#include <CloudServices/DedupWorkerManager.h>
#include <CloudServices/DedupWorkerStatus.h>
#include <CloudServices/CnchBGThreadsMap.h>
#include <Catalog/CatalogUtils.h>
#include <WorkerTasks/ManipulationType.h>
#include <Storages/Kafka/CnchKafkaConsumeManager.h>
#include <Storages/PartCacheManager.h>
#include <Access/AccessControlManager.h>
#include <Access/KVAccessStorage.h>


#if USE_MYSQL
#include <Storages/StorageMaterializeMySQL.h>
#include <Databases/MySQL/MaterializedMySQLSyncThreadManager.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CNCH_KAFKA_TASK_NEED_STOP;
    extern const int UNKNOWN_TABLE;
    extern const int CNCH_TOPOLOGY_NOT_MATCH_ERROR;
    extern const int TOO_MANY_PARTS;
}
namespace AutoStats = Statistics::AutoStats;

namespace
{
    UInt64 getTS(ContextMutablePtr & context)
    {
        TxnTimestamp ts = context->tryGetTimestamp();
        /// TSO server is unavailable now
        if (ts == TxnTimestamp::fallbackTS())
            ts = TxnTimestamp::fromUnixTimestamp(time(nullptr)).toUInt64();
        return ts;
    }
}

/**
 * @brief Set trace info to current thread.
 *
 * @return A owned `QueryScope` that hold the trace information.
 * Under its lifetime, LOG_XXX contains trace info automatically.
 */
static std::unique_ptr<CurrentThread::QueryScope>
getTraceInfo(const Protos::TraceInfo & trace_info, const ContextPtr ctx, ::google::protobuf::RpcController * c)
{
    /// In order to let log entry contains query_id and txn_id, we need to create query session and bind to current thread.
    auto rpc_context = RPCHelpers::createSessionContextForRPC(ctx, *c);
    if (trace_info.has_query_id())
        rpc_context->setCurrentQueryId(trace_info.query_id());
    if (trace_info.has_txn_id())
        rpc_context->setTemporaryTransaction(trace_info.txn_id(), {}, false); // Skip txn_id check.

    return (trace_info.has_query_id() || trace_info.has_txn_id()) ? std::make_unique<CurrentThread::QueryScope>(rpc_context) : nullptr;
}

CnchServerServiceImpl::CnchServerServiceImpl(ContextMutablePtr global_context)
    : WithMutableContext(global_context),
      server_start_time(getTS(global_context)),
      global_gc_manager(global_context),
      log(getLogger("CnchServerService"))
{
}


void CnchServerServiceImpl::commitParts(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::CommitPartsReq * request,
    [[maybe_unused]] Protos::CommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [c = cntl, req = request, rsp = response, done = done, gc = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                // we need to set the response before any exception is thrown.
                rsp->set_commit_timestamp(0);
                /// Resolve request parameters
                if (req->parts_size() != req->paths_size())
                    throw Exception("Incorrect arguments", ErrorCodes::BAD_ARGUMENTS);

                auto cnch_txn = gc->getCnchTransactionCoordinator().getTransaction(req->txn_id());
                /// Create new rpc context and bind the previous created txn to this rpc context.
                auto rpc_context = RPCHelpers::createSessionContextForRPC(gc, *c);
                rpc_context->setCurrentTransaction(cnch_txn, false);

                auto & database_catalog = DatabaseCatalog::instance();
                StorageID table_id(req->database(), req->table());
                auto storage = database_catalog.tryGetTable(table_id, rpc_context);
                if (!storage)
                    throw Exception("Table " + table_id.getFullTableName() + " not found while committing parts", ErrorCodes::LOGICAL_ERROR);
#if USE_MYSQL
                /// For materialized mysql
                if (auto * proxy = dynamic_cast<StorageMaterializeMySQL *>(storage.get()))
                {
                    auto nested = proxy->getNested();
                    storage.swap(nested);
                }
#endif
                auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage.get());
                if (!cnch)
                    throw Exception("MergeTree is expected, but got " + storage->getName() + " for " + table_id.getFullTableName(), ErrorCodes::BAD_ARGUMENTS);

                auto parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch, req->parts(), &req->paths());
                auto staged_parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch, req->staged_parts(), &req->staged_parts_paths());

                DeleteBitmapMetaPtrVector delete_bitmaps;
                delete_bitmaps.reserve(req->delete_bitmaps_size());
                for (const auto & bitmap_model : req->delete_bitmaps())
                    delete_bitmaps.emplace_back(createFromModel(*cnch, bitmap_model));

                /// check and parse offsets
                String consumer_group;
                cppkafka::TopicPartitionList tpl;
                if (req->has_consumer_group())
                {
                    // check if table schema has changed before writing new parts. need reschedule consume task and update storage schema on work side if so.
                    if (!parts.empty())
                    {
                        auto column_commit_time = storage->getPartColumnsCommitTime(*(parts[0]->getColumnsPtr()));
                        if (column_commit_time != storage->commit_time.toUInt64())
                        {
                            LOG_WARNING(getLogger("CnchServerService"), "Kafka consumer cannot commit parts because of underlying table change. Will reschedule consume task.");
                            throw Exception(ErrorCodes::CNCH_KAFKA_TASK_NEED_STOP, "Commit fails because of storage schema change");
                        }
                    }

                    consumer_group = req->consumer_group();
                    tpl.reserve(req->tpl_size());
                    for (const auto & tp : req->tpl())
                        tpl.emplace_back(cppkafka::TopicPartition(tp.topic(), tp.partition(), tp.offset()));

                    LOG_TRACE(getLogger("CnchServerService"), "parsed tpl to commit with size: {}\n", tpl.size());
                }

                MySQLBinLogInfo binlog;
                if (req->has_binlog())
                {
                    auto & binlog_req = req->binlog();
                    binlog.binlog_file = binlog_req.binlog_file();
                    binlog.binlog_position = binlog_req.binlog_position();
                    binlog.executed_gtid_set = binlog_req.executed_gtid_set();
                    binlog.meta_version = binlog_req.meta_version();
                }

                UInt64 peak_memory_usage = 0;
                if (req->has_peak_memory_usage())
                {
                    peak_memory_usage = req->peak_memory_usage();
                }

                CnchDataWriter cnch_writer(
                    *cnch,
                    rpc_context,
                    static_cast<ManipulationType>(req->type()),
                    req->task_id(),
                    std::move(consumer_group),
                    tpl,
                    binlog,
                    peak_memory_usage);

                auto dedup_mode = static_cast<CnchDedupHelper::DedupMode>(req->dedup_mode());
                cnch_writer.setDedupMode(dedup_mode);

                cnch_writer.commitPreparedCnchParts(
                    DumpedData{std::move(parts), std::move(delete_bitmaps), std::move(staged_parts), dedup_mode});

                // If main table uuid is not set, set it. Otherwise, skip it
                if (cnch_txn->getMainTableUUID() == UUIDHelpers::Nil)
                    cnch_txn->setMainTableUUID(cnch->getCnchStorageUUID());

                rsp->set_dedup_impl_version(cnch_txn->getDedupImplVersion(rpc_context));
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(rsp->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::getMinActiveTimestamp(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::GetMinActiveTimestampReq * request,
    [[maybe_unused]] Protos::GetMinActiveTimestampResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);
            try
            {
                auto & txn_coordinator = global_context->getCnchTransactionCoordinator();
                auto storage_id = RPCHelpers::createStorageID(request->storage_id());
                if (auto timestamp = txn_coordinator.getMinActiveTimestamp(storage_id))
                    response->set_timestamp(*timestamp);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}


void CnchServerServiceImpl::createTransaction(
    google::protobuf::RpcController * cntl,
    const Protos::CreateTransactionReq * request,
    Protos::CreateTransactionResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();
    RPCHelpers::serviceHandler(
        done, response, [cntl = cntl, request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);
        // TODO: Use heartbeat to ensure transaction is still being used
        try
        {
            TxnTimestamp primary_txn_id = request->has_primary_txn_id() ? TxnTimestamp(request->primary_txn_id()) : TxnTimestamp(0);
            bool read_only = request->has_read_only() ? request->read_only() : false;
            CnchTransactionInitiator initiator
                = request->has_primary_txn_id() ? CnchTransactionInitiator::Txn : CnchTransactionInitiator::Worker;
            auto transaction
                = global_context.getCnchTransactionCoordinator().createTransaction(CreateTransactionOption()
                                                                                        .setPrimaryTransactionId(primary_txn_id)
                                                                                        .setType(CnchTransactionType::Implicit)
                                                                                        .setInitiator(initiator)
                                                                                        .setReadOnly(read_only));
            auto & controller = static_cast<brpc::Controller &>(*cntl);
            transaction->setCreator(butil::endpoint2str(controller.remote_side()).c_str());

            response->set_txn_id(transaction->getTransactionID());
            response->set_start_time(transaction->getStartTime());

            LOG_TRACE(log, "Create transaction by request: {}\n", transaction->getTransactionID());
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::finishTransaction(
    google::protobuf::RpcController *  /*cntl*/,
    const Protos::FinishTransactionReq * request,
    Protos::FinishTransactionResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
        brpc::ClosureGuard done_guard(done);

        try
        {
            global_context.getCnchTransactionCoordinator().finishTransaction(request->txn_id(), true);

            LOG_TRACE(log, "Finish transaction by request: {}\n", request->txn_id());
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}


void CnchServerServiceImpl::commitTransaction(
    google::protobuf::RpcController *  /*cntl*/,
    const Protos::CommitTransactionReq * request,
    Protos::CommitTransactionResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();

    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);
            response->set_commit_ts(0);

            try
            {
                /// Check validity of consumer before committing parts & offsets in case a new consumer has been scheduled
                if (request->has_kafka_storage_id())
                {
                    auto storage_id = RPCHelpers::createStorageID(request->kafka_storage_id());
                    auto bgthread = global_context.getCnchBGThread(CnchBGThreadType::Consumer, storage_id);
                    auto *manager = dynamic_cast<CnchKafkaConsumeManager *>(bgthread.get());

                    if (!manager->checkWorkerClient(storage_id.getTableName(), request->kafka_consumer_index()))
                        throw Exception(
                            "check validity of worker client for " + storage_id.getFullTableName() + " failed",
                            ErrorCodes::CNCH_KAFKA_TASK_NEED_STOP);
                    LOG_TRACE(
                        log,
                        "Check consumer {} OK. Now commit parts and offsets for Kafka transaction\n",storage_id.getFullTableName());
                }

                auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
                auto txn_id = request->txn_id();
                auto txn = txn_coordinator.getTransaction(txn_id);

                // if (request->has_insertion_label())
                // {
                //     if (UUIDHelpers::Nil == txn->getMainTableUUID())
                //         throw Exception("Main table is not set when using insertion label", ErrorCodes::LOGICAL_ERROR);

                //     txn->setInsertionLabel(std::make_shared<InsertionLabel>(
                //         txn->getMainTableUUID(), request->insertion_label(), txn->getTransactionID().toUInt64()));
                // }

                auto commit_ts = txn->commit();
                response->set_commit_ts(commit_ts.toUInt64());

                LOG_TRACE(log, "Committed transaction from worker side: {}\n", request->txn_id());
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        });
}

void CnchServerServiceImpl::precommitTransaction(
    google::protobuf::RpcController * /*cntl*/,
    const Protos::PrecommitTransactionReq * request,
    Protos::PrecommitTransactionResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();

    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);
        try
        {
            auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
            auto txn = txn_coordinator.getTransaction(request->txn_id());
            auto main_table_uuid = RPCHelpers::createUUID(request->main_table_uuid());
            // If main table uuid is not set, set it. Otherwise, skip it
            if (txn->getMainTableUUID() == UUIDHelpers::Nil)
                txn->setMainTableUUID(main_table_uuid);
            txn->precommit();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::redirectCommitTransaction(
    google::protobuf::RpcController * /*cntl*/,
    const Protos::RedirectCommitTransactionReq * request,
    Protos::RedirectCommitTransactionResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);
        try
        {
            const Protos::TransactionMetadata & transaction_model = request->txn_meta();
            TransactionRecord record{transaction_model.txn_record()};

            CnchServerTransactionPtr txn = std::make_shared<CnchServerTransaction>(global_context, record);
            txn->setCommitTs(TxnTimestamp{transaction_model.commit_time()});
            txn->setMainTableUUID(RPCHelpers::createUUID(transaction_model.main_table()));
            if (transaction_model.has_consumer_group())
            {
                cppkafka::TopicPartitionList tpl;
                RPCHelpers::createKafkaTPL(tpl, transaction_model.tpl());
                txn->setKafkaTpl(transaction_model.consumer_group(), tpl);
            }

            global_context->getGlobalTxnCommitter()->commit(txn);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::rollbackTransaction(
    google::protobuf::RpcController * /*cntl*/,
    const Protos::RollbackTransactionReq * request,
    Protos::RollbackTransactionResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);
        try
        {
            auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
            auto txn = txn_coordinator.getTransaction(request->txn_id());
            if (request->has_only_clean_data() && request->only_clean_data())
                txn->removeIntermediateData();
            else
                response->set_commit_ts(txn->rollback());
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::createTransactionForKafka(
    google::protobuf::RpcController * cntl,
    const Protos::CreateKafkaTransactionReq * request,
    Protos::CreateKafkaTransactionResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();
    RPCHelpers::serviceHandler(
        done, response, [cntl = cntl, request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);

        try
        {
            auto uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->uuid()));
            auto storage = global_context.getCnchCatalog()->getTableByUUID(global_context, uuid, TxnTimestamp::maxTS());
            auto bgthread = global_context.getCnchBGThread(CnchBGThreadType::Consumer, storage->getStorageID());
            auto * manager = dynamic_cast<CnchKafkaConsumeManager *>(bgthread.get());

            if (!manager->checkWorkerClient(request->table_name(), request->consumer_index()))
                throw Exception("check validity of worker client for " + request->table_name() + " failed", ErrorCodes::LOGICAL_ERROR);

            auto transaction = global_context.getCnchTransactionCoordinator().createTransaction(
                CreateTransactionOption().setInitiator(CnchTransactionInitiator::Kafka));
            auto & controller = static_cast<brpc::Controller &>(*cntl);
            transaction->setCreator(butil::endpoint2str(controller.remote_side()).c_str());

            /// Store active transactions in catalog in case of consumer restarting
            manager->setCurrentTransactionForConsumer(request->consumer_index(), transaction->getTransactionID());

            response->set_txn_id(transaction->getTransactionID());
            response->set_start_time(transaction->getStartTime());
            LOG_TRACE(log, "Create transaction by request: {}\n", transaction->getTransactionID().toUInt64());
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }

    });
}

void CnchServerServiceImpl::getTransactionStatus(
    ::google::protobuf::RpcController * /*cntl*/,
    const ::DB::Protos::GetTransactionStatusReq * request,
    ::DB::Protos::GetTransactionStatusResp * response,
    ::google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, &global_context = *context_ptr, log = log]
        {
            brpc::ClosureGuard done_guard(done);
            try
            {
                TxnTimestamp txn_id{request->txn_id()};
                auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
                CnchTransactionStatus status = txn_coordinator.getTransactionStatus(txn_id);

                if (status == CnchTransactionStatus::Inactive && request->need_search_catalog())
                {
                    auto txn_record = global_context.getCnchCatalog()->tryGetTransactionRecord(txn_id);
                    if (txn_record)
                        status = txn_record->status();
                }

                switch (status)
                {
                    case CnchTransactionStatus::Running:
                        response->set_status(Protos::TransactionStatus::Running);
                        break;
                    case CnchTransactionStatus::Finished:
                        response->set_status(Protos::TransactionStatus::Finished);
                        break;
                    case CnchTransactionStatus::Aborted:
                        response->set_status(Protos::TransactionStatus::Aborted);
                        break;
                    case CnchTransactionStatus::Inactive:
                        response->set_status(Protos::TransactionStatus::Inactive);
                        break;

                    default:
                        throw Exception("Unknown transaction status", ErrorCodes::NOT_IMPLEMENTED);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-parameter"
#else
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
void CnchServerServiceImpl::checkConsumerValidity(
    google::protobuf::RpcController * cntl,
    const Protos::CheckConsumerValidityReq * request,
    Protos::CheckConsumerValidityResp * response,
    google::protobuf::Closure * done)
{

}
void CnchServerServiceImpl::reportTaskHeartbeat(
    google::protobuf::RpcController * cntl,
    const Protos::ReportTaskHeartbeatReq * request,
    Protos::ReportTaskHeartbeatResp * response,
    google::protobuf::Closure * done)
{
}

void CnchServerServiceImpl::reportDeduperHeartbeat(
    google::protobuf::RpcController *,
    const Protos::ReportDeduperHeartbeatReq * request,
    Protos::ReportDeduperHeartbeatResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            auto cnch_storage_id = RPCHelpers::createStorageID(request->cnch_storage_id());

            if (auto bg_thread = gc->tryGetDedupWorkerManager(cnch_storage_id))
            {
                const auto & worker_table_name = request->worker_table_name();
                auto & manager = static_cast<DedupWorkerManager &>(*bg_thread);

                auto ret = manager.reportHeartbeat(worker_table_name);

                // NOTE: here we send a response back to let the worker know the result.
                response->set_code(static_cast<UInt32>(ret));
                return;
            }
            else
            {
                LOG_WARNING(log, "Failed to get background thread");
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
        response->set_code(static_cast<UInt32>(DedupWorkerHeartbeatResult::Kill));
    });
}

void CnchServerServiceImpl::fetchDataParts(
    ::google::protobuf::RpcController * c,
    const ::DB::Protos::FetchDataPartsReq * request,
    ::DB::Protos::FetchDataPartsResp * response,
    ::google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [c, request, response, done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);

        /// Init trace info.
        std::unique_ptr<CurrentThread::QueryScope> query_scope
            = request->has_trace_info() ? getTraceInfo(request->trace_info(), gc, c) : nullptr;

        try
        {
            StoragePtr storage
                = gc->getCnchCatalog()->getTable(*gc, request->database(), request->table(), TxnTimestamp{request->table_commit_time()});

            auto calculated_host
                = gc->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true).getRPCAddress();
            if (request->remote_host() != calculated_host)
                throw Exception(
                    "Fetch parts failed because of inconsistent view of topology in remote server, remote_host: " + request->remote_host()
                        + ", calculated_host: " + calculated_host,
                    ErrorCodes::LOGICAL_ERROR);

            if (!isLocalServer(calculated_host, std::to_string(gc->getRPCPort())))
                throw Exception(
                    "Fetch parts failed because calculated host (" + calculated_host + ") is not remote server.",
                    ErrorCodes::LOGICAL_ERROR);

            Strings partition_list;
            for (const auto & partition : request->partitions())
                partition_list.emplace_back(partition);

            std::set<Int64> bucket_numbers;
            for (const auto & bucket_number : request->bucket_numbers())
                bucket_numbers.insert(bucket_number);

            auto parts = gc->getCnchCatalog()->getServerDataPartsInPartitions(
                storage,
                partition_list,
                TxnTimestamp{request->timestamp()},
                nullptr,
                /*visibility=*/Catalog::VisibilityLevel::All,
                bucket_numbers);

            auto & mutable_parts = *response->mutable_parts();
            for (const auto & part : parts)
                *mutable_parts.Add() = part->part_model();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::fetchDeleteBitmaps(
    ::google::protobuf::RpcController * c,
    const ::DB::Protos::FetchDeleteBitmapsReq * request,
    ::DB::Protos::FetchDeleteBitmapsResp * response,
    ::google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [c,request, response, done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);

        /// Init trace info.
        std::unique_ptr<CurrentThread::QueryScope> query_scope
            = request->has_trace_info() ? getTraceInfo(request->trace_info(), gc, c) : nullptr;

        try
        {
            StoragePtr storage
                = gc->getCnchCatalog()->getTable(*gc, request->database(), request->table(), TxnTimestamp{request->table_commit_time()});

            auto calculated_host
                = gc->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true).getRPCAddress();
            if (request->remote_host() != calculated_host)
                throw Exception(
                    "Fetch parts failed because of inconsistent view of topology in remote server, remote_host: " + request->remote_host()
                        + ", calculated_host: " + calculated_host,
                    ErrorCodes::LOGICAL_ERROR);

            if (!isLocalServer(calculated_host, std::to_string(gc->getRPCPort())))
                throw Exception(
                    "Fetch parts failed because calculated host (" + calculated_host + ") is not remote server.",
                    ErrorCodes::LOGICAL_ERROR);

            Strings partition_list;
            for (const auto & partition : request->partitions())
                partition_list.emplace_back(partition);

            std::set<Int64> bucket_numbers;
            for (const auto & bucket_number : request->bucket_numbers())
                bucket_numbers.insert(bucket_number);
            auto bitmaps = gc->getCnchCatalog()->getDeleteBitmapsInPartitions(
                storage, partition_list, TxnTimestamp{request->timestamp()}, nullptr, Catalog::VisibilityLevel::All, bucket_numbers);
            auto & mutable_parts = *response->mutable_delete_bitmaps();
            for (const auto & bitmap : bitmaps)
                *mutable_parts.Add() = *bitmap->getModel();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::fetchPartitions(
    [[maybe_unused]] ::google::protobuf::RpcController* cntl,
    [[maybe_unused]] const ::DB::Protos::FetchPartitionsReq* request,
    [[maybe_unused]] ::DB::Protos::FetchPartitionsResp* response,
    [[maybe_unused]] ::google::protobuf::Closure* done)
{
    RPCHelpers::serviceHandler(
        done, response, [c = cntl, request, response, done, gc = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            /// Init trace info.
            std::unique_ptr<CurrentThread::QueryScope> query_scope
                = request->has_trace_info() ? getTraceInfo(request->trace_info(), gc, c) : nullptr;

            try
            {
                StoragePtr storage = gc->getCnchCatalog()->getTable(*gc, request->database(), request->table(), TxnTimestamp::maxTS());

                auto calculated_host
                    = gc->getCnchTopologyMaster()
                          ->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true)
                          .getRPCAddress();

                if (request->remote_host() != calculated_host)
                    throw Exception(
                        "Fetch partitions failed because of inconsistent view of topology in remote server, remote_host: "
                            + request->remote_host() + ", calculated_host: " + calculated_host,
                        ErrorCodes::LOGICAL_ERROR);

                Names column_names;
                for (const auto & name : request->column_name_filter())
                    column_names.push_back(name);
                auto session_context = Context::createCopy(gc);
                session_context->setCurrentDatabase(request->database());
                ReadBufferFromString rb(request->predicate());
                ASTPtr query_ptr = deserializeAST(rb);
                /// We should to add `database` into AST before calling `buildSelectQueryInfoForQuery`.
                {
                    ASTSelectQuery * select_query = query_ptr->as<ASTSelectQuery>();
                    if (!select_query)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected AST type found in buildSelectQueryInfoForQuery");
                    select_query->replaceDatabaseAndTable(request->database(), request->table());
                }
                SelectQueryInfo query_info = buildSelectQueryInfoForQuery(query_ptr, session_context);

                session_context->setTemporaryTransaction(
                    TxnTimestamp(request->has_txnid() ? request->txnid() : session_context->getTimestamp()), 0, false);
                auto required_partitions = gc->getCnchCatalog()->getPartitionsByPredicate(
                    session_context, storage, query_info, column_names, request->has_ignore_ttl() && request->ignore_ttl());

                response->set_total_size(required_partitions.total_partition_number);
                auto & mutable_partitions = *response->mutable_partitions();
                for (auto & partition : required_partitions.partitions)
                    *mutable_partitions.Add() = std::move(partition);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        });
}

void CnchServerServiceImpl::fetchUniqueTableMeta(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::FetchUniqueTableMetaReq * request,
    ::DB::Protos::FetchUniqueTableMetaResp * response,
    ::google::protobuf::Closure * done)
{
}

void CnchServerServiceImpl::getBackgroundThreadStatus(
    google::protobuf::RpcController *,
    const Protos::BackgroundThreadStatusReq * request,
    Protos::BackgroundThreadStatusResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                auto type = toServerBGThreadType(request->type());
                std::map<StorageID, CnchBGThreadStatus> res = global_context->getCnchBGThreadsMap(type)->getStatusMap();

                for (const auto & [storage_id, status] : res)
                {
                    auto * item = response->mutable_status()->Add();
                    RPCHelpers::fillStorageID(storage_id, *(item->mutable_storage_id()));
                    item->set_status(status);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::getNumBackgroundThreads(
    google::protobuf::RpcController * cntl,
    const Protos::BackgroundThreadNumReq * request,
    Protos::BackgroundThreadNumResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::controlCnchBGThread(
    google::protobuf::RpcController * cntl,
    const Protos::ControlCnchBGThreadReq * request,
    Protos::ControlCnchBGThreadResp * response,
    google::protobuf::Closure * done)
{
    ContextPtr context_ptr = getContext();
    RPCHelpers::serviceHandler(
        done,
        response,
        [cntl = cntl, request = request, response = response, done = done, & global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                StorageID storage_id = StorageID::createEmpty();
                if (!request->storage_id().table().empty())
                    storage_id = RPCHelpers::createStorageID(request->storage_id());

                auto type = toServerBGThreadType(request->type());
                auto action = toCnchBGThreadAction(request->action());
                auto & controller = static_cast<brpc::Controller &>(*cntl);
                LOG_DEBUG(log, "Received controlBGThread for {} type {} action {} from {}",
                    storage_id.empty() ? "empty storage" : storage_id.getNameForLogs(),
                    toString(type), toString(action), butil::endpoint2str(controller.remote_side()).c_str());
                global_context.controlCnchBGThread(storage_id, type, action);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );

}
void CnchServerServiceImpl::getTablePartitionInfo(
    google::protobuf::RpcController * cntl,
    const Protos::GetTablePartitionInfoReq * request,
    Protos::GetTablePartitionInfoResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::getTableInfo(
    google::protobuf::RpcController * cntl,
    const Protos::GetTableInfoReq * request,
    Protos::GetTableInfoResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, gc = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                auto part_cache_manager = gc->getPartCacheManager();
                if (!part_cache_manager)
                    return;
                for (auto & table_id : request->table_ids())
                {
                    UUID uuid(stringToUUID(table_id.uuid()));
                    Protos::DataModelTableInfo * table_info = response->add_table_infos();
                    table_info->set_database(table_id.database());
                    table_info->set_table(table_id.name());
                    table_info->set_last_modification_time(part_cache_manager->getTableLastUpdateTime(uuid));
                    table_info->set_cluster_status(part_cache_manager->getTableClusterStatus(uuid));
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::invalidateBytepond(
    google::protobuf::RpcController * cntl,
    const Protos::InvalidateBytepondReq * request,
    Protos::InvalidateBytepondResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::commitWorkerRPCByKey(
    google::protobuf::RpcController * cntl,
    const Protos::CommitWorkerRPCByKeyReq * request,
    Protos::CommitWorkerRPCByKeyResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::cleanTransaction(
    google::protobuf::RpcController * cntl,
    const Protos::CleanTransactionReq * request,
    Protos::CleanTransactionResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, gc = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            auto & txn_cleaner = gc->getCnchTransactionCoordinator().getTxnCleaner();
            TransactionRecord txn_record{request->txn_record()};

            try
            {
                txn_cleaner.cleanTransaction(txn_record);
            }
            catch (...)
            {
                LOG_WARNING(log, "Clean txn record {} failed.", txn_record.toString());
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}
void CnchServerServiceImpl::cleanUndoBuffers(
    google::protobuf::RpcController *,
    const Protos::CleanUndoBuffersReq * request,
    Protos::CleanUndoBuffersResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request, response, done, gc = getContext(), this] {
        brpc::ClosureGuard done_guard(done);

        auto & txn_cleaner = gc->getCnchTransactionCoordinator().getTxnCleaner();
        TransactionRecord txn_record{request->txn_record()};

        try
        {
            txn_cleaner.cleanUndoBuffers(txn_record );
        }
        catch (...)
        {
            LOG_WARNING(log, "Clean txn record {} failed.", txn_record.toString());
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}
void CnchServerServiceImpl::acquireLock(
    google::protobuf::RpcController * cntl,
    const Protos::AcquireLockReq * request,
    Protos::AcquireLockResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [req = request, resp = response, d = done, gc = getContext(), logger = log] {
        brpc::ClosureGuard done_guard(d);
        try
        {
            LockInfoPtr info = createLockInfoFromModel(req->lock());
            LockManager::instance().lock(info, *gc);
            resp->set_lock_status(to_underlying(info->status));
        }
        catch (...)
        {
            tryLogCurrentException(logger, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(resp->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::releaseLock(
    google::protobuf::RpcController * cntl,
    const Protos::ReleaseLockReq * request,
    Protos::ReleaseLockResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [req = request, resp = response, d = done, logger = log] {
        brpc::ClosureGuard done_guard(d);

        try
        {
            LockInfoPtr info = createLockInfoFromModel(req->lock());
            LockManager::instance().unlock(info);
        }
        catch (...)
        {
            tryLogCurrentException(logger, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(resp->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::assertLockAcquired(
    google::protobuf::RpcController *,
    const Protos::AssertLockReq * request,
    Protos::AssertLockResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [req = request, resp = response, d = done, logger = log] {
        brpc::ClosureGuard done_guard(d);

        try
        {
            LockManager::instance().assertLockAcquired(req->txn_id(), req->lock_id());
        }
        catch (...)
        {
            tryLogCurrentException(logger, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(resp->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::reportCnchLockHeartBeat(
    google::protobuf::RpcController * cntl,
    const Protos::ReportCnchLockHeartBeatReq * request,
    Protos::ReportCnchLockHeartBeatResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [req = request, resp = response, d = done, logger = log]
    {
        brpc::ClosureGuard done_guard(d);

        try
        {
            auto tp = LockManager::Clock::now() + std::chrono::milliseconds(req->expire_time());
            LockManager::instance().updateExpireTime(req->txn_id(), tp);
        }
        catch (...)
        {
            tryLogCurrentException(logger, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(resp->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::getServerStartTime(
    google::protobuf::RpcController *,
    const Protos::GetServerStartTimeReq *,
    Protos::GetServerStartTimeResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    response->set_server_start_time(server_start_time);
}

void CnchServerServiceImpl::getDedupImplVersion(
    google::protobuf::RpcController *,
    const Protos::GetDedupImplVersionReq * request,
    Protos::GetDedupImplVersionResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            auto cnch_txn = gc->getCnchTransactionCoordinator().getTransaction(request->txn_id());
            if (cnch_txn->getMainTableUUID() == UUIDHelpers::Nil)
                cnch_txn->setMainTableUUID(RPCHelpers::createUUID(request->uuid()));
            response->set_version(cnch_txn->getDedupImplVersion(gc));
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
            response->set_version(1);
        }
    });
}

// About Auto Statistics
void CnchServerServiceImpl::queryUdiCounter(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    const Protos::QueryUdiCounterReq* request,
    Protos::QueryUdiCounterResp* response,
    google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        Statistics::AutoStats::queryUdiCounter(getContext(), request, response);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchServerServiceImpl::redirectUdiCounter(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    const Protos::RedirectUdiCounterReq* request,
    Protos::RedirectUdiCounterResp* response,
    google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        Statistics::AutoStats::redirectUdiCounter(getContext(), request, response);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

// About Auto Statistics
void CnchServerServiceImpl::scheduleDistributeUdiCount(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    const Protos::ScheduleDistributeUdiCountReq* request,
    Protos::ScheduleDistributeUdiCountResp* response,
    google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        (void)request;
        if (auto * auto_stats_manager = getContext()->getAutoStatisticsManager())
            auto_stats_manager->scheduleDistributeUdiCount();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

// About Auto Statistics
void CnchServerServiceImpl::scheduleAutoStatsCollect(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    const Protos::ScheduleAutoStatsCollectReq* request,
    Protos::ScheduleAutoStatsCollectResp* response,
    google::protobuf::Closure* done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        (void)request;
        auto context = getContext();
        if (auto * auto_stats_manager = context->getAutoStatisticsManager())
            auto_stats_manager->scheduleCollect();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchServerServiceImpl::redirectAsyncStatsTasks(
    [[maybe_unused]] google::protobuf::RpcController * controller,
    const Protos::RedirectAsyncStatsTasksReq * request,
    Protos::RedirectAsyncStatsTasksResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);
            try
            {
                Statistics::AutoStats::redirectAsyncStatsTasks(global_context, request, response);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        });
}

void CnchServerServiceImpl::scheduleGlobalGC(
    google::protobuf::RpcController * cntl,
    const Protos::ScheduleGlobalGCReq * request,
    Protos::ScheduleGlobalGCResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, & global_gc_manager = this->global_gc_manager, log = log] {
            brpc::ClosureGuard done_guard(done);

            std::vector<Protos::DataModelTable> tables (request->tables().begin(), request->tables().end());
            LOG_DEBUG(log, "Receive {} tables from DM, they are", tables.size());

            for (size_t i = 0; i < tables.size(); ++i)
            {
                LOG_DEBUG(log, "table {} : {}.{}", i + 1, tables[i].database(), tables[i].name());
            }

            try
            {
                bool ret = global_gc_manager->schedule(std::move(tables));
                response->set_ret(ret);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                response->set_ret(false);
            }
        }
    );
}

void CnchServerServiceImpl::getNumOfTablesCanSendForGlobalGC(
    google::protobuf::RpcController * cntl,
    const Protos::GetNumOfTablesCanSendForGlobalGCReq * request,
    Protos::GetNumOfTablesCanSendForGlobalGCResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        response->set_num_of_tables_can_send(GlobalGCHelpers::amountOfWorkCanReceive(global_gc_manager->getMaxThreads(), global_gc_manager->getNumberOfDeletingTables()));
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        response->set_num_of_tables_can_send(0);
    }
}

void CnchServerServiceImpl::getDeletingTablesInGlobalGC(
    google::protobuf::RpcController * cntl,
    const Protos::GetDeletingTablesInGlobalGCReq * request,
    Protos::GetDeletingTablesInGlobalGCResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        auto uuids = global_gc_manager->getDeletingUUIDs();
        for (const auto & uuid : uuids)
        {
            auto * item = response->mutable_uuids()->Add();
            RPCHelpers::fillUUID(uuid, *item);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void CnchServerServiceImpl::handleRedirectCommitRequest(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    [[maybe_unused]] const Protos::RedirectCommitPartsReq * request,
    [[maybe_unused]] Protos::RedirectCommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done,
    bool final_commit)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, final_commit=final_commit, global_context = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            String table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->uuid()));
            StoragePtr storage = global_context->getCnchCatalog()->tryGetTableByUUID(
                *global_context, table_uuid, TxnTimestamp::maxTS());

            if (!storage)
                throw Exception("Table with uuid " + table_uuid + " not found.", ErrorCodes::UNKNOWN_TABLE);

            auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage.get());
            if (!cnch)
                throw Exception("Table is not of MergeTree class", ErrorCodes::BAD_ARGUMENTS);

            auto parts = createPartVectorFromModels<MergeTreeDataPartCNCHPtr>(*cnch, request->parts(), nullptr);
            auto staged_parts = createPartVectorFromModels<MergeTreeDataPartCNCHPtr>(*cnch, request->staged_parts(), nullptr);
            DeleteBitmapMetaPtrVector delete_bitmaps;
            delete_bitmaps.reserve(request->delete_bitmaps_size());
            for (auto & bitmap_model : request->delete_bitmaps())
                delete_bitmaps.emplace_back(createFromModel(*cnch, bitmap_model));


            if (!final_commit)
            {
                TxnTimestamp txnID{request->txn_id()};
                bool write_manifest = cnch->getSettings()->enable_publish_version_on_commit;
                global_context->getCnchCatalog()->writeParts(storage, txnID,
                    Catalog::CommitItems{parts, delete_bitmaps, staged_parts}, request->from_merge_task(), request->preallocate_mode(), write_manifest);
            }
            else
            {
                TxnTimestamp commitTs {request->commit_ts()};
                global_context->getCnchCatalog()->setCommitTime(storage, Catalog::CommitItems{parts, delete_bitmaps, staged_parts},
                    commitTs, request->txn_id());
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::redirectCommitParts(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    [[maybe_unused]] const Protos::RedirectCommitPartsReq * request,
    [[maybe_unused]] Protos::RedirectCommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    handleRedirectCommitRequest(controller, request, response, done, false);
}

void CnchServerServiceImpl::redirectClearParts(
    [[maybe_unused]] google::protobuf::RpcController * controller,
    const Protos::RedirectClearPartsReq * request,
    Protos::RedirectClearPartsResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);
            try
            {
                String table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->uuid()));
                StoragePtr storage
                    = global_context->getCnchCatalog()->tryGetTableByUUID(*global_context, table_uuid, TxnTimestamp::maxTS());

                if (!storage)
                    throw Exception("Table with uuid " + table_uuid + " not found.", ErrorCodes::UNKNOWN_TABLE);

                auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage.get());
                if (!cnch)
                    throw Exception("Table is not of MergeTree class", ErrorCodes::BAD_ARGUMENTS);

                auto parts = createPartVectorFromModels<MergeTreeDataPartCNCHPtr>(*cnch, request->parts(), nullptr);
                auto staged_parts = createPartVectorFromModels<MergeTreeDataPartCNCHPtr>(*cnch, request->staged_parts(), nullptr);
                DeleteBitmapMetaPtrVector delete_bitmaps;
                delete_bitmaps.reserve(request->delete_bitmaps_size());
                for (const auto & bitmap_model : request->delete_bitmaps())
                    delete_bitmaps.emplace_back(createFromModel(*cnch, bitmap_model));
                global_context->getCnchCatalog()->clearParts(storage, Catalog::CommitItems{parts, delete_bitmaps, staged_parts});
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        });
}

void CnchServerServiceImpl::redirectSetCommitTime(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    [[maybe_unused]] const Protos::RedirectCommitPartsReq * request,
    [[maybe_unused]] Protos::RedirectCommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    handleRedirectCommitRequest(controller, request, response, done, true);
}

void CnchServerServiceImpl::redirectAttachDetachedS3Parts(
        [[maybe_unused]] google::protobuf::RpcController* controller,
        const Protos::RedirectAttachDetachedS3PartsReq * request,
        Protos::RedirectAttachDetachedS3PartsResp * response,
        google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, global_context = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            auto throw_if_non_host = [&] (const StoragePtr & storage)
            {
                auto target_server = global_context->getCnchTopologyMaster()->
                        getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
                if (target_server.empty() || !isLocalServer(target_server.getRPCAddress(), std::to_string(global_context->getRPCPort())))
                    throw Exception("Redirect detach/attach parts failed because choose wrong host server.", ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
            };

            String from_table_uuid = request->has_from_table_uuid() ?
                UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->from_table_uuid())) : "";
            String to_table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->to_table_uuid()));

            StoragePtr from_table, to_table;
            if (request->has_from_table_uuid())
            {
                from_table = global_context->getCnchCatalog()->tryGetTableByUUID(*global_context, from_table_uuid, TxnTimestamp::maxTS());
            }
            to_table = global_context->getCnchCatalog()->tryGetTableByUUID(*global_context, to_table_uuid, TxnTimestamp::maxTS());

            Strings detached_part_names, detached_bitmap_names;
            for (const auto & name : request->detached_part_names())
                detached_part_names.push_back(name);
            for (const auto & name : request->detached_bitmap_names())
                detached_bitmap_names.push_back(name);

            switch (request->type())
            {
                case Protos::DetachAttachType::ATTACH_DETACHED_PARTS:
                {
                    if (!to_table || !from_table)
                        throw Exception("Cannot get table by UUID " + to_table_uuid + " or " + from_table_uuid, ErrorCodes::UNKNOWN_TABLE);
                    auto * to_cnch = dynamic_cast<MergeTreeMetaBase *>(to_table.get());
                    if (!to_cnch)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table " + to_table->getStorageID().getNameForLogs() + " is not of MergeTree class");
                    auto * from_cnch = dynamic_cast<MergeTreeMetaBase *>(from_table.get());
                    if (!from_cnch)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table " + from_table->getStorageID().getNameForLogs() + "is not of MergeTree class");

                    throw_if_non_host(to_table);
                    IMergeTreeDataPartsVector commit_parts, commit_staged_parts;
                    commit_parts = createPartVectorFromModels<IMergeTreeDataPartPtr>(*to_cnch, request->commit_parts(), nullptr);
                    commit_staged_parts = createPartVectorFromModels<IMergeTreeDataPartPtr>(*to_cnch, request->commit_staged_parts(), nullptr);
                    DeleteBitmapMetaPtrVector detached_bitmaps, bitmaps;
                    for (auto & bitmap_model : request->detached_bitmaps())
                        detached_bitmaps.emplace_back(createFromModel(*from_cnch, bitmap_model));
                    for (auto & bitmap_model : request->bitmaps())
                        bitmaps.emplace_back(createFromModel(*to_cnch, bitmap_model));

                    global_context->getCnchCatalog()->attachDetachedParts(
                        from_table,
                        to_table,
                        detached_part_names,
                        commit_parts,
                        commit_staged_parts,
                        detached_bitmaps,
                        bitmaps,
                        request->has_txn_id() ? request->txn_id() : 0);
                    break;
                }
                case Protos::DetachAttachType::ATTACH_DETACHED_RAW:
                {
                    if (!to_table)
                        throw Exception("Cannot get table by UUID " + to_table_uuid, ErrorCodes::UNKNOWN_TABLE);
                    throw_if_non_host(to_table);
                    size_t detached_visible_part_size = request->detached_visible_part_size();
                    size_t detached_staged_part_size = request->detached_staged_part_size();
                    global_context->getCnchCatalog()->attachDetachedPartsRaw(
                        to_table, detached_part_names, detached_visible_part_size, detached_staged_part_size, detached_bitmap_names);
                    break;
                }
                default:
                    throw Exception("Unknown RedirectAttachDetachedS3PartsReq status", ErrorCodes::NOT_IMPLEMENTED);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::redirectDetachAttachedS3Parts(
        [[maybe_unused]] google::protobuf::RpcController* controller,
        const Protos::RedirectDetachAttachedS3PartsReq * request,
        Protos::RedirectDetachAttachedS3PartsResp * response,
        google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, global_context = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            auto throw_if_non_host = [&] (const StoragePtr & storage)
            {
                auto target_server = global_context->getCnchTopologyMaster()->
                        getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
                if (target_server.empty() || !isLocalServer(target_server.getRPCAddress(), std::to_string(global_context->getRPCPort())))
                    throw Exception("Redirect detach/attach parts failed because choose wrong host server.", ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
            };

            String from_table_uuid = request->has_from_table_uuid() ?
                UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->from_table_uuid())) : "";
            String to_table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->to_table_uuid()));

            StoragePtr from_table, to_table;
            if (request->has_from_table_uuid())
            {
                from_table = global_context->getCnchCatalog()->tryGetTableByUUID(*global_context, from_table_uuid, TxnTimestamp::maxTS());
            }
            to_table = global_context->getCnchCatalog()->tryGetTableByUUID(*global_context, to_table_uuid, TxnTimestamp::maxTS());

            switch (request->type())
            {
                case Protos::DetachAttachType::DETACH_ATTACHED_PARTS:
                {
                    if (!to_table || !from_table)
                        throw Exception("Cannot get table by UUID " + to_table_uuid + " or " + from_table_uuid, ErrorCodes::UNKNOWN_TABLE);
                    auto * to_cnch = dynamic_cast<MergeTreeMetaBase *>(to_table.get());
                    if (!to_cnch)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table " + to_table->getStorageID().getNameForLogs() + " is not of MergeTree class");
                    auto * from_cnch = dynamic_cast<MergeTreeMetaBase *>(from_table.get());
                    if (!from_cnch)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table " + from_table->getStorageID().getNameForLogs() + "is not of MergeTree class");
                    throw_if_non_host(from_table);

                    IMergeTreeDataPartsVector attached_parts, attached_staged_parts, commit_parts;
                    const pb::RepeatedPtrField<Protos::DataModelPart> & part_models = request->commit_parts();
                    commit_parts.reserve(part_models.size());
                    for (int i = 0; i < part_models.size(); ++i)
                    {
                        // reconstruct commit parts and may insert nullptr if part model is empty
                        if (part_models[i].has_txnid())
                            commit_parts.push_back(createPartFromModel(*to_cnch, part_models[i], std::nullopt));
                        else
                            commit_parts.push_back(nullptr);
                    }
                    attached_parts = createPartVectorFromModels<IMergeTreeDataPartPtr>(*from_cnch, request->attached_parts(), nullptr);
                    attached_staged_parts = createPartVectorFromModels<IMergeTreeDataPartPtr>(*from_cnch, request->attached_staged_parts(), nullptr);
                    DeleteBitmapMetaPtrVector attached_bitmaps, bitmaps;
                    for (auto & bitmap_model : request->attached_bitmaps())
                        attached_bitmaps.emplace_back(createFromModel(*from_cnch, bitmap_model));
                    for (auto & bitmap_model : request->bitmaps())
                        bitmaps.emplace_back(createFromModel(*to_cnch, bitmap_model));

                    global_context->getCnchCatalog()->detachAttachedParts(
                        from_table,
                        to_table,
                        attached_parts,
                        attached_staged_parts,
                        commit_parts,
                        attached_bitmaps,
                        bitmaps,
                        request->has_txn_id() ? request->txn_id() : 0);
                    break;
                }
                case Protos::DetachAttachType::DETACH_ATTACHED_RAW:
                {
                    if (!from_table)
                        throw Exception("Cannot get table by UUID " + from_table_uuid, ErrorCodes::UNKNOWN_TABLE);
                    throw_if_non_host(from_table);
                    Strings attached_part_names, attached_bitmap_names;
                    for (const auto & name : request->attached_part_names())
                        attached_part_names.push_back(name);
                    for (const auto & name : request->attached_bitmap_names())
                        attached_bitmap_names.push_back(name);

                    std::vector<std::pair<String, String>> detached_parts_metas, detached_bitmap_metas;
                    for (const auto & elem : request->detached_part_metas())
                        detached_parts_metas.emplace_back(elem.name(), elem.meta());
                    for (const auto & elem : request->detached_bitmap_metas())
                        detached_bitmap_metas.emplace_back(elem.name(), elem.meta());

                    global_context->getCnchCatalog()->detachAttachedPartsRaw(
                        from_table, to_table_uuid, attached_part_names, detached_parts_metas, attached_bitmap_names, detached_bitmap_metas);
                    break;
                }
                default:
                    throw Exception("Unknown RedirectDetachAttachedS3PartsReq status", ErrorCodes::NOT_IMPLEMENTED);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::removeMergeMutateTasksOnPartitions(
    google::protobuf::RpcController * cntl,
    const Protos::RemoveMergeMutateTasksOnPartitionsReq * request,
    Protos::RemoveMergeMutateTasksOnPartitionsResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, gc = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                auto storage_id = RPCHelpers::createStorageID(request->storage_id());
                std::unordered_set<String> partitions;
                for (const auto & p : request->partitions())
                    partitions.insert(p);
                bool ret = gc->removeMergeMutateTasksOnPartitions(storage_id, partitions);
                response->set_ret(ret);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::submitQueryWorkerMetrics(
    google::protobuf::RpcController * /*cntl*/,
    const Protos::SubmitQueryWorkerMetricsReq * /*request*/,
    Protos::SubmitQueryWorkerMetricsResp * /*response*/,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
}

void CnchServerServiceImpl::submitPreloadTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitPreloadTaskReq * request,
    Protos::SubmitPreloadTaskResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [c = cntl, request, response, done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);

        try
        {
            auto rpc_context = RPCHelpers::createSessionContextForRPC(gc, *c);
            const TxnTimestamp txn_id = rpc_context->getTimestamp();
            rpc_context->setTemporaryTransaction(txn_id, {}, /*check catalog*/ false);

            String table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->uuid()));
            auto storage = rpc_context->getCnchCatalog()->tryGetTableByUUID(*rpc_context, table_uuid, TxnTimestamp::maxTS());
            if (!storage)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table with uuid {} not found.", table_uuid);

            auto * cnch = dynamic_cast<StorageCnchMergeTree *>(storage.get());
            if (!cnch)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not of MergeTree class");

            ServerDataPartsVector parts = createServerPartsFromModels(*cnch, request->parts());
            if (parts.empty())
                return;

            cnch->sendPreloadTasks(
                rpc_context,
                std::move(parts),
                cnch->getSettings()->enable_parts_sync_preload,
                (cnch->getSettings()->enable_preload_parts ? PreloadLevelSettings::AllPreload
                                                           : cnch->getSettings()->parts_preload_level.value),
                request->ts());
        }
        catch (...)
        {
            tryLogCurrentException(log);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

#if USE_MYSQL
void CnchServerServiceImpl::submitMaterializedMySQLDDLQuery(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    const Protos::SubmitMaterializedMySQLDDLQueryReq * request,
    Protos::SubmitMaterializedMySQLDDLQueryResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                auto database_ptr = DatabaseCatalog::instance().getDatabase(request->database_name(), global_context);
                auto * materialized_mysql = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database_ptr.get());
                if (!materialized_mysql)
                    throw Exception("Expect CnchMaterializedMySQL but got " + database_ptr->getEngineName(), ErrorCodes::LOGICAL_ERROR);

                MySQLBinLogInfo binlog;
                binlog.binlog_file = request->binlog_file();
                binlog.binlog_position = request->binlog_position();
                binlog.executed_gtid_set = request->executed_gtid_set();
                binlog.meta_version = request->meta_version();

                auto bg_thread = global_context->getCnchBGThread(CnchBGThreadType::MaterializedMySQL, materialized_mysql->getStorageID());
                auto * manager = dynamic_cast<MaterializedMySQLSyncThreadManager*>(bg_thread.get());
                if (!manager)
                    throw Exception("Convert to MaterializedMySQLSyncThreadManager from bg_thread failed", ErrorCodes::LOGICAL_ERROR);
                manager->executeDDLQuery(request->ddl_query(), request->thread_key(), binlog);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__ );
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::reportHeartbeatForSyncThread(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    const Protos::ReportHeartbeatForSyncThreadReq * request,
    Protos::ReportHeartbeatForSyncThreadResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                auto database_ptr = DatabaseCatalog::instance().getDatabase(request->database_name(), global_context);
                auto * materialized_mysql = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database_ptr.get());
                if (!materialized_mysql)
                    throw Exception("Expect CnchMaterializedMySQL but got " + database_ptr->getEngineName(), ErrorCodes::LOGICAL_ERROR);

                auto bg_thread = global_context->getCnchBGThread(CnchBGThreadType::MaterializedMySQL, materialized_mysql->getStorageID());
                auto * manager = dynamic_cast<MaterializedMySQLSyncThreadManager*>(bg_thread.get());
                if (!manager)
                    throw Exception("Convert to MaterializedMySQLSyncThreadManager from bg_thread failed", ErrorCodes::LOGICAL_ERROR);
                manager->handleHeartbeatOfSyncThread(request->thread_key());
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__ );
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::reportSyncFailedForSyncThread(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    const Protos::ReportSyncFailedForSyncThreadReq * request,
    Protos::ReportSyncFailedForSyncThreadResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                auto database_ptr = DatabaseCatalog::instance().getDatabase(request->database_name(), global_context);
                auto * materialized_mysql = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database_ptr.get());
                if (!materialized_mysql)
                    throw Exception("Expect CnchMaterializedMySQL but got " + database_ptr->getEngineName(), ErrorCodes::LOGICAL_ERROR);

                auto bg_thread = global_context->getCnchBGThread(CnchBGThreadType::MaterializedMySQL, materialized_mysql->getStorageID());
                auto * manager = dynamic_cast<MaterializedMySQLSyncThreadManager*>(bg_thread.get());
                if (!manager)
                    throw Exception("Convert to MaterializedMySQLSyncThreadManager from bg_thread failed", ErrorCodes::LOGICAL_ERROR);
                manager->handleSyncFailedThread(request->thread_key());
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__ );
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}
#endif

void CnchServerServiceImpl::handleRefreshTaskOnFinish(
    google::protobuf::RpcController *,
    const Protos::handleRefreshTaskOnFinishReq * request,
    Protos::handleRefreshTaskOnFinishResp * response,
    google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        auto storage_id = RPCHelpers::createStorageID(request->mv_storage_id());
        auto bg_thread = getContext()->getCnchBGThread(CnchBGThreadType::CnchRefreshMaterializedView, storage_id);

        auto * mv_refresh_thread = dynamic_cast<CnchRefreshMaterializedViewThread *>(bg_thread.get());
        mv_refresh_thread->handleRefreshTaskOnFinish(request->task_id(), request->txn_id());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchServerServiceImpl::executeOptimize(
    google::protobuf::RpcController *,
    const Protos::ExecuteOptimizeQueryReq * request,
    Protos::ExecuteOptimizeQueryResp * response,
    google::protobuf::Closure *done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, global_context = getContext(), log = log] {

            brpc::ClosureGuard done_guard(done);
            try
            {
                auto enable_try = request->enable_try();
                const auto & partition_id = request->partition_id();

                auto storage_id = RPCHelpers::createStorageID(request->storage_id());
                LOG_TRACE(log, "Received OPTIMIZE query for {}", storage_id.getNameForLogs());
                auto bg_thread = global_context->getCnchBGThread(CnchBGThreadType::MergeMutate, storage_id);

                auto & database_catalog = DatabaseCatalog::instance();
                auto istorage = database_catalog.getTable(storage_id, global_context);

                if (istorage && istorage->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
                {
                    if (auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(istorage.get()))
                    {
                        LOG_TRACE(
                            log,
                            "Object schema snapshot:{}",
                            cnch_table->getStorageSnapshot(cnch_table->getInMemoryMetadataPtr(), nullptr)->object_columns.toString());
                    }
                }

                auto * merge_mutate_thread = dynamic_cast<CnchMergeMutateThread *>(bg_thread.get());
                auto task_id = merge_mutate_thread->triggerPartMerge(istorage, partition_id, false, enable_try, false);
                if (request->mutations_sync())
                {
                    auto timeout = request->has_timeout_ms() ? request->timeout_ms() : 0;
                    merge_mutate_thread->waitTasksFinish({task_id}, timeout);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        }
    );
}

void CnchServerServiceImpl::submitBackupTask(
    google::protobuf::RpcController *,
    const Protos::SubmitBackupTaskReq * request,
    Protos::SubmitBackupTaskResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    const String & backup_id = request->id();
    const String & backup_command = request->command();

    try
    {
        ThreadFromGlobalPool async_thread([=, global_context = getContext(), log = log] {
            ContextMutablePtr context_ptr = Context::createCopy(global_context);

            ParserBackupQuery backup_parser;
            ASTPtr backup_ast = parseQuery(backup_parser, backup_command, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            const ASTBackupQuery & backup_query = backup_ast->as<const ASTBackupQuery &>();

            if (backup_query.kind == ASTBackupQuery::BACKUP)
                BackupsWorker::doBackup(backup_id, backup_ast, context_ptr);
            else
                BackupsWorker::doRestore(backup_id, backup_ast, context_ptr);
        });
        async_thread.detach();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchServerServiceImpl::getRunningBackupTask(
    google::protobuf::RpcController *,
    const Protos::GetRunningBackupTaskReq *,
    Protos::GetRunningBackupTaskResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [response = response, done = done, global_context = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            std::optional<String> backup_id = global_context->getRunningBackupTask();
            if (backup_id)
                response->set_ret(backup_id.value());
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::removeRunningBackupTask(
    google::protobuf::RpcController *,
    const Protos::RemoveRunningBackupTaskReq * request,
    Protos::RemoveRunningBackupTaskResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, global_context = getContext(), log = log] {
            brpc::ClosureGuard done_guard(done);
            try
            {
                const String & backup_id = request->id();
                global_context->removeRunningBackupTask(backup_id);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(response->mutable_exception());
            }
        });
}

void CnchServerServiceImpl::forceRecalculateMetrics(
    google::protobuf::RpcController *,
    const Protos::ForceRecalculateMetricsReq * request,
    Protos::ForceRecalculateMetricsResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        auto storage_id = RPCHelpers::createStorageID(request->storage_id());

        auto istorage = getContext()->getCnchCatalog()->getTable(*getContext(), storage_id.database_name, storage_id.table_name);

        if (auto mgr = getContext()->getPartCacheManager(); mgr)
        {
            mgr->forceRecalculate(istorage);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PartCacheManager not found");
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchServerServiceImpl::getLastModificationTimeHints(
    google::protobuf::RpcController *,
    const Protos::getLastModificationTimeHintsReq * request,
    Protos::getLastModificationTimeHintsResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            auto storage_id = RPCHelpers::createStorageID(request->storage_id());

            auto istorage = gc->getCnchCatalog()->getTable(*gc, storage_id.database_name, storage_id.table_name);

            std::vector<Protos::LastModificationTimeHint> hints = gc->getCnchCatalog()->getLastModificationTimeHints(istorage);

            *response->mutable_last_modification_time_hints() = {hints.begin(), hints.end()};
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::notifyTableCreated(
    google::protobuf::RpcController *,
    const Protos::notifyTableCreatedReq * request,
    Protos::notifyTableCreatedResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            if (auto pcm = gc->getPartCacheManager())
            {
                UUID uuid = RPCHelpers::createUUID(request->storage_uuid());
                auto storage = gc->getCnchCatalog()->getTableByUUID(*gc, UUIDHelpers::UUIDToString(uuid), TxnTimestamp::maxTS());
                if (storage)
                {
                    auto host_port = gc->getCnchTopologyMaster()->getTargetServer(
                        UUIDHelpers::UUIDToString(storage->getStorageID().uuid), storage->getServerVwName(), true);
                    pcm->mayUpdateTableMeta(*storage, host_port.topology_version, true);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            (void)response;
            //RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::notifyAccessEntityChange(
    google::protobuf::RpcController *,
    const Protos::notifyAccessEntityChangeReq * request,
    Protos::notifyAccessEntityChangeResp * response,
    google::protobuf::Closure *done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, gc = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);

        try
        {
            String entity_type = request->type();
            String name = request->name();
            UUID id = RPCHelpers::createUUID(request->uuid());
            for (auto type : collections::range(IAccessEntity::Type::MAX))
            {
                // KVAccessStorage::find will find the newly update/deleted access entity and notify all subscribers
                if (toString(type) == entity_type)
                {
                    const auto storage = gc->getAccessControlManager().getStorage(id);
                    if (storage)
                        storage->find(type, name);
                }

            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RPCHelpers::handleException(response->mutable_exception());
        }
    });
}

void CnchServerServiceImpl::checkDelayInsertOrThrowIfNeeded(
        google::protobuf::RpcController * cntl,
        const Protos::checkDelayInsertOrThrowIfNeededReq * request,
        Protos::checkDelayInsertOrThrowIfNeededResp * response,
        google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, context = getContext(), log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            UUID uuid = RPCHelpers::createUUID(request->storage_uuid());
            auto storage = context->getCnchCatalog()->getTableByUUID(*context, UUIDHelpers::UUIDToString(uuid), TxnTimestamp::maxTS());
            auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree*>(storage.get());
            if (!cnch_merge_tree)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "CnchMergeTree is expected for checkDelayInsertOrThrowIfNeeded but got " + storage->getName());

            cnch_merge_tree->cnchDelayInsertOrThrowIfNeeded();
        }
        catch (Exception & e)
        {
            /// Only TOO_MANY_PARTS matters
            if (e.code() == ErrorCodes::TOO_MANY_PARTS)
                RPCHelpers::handleException(response->mutable_exception());
            else
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    });
}

#if defined(__clang__)
    #pragma clang diagnostic pop
#else
    #pragma GCC diagnostic pop
#endif
}
