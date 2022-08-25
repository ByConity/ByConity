#include <CloudServices/CnchServerServiceImpl.h>

#include <Protos/RPCHelpers.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Interpreters/Context.h>
#include <Protos/RPCHelpers.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/Exception.h>
#include <CloudServices/commitCnchParts.h>
#include <WorkerTasks/ManipulationType.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CNCH_KAFKA_TASK_NEED_STOP;
    extern const int UNKNOWN_TABLE;
}

CnchServerServiceImpl::CnchServerServiceImpl(ContextMutablePtr global_context)
    : WithMutableContext(global_context),
      server_start_time(global_context->getTimestamp()),
      global_gc_manager(global_context),

      log(&Poco::Logger::get("CnchServerService"))
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

                TransactionCnchPtr cnch_txn;
                cnch_txn = gc->getCnchTransactionCoordinator().getTransaction(req->txn_id());

                /// Create new rpc context and bind the previous created txn to this rpc context.
                auto rpc_context = RPCHelpers::createSessionContextForRPC(gc, *c);
                rpc_context->setCurrentTransaction(cnch_txn, false);

                auto & database_catalog = DatabaseCatalog::instance();
                StorageID table_id(req->database(), req->table());
                auto storage = database_catalog.tryGetTable(table_id, rpc_context);

                auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage.get());
                if (!cnch)
                    throw Exception("Table is not of MergeTree class", ErrorCodes::BAD_ARGUMENTS);

                auto parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch, req->parts(), &req->paths());
                auto staged_parts = createPartVectorFromModels<MutableMergeTreeDataPartCNCHPtr>(*cnch, req->staged_parts(), &req->staged_parts_paths());

                DeleteBitmapMetaPtrVector delete_bitmaps;
                delete_bitmaps.reserve(req->delete_bitmaps_size());
                for (const auto & bitmap_model : req->delete_bitmaps())
                    delete_bitmaps.emplace_back(createFromModel(*cnch, bitmap_model));

                String from_buffer_uuid;
                if (req->has_from_buffer_uuid())
                    from_buffer_uuid = req->from_buffer_uuid();

                /// check and parse offsets
                String consumer_group;
                cppkafka::TopicPartitionList tpl;
                if (req->has_consumer_group())
                {
                    consumer_group = req->consumer_group();
                    tpl.reserve(req->tpl_size());
                    for (const auto & tp : req->tpl())
                        tpl.emplace_back(cppkafka::TopicPartition(tp.topic(), tp.partition(), tp.offset()));

                    LOG_TRACE(&Poco::Logger::get("CnchServerService"), "parsed tpl to commit with size: {}\n", tpl.size());
                }
                CnchDataWriter cnch_writer(
                    *cnch,
                    *rpc_context,
                    ManipulationType(req->type()),
                    req->task_id(),
                    std::move(from_buffer_uuid),
                    std::move(consumer_group),
                    tpl);

                TxnTimestamp commit_time
                    = cnch_writer.commitPreparedCnchParts(DumpedData{std::move(parts), std::move(delete_bitmaps), std::move(staged_parts)});

                rsp->set_commit_timestamp(commit_time);
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
    /*
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, &global_context = global_context, log = log] {
            brpc::ClosureGuard done_guard(done);
            try
            {
                auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
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
    */
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
            CnchTransactionInitiator initiator
                = request->has_primary_txn_id() ? CnchTransactionInitiator::Txn : CnchTransactionInitiator::Worker;
            auto transaction
                = global_context.getCnchTransactionCoordinator().createTransaction(CreateTransactionOption()
                                                                                        .setPrimaryTransactionId(primary_txn_id)
                                                                                        .setType(CnchTransactionType::Implicit)
                                                                                        .setInitiator(initiator));
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
            global_context.getCnchTransactionCoordinator().finishTransaction(request->txn_id());

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
                // if (request->has_kafka_storage_id())
                // {
                //     auto storage_id = RPCHelpers::createStorageID(request->kafka_storage_id());
                //     auto bgthread = global_context.getConsumeManager(storage_id);
                //     auto manager = dynamic_cast<KafkaConsumeManager *>(bgthread.get());

                //     if (!manager->checkWorkerClient(storage_id.getTableName(), request->kafka_consumer_index()))
                //         throw Exception(
                //             "check validity of worker client for " + storage_id.getFullTableName() + " failed",
                //             ErrorCodes::CNCH_KAFKA_TASK_NEED_STOP);
                //     LOG_TRACE(
                //         log,
                //         "Check consumer {} OK. Now commit parts and offsets for Kafka transaction\n",storage_id.getFullTableName());
                // }

                auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
                auto txn_id = request->txn_id();
                auto txn = txn_coordinator.getTransaction(txn_id);

                if (request->has_insertion_label())
                {
                    if (UUIDHelpers::Nil == txn->getMainTableUUID())
                        throw Exception("Main table is not set when using insertion label", ErrorCodes::LOGICAL_ERROR);

                    txn->setInsertionLabel(std::make_shared<InsertionLabel>(
                        txn->getMainTableUUID(), request->insertion_label(), txn->getTransactionID().toUInt64()));
                }

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
            // auto bgthread = global_context.getConsumeManager(storage->getStorageID());
            // auto manager = dynamic_cast<KafkaConsumeManager *>(bgthread.get());

            // if (!manager->checkWorkerClient(request->table_name(), request->consumer_index()))
            //     throw Exception("check validity of worker client for " + request->table_name() + " failed", ErrorCodes::LOGICAL_ERROR);

            auto transaction = global_context.getCnchTransactionCoordinator().createTransaction(
                CreateTransactionOption().setInitiator(CnchTransactionInitiator::Kafka));
            auto & controller = static_cast<brpc::Controller &>(*cntl);
            transaction->setCreator(butil::endpoint2str(controller.remote_side()).c_str());

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
void CnchServerServiceImpl::reportBufferHeartbeat(
    google::protobuf::RpcController * cntl,
    const Protos::ReportBufferHeartbeatReq * request,
    Protos::ReportBufferHeartbeatResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::reportDeduperHeartbeat(
    google::protobuf::RpcController * cntl,
    const Protos::ReportDeduperHeartbeatReq * request,
    Protos::ReportDeduperHeartbeatResp * response,
    google::protobuf::Closure * done)
{
}

void CnchServerServiceImpl::fetchDataParts(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::FetchDataPartsReq * request,
    ::DB::Protos::FetchDataPartsResp * response,
    ::google::protobuf::Closure * done)
{
}

void CnchServerServiceImpl::fetchUniqueTableMeta(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::FetchUniqueTableMetaReq * request,
    ::DB::Protos::FetchUniqueTableMetaResp * response,
    ::google::protobuf::Closure * done)
{
}

void CnchServerServiceImpl::getWorkerListWithBuffer(
    google::protobuf::RpcController * cntl,
    const Protos::GetWorkerListWithBufferReq * request,
    Protos::GetWorkerListWithBufferResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::getBackgroundThreadStatus(
    google::protobuf::RpcController * cntl,
    const Protos::BackgroundThreadStatusReq * request,
    Protos::BackgroundThreadStatusResp * response,
    google::protobuf::Closure * done)
{
    RPCHelpers::serviceHandler(
        done,
        response,
        [request = request, response = response, done = done, log = log] {
            brpc::ClosureGuard done_guard(done);

            try
            {
                std::map<StorageID, CnchBGThreadStatus> res;

                auto type = CnchBGThreadType(request->type());
                if (
                    type == CnchBGThreadType::PartGC ||
                    type == CnchBGThreadType::MergeMutate ||
                    type == CnchBGThreadType::MemoryBuffer ||
                    type == CnchBGThreadType::Consumer ||
                    type == CnchBGThreadType::DedupWorker ||
                    type == CnchBGThreadType::Clustering)
                {
#if 0
                    auto threads = global_context.getCnchBGThreads(type);
                    res = threads->getStatusMap();
#endif
                }
                else
                {
                    throw Exception("Not support type " + toString(int(request->type())), ErrorCodes::NOT_IMPLEMENTED);
                }

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
void CnchServerServiceImpl::acquireLock(
    google::protobuf::RpcController * cntl,
    const Protos::AcquireLockReq * request,
    Protos::AcquireLockResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::releaseLock(
    google::protobuf::RpcController * cntl,
    const Protos::ReleaseLockReq * request,
    Protos::ReleaseLockResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::reportCnchLockHeartBeat(
    google::protobuf::RpcController * cntl,
    const Protos::ReportCnchLockHeartBeatReq * request,
    Protos::ReportCnchLockHeartBeatResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::getServerStartTime(
    google::protobuf::RpcController * cntl,
    const Protos::GetServerStartTimeReq * request,
    Protos::GetServerStartTimeResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    response->set_server_start_time(server_start_time);
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
void CnchServerServiceImpl::redirectCommitParts(
    google::protobuf::RpcController * controller,
    const Protos::RedirectCommitPartsReq * request,
    Protos::RedirectCommitPartsResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::redirectSetCommitTime(
    google::protobuf::RpcController * controller,
    const Protos::RedirectCommitPartsReq * request,
    Protos::RedirectCommitPartsResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::removeMergeMutateTasksOnPartition(
    google::protobuf::RpcController * cntl,
    const Protos::RemoveMergeMutateTasksOnPartitionReq * request,
    Protos::RemoveMergeMutateTasksOnPartitionResp * response,
    google::protobuf::Closure * done)
{
}
void CnchServerServiceImpl::submitQueryWorkerMetrics(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitQueryWorkerMetricsReq * request,
    Protos::SubmitQueryWorkerMetricsResp * response,
    google::protobuf::Closure * done)
{
}

#if defined(__clang__)
    #pragma clang diagnostic pop
#else
    #pragma GCC diagnostic pop
#endif

}
