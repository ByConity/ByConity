#include <CloudServices/CnchServerServiceImpl.h>

#include <Protos/RPCHelpers.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Interpreters/Context.h>
#include <Protos/RPCHelpers.h>
#include <Protos/DataModelHelpers.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/Exception.h>
#include <Common/serverLocality.h>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <Storages/PartCacheManager.h>

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
    : WithMutableContext(global_context), log(&Poco::Logger::get("CnchServerService"))
{
}


void CnchServerServiceImpl::commitParts(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::CommitPartsReq * request,
    [[maybe_unused]] Protos::CommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        // we need to set the response before any exception is thrown.
        response->set_commit_timestamp(0);
        /// Resolve request parameters
        if (request->parts_size() != request->paths_size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect arguments");

        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        TransactionCnchPtr cnch_txn = rpc_context->getCnchTransactionCoordinator().getTransaction(request->txn_id());

        /// TODO: find table by uuid ?
        /// The table schema in catalog has not been updated, use storage in query_cache
        // auto storage = rpc_context->getTable(request->database(), request->table());

        // auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage.get());
        // if (!cnch)
        //     throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table is not of MergeTree class");

        /// TODO:
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
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
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);
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
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);
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
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);

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
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);

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
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);
    RPCHelpers::serviceHandler(
        done, response, [request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
            brpc::ClosureGuard done_guard(done);
        try
        {
            auto & txn_coordinator = global_context.getCnchTransactionCoordinator();
            auto txn = txn_coordinator.getTransaction(request->txn_id());
            if (request->has_only_clean_data() && request->only_clean_data())
                txn->cleanWrittenData();
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
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR); 
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

void CnchServerServiceImpl::fetchDataParts(
    [[maybe_unused]] ::google::protobuf::RpcController* controller,
    [[maybe_unused]] const ::DB::Protos::FetchDataPartsReq* request,
    [[maybe_unused]] ::DB::Protos::FetchDataPartsResp* response,
    [[maybe_unused]] ::google::protobuf::Closure* done)
{
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, &global_context = *context_ptr, log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            StoragePtr storage = global_context.getCnchCatalog()->getTable(
                global_context, request->database(), request->table(), TxnTimestamp{request->table_commit_time()});

            auto calculated_host = global_context.getCnchTopologyMaster()
                                       ->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), true)
                                       .getRPCAddress();
            if (request->remote_host() != calculated_host)
                throw Exception(
                    "Fetch parts failed because of inconsistent view of topology in remote server, remote_host: " + request->remote_host()
                        + ", calculated_host: " + calculated_host,
                    ErrorCodes::LOGICAL_ERROR);

            if (!isLocalServer(calculated_host, std::to_string(global_context.getRPCPort())))
                throw Exception(
                    "Fetch parts failed because calculated host (" + calculated_host + ") is not remote server.",
                    ErrorCodes::LOGICAL_ERROR);

            Strings partition_list;
            for (const auto & partition : request->partitions())
                partition_list.emplace_back(partition);

            auto parts
                = global_context.getCnchCatalog()->getServerDataPartsInPartitions(storage, partition_list, TxnTimestamp{request->timestamp()}, nullptr);
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

void CnchServerServiceImpl::handleRedirectCommitRequest(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    [[maybe_unused]] const Protos::RedirectCommitPartsReq * request,
    [[maybe_unused]] Protos::RedirectCommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done,
    bool final_commit)
{
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);
    RPCHelpers::serviceHandler(done, response, [request = request, response = response, done = done, final_commit=final_commit, &global_context = *context_ptr, log = log] {
        brpc::ClosureGuard done_guard(done);
        try
        {
            String table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(request->uuid()));
            StoragePtr storage = global_context.getCnchCatalog()->tryGetTableByUUID(
                global_context, table_uuid, TxnTimestamp::maxTS());
            
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
                global_context.getCnchCatalog()->writeParts(storage, txnID, 
                    Catalog::CommitItems{parts, delete_bitmaps, staged_parts}, request->from_merge_task(), request->preallocate_mode());
            }
            else
            {
                TxnTimestamp commitTs {request->commit_ts()};
                global_context.getCnchCatalog()->setCommitTime(storage, Catalog::CommitItems{parts, delete_bitmaps, staged_parts},
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

void CnchServerServiceImpl::redirectSetCommitTime(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    [[maybe_unused]] const Protos::RedirectCommitPartsReq * request,
    [[maybe_unused]] Protos::RedirectCommitPartsResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    handleRedirectCommitRequest(controller, request, response, done, true);
}

void CnchServerServiceImpl::getTableInfo(
    [[maybe_unused]] google::protobuf::RpcController* controller,
    [[maybe_unused]] const Protos::GetTableInfoReq* request,
    [[maybe_unused]] Protos::GetTableInfoResp* response,
    [[maybe_unused]] google::protobuf::Closure* done)
{
    ContextPtr context_ptr = context.lock();
    if (!context_ptr)
        throw Exception("Global context expried while running rpc call", ErrorCodes::LOGICAL_ERROR);
    RPCHelpers::serviceHandler(
        done,
        response,
        [ req = request, rsp = response, d = done, &global_context = *context_ptr, logger=log] {
            brpc::ClosureGuard inner_guard(d);
            try
            {
                auto part_cache_manager = global_context.getPartCacheManager();
                for (auto & table_id : req->table_ids())
                {
                    UUID uuid(stringToUUID(table_id.uuid()));
                    Protos::DataModelTableInfo * table_info = rsp->add_table_infos();
                    table_info->set_database(table_id.database());
                    table_info->set_table(table_id.name());
                    table_info->set_last_modification_time(part_cache_manager->getTableLastUpdateTime(uuid));
                    table_info->set_cluster_status(part_cache_manager->getTableClusterStatus(uuid));
                }
            }
            catch (...)
            {
                tryLogCurrentException(logger, __PRETTY_FUNCTION__);
                RPCHelpers::handleException(rsp->mutable_exception());
            }
        }
    );
}

}
