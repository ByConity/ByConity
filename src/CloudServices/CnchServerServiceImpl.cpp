#include <CloudServices/CnchServerServiceImpl.h>

#include <Protos/RPCHelpers.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

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
}


}
