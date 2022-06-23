#include <CloudServices/CnchWorkerServiceImpl.h>

#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <WorkerTasks/ManipulationTask.h>
#include <Protos/DataModelHelpers.h>
#include <CloudServices/CnchCreateQueryHelper.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BRPC_EXCEPTION;
    extern const int TOO_MANY_SIMULTANEOUS_TASKS;
    extern const int PREALLOCATE_TOPOLOGY_ERROR;
    extern const int PREALLOCATE_QUERY_INTENT_NOT_FOUND;
}

CnchWorkerServiceImpl::CnchWorkerServiceImpl(ContextPtr global_context)
    : WithContext(global_context), log(&Poco::Logger::get("CnchWorkerService"))
{
}

CnchWorkerServiceImpl::~CnchWorkerServiceImpl() = default;


void CnchWorkerServiceImpl::submitManipulationTask(
    [[maybe_unused]] google::protobuf::RpcController * cntl,
    [[maybe_unused]] const Protos::SubmitManipulationTaskReq * request,
    [[maybe_unused]] Protos::SubmitManipulationTaskResp * response,
    [[maybe_unused]] google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (request->task_id().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Require non-empty task_id");

        auto local_context = getContext();

        auto settings = local_context->getSettings();
        /// UInt64 max_running_task = settings.max_ratio_of_cnch_tasks_to_threads * settings.max_threads;
        UInt64 max_running_task = 0;
        if (local_context->getManipulationList().size() > max_running_task)
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_TASKS, "Too many simultaneous tasks. Maximum: {}", max_running_task);

        StoragePtr storage = createStorageFromQuery(request->create_table_query(), *local_context);
        auto data = dynamic_cast<MergeTreeData *>(storage.get());
        if (!data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not CloudMergeTree", storage->getStorageID().getNameForLogs());

        auto params = std::make_shared<ManipulationTaskParams>(storage);
        params->type = ManipulationType(request->type());
        params->task_id = request->task_id();
        params->rpc_port = static_cast<UInt16>(request->rpc_port());
        params->txn_id = request->txn_id();
        params->columns_commit_time = request->columns_commit_time();
        params->is_bucket_table = request->is_bucket_table();

        auto rpc_context = RPCHelpers::createSessionContextForRPC(local_context, *cntl);
        rpc_context->setCurrentQueryId(params->task_id);
        rpc_context->getClientInfo().rpc_port = params->rpc_port;

        // TODO: rpc_context->setCurrentTransaction(std::make_shared<CnchWorkerTransaction>(*rpc_context, TxnTimestamp(request->txn_id())));

        /// DO NOT calc visible parts here. Task need hold unvisible parts
        // TODO: params->source_data_parts = createPartVectorFromModelsForSend<IMergeTreeDataPartPtr>(*data, request->source_parts());

        if (params->type == ManipulationType::Mutate)
        {
            params->mutation_commit_time = request->mutation_commit_time();
            auto read_buf = ReadBufferFromString(request->mutate_commands());
            params->mutation_commands = std::make_shared<MutationCommands>();
            params->mutation_commands->readText(read_buf);
        }

        auto remote_address
            = addBracketsIfIpv6(rpc_context->getClientInfo().current_address.host().toString()) + ':' + toString(params->rpc_port);

        LOG_DEBUG(log, "Received manipulation from {} :{}", remote_address, params->toDebugString());

        ThreadFromGlobalPool([p = std::move(params), c = std::move(rpc_context)] {
            /// CurrentThread::attachQueryContext(c);
            DB::executeManipulationTask(*p, c);
        }).detach();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

}
