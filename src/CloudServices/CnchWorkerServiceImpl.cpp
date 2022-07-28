#include <CloudServices/CnchWorkerServiceImpl.h>

#include <CloudServices/CnchWorkerResource.h>
#include <CloudServices/CnchCreateQueryHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/NamedSession.h>
#include <Protos/RPCHelpers.h>
#include <Protos/DataModelHelpers.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <WorkerTasks/ManipulationList.h>
#include <WorkerTasks/ManipulationTask.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <IO/ReadBufferFromString.h>

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <condition_variable>
#include <mutex>

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

CnchWorkerServiceImpl::CnchWorkerServiceImpl(ContextPtr context_)
    : WithMutableContext(context_->getGlobalContext()), log(&Poco::Logger::get("CnchWorkerService"))
{
}

CnchWorkerServiceImpl::~CnchWorkerServiceImpl() = default;


void CnchWorkerServiceImpl::submitManipulationTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitManipulationTaskReq * request,
    Protos::SubmitManipulationTaskResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        if (request->task_id().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Require non-empty task_id");

        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);
        rpc_context->setCurrentQueryId(request->task_id());
        rpc_context->getClientInfo().rpc_port = request->rpc_port();
        // TODO: rpc_context->setCurrentTransaction(std::make_shared<CnchWorkerTransaction>(*rpc_context, TxnTimestamp(request->txn_id())));

        /// const auto & settings = global_context->getSettingsRef();
        /// UInt64 max_running_task = settings.max_ratio_of_cnch_tasks_to_threads * settings.max_threads;
        // if (global_context->getManipulationList().size() > max_running_task)
        //     throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_TASKS, "Too many simultaneous tasks. Maximum: {}", max_running_task);

        StoragePtr storage = createStorageFromQuery(request->create_table_query(), rpc_context);
        auto * data = dynamic_cast<StorageCloudMergeTree *>(storage.get());
        if (!data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not CloudMergeTree", storage->getStorageID().getNameForLogs());

        auto params = ManipulationTaskParams(storage);
        params.type = ManipulationType(request->type());
        params.task_id = request->task_id();
        params.rpc_port = static_cast<UInt16>(request->rpc_port());
        params.txn_id = request->txn_id();
        params.columns_commit_time = request->columns_commit_time();
        params.is_bucket_table = request->is_bucket_table();
        params.source_data_parts = createPartVectorFromModelsForSend<IMergeTreeDataPartPtr>(*data, request->source_parts());

        if (params.type == ManipulationType::Mutate)
        {
            params.mutation_commit_time = request->mutation_commit_time();
            auto read_buf = ReadBufferFromString(request->mutate_commands());
            params.mutation_commands = std::make_shared<MutationCommands>();
            params.mutation_commands->readText(read_buf);
        }

        auto remote_address = addBracketsIfIpv6(rpc_context->getClientInfo().current_address.host().toString()) + ':' + toString(params.rpc_port);

        LOG_DEBUG(log, "Received manipulation from {} :{}", remote_address, params.toDebugString());

        ThreadFromGlobalPool([p = std::move(params), c = std::move(rpc_context)] {
            /// CurrentThread::attachQueryContext(c);
            executeManipulationTask(p, c);
        }).detach();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::shutdownManipulationTasks(
    google::protobuf::RpcController *,
    const Protos::ShutdownManipulationTasksReq * request,
    Protos::ShutdownManipulationTasksResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        auto uuid = RPCHelpers::createUUID(request->table_uuid());

        getContext()->getManipulationList().apply([&](std::list<ManipulationListElement> & container)
        {
            for (auto & e : container)
            {
                if (uuid != e.storage_id.uuid)
                    continue;

                e.is_cancelled.store(true, std::memory_order_relaxed);
            }
        });
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::touchManipulationTasks(
    google::protobuf::RpcController *,
    const Protos::TouchManipulationTasksReq * request,
    Protos::TouchManipulationTasksResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        auto uuid = RPCHelpers::createUUID(request->table_uuid());
        std::unordered_set<String> request_tasks(request->tasks_id().begin(), request->tasks_id().end());

        getContext()->getManipulationList().apply([&](std::list<ManipulationListElement> & container)
        {
            auto now = time(nullptr);
            for (auto & e : container)
            {
                if (uuid != e.storage_id.uuid)
                    continue;

                if (request_tasks.count(e.task_id))
                {
                    e.last_touch_time.store(now, std::memory_order_relaxed);
                }
                else
                {
                    /// TBD: cancel tasks?
                }

                response->add_tasks_id(e.task_id);
            }
        });
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::getManipulationTasksStatus(
    google::protobuf::RpcController *,
    const Protos::GetManipulationTasksStatusReq *,
    Protos::GetManipulationTasksStatusResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        getContext()->getManipulationList().apply([&](std::list<ManipulationListElement> & container)
        {
            for (auto & e : container)
            {
                auto * task_info = response->add_tasks();

                task_info->set_task_id(e.task_id);
                task_info->set_type(UInt32(e.type));
                RPCHelpers::fillStorageID(e.storage_id, *task_info->mutable_storage_id());
                task_info->set_elapsed(e.watch.elapsedSeconds());
                task_info->set_num_parts(e.num_parts);
                for (auto & source_part_name : e.source_part_names)
                    task_info->add_source_part_names(source_part_name);
                for (auto & result_part_name : e.result_part_names)
                    task_info->add_result_part_names(result_part_name);
                task_info->set_partition_id(e.partition_id);
                task_info->set_total_size_bytes_compressed(e.total_size_bytes_compressed);
                task_info->set_total_size_marks(e.total_size_marks);
                task_info->set_progress(e.progress.load(std::memory_order_relaxed));
                task_info->set_bytes_read_uncompressed(e.bytes_read_uncompressed.load(std::memory_order_relaxed));
                task_info->set_bytes_written_uncompressed(e.bytes_written_uncompressed.load(std::memory_order_relaxed));
                task_info->set_rows_read(e.rows_read.load(std::memory_order_relaxed));
                task_info->set_rows_written(e.rows_written.load(std::memory_order_relaxed));
                task_info->set_columns_written(e.columns_written.load(std::memory_order_relaxed));
                task_info->set_memory_usage(e.memory_tracker.get());
                task_info->set_thread_id(e.thread_id);
            }
        });
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::sendCreateQuery(
    google::protobuf::RpcController * cntl,
    const Protos::SendCreateQueryReq * request,
    Protos::SendCreateQueryResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        LOG_TRACE(log, "Receiving create queries for Session: ", request->txn_id());
        /// set client_info.
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        auto timeout = std::chrono::seconds(request->timeout());
        auto & query_context = rpc_context->acquireNamedCnchSession(request->txn_id(), timeout, false)->context;
        // session->context->setTemporaryTransaction(request->txn_id(), request->primary_txn_id());
        auto worker_resource = query_context->getCnchWorkerResource();

        /// store cloud tables in cnch_session_resource.
        for (const auto & create_query: request->create_queries())
        {
            /// create a copy of session_context to avoid modify in SessionResource
            worker_resource->executeCreateQuery(rpc_context, create_query);
        }

        // query_context.worker_type = WorkerType::normal_worker;
        LOG_TRACE(log, "Successfully create {} queries for Session: {}", request->create_queries_size(), request->txn_id());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::sendQueryDataParts(
    google::protobuf::RpcController *,
    const Protos::SendDataPartsReq * request,
    Protos::SendDataPartsResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        const auto & query_context = getContext()->acquireNamedCnchSession(request->txn_id(), {}, true)->context;

        auto storage = DatabaseCatalog::instance().getTable({request->database_name(), request->table_name()}, query_context);
        auto & cloud_merge_tree = dynamic_cast<StorageCloudMergeTree &>(*storage);

        LOG_DEBUG(log,
            "Receiving {} parts for table {}(txn_id: {})",
            request->parts_size(), cloud_merge_tree.getStorageID().getNameForLogs(), request->txn_id());

        MergeTreeMutableDataPartsVector data_parts;
        if (cloud_merge_tree.getInMemoryMetadata().hasUniqueKey())
            data_parts = createBasePartAndDeleteBitmapFromModelsForSend<IMergeTreeMutableDataPartPtr>(cloud_merge_tree, request->parts(), request->bitmaps());
        else
            data_parts = createPartVectorFromModelsForSend<IMergeTreeMutableDataPartPtr>(cloud_merge_tree, request->parts());
        cloud_merge_tree.loadDataParts(data_parts);

        LOG_DEBUG(log, "Received and loaded {} server parts.", data_parts.size());

        // TODO:
        // std::unordered_set<Int64> required_bucket_numbers;
        // for (const auto & bucket_number: request->bucket_numbers())
        //     required_bucket_numbers.insert(bucket_number);

        // cloud_merge_tree.setRequiredBucketNumbers(required_bucket_numbers);

        // std::map<String, UInt64> udf_infos;
        // for (const auto & udf_info: request->udf_infos())
        //     udf_infos.emplace(udf_info.function_name(), udf_info.version());

        // UserDefinedObjectsLoader::instance().checkAndLoadUDFFromStorage(query_context, udf_infos, true);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::sendCnchHiveDataParts(
    google::protobuf::RpcController *,
    const Protos::SendCnchHiveDataPartsReq *,
    Protos::SendCnchHiveDataPartsResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        // TODO
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::checkDataParts(
    google::protobuf::RpcController * cntl,
    const Protos::CheckDataPartsReq * request,
    Protos::CheckDataPartsResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        /// set client_info
        auto rpc_context = RPCHelpers::createSessionContextForRPC(getContext(), *cntl);

        auto session = rpc_context->acquireNamedCnchSession(request->txn_id(), {}, false);
        auto & query_context = session->context;

        auto worker_resource = query_context->getCnchWorkerResource();
        worker_resource->executeCreateQuery(query_context, request->create_query());

        auto storage = DatabaseCatalog::instance().getTable({request->database_name(), request->table_name()}, query_context);
        auto & cloud_merge_tree = dynamic_cast<StorageCloudMergeTree &>(*storage);

        LOG_DEBUG(log, "Receiving parts for table {} to check.", cloud_merge_tree.getStorageID().getNameForLogs());

        // auto data_parts = createPartVectorFromModelsForSend<MutableDataPartPtr>(cloud_merge_tree, request->parts());
        MutableMergeTreeDataPartsCNCHVector data_parts = {};

        bool is_passed = false;
        String message;

        for (auto & part : data_parts)
        {
            try
            {
                // TODO: checkDataParts(part);
                // dynamic_cast<MergeTreeDataPartCNCH*>(part.get())->loadFromFileSystem(storage.get());
                is_passed = true;
                message.clear();
            }
            catch (const Exception & e)
            {
                is_passed = false;
                message = e.message();
            }

            *response->mutable_part_path()->Add() = part->name;
            *response->mutable_is_passed()->Add() = is_passed;
            *response->mutable_message()->Add() = message;
        }

        session->release();
        LOG_DEBUG(log, "Send check results back for {} parts.", data_parts.size());
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::sendOffloading(
    google::protobuf::RpcController *,
    const Protos::SendOffloadingReq *,
    Protos::SendOffloadingResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);

    try
    {
        /// TODO
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

void CnchWorkerServiceImpl::removeWorkerResource(
    google::protobuf::RpcController *,
    const Protos::RemoveWorkerResourceReq * request,
    Protos::RemoveWorkerResourceResp * response,
    google::protobuf::Closure * done)
{
    brpc::ClosureGuard done_guard(done);
    try
    {
        auto session = getContext()->acquireNamedCnchSession(request->txn_id(), {}, true);
        /// remove resource in worker
        session->release();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        RPCHelpers::handleException(response->mutable_exception());
    }
}

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-parameter"
#else
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

void CnchWorkerServiceImpl::executeSimpleQuery(
    google::protobuf::RpcController * cntl,
    const Protos::ExecuteSimpleQueryReq * request,
    Protos::ExecuteSimpleQueryResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::GetPreallocatedStatus(
    google::protobuf::RpcController *,
    const Protos::GetPreallocatedStatusReq * request,
    Protos::GetPreallocatedStatusResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::SetQueryIntent(
    google::protobuf::RpcController *,
    const Protos::SetQueryIntentReq * request,
    Protos::SetQueryIntentResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::SubmitSyncTask(
    google::protobuf::RpcController *,
    const Protos::SubmitSyncTaskReq * request,
    Protos::SubmitSyncTaskResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::ResetQueryIntent(
    google::protobuf::RpcController *,
    const Protos::ResetQueryIntentReq * request,
    Protos::ResetQueryIntentResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::SubmitScaleTask(
    google::protobuf::RpcController *,
    const Protos::SubmitScaleTaskReq * request,
    Protos::SubmitScaleTaskResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::ClearPreallocatedDataParts(
    google::protobuf::RpcController *,
    const Protos::ClearPreallocatedDataPartsReq * request,
    Protos::ClearPreallocatedDataPartsResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::createDedupWorker(
    google::protobuf::RpcController *,
    const Protos::CreateDedupWorkerReq * request,
    Protos::CreateDedupWorkerResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::dropDedupWorker(
    google::protobuf::RpcController *,
    const Protos::DropDedupWorkerReq * request,
    Protos::DropDedupWorkerResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::getDedupWorkerStatus(
    google::protobuf::RpcController *,
    const Protos::GetDedupWorkerStatusReq * request,
    Protos::GetDedupWorkerStatusResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::submitKafkaConsumeTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitKafkaConsumeTaskReq * request,
    Protos::SubmitKafkaConsumeTaskResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::getConsumerStatus(
    google::protobuf::RpcController * cntl,
    const Protos::GetConsumerStatusReq * request,
    Protos::GetConsumerStatusResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::getOffsetsFromMemoryBuffer(
    google::protobuf::RpcController * cntl,
    const Protos::GetOffsetsFromMemoryBufferReq * request,
    Protos::GetOffsetsFromMemoryBufferResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::preloadChecksumsAndPrimaryIndex(
    google::protobuf::RpcController * cntl,
    const Protos::PreloadChecksumsAndPrimaryIndexReq * request,
    Protos::PreloadChecksumsAndPrimaryIndexResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::createCloudMemoryBuffer(
    google::protobuf::RpcController * cntl,
    const Protos::CreateCloudMemoryBufferReq * request,
    Protos::CreateCloudMemoryBufferResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::dropCloudMemoryBuffer(
    google::protobuf::RpcController * cntl,
    const Protos::DropCloudMemoryBufferReq * request,
    Protos::DropCloudMemoryBufferResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::flushCloudMemoryBuffer(
    google::protobuf::RpcController * cntl,
    const Protos::FlushCloudMemoryBufferReq * request,
    Protos::FlushCloudMemoryBufferResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::getCloudMemoryBufferStatus(
    google::protobuf::RpcController * cntl,
    const Protos::GetCloudMemoryBufferStatusReq * request,
    Protos::GetCloudMemoryBufferStatusResp * response,
    google::protobuf::Closure * done)
{
}
void CnchWorkerServiceImpl::getCloudMergeTreeStatus(
    google::protobuf::RpcController * cntl,
    const Protos::GetCloudMergeTreeStatusReq * request,
    Protos::GetCloudMergeTreeStatusResp * response,
    google::protobuf::Closure * done)
{
}

void CnchWorkerServiceImpl::submitIngestPartitionTask(
    google::protobuf::RpcController * cntl,
    const Protos::SubmitIngestPartitionTaskReq * request,
    Protos::SubmitIngestPartitionTaskResp * response,
    google::protobuf::Closure * done)
{
}

#if defined(__clang__)
    #pragma clang diagnostic pop
#else
    #pragma GCC diagnostic pop
#endif

}
