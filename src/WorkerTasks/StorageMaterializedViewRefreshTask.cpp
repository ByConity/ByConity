#include <WorkerTasks/StorageMaterializedViewRefreshTask.h>

#include <Interpreters/Context.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Transaction/ICnchTransaction.h>
#include <Interpreters/executeQuery.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

StorageMaterializedViewRefreshTask::StorageMaterializedViewRefreshTask(
    StorageCloudMergeTree & storage_,
    ManipulationTaskParams params_,
    StorageID mv_storage_id_,
    ContextPtr context_,
    CnchServerClientPtr client)
    : ManipulationTask(std::move(params_), std::move(context_))
    , storage(storage_), mv_storage_id(mv_storage_id_), server_client(client)
{
    LOG_DEBUG(getLogger("StorageMaterializedViewRefreshTask"), "construct StorageMaterializedViewRefreshTask for {}.", storage.getTableName());
}

void StorageMaterializedViewRefreshTask::refreshAsyncOnWorker(AsyncRefreshParam & mv_refresh_param, ContextMutablePtr insert_context)
{
    if (insert_context->getServerType() != ServerType::cnch_worker)
        throw Exception("refreshAsyncOnWorker should run in worker", ErrorCodes::LOGICAL_ERROR);

    // INSERT SELECT
    LOG_DEBUG(getLogger("StorageMaterializedViewRefreshTask"), "materialized view refresh in worker insert select query: {}", mv_refresh_param.insert_select_query);
    BlockIO insert_io;
    try
    {
        insert_io = executeQuery(mv_refresh_param.insert_select_query, insert_context, false);
        if (insert_io.pipeline.initialized())
        {
            auto & pipeline = insert_io.pipeline;
            PullingAsyncPipelineExecutor executor(pipeline);
            Block block;
            while (executor.pull(block))
            {
            }
        }
        else if (insert_io.in)
        {
            AsynchronousBlockInputStream async_in(insert_io.in);
            async_in.readPrefix();
            while (true)
            {
                const auto block = async_in.read();
                if (!block)
                    break;
            }
            async_in.readSuffix();
        }
        insert_io.onFinish();
    }
    catch (...)
    {
        insert_io.onException();
        throw;
    }
}

void StorageMaterializedViewRefreshTask::executeImpl()
{
    // refreshAsyncOnWorker(*params.mv_refresh_param, const_cast<Context &>(*getContext()).shared_from_this());

    LOG_DEBUG(getLogger("StorageMaterializedViewRefreshTask"), "insert select finished.");
    server_client->handleRefreshTaskOnFinish(mv_storage_id, params.task_id, params.txn_id);

}

}
