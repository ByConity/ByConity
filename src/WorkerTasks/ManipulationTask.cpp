#include <WorkerTasks/ManipulationTask.h>

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

ManipulationTask::ManipulationTask(ManipulationTaskParams params_, ContextPtr context_) : params(std::move(params_)), context(context_)
{
    if (/*params.source_parts.empty() && */params.source_data_parts.empty())
        throw Exception("Expected non-empty source parts in ManipulationTaskParams", ErrorCodes::BAD_ARGUMENTS);

    if (params.new_part_names.empty())
        throw Exception("Expected non-empty new part names in ManipulationTaskParams", ErrorCodes::BAD_ARGUMENTS);
}

void executeManipulationTask(const ManipulationTaskParams & params, [[maybe_unused]] ContextPtr context)
{
    auto log = &Poco::Logger::get(__func__);

    try
    {
        if (!params.storage)
            throw Exception("No storage in manipulate task parameters", ErrorCodes::LOGICAL_ERROR);

        /// auto task = params.storage->manipulate(params, context);

        /// XXX
        // task->setManipulationEntry(context.getManipulationList().insert(task->getParams()));
        // task->getManipulationListElement()->related_node = context.getClientInfo().current_address.host().toString() + ":" + toString(params.rpc_port);

        // task->execute();

        LOG_DEBUG(log, "Finished manipulate {} ", params.task_id);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to execute " + params.toDebugString());

        /// try send empty part to server.
        // if (auto * cloud_table = dynamic_cast<MergeTreeMetaBase *>(params.storage.get());
        //     cloud_table && params.type == ManipulationType::Merge)
        //     dumpAndCommitCnchParts(*cloud_table, ManipulationType::Merge, {}, context, params.task_id);
    }
}

}
