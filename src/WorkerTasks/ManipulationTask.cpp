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

#include <WorkerTasks/ManipulationTask.h>

#include <CloudServices/CnchPartsHelper.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCloudMergeTree.h>
#include <WorkerTasks/ManipulationList.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

ManipulationTask::ManipulationTask(ManipulationTaskParams params_, ContextPtr context_)
    : params(std::move(params_))
    , context(std::move(context_))
{
}

void ManipulationTask::setManipulationEntry()
{
    auto global_context = context->getGlobalContext();
    manipulation_entry = global_context->getManipulationList().insert(params, false, global_context);

    auto * element = manipulation_entry->get();
    element->related_node = context->getClientInfo().current_address.toString() + ":" + toString(params.rpc_port);
}

void ManipulationTask::execute()
{
    /// Make out memory tracker a parent of current thread memory tracker
    std::optional<ThreadGroupSwitcher> switcher;
    if (manipulation_entry)
    {
        switcher.emplace((*manipulation_entry)->thread_group);
    }

    executeImpl();
}

void executeManipulationTask(ManipulationTaskPtr task, MergeTreeMutableDataPartsVector all_parts)
{
    auto log = getLogger(__func__);

    try
    {
        auto storage = task->getStorage();
        auto & data = dynamic_cast<StorageCloudMergeTree &>(*storage);
        data.loadDataParts(all_parts);
        task->execute();

        LOG_DEBUG(log, "Finished manipulate {}", task->getTaskID());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to execute " + task->getParams().toDebugString());
    }
}

}
