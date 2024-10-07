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

#pragma once

#include <Common/Logger.h>
#include <Core/Types.h>
#include <WorkerTasks/ManipulationList.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <WorkerTasks/ManipulationType.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <atomic>
#include <optional>
#include <boost/noncopyable.hpp>

namespace DB
{

class ManipulationTask: private boost::noncopyable
{
public:
    ManipulationTask(ManipulationTaskParams params, ContextPtr context_);
    virtual ~ManipulationTask() = default;

    virtual void executeImpl() = 0;

    void execute();

    bool isCancelled(UInt64 timeout = 0)
    {
        if (getManipulationListElement()->is_cancelled.load(std::memory_order_relaxed))
            return true;

        if (!timeout)
            return false;

        if (static_cast<UInt64>(time(nullptr) - getManipulationListElement()->last_touch_time.load(std::memory_order_relaxed)) > timeout)
        {
            LOG_TRACE(getLogger("ManipulationTask"),
                      "Set is_cancelled for task {} as no heartbeat from server.", getManipulationListElement()->task_id);
            setCancelled();
            return true;
        }
        return false;
    }

    virtual void setCancelled() { getManipulationListElement()->is_cancelled.store(true, std::memory_order_relaxed); }

    void setManipulationEntry();

    ManipulationListElement * getManipulationListElement() { return manipulation_entry->get(); }

    auto & getParams() { return params; }

    StoragePtr getStorage() const { return params.storage; }

    auto getTaskID() const { return params.task_id; }

    ContextPtr getContext() const { return context; }

protected:
    ManipulationTaskParams params;

    std::unique_ptr<ManipulationListEntry> manipulation_entry;

    /// ManipulationTask should hold context by itself, because of ManipulationTask is executed in BackgroundPool
    ContextPtr context;
};

using ManipulationTaskPtr = std::shared_ptr<ManipulationTask>;

void executeManipulationTask(ManipulationTaskPtr task, MergeTreeMutableDataPartsVector all_parts);

}
