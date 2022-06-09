#pragma once

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

class ManipulationTask : private boost::noncopyable
{
public:
    ManipulationTask(ManipulationTaskParams params, ContextPtr context_);
    virtual ~ManipulationTask() {}

    virtual void execute() {}

    virtual bool isCancelled() { return getManipulationListElement()->is_cancelled.load(std::memory_order_relaxed); }
    virtual void setCancelled() { getManipulationListElement()->is_cancelled.store(true, std::memory_order_relaxed); }

    void setManipulationEntry(ManipulationListEntry && entry) { manipulation_entry.emplace(std::move(entry)); }

    ManipulationListElement * getManipulationListElement() { return manipulation_entry->get(); }

    auto & getParams() { return params; }

protected:
    ManipulationTaskParams params;
    ContextPtr context;

    std::optional<ManipulationListEntry> manipulation_entry;
    //std::atomic_bool is_cancelled{false};
};

using ManipulationTaskPtr = std::shared_ptr<ManipulationTask>;

/// Sync
void executeManipulationTask(const ManipulationTaskParams & params, ContextPtr context);

}
