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

class ManipulationTask : public WithContext, private boost::noncopyable
{
public:
    ManipulationTask(ManipulationTaskParams params, ContextPtr context_);
    virtual ~ManipulationTask() = default;

    virtual void executeImpl() = 0;

    void execute();

    virtual bool isCancelled() { return getManipulationListElement()->is_cancelled.load(std::memory_order_relaxed); }
    virtual void setCancelled() { getManipulationListElement()->is_cancelled.store(true, std::memory_order_relaxed); }

    void setManipulationEntry();

    ManipulationListElement * getManipulationListElement() { return manipulation_entry->get(); }

    auto & getParams() { return params; }

protected:
    ManipulationTaskParams params;

    std::unique_ptr<ManipulationListEntry> manipulation_entry;
};

using ManipulationTaskPtr = std::shared_ptr<ManipulationTask>;

/// Async
void executeManipulationTask(ManipulationTaskParams params, ContextPtr context);

}
