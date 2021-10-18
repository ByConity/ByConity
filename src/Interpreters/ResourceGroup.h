#pragma once

#include <common/types.h>

namespace DB
{
class ResourceGroup
{
public:
    Int64 getSoftMaxMemoryUsage() const { return soft_max_memory_usage; }

    void setSoftMaxMemoryUsage(Int64 softMaxMemoryUsage) { soft_max_memory_usage = softMaxMemoryUsage; }

    Int64 getMinQueryMemoryUsage() const { return min_query_memory_usage; }

    void setMinQueryMemoryUsage(Int64 minQueryMemoryUsage) { min_query_memory_usage = minQueryMemoryUsage; }

    Int32 getMaxConcurrentQueries() const { return max_concurrent_queries; }

    void setMaxConcurrentQueries(Int32 maxConcurrentQueries) { max_concurrent_queries = maxConcurrentQueries; }

    Int32 getMaxQueued() const { return max_queued; }

    void setMaxQueued(Int32 maxQueued) { max_queued = maxQueued; }

    Int32 getMaxQueuedWaitingMs() const { return max_queued_waiting_ms; }

    void setMaxQueuedWaitingMs(Int32 maxQueuedWaitingMs) { max_queued_waiting_ms = maxQueuedWaitingMs; }

    Int32 getPriority() const { return priority; }

    void setPriority(Int32 priority_) { priority = priority_; }

    const String & getName() const { return name; }

    void setName(const String & name_) { name = name_; }

    const String & getParentResourceGroup() const { return parent_resource_group; }

    void setParentResourceGroup(const String & parentResourceGroup) { parent_resource_group = parentResourceGroup; }

protected:
    Int64 soft_max_memory_usage = 0;
    Int64 min_query_memory_usage = 0;
    Int32 max_concurrent_queries = 0;
    Int32 max_queued = 0;
    Int32 max_queued_waiting_ms = 0;
    Int32 priority = 0;
    String name;
    String parent_resource_group;
};

}
