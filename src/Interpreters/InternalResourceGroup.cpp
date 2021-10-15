#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalResourceGroup.h>
#include <Interpreters/ProcessList.h>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_NOT_ENOUGH;
    extern const int WAIT_FOR_RESOURCE_TIMEOUT;
    extern const int RESOURCE_GROUP_INTERNAL_ERROR;
}

InternalResourceGroup* InternalResourceGroup::getParent() const
{
    return parent;
}

void InternalResourceGroup::setParent(InternalResourceGroup* parent_)
{
    parent = parent_;
    root = parent->root;
    parent->subGroups[name] = this;
}

InternalResourceGroup::QueryEntity::QueryEntity(InternalResourceGroup *group_, const String &query_,
    const Context &query_context_, QueryStatusType statusType_)
    : group(group_), query(query_), query_context(&query_context_),
    statusType(statusType_), id(group->root->id.fetch_add(1, std::memory_order_relaxed)) {}

bool InternalResourceGroup::canRunMore() const
{
    return (max_concurrent_queries == 0 ||
            static_cast<Int32>(runningQueries.size()) + descendentRunningQueries < max_concurrent_queries)
    && (soft_max_memory_usage == 0 || cachedMemoryUsageBytes < soft_max_memory_usage);
}

bool InternalResourceGroup::canQueueMore() const
{
    return static_cast<Int32>(queuedQueries.size()) + descendentQueuedQueries < max_queued;
}

void InternalResourceGroup::queryFinished(InternalResourceGroup::Container::iterator entityIt)
{
    std::unique_lock lock(root->mutex);
    runningQueries.erase(entityIt);
    InternalResourceGroup *group = parent;
    while (group)
    {
        --group->descendentRunningQueries;
        group = group->parent;
    }
}

InternalResourceGroup::Container::iterator InternalResourceGroup::run(const String &query, const Context &query_context)
{
    std::unique_lock lock(root->mutex);
    bool canRun = true;
    bool canQueue = true;
    InternalResourceGroup *group = this;
    while (group)
    {
        canRun &= group->canRunMore();
        canQueue &= group->canQueueMore();
        group = group->parent;
    }
    if (!canQueue && !canRun)
        throw Exception("The resource is not enough for group " + name, ErrorCodes::RESOURCE_NOT_ENOUGH);
    InternalResourceGroup::Element element = std::make_shared<InternalResourceGroup::QueryEntity>(this, query, query_context);
    if (canRun)
        return runQuery(element);
    auto it = enqueueQuery(element);
    if (!root->can_run.wait_for(lock, std::chrono::milliseconds(max_queued_waiting_ms), [&]{ return element->statusType != QueryStatusType::WAITING; }))
    {
        queuedQueries.erase(it);
        InternalResourceGroup *s_group = parent;
        while (s_group)
        {
            --s_group->descendentQueuedQueries;
            s_group = s_group->parent;
        }
        throw Exception("Waiting for resource timeout", ErrorCodes::WAIT_FOR_RESOURCE_TIMEOUT);
    }
    auto res = std::find(runningQueries.begin(), runningQueries.end(), element);
    if (res == runningQueries.end())
        throw Exception("The running query can not be found in the resource group " + name, ErrorCodes::RESOURCE_GROUP_INTERNAL_ERROR);
    return res;
}

InternalResourceGroup::Container::iterator InternalResourceGroup::enqueueQuery(InternalResourceGroup::Element & element)
{
    Container::iterator it = queuedQueries.emplace(queuedQueries.end(), element);
    InternalResourceGroup *group = parent;
    while (group)
    {
        ++group->descendentQueuedQueries;
        group = group->parent;
    }
    return it;
}

InternalResourceGroup::Container::iterator InternalResourceGroup::runQuery(InternalResourceGroup::Element & element)
{
    element->queryStatus = nullptr;
    Container::iterator it = runningQueries.emplace(runningQueries.end(), element);
    element->statusType = QueryStatusType::RUNNING;
    cachedMemoryUsageBytes += min_query_memory_usage;
    InternalResourceGroup *group = parent;
    while (group)
    {
        ++group->descendentRunningQueries;
        group->cachedMemoryUsageBytes += min_query_memory_usage;
        group = group->parent;
    }
    return it;
}

void InternalResourceGroup::internalRefreshStats()
{
    Int64 newCacheMemoryUsage = 0, queryMemoryUsage;
    for (auto const & query : runningQueries)
    {
        queryMemoryUsage = 0;
        if (query->queryStatus != nullptr) queryMemoryUsage = query->queryStatus->getUsedMemory();
        newCacheMemoryUsage += queryMemoryUsage < min_query_memory_usage ? min_query_memory_usage : queryMemoryUsage;
    }
    for (auto const & item : subGroups)
    {
        item.second->internalRefreshStats();
        newCacheMemoryUsage += item.second->cachedMemoryUsageBytes;
    }
    cachedMemoryUsageBytes = newCacheMemoryUsage;
}

bool InternalResourceGroup::internalProcessNext()
{
    if (!canRunMore())
        return false;
    if (eligibleGroups.size() != subGroups.size() + 1)
    {
        eligibleGroups.clear();
        eligibleGroups.push_back(this);
        for (auto & item : subGroups)
            eligibleGroups.push_back(item.second);
        eligibleGroupIterator = eligibleGroups.begin();
    }
    size_t inEligibleGroupsNum = 0;
    while (inEligibleGroupsNum < eligibleGroups.size())
    {
        if (eligibleGroupIterator == eligibleGroups.end())
            eligibleGroupIterator = eligibleGroups.begin();
        if (*eligibleGroupIterator == this)
        {
            if (!queuedQueries.empty())
            {
                runQuery(queuedQueries.front());
                queuedQueries.erase(queuedQueries.begin());
                InternalResourceGroup *group = parent;
                while (group)
                {
                    --group->descendentQueuedQueries;
                    group = group->parent;
                }
                eligibleGroupIterator++;
                return true;
            }
            else
            {
                eligibleGroupIterator++;
                ++inEligibleGroupsNum;
            }
        }
        else
        {
            if ((*eligibleGroupIterator)->internalProcessNext())
            {
                eligibleGroupIterator++;
                return true;
            }
            else
            {
                eligibleGroupIterator++;
                ++inEligibleGroupsNum;
            }
        }
    }
    return false;
}

void InternalResourceGroup::setRoot()
{
    root = this;
}

void InternalResourceGroup::processQueuedQueues()
{
    std::unique_lock lock(root->mutex);
    internalRefreshStats();
    bool processed = false;
    while (internalProcessNext())
    {
        // process all
        processed = true;
    }
    if (processed) root->can_run.notify_all();
}

InternalResourceGroupInfo InternalResourceGroup::getInfo() const
{
    std::unique_lock lock(root->mutex);
    InternalResourceGroupInfo info;
    info.name = name;
    info.can_run_more = canRunMore();
    info.can_queue_more = canQueueMore();
    info.soft_max_memory_usage = soft_max_memory_usage;
    info.cachedMemoryUsageBytes = cachedMemoryUsageBytes;
    info.max_concurrent_queries = max_concurrent_queries;
    info.runningQueries = runningQueries.size() + descendentRunningQueries;
    info.max_queued = max_queued;
    info.queuedQueries = queuedQueries.size() + descendentQueuedQueries;
    info.priority = priority;
    info.parent_resource_group = parent_resource_group;
    return info;
}

}
