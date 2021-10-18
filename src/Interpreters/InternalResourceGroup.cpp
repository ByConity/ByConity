#include <chrono>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalResourceGroup.h>
#include <Interpreters/ProcessList.h>
#include <Common/Exception.h>

namespace DB
{
#define RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(group, func) \
    do \
    { \
        for (auto * s_group = group; s_group; s_group = s_group->parent) \
        { \
            func(s_group); \
            s_group = s_group->parent; \
        } \
    } while (0)


namespace ErrorCodes
{
    extern const int RESOURCE_NOT_ENOUGH;
    extern const int WAIT_FOR_RESOURCE_TIMEOUT;
    extern const int RESOURCE_GROUP_INTERNAL_ERROR;
}

InternalResourceGroup * InternalResourceGroup::getParent() const
{
    return parent;
}

void InternalResourceGroup::setParent(InternalResourceGroup * parent_)
{
    parent = parent_;
    root = parent->root;
    parent->subGroups[name] = this;
}

InternalResourceGroup::QueryEntity::QueryEntity(
    InternalResourceGroup * group_, const String & query_, const Context & query_context_, QueryStatusType statusType_)
    : group(group_)
    , query(query_)
    , query_context(&query_context_)
    , statusType(statusType_)
    , id(group->root->id.fetch_add(1, std::memory_order_relaxed))
{
}

bool InternalResourceGroup::canRunMore() const
{
    return (max_concurrent_queries == 0 || static_cast<Int32>(runningQueries.size()) + descendentRunningQueries < max_concurrent_queries)
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
    RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(parent, [](InternalResourceGroup * group) { --group->descendentRunningQueries; });
}

InternalResourceGroup::Container::iterator InternalResourceGroup::run(const String & query, const Context & query_context)
{
    std::unique_lock lock(root->mutex);

    bool canRun = true;
    bool canQueue = true;
    RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(this, [&](InternalResourceGroup * group) {
        canRun &= group->canRunMore();
        canQueue &= group->canQueueMore();
    });

    if (!canQueue && !canRun)
    {
        throw Exception("The resource is not enough for group " + name, ErrorCodes::RESOURCE_NOT_ENOUGH);
    }

    auto element = std::make_shared<InternalResourceGroup::QueryEntity>(this, query, query_context);
    if (canRun)
    {
        return runQuery(element);
    }

    auto it = enqueueQuery(element);
    if (!root->can_run.wait_for(
            lock, std::chrono::milliseconds(max_queued_waiting_ms), [&] { return element->statusType != QueryStatusType::WAITING; }))
    {
        queuedQueries.erase(it);
        RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(parent, [](InternalResourceGroup * group) { --group->descendentQueuedQueries; });

        throw Exception("Waiting for resource timeout", ErrorCodes::WAIT_FOR_RESOURCE_TIMEOUT);
    }

    if (auto res = std::find(runningQueries.begin(), runningQueries.end(), element); res != runningQueries.end())
    {
        return res;
    }
    throw Exception("The running query can not be found in the resource group " + name, ErrorCodes::RESOURCE_GROUP_INTERNAL_ERROR);
}

InternalResourceGroup::Container::iterator InternalResourceGroup::enqueueQuery(InternalResourceGroup::Element & element)
{
    Container::iterator it = queuedQueries.emplace(queuedQueries.end(), element);
    RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(parent, [](InternalResourceGroup * group) { ++group->descendentQueuedQueries; });
    return it;
}

InternalResourceGroup::Container::iterator InternalResourceGroup::runQuery(InternalResourceGroup::Element & element)
{
    element->queryStatus = nullptr;
    Container::iterator it = runningQueries.emplace(runningQueries.end(), element);
    element->statusType = QueryStatusType::RUNNING;
    cachedMemoryUsageBytes += min_query_memory_usage;
    RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(parent, [this](InternalResourceGroup * group) {
        ++group->descendentRunningQueries;
        group->cachedMemoryUsageBytes += min_query_memory_usage;
    });
    return it;
}

void InternalResourceGroup::internalRefreshStats()
{
    Int64 newCacheMemoryUsage = 0, queryMemoryUsage;
    for (auto const & query : runningQueries)
    {
        queryMemoryUsage = 0;
        if (query->queryStatus != nullptr)
        {
            queryMemoryUsage = query->queryStatus->getUsedMemory();
        }
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
    {
        return false;
    }

    if (eligibleGroups.size() != subGroups.size() + 1)
    {
        eligibleGroups.clear();
        eligibleGroups.push_back(this);
        for (auto & item : subGroups)
        {
            eligibleGroups.push_back(item.second);
        }
        eligibleGroupIterator = eligibleGroups.begin();
    }

    for (size_t inEligibleGroupNum = 0; inEligibleGroupNum < std::size(eligibleGroups); ++inEligibleGroupNum, ++eligibleGroupIterator)
    {
        if (eligibleGroupIterator == eligibleGroups.end())
        {
            eligibleGroupIterator = eligibleGroups.begin();
        }

        if (*eligibleGroupIterator == this)
        {
            if (!queuedQueries.empty())
            {
                runQuery(queuedQueries.front());
                queuedQueries.erase(queuedQueries.begin());
                RECURSIVE_UPDATE_INTERNAL_RESOURCE_GROUP(parent, [](InternalResourceGroup * group) { --group->descendentRunningQueries; });
                ++eligibleGroupIterator;
                return true;
            }
        }
        else if ((*eligibleGroupIterator)->internalProcessNext())
        {
            ++eligibleGroupIterator;
            return true;
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
    if (processed)
    {
        root->can_run.notify_all();
    }
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

} // namespace DB
