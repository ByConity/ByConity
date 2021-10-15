#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

#include <Core/Types.h>
#include <Interpreters/ResourceGroup.h>

namespace DB
{

enum class QueryStatusType
{
    WAITING,
    RUNNING
};

class QueryStatus;
class Context;

struct InternalResourceGroupInfo
{
    String name;
    UInt8 can_run_more;
    UInt8 can_queue_more;
    Int64 soft_max_memory_usage;
    Int32 max_concurrent_queries;
    Int32 max_queued;
    Int32 priority;
    String parent_resource_group;
    Int64 cachedMemoryUsageBytes;
    Int32 runningQueries;
    Int32 queuedQueries;
};

class InternalResourceGroup : public ResourceGroup
{
public:
    struct QueryEntity
    {
    public:
        InternalResourceGroup *group;
        String query;
        const Context *query_context;
        QueryStatusType statusType = QueryStatusType::WAITING;
        Int32 id;
        /// set after run
        QueryStatus *queryStatus;
        bool operator==(const QueryEntity & other) { return id == other.id;}
        QueryEntity(InternalResourceGroup *group_, const String &query_,
                    const Context &query_context_, QueryStatusType statusType_ = QueryStatusType::WAITING);
    };

    using Element = std::shared_ptr<QueryEntity>;
    using Container = std::list<Element>;

    struct QueryEntityHandler
    {
    private:
        Container::iterator entityIt;

    public:
        QueryEntityHandler(Container::iterator entityIt_): entityIt(entityIt_) {}
        ~QueryEntityHandler()
        {
            InternalResourceGroup *group = (*entityIt)->group;
            group->queryFinished(entityIt);
        }
    };

    using Handle = std::shared_ptr<QueryEntityHandler>;

    Handle insert(Container::iterator entityId)
    {
        return std::make_shared<QueryEntityHandler>(entityId);
    }

    InternalResourceGroup* getParent() const;
    void setParent(InternalResourceGroup* parent);
    Container::iterator run(const String & query, const Context & query_context);
    void processQueuedQueues();
    void setRoot();
    InternalResourceGroupInfo getInfo() const;
    bool canRunMore() const;
    bool canQueueMore() const;
    bool isLeaf() const { return subGroups.empty(); }

protected:
    void internalRefreshStats();
    bool internalProcessNext();
    void queryFinished(Container::iterator entityIt);
    Container::iterator enqueueQuery(Element & element);
    Container::iterator runQuery(Element & element);

    InternalResourceGroup* root = nullptr;
    InternalResourceGroup* parent = nullptr;
    std::unordered_map<String, InternalResourceGroup*> subGroups;
    std::list<InternalResourceGroup*> eligibleGroups;
    std::list<InternalResourceGroup*>::iterator eligibleGroupIterator;
    Container  runningQueries;
    Int32 descendentRunningQueries = 0;
    Container queuedQueries;
    Int32 descendentQueuedQueries = 0;
    Int64 cachedMemoryUsageBytes = 0;
    std::atomic<Int32> id {0};
    mutable std::mutex mutex;
    mutable std::condition_variable can_run;
};

}
