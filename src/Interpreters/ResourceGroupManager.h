#pragma once

#include <Interpreters/InternalResourceGroup.h>
#include <Parsers/IAST.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Timer.h>

#include <atomic>
#include <regex>
#include <vector>
#include <unordered_set>

namespace DB
{

struct ResourceSelectCase
{
    enum QueryType
    {
        DDL,
        DATA,
        SELECT,
        OTHER
    };
    static std::shared_ptr<QueryType> translateQueryType(const String & queryType);
    static QueryType getQueryType(const IAST *ast);
    using Element = std::shared_ptr<std::regex>;
    String name;
    Element user;
    Element queryId;
    std::shared_ptr<QueryType> queryType;
    InternalResourceGroup *group;
};

class ResourceGroupManager
{
private:
    using Container = std::unordered_map<String, InternalResourceGroup>;
    std::list<InternalResourceGroup*> rootGroups;
    Container groups;
    std::list<ResourceSelectCase> selectCases;
    std::atomic<bool> started{false};
    std::atomic<bool> disabled{false};

    class ResourceTask : public Poco::Util::TimerTask
    {
    public:
        ResourceTask(ResourceGroupManager *manager_)
                :manager(manager_) {}
        virtual void run() override
        {
            for (auto group : manager->rootGroups)
            {
                group->processQueuedQueues();
            }
        }

    private:
        ResourceGroupManager *manager;
    };

    Poco::Util::Timer timer;
public:
    InternalResourceGroup* selectGroup(const Context & query_context, const IAST * ast);
    bool isInUse() const { return !disabled.load(std::memory_order_relaxed) && started.load(std::memory_order_relaxed); }
    void enable();
    void disable();
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);
    ResourceGroupManager() {}
    ~ResourceGroupManager();

    using Info = std::vector<InternalResourceGroupInfo>;
    Info getInfo() const;
};

}
