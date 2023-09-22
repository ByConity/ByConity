#pragma once

#include <Interpreters/Context.h>
#include <Poco/ExpireLRUCache.h>
#include <Common/HashTable/Hash.h>
#include <QueryPlan/PlanVisitor.h>

namespace DB
{

namespace PlanCacheConfig
{
    constexpr UInt64 max_cache_size = 1024UL ;
    constexpr UInt64 cache_expire_time = 600; // in seconds
}


class PlanCacheManager
{
public:
    struct PlanObjectValue
    {
        PlanNodePtr plan_root;
        std::unordered_map<CTEId, PlanNodePtr> cte_map;
    };
    using CacheType = Poco::ExpireLRUCache<UInt128, PlanObjectValue>;

    static void initialize(ContextMutablePtr context);
    // for testing
    void initialize(UInt64 max_size, std::chrono::seconds expire_time);

    CacheType & instance()
    {
        if (!cache)
        {
            throw Exception("plan cache has to be initialized", ErrorCodes::LOGICAL_ERROR);
        }
        return *cache;
    }

    // invalidate cache on current server
    void invalidate(ContextMutablePtr context);

    static UInt128 hash(const ASTPtr & query_ast, const Settings & settings);

    static PlanNodePtr getNewPlanNode(PlanNodePtr node, ContextMutablePtr & context, bool cache_plan, PlanNodeId & max_id);
private:
    std::unique_ptr<CacheType> cache;
};


}
