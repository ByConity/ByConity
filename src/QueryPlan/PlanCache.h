#pragma once

#include <unordered_set>
#include <Interpreters/Context.h>
#include <QueryPlan/PlanVisitor.h>
#include <Poco/ExpireLRUCache.h>
#include <Common/HashTable/Hash.h>
#include <Interpreters/StorageID.h>

namespace DB
{
struct Analysis;
using AnalysisPtr = std::shared_ptr<Analysis>;

namespace PlanCacheConfig
{
    constexpr UInt64 max_cache_size = 1024UL ;
    constexpr UInt64 cache_expire_time = 600; // in seconds
}


class PlanCacheManager
{
public:
    struct PlanCacheInfo
    {
        // database_name->table_name->column_names
        std::unordered_map<String, std::unordered_map<String, std::vector<String>>> query_access_info;
        std::unordered_map<StorageID, Int64> stats_version;
        std::unordered_map<StorageID, UInt64> tables_version;
    };
    struct PlanObjectValue
    {
        PlanNodePtr plan_root;
        std::unordered_map<CTEId, PlanNodePtr> cte_map;
        std::shared_ptr<PlanCacheInfo> query_info;
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

    static UInt128 hash(const ASTPtr & query_ast, ContextMutablePtr & context);

    static PlanNodePtr getNewPlanNode(PlanNodePtr node, ContextMutablePtr & context, bool cache_plan, PlanNodeId & max_id);

    static QueryPlanPtr getPlanFromCache(UInt128 query_hash, ContextMutablePtr & context);
    static bool addPlanToCache(UInt128 query_hash, QueryPlanPtr & plan, AnalysisPtr analysis, ContextMutablePtr & context);

    static bool enableCachePlan(const ASTPtr & query_ast, ContextPtr context);
private:
    std::unique_ptr<CacheType> cache;
};


}
