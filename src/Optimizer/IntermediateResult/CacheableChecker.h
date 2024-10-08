#pragma once

#include <Common/Logger.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Poco/Logger.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SimplePlanVisitor.h>

#include <optional>
#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace CacheableChecker
{
    bool isValidForCache(PlanNodePtr node, ContextPtr context);

    // checks subtree only contains allowed nodes and only deterministic (cross queries) functions
    bool isSubtreeDeterministic(PlanNodePtr node, ContextPtr context);
    bool isAllowedInLeftMostPath(PlanNodePtr node, bool allow_join);

    struct RuntimeFilterBuildInfo
    {
        PlanNodePtr node;
        std::string symbol;
        RuntimeFilterBuildInfos info;
    };

    /**
     * @class RuntimeFilterBuildsAndProbes builder and probers collected from the QueryPlan / subtree
     */
    struct RuntimeFilterBuildsAndProbes
    {
        std::unordered_map<RuntimeFilterId, RuntimeFilterBuildInfo> builds;
        std::unordered_map<RuntimeFilterId, std::unordered_set<PlanNodePtr>> probes;
    };

    class RuntimeFilterCollector
    {
    public:
        static RuntimeFilterBuildsAndProbes collect(PlanNodePtr plan, const CTEInfo & cte_info);
        static RuntimeFilterBuildsAndProbes collect(const QueryPlan & plan) { return collect(plan.getPlanNode(), plan.getCTEInfo()); }
    private:
        static void collectImpl(PlanNodePtr node, const CTEInfo & cte_info,
                                RuntimeFilterBuildsAndProbes & res, std::unordered_set<CTEId> & visited_ctes);
        static void collectProbesImpl(ConstASTPtr filter, PlanNodePtr node, RuntimeFilterBuildsAndProbes & res);
    };

    /**
     * @class CacheableRuntimeFilters a set of cacheable runtime filters with respect to the cache subtree
     */
    struct CacheableRuntimeFilters
    {
        // a runtime filter is ignorable if all its builder and probers are in the cache subtree
        std::unordered_set<RuntimeFilterId> ignorable;
        // a runtime filter is uncacheable if
        // 1) its builder is in the cache subtree, but its prober is outside this subtree; or
        // 2) the right subtree of the builder contains non-deterministic or unsupported steps; or
        // 3) the right subtree of the builder contains a runtime filter that is not cacheable
        // otherwise the runtime filter is cacheable, and the builder info must be included in the cache digest
        std::unordered_map<RuntimeFilterId, RuntimeFilterBuildInfo> cacheable;
        
        bool isChecked(RuntimeFilterId id) const { return ignorable.contains(id) || cacheable.contains(id); }
    };

    class RuntimeFilterChecker
    {
    public:
        explicit RuntimeFilterChecker(RuntimeFilterBuildsAndProbes _query_runtime_filter_info, ContextPtr _context)
            : query_runtime_filter_info(std::move(_query_runtime_filter_info)), context(_context)
        {}

        std::optional<CacheableRuntimeFilters> check(PlanNodePtr subtree);

    private:
        const RuntimeFilterBuildsAndProbes query_runtime_filter_info;
        ContextPtr context;
        LoggerPtr log = getLogger("RuntimeFilterCacheableChecker");

        // add the rtf to checked if cachable; also add extra rtfs to unchecked_ids
        bool checkRuntimeFilterId(RuntimeFilterId id,
                                  std::unordered_set<RuntimeFilterId> & unchecked_ids,
                                  CacheableRuntimeFilters & checked);
        bool checkRecursive(std::unordered_set<RuntimeFilterId> & unchecked_ids, CacheableRuntimeFilters & checked);
    };
}

} // DB
