#pragma once

#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Optimizer/IntermediateResult/CacheParam.h>
#include <Optimizer/IntermediateResult/CacheableChecker.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/IntermediateResultCacheStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/TableScanStep.h>
#include <Common/LinkedHashMap.h>
#include <Common/SipHash.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

class CacheParamBuilder : private PlanSignatureProvider
{
public:
    explicit CacheParamBuilder(PlanNodePtr _cache_root,
                               ContextPtr _context,
                               CacheableChecker::CacheableRuntimeFilters _runtime_filters,
                               PlanSignatureProvider base_provider)
        : PlanSignatureProvider(std::move(base_provider)), cache_root(_cache_root), context(_context), cacheable_runtime_filters(std::move(_runtime_filters))
    {}

    CacheParam buildCacheParam();
    /**
     * A digest for semantic equivalence. Same digest means same plan. Output order is allowed to differ.
     */
    PlanSignature getDigest()
    {
        auto plan_signature = computeSignature(cache_root);
        return combineSettings(plan_signature, context->getSettingsRef().changes());
    }
    /**
     * The cache may have a different column order wrt cache_root.
     * e.g. AggregationStep outputs [a, sum(), count()], but in cache, the order may be [count(), a, sum()]
     * this function gets the output order of cache, namely [count(), a, sum()] above.
     * Comparing it with root->output_stream gives the matching between cache column order and query column order
     */
    Block getCacheOrder() { return computeNormalOutputOrder(cache_root); }
    /**
     * The normal plan is a plan in "index-reference".
     * e.g. TableScan['a','b']=>TableScan['__$0','__$1'], Projection[expr=a+b]=>Projection[__$2=__$0+__$1]
     * Compared to cache_root, the tree structure is the same, but the symbols are remapped, and the output/expr orders are changed
     */
    PlanNodePtr getNormalPlan() { return computeNormalPlan(cache_root); }
    /**
     * For cache to match, runtime filters must match as well.
     * For a cached runtime filter, the right subtree of its builder is included into the digest
     * Its table name is also collected for matching
     */
    std::unordered_set<RuntimeFilterId> getIncludedRuntimeFilters()
    {
        std::unordered_set<RuntimeFilterId> res;
        for (const auto & pair : cacheable_runtime_filters.cacheable)
            res.insert(pair.first);
        return res;
    }
    /**
     * For cache to match, runtime filters must match as well.
     * For a ignored runtime filter, it is not used outside of the cache, so the digest does tnot need to include it
     */
    std::unordered_set<RuntimeFilterId> getIgnoredRuntimeFilters() { return cacheable_runtime_filters.ignorable; }

protected:
    size_t computeStepHash(PlanNodePtr node) override;

    // before calling hash, need to remove runtime filter effects
    size_t computeJoinHash(std::shared_ptr<JoinStep> join_step);
    size_t computeFilterHash(std::shared_ptr<FilterStep> filter_step);
    size_t computeTableScanHash(std::shared_ptr<TableScanStep> table_scan_step);
    size_t computeAggregatingHash(std::shared_ptr<AggregatingStep> aggregating_step);

    size_t computeRuntimeFilterHash(RuntimeFilterId id);
    ASTPtr replaceRuntimeFilterIdByHash(ConstASTPtr filter);

private:
    PlanNodePtr cache_root;
    ContextPtr context;
    const CacheableChecker::CacheableRuntimeFilters cacheable_runtime_filters;
    mutable std::unordered_map<RuntimeFilterId, size_t> runtime_filter_hash;

    void collectAndMarkTablesDFS(PlanNodePtr root, std::vector<StorageID> & tables, const std::string & digest);
};

}
