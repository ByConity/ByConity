#include <memory>
#include <QueryPlan/PlanCache.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

void PlanCacheManager::initialize(ContextMutablePtr context)
{
    if (!context->getPlanCacheManager())
    {
        auto manager_instance = std::make_unique<PlanCacheManager>();
        context->setPlanCacheManager(std::move(manager_instance));
    }

    auto * manager_instance = context->getPlanCacheManager();
    if (manager_instance->cache)
    {
        LOG_WARNING(&Poco::Logger::get("PlanCacheManager"), "PlanCacheManager already initialized");
        return;
    }

    auto max_size = context->getConfigRef().getUInt64("optimizer.plancache.max_cache_size", PlanCacheConfig::max_cache_size);
    auto expire_time = std::chrono::seconds(
        context->getConfigRef().getUInt64("optimizer.plancache.cache_expire_time", PlanCacheConfig::cache_expire_time));
    manager_instance->initialize(max_size, expire_time);
}

void PlanCacheManager::initialize(UInt64 max_size, std::chrono::seconds expire_time)
{
    Poco::Timestamp::TimeDiff the_time = expire_time.count() * 1000;
    cache = std::make_unique<CacheType>(max_size, the_time);
}

UInt128 PlanCacheManager::hash(const ASTPtr & query_ast, const Settings & settings)
{
    String query;
    WriteBufferFromString query_buffer(query);
    serializeAST(query_ast, query_buffer);

    String settings_string;
    WriteBufferFromString buffer(settings_string);
    settings.write(buffer);

    UInt128 key;
    SipHash hash;
    hash.update(query.data(), query.size());
    hash.update(settings_string.data(), settings_string.size());
    hash.get128(key);

    return key;
}

PlanNodePtr PlanCacheManager::getNewPlanNode(PlanNodePtr node, ContextMutablePtr & context, bool cache_plan, PlanNodeId & max_id)
{
    if (max_id < node->getId())
        max_id = node->getId();

    if (node->getType() == IQueryPlanStep::Type::TableScan)
    {
        auto step = node->getStep()->copy(context);
        auto * table_step = dynamic_cast<TableScanStep *>(step.get());
        if (cache_plan)
            table_step->cleanStorage();
        else
            table_step->setStorage(context);
        return PlanNodeBase::createPlanNode(node->getId(), step, {});
    }

    PlanNodes children;
    for (auto & child : node->getChildren())
    {
        auto result_node = getNewPlanNode(child, context, cache_plan, max_id);
        if (result_node)
            children.emplace_back(result_node);
    }

    return PlanNodeBase::createPlanNode(node->getId(), node->getStep()->copy(context), children);
}

void PlanCacheManager::invalidate(ContextMutablePtr)
{
//     if (!cache)
//         throw Exception("CacheManager not initialized", ErrorCodes::LOGICAL_ERROR);

//     auto catalog = createConstCatalogAdaptor(context);
//     auto columns = catalog->getCollectableColumns(table);
//     cache->remove(std::make_pair(table.getUniqueKey(), ""));
//     for (auto & pr : columns)
//     {
//         auto & col_name = pr.name;
//         auto key = std::make_pair(table.getUniqueKey(), col_name);
//         cache->remove(key);
//     }
}

}
