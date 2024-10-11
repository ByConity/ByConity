#include <memory>
#include <Analyzers/Analysis.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanCache.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/VersionHelper.h>
#include <Interpreters/StorageID.h>
#include <QueryPlan/ReadStorageRowCountStep.h>

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
        LOG_WARNING(getLogger("PlanCacheManager"), "PlanCacheManager already initialized");
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

UInt128 PlanCacheManager::hash(const ASTPtr & query_ast, ContextMutablePtr & context)
{
    const auto & settings = context->getSettingsRef();
    String query;
    WriteBufferFromString query_buffer(query);
    serializeAST(query_ast, query_buffer);

    String settings_string;
    WriteBufferFromString buffer(settings_string);
    const static std::unordered_set<String> whitelist{"enable_plan_cache", "force_plan_cache"};
    settings.write(buffer, SettingsWriteFormat::DEFAULT, whitelist);

    UInt128 key;
    SipHash hash;
    hash.update(query.data(), query.size());
    hash.update(settings_string.data(), settings_string.size());

    String current_database = context->getCurrentDatabase();
    hash.update(current_database.data(), current_database.size());

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

QueryPlanPtr PlanCacheManager::getPlanFromCache(UInt128 query_hash, ContextMutablePtr & context)
{
    if (!context->getPlanCacheManager())
        throw Exception("plan cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    auto & cached = context->getPlanCacheManager()->instance();
    try
    {
        auto plan_object = cached.get(query_hash);
        if (!plan_object || !plan_object->plan_root)
            return nullptr;

        // check storage version
        for (auto & item : plan_object->query_info->tables_version)
        {
            auto storage = DatabaseCatalog::instance().tryGetTable(item.first, context);
            if (!storage || storage->latest_version.toUInt64() != item.second)
            {
                cached.remove(query_hash);
                return nullptr;
            }
        }

        // check statistic version
        for (auto & item : plan_object->query_info->stats_version)
        {
            Statistics::StatsTableIdentifier table_identifier{item.first};
            auto version_value = Statistics::getVersion(context, table_identifier);
            Int64 version = version_value.has_value() ? version_value.value().convertTo<Int64>() : 0;
            if (version != item.second)
            {
                cached.remove(query_hash);
                return nullptr;
            }      
        }

        PlanNodeId max_id;
        auto root  = PlanCacheManager::getNewPlanNode(plan_object->plan_root, context, false, max_id);
        CTEInfo cte_info;
        for (auto & cte : plan_object->cte_map)
            cte_info.add(cte.first, PlanCacheManager::getNewPlanNode(cte.second, context, false, max_id));

        if (plan_object->query_info && context->hasQueryContext())
        {
            for (auto & [database, table_info] : plan_object->query_info->query_access_info)
            {
                for (auto & [table, columns] : table_info)
                    context->addQueryAccessInfo(database, table, columns);
            }
        }

        context->createPlanNodeIdAllocator(max_id+1);
        return  std::make_unique<QueryPlan>(root, cte_info, context->getPlanNodeIdAllocator());
    }
    catch (...)
    {
        cached.remove(query_hash);
        return nullptr;
    }
}

bool PlanCacheManager::addPlanToCache(UInt128 query_hash, QueryPlanPtr & plan, AnalysisPtr analysis, ContextMutablePtr & context)
{
    if (!context->getPlanCacheManager())
        throw Exception("plan cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    auto & cache = context->getPlanCacheManager()->instance();
    auto root = plan->getPlanNode();
    UInt32 size = QueryPlan::getPlanNodeCount(root);

    if (size > context->getSettingsRef().max_plannode_count)
        return false;

    PlanNodeId max_id;
    PlanCacheManager::PlanObjectValue plan_object{};
    plan_object.plan_root = PlanCacheManager::getNewPlanNode(root, context, true, max_id);

    for (const auto & cte : plan->getCTEInfo().getCTEs())
        plan_object.cte_map.emplace(cte.first, PlanCacheManager::getNewPlanNode(cte.second, context, true, max_id));

    plan_object.query_info = std::make_shared<PlanCacheManager::PlanCacheInfo>();
    const auto & used_columns_map = analysis->getUsedColumns();
    for (const auto & [table_ast, storage_analysis] : analysis->getStorages())
    {
        if (!storage_analysis.storage)
            continue;
        auto storage_id = storage_analysis.storage->getStorageID();
        if (auto it = used_columns_map.find(storage_id); it != used_columns_map.end())
        {
            for (const auto & column : it->second)
                plan_object.query_info->query_access_info[backQuoteIfNeed(storage_id.getDatabaseName())][storage_id.getFullTableName()].emplace_back(column);
        }

        Statistics::StatsTableIdentifier table_identifier{storage_id};
        auto version_value = Statistics::getVersion(context, table_identifier);
        Int64 version = version_value.has_value() ? version_value.value().convertTo<Int64>() : 0;
        plan_object.query_info->stats_version[storage_id] = version;
        plan_object.query_info->tables_version[storage_id] = storage_analysis.storage->latest_version.toUInt64();
    }
    cache.add(query_hash, plan_object);
    return true;
}

bool PlanCacheManager::enableCachePlan(const ASTPtr & query_ast, ContextPtr context)
{
    if (!context->getSettingsRef().enable_plan_cache)
        return false;
    
    return query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>() || query_ast->as<ASTSelectIntersectExceptQuery>();
}

}
