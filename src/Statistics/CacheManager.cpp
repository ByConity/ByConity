#include <memory>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/Parameters.h>

namespace DB::Statistics
{

std::unique_ptr<CacheManager::CacheType> CacheManager::cache;

void CacheManager::initialize(ContextPtr context)
{
    if (cache)
    {
        LOG_WARNING(&Poco::Logger::get("CacheManager"), "CacheManager already initialized");
        return;
    }
    auto max_size = context->getConfigRef().getUInt64("optimizer.statistics.max_cache_size", ConfigParameters::max_cache_size);

    auto expire_time = std::chrono::seconds(
        context->getConfigRef().getUInt64("optimizer.statistics.cache_expire_time", ConfigParameters::cache_expire_time));
    initialize(max_size, expire_time);
}

void CacheManager::initialize(UInt64 max_size, std::chrono::seconds expire_time)
{
    Poco::Timestamp::TimeDiff the_time = expire_time.count() * 1000;
    cache = std::make_unique<CacheType>(max_size, the_time);
}

void CacheManager::invalidate(ContextPtr context, const StatsTableIdentifier & table)
{
    if (!cache)
        throw Exception("CacheManager not initialized", ErrorCodes::LOGICAL_ERROR);

    auto catalog = createConstCatalogAdaptor(context);
    auto columns = catalog->getCollectableColumns(table);
    cache->remove(std::make_pair(table.getUniqueKey(), ""));
    for (auto & pr : columns)
    {
        auto & col_name = pr.name;
        auto key = std::make_pair(table.getUniqueKey(), col_name);
        cache->remove(key);
    }
}

} // namespace DB::Statistics
