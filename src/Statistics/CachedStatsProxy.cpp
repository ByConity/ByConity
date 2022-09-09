#include <Statistics/CacheManager.h>
#include <Statistics/CachedStatsProxy.h>
#include <Statistics/CommonTools.h>
#include <Poco/SharedPtr.h>

namespace DB::Statistics
{

// No cache policy
class CachedStatsProxyDirectImpl : public CachedStatsProxy
{
public:
    CachedStatsProxyDirectImpl(const CatalogAdaptorPtr & catalog_) : catalog(catalog_) { }
    StatsData get(const StatsTableIdentifier & table_id) override
    {
        auto columns = catalog->getCollectableColumns(table_id);
        return get(table_id, true, columns);
    }

    StatsData get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns) override
    {
        StatsData result;
        if (table_info)
        {
            result.table_stats = catalog->readSingleStats(table_id, std::nullopt);
        }

        for (auto & pr : columns)
        {
            auto & col_name = pr.name;
            result.column_stats.emplace(pr.name, catalog->readSingleStats(table_id, col_name));
        }
        return result;
    }
    void put(const StatsTableIdentifier & table_id, StatsData && data) override { catalog->writeStatsData(table_id, data); }
    void drop(const StatsTableIdentifier & table_id) override { catalog->dropStatsData(table_id); }

private:
    CatalogAdaptorPtr catalog;
};


// cached policy
class CachedStatsProxyImpl : public CachedStatsProxy
{
public:
    CachedStatsProxyImpl(const CatalogAdaptorPtr & catalog_) : catalog(catalog_) { }
    StatsData get(const StatsTableIdentifier & table_id) override;
    StatsData get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns) override;
    void put(const StatsTableIdentifier & table_id, StatsData && data) override;
    void drop(const StatsTableIdentifier & table_id) override;

private:
    CatalogAdaptorPtr catalog;
};


StatsData CachedStatsProxyImpl::get(const StatsTableIdentifier & table_id)
{
    auto columns = catalog->getCollectableColumns(table_id);
    return get(table_id, true, columns);
}

template <typename T, typename... Args>
static Poco::SharedPtr<T> makePocoShared(Args &&... args)
{
    return Poco::SharedPtr<T>(new T(std::forward<Args>(args)...));
}

StatsData CachedStatsProxyImpl::get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns)
{
    StatsData result;
    auto & cache = Statistics::CacheManager::instance();

    if (table_info)
    {
        // collect table stats
        auto col_name = ""; // empty string represent table info
        auto key = std::make_pair(table_id.getUniqueKey(), col_name);
        auto item_ptr = cache.get(key);
        if (!item_ptr)
        {
            item_ptr = makePocoShared<StatsCollection>(catalog->readSingleStats(table_id, std::nullopt));
            cache.update(key, item_ptr);
        }
        result.table_stats = *item_ptr;
    }

    for (auto & pr : columns)
    {
        auto & col_name = pr.name;
        auto key = std::make_pair(table_id.getUniqueKey(), col_name);
        auto item_ptr = cache.get(key);
        if (!item_ptr)
        {
            item_ptr = makePocoShared<StatsCollection>(catalog->readSingleStats(table_id, col_name));
            cache.update(key, item_ptr);
        }
        result.column_stats.emplace(pr.name, *item_ptr);
    }
    return result;
}

void CachedStatsProxyImpl::put(const StatsTableIdentifier & table_id, StatsData && data)
{
    catalog->writeStatsData(table_id, data);
}

void CachedStatsProxyImpl::drop(const StatsTableIdentifier & table_id)
{
    catalog->dropStatsData(table_id);
}


// dispatcher
CachedStatsProxyPtr createCachedStatsProxy(const CatalogAdaptorPtr & catalog)
{
    if (catalog->getSettingsRef().enable_memory_catalog)
    {
        return std::make_unique<CachedStatsProxyDirectImpl>(catalog);
    }
    else
    {
        return std::make_unique<CachedStatsProxyImpl>(catalog);
    }
}


} // namespace DB
