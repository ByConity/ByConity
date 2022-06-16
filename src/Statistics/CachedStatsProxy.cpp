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
        auto all_stats = catalog->readStatsData(table_id);
        if (table_info)
        {
            result.table_stats = all_stats.table_stats;
        }

        for (const auto & pr : columns)
        {
            const auto & col_name = pr.name;
            if (all_stats.column_stats.count(col_name))
            {
                result.column_stats.emplace(pr.name, all_stats.column_stats.at(col_name));
            }
            else
            {
                result.column_stats.emplace(pr.name, StatsCollection{});
            }
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
    std::optional<StatsData> all_stats_opt;

    auto fetch_data = [&]() {
        if (!all_stats_opt.has_value())
        {
            // read all data once instead of repeatedly read
            all_stats_opt = catalog->readStatsData(table_id);
        }
    };

    if (table_info)
    {
        // collect table stats
        auto col_name = ""; // empty string represent table info
        auto key = std::make_pair(table_id.getUniqueKey(), col_name);
        auto item_ptr = cache.get(key);
        if (!item_ptr)
        {
            fetch_data();
            item_ptr = makePocoShared<StatsCollection>(all_stats_opt.value().table_stats);
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
            fetch_data();
            if (all_stats_opt.value().column_stats.count(col_name))
            {
                item_ptr = makePocoShared<StatsCollection>(all_stats_opt.value().column_stats.at(col_name));
            }
            else
            {
                item_ptr = makePocoShared<StatsCollection>();
            }
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
