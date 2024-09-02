/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/TypeUtils.h>
#include <Poco/SharedPtr.h>
#include <Statistics/OptimizerStatisticsClient.h>

namespace DB::Statistics
{

// No cache policy
class NoCacheCatalogAdaptorProxy : public CatalogAdaptorProxy
{
public:
    NoCacheCatalogAdaptorProxy(const CatalogAdaptorPtr & catalog_) : catalog(catalog_) { }

    void put(const StatsTableIdentifier & table_id, StatsData && data) override { catalog->writeStatsData(table_id, data); }

    StatsData get(const StatsTableIdentifier & table_id) override 
    {
        // no cache means read all
        return catalog->readStatsData(table_id); 
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
    void drop(const StatsTableIdentifier & table_id) override { catalog->dropStatsData(table_id); }

    void dropColumns(const StatsTableIdentifier & table_id, const ColumnDescVector & cols_desc) override
    {
        catalog->dropStatsColumnData(table_id, cols_desc);
    }

private:
    CatalogAdaptorPtr catalog;
};


// cached policy
template <bool cache_only = false>
class CacheCatalogAdaptorProxy : public CatalogAdaptorProxy
{
public:
    explicit CacheCatalogAdaptorProxy(const CatalogAdaptorPtr & catalog_) : catalog(catalog_) { }

    void put(const StatsTableIdentifier & table_id, StatsData && data) override;
    StatsData get(const StatsTableIdentifier & table_id) override;
    StatsData get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns) override;
    StatsData getImpl(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns);

    void drop(const StatsTableIdentifier & table_id) override;
    void dropColumns(const StatsTableIdentifier & table_id, const ColumnDescVector & cols_desc) override;

private:
    CatalogAdaptorPtr catalog;
};

template <bool cache_only>
void CacheCatalogAdaptorProxy<cache_only>::put(const StatsTableIdentifier & table_id, StatsData && data)
{
    // cache only create stats don't make sense
    if (cache_only)
    {
        return;
    }

    catalog->writeStatsData(table_id, data);
    refreshClusterStatsCache(catalog->getContext(), table_id, false);
}

template <bool cache_only>
StatsData CacheCatalogAdaptorProxy<cache_only>::get(const StatsTableIdentifier & table_id)
{
    // we must visit the real catalog to get which columns should be visited
    // this use prefix scan and can be slow
    // since it is only used for show stats ...
    // it should be fine.
    auto columns = catalog->getAllCollectableColumns(table_id);
    return getImpl(table_id, true, columns);
}

template <bool cache_only>
StatsData
CacheCatalogAdaptorProxy<cache_only>::get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns)
{
    return getImpl(table_id, table_info, columns);
}

template <bool cache_only>
StatsData
CacheCatalogAdaptorProxy<cache_only>::getImpl(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns)
{
    auto & cache = Statistics::CacheManager::instance();

    auto unique_key = table_id.getUniqueKey();
    (void)table_info;
    (void)columns;

    auto item_ptr = cache.get(unique_key);

    if (!item_ptr)
    {
        if constexpr (cache_only)
        {
            item_ptr = std::make_shared<StatsData>();
        }
        else
        {
            item_ptr = std::make_shared<StatsData>(catalog->readStatsData(table_id));
            cache.update(unique_key, item_ptr);
        }
    }
    // we only return needed data to result
    StatsData result;
    if (table_info)
    {
        result.table_stats = item_ptr->table_stats;
    }
    for (const auto & pr : columns)
    {
        const auto & col_name = pr.name;
        if (item_ptr->column_stats.contains(col_name))
        {
            result.column_stats[col_name] = item_ptr->column_stats.at(col_name);
        }
    }
    return result;
}


template <bool cache_only>
void CacheCatalogAdaptorProxy<cache_only>::drop(const StatsTableIdentifier & table_id)
{
    if (cache_only)
    {
        auto & cache = Statistics::CacheManager::instance();

        // drop table stats
        cache.invalidate(table_id.getUniqueKey());
        return;
    }

    catalog->dropStatsData(table_id);
    refreshClusterStatsCache(catalog->getContext(), table_id, true);
}

template <bool cache_only>
void CacheCatalogAdaptorProxy<cache_only>::dropColumns(
    const DB::Statistics::StatsTableIdentifier & table_id, const DB::Statistics::ColumnDescVector & cols_desc)
{
    if (cache_only)
    {
        throw Exception("drop cache on columns is not supported, since cache is now based on table", ErrorCodes::NOT_IMPLEMENTED);
    }

    catalog->dropStatsColumnData(table_id, cols_desc);
    refreshClusterStatsCache(catalog->getContext(), table_id, true);
}

// dispatcher
CatalogAdaptorProxyPtr createCatalogAdaptorProxy(const CatalogAdaptorPtr & catalog, StatisticsCachePolicy policy)
{
    // when enable_memory_catalog is true, we won't use cache
    if (catalog->getSettingsRef().enable_memory_catalog)
    {
        if (policy != StatisticsCachePolicy::Default)
            throw Exception("memory catalog don't support cache policy", ErrorCodes::BAD_ARGUMENTS);

        return std::make_unique<NoCacheCatalogAdaptorProxy>(catalog);
    }

    if (policy == StatisticsCachePolicy::Default)
    {
        return std::make_unique<CacheCatalogAdaptorProxy<false>>(catalog);
    }
    else if (policy == StatisticsCachePolicy::Catalog)
    {
        return std::make_unique<NoCacheCatalogAdaptorProxy>(catalog);
    }
    else if (policy == StatisticsCachePolicy::Cache)
    {
        return std::make_unique<CacheCatalogAdaptorProxy<true>>(catalog);
    }
    else
    {
        throw Exception("Unknown statistics cache policy", ErrorCodes::LOGICAL_ERROR);
    }
}


} // namespace DB
