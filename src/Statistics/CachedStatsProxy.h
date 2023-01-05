#pragma once
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatsTableIdentifier.h>


namespace DB::Statistics
{
class CachedStatsProxy
{
public:
    virtual StatsData get(const StatsTableIdentifier & table_id) = 0;
    virtual StatsData get(const StatsTableIdentifier & table_id, bool table_info, const ColumnDescVector & columns) = 0;
    // usually need to std::move
    virtual void put(const StatsTableIdentifier & table_id, StatsData && data) = 0;
    virtual void drop(const StatsTableIdentifier & table_id) = 0;
    virtual void dropColumns(const StatsTableIdentifier & table_id, const ColumnDescVector & cols_desc) = 0;
    virtual ~CachedStatsProxy() = default;
};

using CachedStatsProxyPtr = std::unique_ptr<CachedStatsProxy>;
CachedStatsProxyPtr createCachedStatsProxy(const CatalogAdaptorPtr & catalog);

} // namespace DB;
