#include <Core/SettingsEnums.h>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatsTableBasic.h>
#include <Storages/StorageDistributed.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
extern const int UNKNOWN_DATABASE;
}


namespace DB::Statistics
{
void dropStatsTable(ContextPtr context, const StatsTableIdentifier & table, StatisticsCachePolicy cache_policy, bool throw_exception)
{
    try
    {
        auto catalog = createCatalogAdaptor(context);
        catalog->checkHealth(/*is_write=*/true);
        auto proxy = createCatalogAdaptorProxy(catalog, cache_policy);

        proxy->drop(table);
    }
    catch (...)
    {
        if (throw_exception)
            throw;
    }
}

void dropStatsColumns(
    ContextPtr context,
    const StatsTableIdentifier & table,
    const std::vector<String> & columns,
    StatisticsCachePolicy cache_policy,
    bool throw_exception)
{
    try
    {
        auto catalog = createCatalogAdaptor(context);
        catalog->checkHealth(/*is_write=*/true);
        auto proxy = createCatalogAdaptorProxy(catalog, cache_policy);
        auto cols_desc = catalog->filterCollectableColumns(table, columns, true);
        proxy->dropColumns(table, cols_desc);
    }
    catch (...)
    {
        if (throw_exception)
            throw;
    }
}


}
