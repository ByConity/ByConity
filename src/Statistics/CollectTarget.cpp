#include <Statistics/CollectTarget.h>
#include "DataTypes/DataTypeMap.h"
#include "DataTypes/IDataType.h"
#include "Statistics/StatsTableIdentifier.h"

namespace DB::Statistics
{
// return row_count
std::optional<UInt64> collectStatsOnTarget(const ContextPtr & context, const CollectTarget & collect_target)
{
    const auto & settings = collect_target.settings;
    auto catalog = createCatalogAdaptor(context);

    if (settings.if_not_exists && catalog->hasStatsData(collect_target.table_identifier))
        return std::nullopt;

    StatisticsCollector impl(context, catalog, collect_target.table_identifier, collect_target.settings);
    impl.collect(collect_target.columns_desc);

    impl.writeToCatalog();
    auto row_count = impl.getTableStats().basic->getRowCount();

    return row_count;
}

void CollectTarget::init(const ContextPtr & context, const std::vector<String> & columns_name)
{
    auto catalog = createCatalogAdaptor(context);

    implicit_all_columns = columns_name.empty();
    if (implicit_all_columns)
    {
        columns_desc = catalog->getAllCollectableColumns(table_identifier);
    }
    else
        columns_desc = catalog->filterCollectableColumns(table_identifier, columns_name, true);
}

}
