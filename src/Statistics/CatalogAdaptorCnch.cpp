#include <Catalog/Catalog.h>
#include <Interpreters/executeQuery.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CommonTools.h>
#include <Statistics/SubqueryHelper.h>
#include <boost/algorithm/string.hpp>

#if 0
namespace DB::Statistics
{
using Catalog::CatalogPtr;
class CatalogAdaptorCnch : public CatalogAdaptor
{
public:
    bool hasStatsData(const StatsTableIdentifier & table) override;
    StatsData readStatsData(const StatsTableIdentifier & table) override;
    void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) override;
    StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name) override;
    void dropStatsData(const StatsTableIdentifier & table) override;
    void dropStatsDataAll(const String & database) override;
    void invalidateClusterStatsCache(const StatsTableIdentifier & table) override;
    void invalidateServerStatsCache(const StatsTableIdentifier & table) const override;
    std::vector<StatsTableIdentifier> getAllTablesID(const String & database_name) const override;
    std::optional<StatsTableIdentifier> getTableIdByName(const String & database_name, const String & table_name) const override;
    StoragePtr getStorageByTableId(const StatsTableIdentifier & identifier) const override;
    UInt64 getUpdateTime() override;
    ~CatalogAdaptorCnch() = default;
    std::vector<String> getPartitionColumns(const StatsTableIdentifier & identifier) const override;
    bool isTableCollectable(const StatsTableIdentifier & identifier) const override;
    bool isTableAutoUpdated(const StatsTableIdentifier & table) const override;
    ColumnDescVector getCollectableColumns(const StatsTableIdentifier & identifier) const override;
    const Settings & getSettingsRef() const override { return context.getSettingsRef(); }

    CatalogAdaptorCnch(ContextPtr context_, Catalog::CatalogPtr catalog_) : context(context_), catalog(catalog_) { }

private:
    ContextPtr context;
    CatalogPtr catalog;
};


bool CatalogAdaptorCnch::hasStatsData(const StatsTableIdentifier & table)
{
    /// return whether table_stats of the corresponding table is non-empty
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    auto tags = catalog->getAvailableTableStatisticsTags(uuid_str);
    return !tags.empty();
}

StatsCollection CatalogAdaptorCnch::readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name_opt)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    if (!column_name_opt.has_value())
    {
        // table stats
        auto tags = catalog->getAvailableTableStatisticsTags(uuid_str);
        if (!tags.empty())
        {
            auto stats = catalog->getTableStatistics(uuid_str, tags);
            return stats;
        }
    }
    else
    {
        // column_stats
        auto column_name = column_name_opt.value();
        auto tags = catalog->getAvailableColumnStatisticsTags(uuid_str, column_name);
        if (!tags.empty())
        {
            auto stats = catalog->getColumnStatistics(uuid_str, column_name, tags);
            return stats;
        }
    }
    return {};
}

StatsData CatalogAdaptorCnch::readStatsData(const StatsTableIdentifier & table)
{
    StatsData result;
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());

    // step 1: read table stats
    result.table_stats = readSingleStats(table, std::nullopt);

    // step 2: read column stats
    auto columns_desc = this->getCollectableColumns(table);
    for (auto & desc : columns_desc)
    {
        auto column_name = desc.name;
        auto stats = readSingleStats(table, column_name);
        result.column_stats.emplace(column_name, std::move(stats));
    }

    return result;
}

void CatalogAdaptorCnch::writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());

    // step 1: write table stats
    if (!stats_data.table_stats.empty())
    {
        catalog->updateTableStatistics(uuid_str, stats_data.table_stats);
    }

    // step 2: write column stats
    for (auto & [col_name, stats_col] : stats_data.column_stats)
    {
        catalog->updateColumnStatistics(uuid_str, col_name, stats_col);
    }
}
void CatalogAdaptorCnch::dropStatsData(const StatsTableIdentifier & table)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());

    // step 1: write table stats
    {
        auto tags = catalog->getAvailableTableStatisticsTags(uuid_str);
        if (!tags.empty())
        {
            catalog->removeTableStatistics(uuid_str, tags);
        }
    }

    // step 2: write column stats
    auto columns_desc = this->getCollectableColumns(table);
    for (auto & desc : columns_desc)
    {
        auto col_name = desc.name;
        auto tags = catalog->getAvailableColumnStatisticsTags(uuid_str, col_name);
        if (!tags.empty())
        {
            catalog->removeColumnStatistics(uuid_str, col_name, tags);
        }
    }
}
void CatalogAdaptorCnch::dropStatsDataAll(const String & database_name)
{
    auto tables = this->getAllTablesID(database_name);
    for (auto & identifier : tables)
    {
        dropStatsData(identifier);
    }
}
std::vector<StatsTableIdentifier> CatalogAdaptorCnch::getAllTablesID(const String & database_name) const
{
    std::vector<StatsTableIdentifier> results;
    auto tables = catalog->getAllTablesID(database_name);
    for (auto & table_pb : tables)
    {
        auto table_name = table_pb->name();
        auto uuid = parseFromString<UUID>(table_pb->uuid());
        results.emplace_back(database_name, table_name, uuid);
    }
    return results;
}

std::optional<StatsTableIdentifier> CatalogAdaptorCnch::getTableIdByName(const String & database_name, const String & table_name) const
{
    auto table_pb = catalog->getTableIDByName(database_name, table_name);
    if (!table_pb)
    {
        return std::nullopt;
    }
    auto uuid = parseFromString<UUID>(table_pb->uuid());
    return StatsTableIdentifier(database_name, table_name, uuid);
}

StoragePtr CatalogAdaptorCnch::getStorageByTableId(const StatsTableIdentifier & identifier) const
{
    auto uuid_str = UUIDHelpers::UUIDToString(identifier.getUUID());
    return catalog->getTableByUUID(context, uuid_str, TxnTimestamp::maxTS());
}

UInt64 CatalogAdaptorCnch::getUpdateTime()
{
    // TODO: support cache invalidate strategy
    return 0;
}
std::vector<String> CatalogAdaptorCnch::getPartitionColumns(const StatsTableIdentifier & identifier) const
{
    auto storage = getStorageByTableId(identifier);
    return storage->getColumnsRequiredForPartitionKey();
}

bool CatalogAdaptorCnch::isTableCollectable(const StatsTableIdentifier & identifier) const
{
    auto storage = getStorageByTableId(identifier);
    auto storage_name = storage->getName();

    if (boost::algorithm::ends_with(storage_name, "MergeTree"))
    {
        return true;
    }

    // TODO: configure this in xml file
    static std::set<String> allowed_storage_names = {
        "CnchMergeTree",
        "CloudMergeTree",
        "Log",
        "TinyLog",
        "Memory",
    };

    if (allowed_storage_names.count(storage_name))
    {
        return true;
    }

    return false;
}

bool CatalogAdaptorCnch::isTableAutoUpdated(const StatsTableIdentifier & identifier) const
{
    auto storage = getStorageByTableId(identifier);
    auto storage_name = storage->getName();

    // TODO: configure this in xml file
    // currently, just auto collect cnch/cloud MergeTree
    static std::set<String> auto_storage_names = {
        "CnchMergeTree",
        "CloudMergeTree",
    };

    return auto_storage_names.count(storage_name);
}

ColumnDescVector CatalogAdaptorCnch::getCollectableColumns(const StatsTableIdentifier & identifier) const
{
    ColumnDescVector result;
    auto storage = getStorageByTableId(identifier);
    for (const auto & name_type_pr : storage->getColumns().getAll())
    {
        if (!Statistics::isCollectableType(name_type_pr.type))
        {
            continue;
        }
        result.emplace_back(name_type_pr);
    }
    return result;
}

void CatalogAdaptorCnch::invalidateClusterStatsCache(const StatsTableIdentifier & table)
{
    auto sql = fmt::format(
        FMT_STRING("select host(), invalidateStatsCache('{}', '{}') from cnch(server, system.one)"),
        table.getDatabaseName(),
        table.getTableName());
    // TODO: remove it when this bug is fixed
    sql += " SETTINGS enable_optimizer=0";

    executeSubQuery(context, sql);
}

void CatalogAdaptorCnch::invalidateServerStatsCache(const StatsTableIdentifier & table) const
{
    Statistics::CacheManager::invalidate(context, table);
}

CatalogAdaptorPtr createCatalogAdaptorCnch(ContextPtr context)
{
    auto catalog = context.getCnchCatalog();
    if (!catalog)
    {
        throw Exception("getCnchCatalog returns nullptr", ErrorCodes::LOGICAL_ERROR);
    }
    return std::make_shared<CatalogAdaptorCnch>(context, std::move(catalog));
}

}
#endif
