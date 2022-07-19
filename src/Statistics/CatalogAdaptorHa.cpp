#include <Interpreters/DatabaseCatalog.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CommonTools.h>
#include <Statistics/SerdeUtils.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsDataSource.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Storages/IStorage.h>
#include <fmt/core.h>

namespace DB::Statistics
{

// CatalogAdaptor for HaMergeTree
class CatalogAdaptorHa : public CatalogAdaptor
{
public:
    CatalogAdaptorHa(ContextPtr context_) : context(context_) { }

    bool hasStatsData(const StatsTableIdentifier & table) override
    {
        StatsDataSource source(context);
        return source.has(table);
    }

    StatsData readStatsData(const StatsTableIdentifier & table) override
    {
        StatsDataSource source(context);
        return source.get(table);
    }

    StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name) override
    {
        StatsDataSource source(context);
        return source.getSingle(table, column_name);
    }

    void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) override
    {
        StatsDataSource source(context);
        source.set(table, stats_data);
    }

    void dropStatsData(const StatsTableIdentifier & table) override
    {
        StatsDataSource source(context);
        source.drop(table);
    }

    void dropStatsDataAll(const String & database_name) override
    {
        StatsDataSource source(context);
        auto tables = getAllTablesID(database_name);
        for (auto & table : tables)
        {
            source.drop(table);
        }
    }

    void invalidateClusterStatsCache(const StatsTableIdentifier & table) override
    {
        StatsDataSource source(context);
        source.invalidateClusterCache(table);
    }

    // const because it should use ConstContext
    void invalidateServerStatsCache(const StatsTableIdentifier & table) const override
    {
        Statistics::CacheManager::invalidate(context, table);
    }


    std::vector<StatsTableIdentifier> getAllTablesID(const String & database_name) const override
    {
        std::vector<StatsTableIdentifier> results;
        auto db = DatabaseCatalog::instance().getDatabase(database_name);
        for (auto iter = db->getTablesIterator(context); iter->isValid(); iter->next())
        {
            auto table = iter->table();
            StatsTableIdentifier table_id(table->getStorageID());
            results.emplace_back(table_id);
        }
        return results;
    }

    std::optional<StatsTableIdentifier> getTableIdByName(const String & database_name, const String & table_name) const override
    {
        auto & ins = DatabaseCatalog::instance();
        auto db_storage = ins.getDatabase(database_name);
        auto table = db_storage->tryGetTable(table_name, context);
        if (!table)
        {
            return std::nullopt;
        }
        return StatsTableIdentifier(table->getStorageID());
    }

    StoragePtr getStorageByTableId(const StatsTableIdentifier & identifier) const override
    {
        auto & ins = DatabaseCatalog::instance();
        return ins.getTable(identifier.getStorageID(), context);
    }

    UInt64 getUpdateTime() override
    {
        StatsDataSource source(context);
        return source.getUpdateTime();
    }

    std::vector<String> getPartitionColumns(const StatsTableIdentifier & identifier) const override
    {
        auto table = getStorageByTableId(identifier);
        auto snapshot = table->getInMemoryMetadataPtr();
        return snapshot->getColumnsRequiredForPartitionKey();
    }

    ColumnDescVector getCollectableColumns(const StatsTableIdentifier & identifier) const override
    {
        ColumnDescVector result;
        auto storage = getStorageByTableId(identifier);
        auto snapshot = storage->getInMemoryMetadataPtr();
        for (const auto & name_type_pr : snapshot->getColumns().getAll())
        {
            if (!Statistics::isCollectableType(name_type_pr.type))
            {
                continue;
            }
            result.emplace_back(name_type_pr);
        }
        return result;
    }

    bool isTableCollectable(const StatsTableIdentifier & table) const override
    {
        static const std::set<String> valid_storages{"Distributed"};
        auto table_type = getStorageByTableId(table)->getName();
        return valid_storages.count(table_type);
    }

    void resetAllStats() override
    {
        StatsDataSource source(context);
        source.resetAllStats();
    }
    const Settings & getSettingsRef() const override { return context->getSettingsRef(); }

private:
    ContextPtr context;
};

CatalogAdaptorPtr createCatalogAdaptorHa(ContextPtr context)
{
    return std::make_shared<CatalogAdaptorHa>(context);
}

}
