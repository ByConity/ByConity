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

#include <hive_metastore_types.h>
#include <Catalog/Catalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/StorageID.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/HiveConverter.h>
#include <Statistics/StatisticsCollectorObjects.h>
#include <Statistics/SubqueryHelper.h>
#include <Statistics/TypeUtils.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <boost/algorithm/string.hpp>
#include <hive_metastore_types.h>
#include <Statistics/HiveConverter.h>
#include <Statistics/StatsUdiCounter.h>
#include <boost/regex.hpp>

namespace DB::Statistics
{
using Catalog::CatalogPtr;
class CatalogAdaptorCnch : public CatalogAdaptor
{
public:
    bool hasStatsData(const StatsTableIdentifier & table) override;
    StatsData readStatsData(const StatsTableIdentifier & table) override;
    StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name) override;
    void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) override;
    void dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc) override;
    void dropStatsData(const StatsTableIdentifier & table) override;
    void dropStatsDataAll(const String & database) override;

    void invalidateClusterStatsCache(const StatsTableIdentifier & table) override;
    void invalidateServerStatsCache(const StatsTableIdentifier & table) override;

    std::vector<StatsTableIdentifier> getAllTablesID(const String & database_name) override;
    std::optional<StatsTableIdentifier> getTableIdByName(const String & database_name, const String & table_name) override;
    std::optional<StatsTableIdentifier> getTableIdByUUID(const UUID & uuid) override;
    StoragePtr getStorageByTableId(const StatsTableIdentifier & identifier) override;
    StoragePtr tryGetStorageByUUID(const UUID & uuid) override;
    void invalidateAllServerStatsCache() override { Statistics::CacheManager::reset(); }
    UInt64 getUpdateTime() override;
    bool isTableCollectable(const StatsTableIdentifier & identifier) override;
    bool isTableAutoUpdated(const StatsTableIdentifier & table) override;
    ColumnDescVector getCollectableColumns(const StatsTableIdentifier & identifier) override;
    const Settings & getSettingsRef() override { return context->getSettingsRef(); }

    CatalogAdaptorCnch(ContextPtr context_, Catalog::CatalogPtr catalog_) : context(context_), catalog(catalog_) { }
    UInt64 fetchAddUdiCount(const StatsTableIdentifier & table, UInt64 count) override;
    void removeUdiCount(const StatsTableIdentifier & table) override;
    ~CatalogAdaptorCnch() override = default;

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

UInt64 CatalogAdaptorCnch::fetchAddUdiCount(const StatsTableIdentifier & table, UInt64 count)
{
    // this function is NOT atomic, and should be called only by daemon manager
    constexpr auto tag = StatisticsTag::UdiCounter;
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    auto stats_collection = catalog->getTableStatistics(uuid_str, {tag});

    UInt64 old_count = 0;
    if (stats_collection.count(tag))
    {
        auto ptr = stats_collection.at(tag);
        old_count = std::dynamic_pointer_cast<StatsUdiCounter>(ptr)->getUdiCount();
    }

    if (count == 0)
    {
        return old_count;
    }

    auto new_ptr = std::make_shared<StatsUdiCounter>();
    new_ptr->setUdiRowCount(old_count + count);
    stats_collection[tag] = std::move(new_ptr);
    catalog->updateTableStatistics(uuid_str, stats_collection);

    return old_count;
}

void CatalogAdaptorCnch::removeUdiCount(const StatsTableIdentifier & table)
{
    constexpr auto tag = StatisticsTag::UdiCounter;
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    catalog->removeTableStatistics(uuid_str, {tag});
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
    auto storage = getStorageByTableId(table);
    auto columns_desc = this->getCollectableColumns(table);
    StatsData result;

    if (storage->getName() == "CnchHive")
    {
        auto hive_storage = std::dynamic_pointer_cast<StorageCnchHive>(storage);
        std::vector<String> cols_name;
        NameToType name_to_type;
        for (auto desc : columns_desc)
        {
            cols_name.emplace_back(desc.name);
            name_to_type[desc.name] = desc.type;
        }

        auto [row_count, hive_stats] = hive_storage->getTableStats(cols_name, context);
        auto [table_stats, column_stats] = StatisticsImpl::convertHiveToStats(row_count, name_to_type, hive_stats);
        result.table_stats = table_stats.writeToCollection();
        for (auto & [k, v] : column_stats)
        {
            result.column_stats[k] = v.writeToCollection();
        }
        return result;
    }

    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());

    // step 1: read table stats
    result.table_stats = readSingleStats(table, std::nullopt);

    // step 2: read column stats
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

void CatalogAdaptorCnch::dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    for (auto & desc : cols_desc)
    {
        auto col_name = desc.name;
        auto tags = catalog->getAvailableColumnStatisticsTags(uuid_str, col_name);
        if (!tags.empty())
        {
            catalog->removeColumnStatistics(uuid_str, col_name, tags);
        }
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

std::vector<StatsTableIdentifier> CatalogAdaptorCnch::getAllTablesID(const String & database_name)
{
    auto db = DatabaseCatalog::instance().getDatabase(database_name, context);
    if (!db)
    {
        return {};
    }

    auto table_identifiers = context->getCnchCatalog()->getAllTablesID(database_name);
    std::vector<StatsTableIdentifier> results;
    for (auto & identifier : table_identifiers)
        results.emplace_back(StorageID{identifier->database(), identifier->name(), UUIDHelpers::toUUID(identifier->uuid())});
    return results;
}

std::optional<StatsTableIdentifier> CatalogAdaptorCnch::getTableIdByName(const String & database_name, const String & table_name)
{
    auto & ins = DatabaseCatalog::instance();
    auto db_storage = ins.getDatabase(database_name, context);
    auto table = db_storage->tryGetTable(table_name, context);
    if (!table)
    {
        return std::nullopt;
    }
    auto result = table->getStorageID();

    return StatsTableIdentifier(result);
}

std::optional<StatsTableIdentifier> CatalogAdaptorCnch::getTableIdByUUID(const UUID & uuid)
{
    auto storage = tryGetStorageByUUID(uuid);
    if (!storage)
    {
        return std::nullopt;
    }
    auto id = storage->getStorageID();
    return StatsTableIdentifier(id);
}

StoragePtr CatalogAdaptorCnch::tryGetStorageByUUID(const UUID & uuid)
{
    auto uuid_str = UUIDHelpers::UUIDToString(uuid);
    auto storage = catalog->tryGetTableByUUID(*context, uuid_str, TxnTimestamp::maxTS());
    return storage;
}

StoragePtr CatalogAdaptorCnch::getStorageByTableId(const StatsTableIdentifier & identifier)
{
    auto & ins = DatabaseCatalog::instance();
    return ins.getTable(identifier.getStorageID(), context);
}

UInt64 CatalogAdaptorCnch::getUpdateTime()
{
    // TODO: support cache invalidate strategy
    return 0;
}

bool CatalogAdaptorCnch::isTableCollectable(const StatsTableIdentifier & identifier)
{
    auto storage = getStorageByTableId(identifier);

    auto & pattern = context->getSettingsRef().statistics_exclude_tables_regex.value;
    if (!pattern.empty())
    {
        try
        {
            boost::regex re(pattern);
            boost::cmatch tmp;
            if (boost::regex_match(identifier.getTableName().data(), tmp, re))
            {
                return false;
            }
        }
        catch (boost::wrapexcept<boost::regex_error> & e)
        {
            auto err_msg = std::string("regex match error: ") + e.what();
            throw Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
        }
    }

    auto storage_name = storage->getName();
    if (boost::algorithm::ends_with(storage_name, "MergeTree"))
    {
        return true;
    }

    // TODO: configure this in xml file
    static std::set<String> allowed_storage_names = {
        "CnchMergeTree",
        "CloudMergeTree",
        "CnchHive",
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

bool CatalogAdaptorCnch::isTableAutoUpdated(const StatsTableIdentifier & identifier)
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

ColumnDescVector CatalogAdaptorCnch::getCollectableColumns(const StatsTableIdentifier & identifier)
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

void CatalogAdaptorCnch::invalidateClusterStatsCache(const StatsTableIdentifier & table)
{
#if 0
    auto sql = fmt::format(
        FMT_STRING("select host(), invalidateStatsCache('{}', '{}') from cnch(server, system.one)"),
        table.getDatabaseName(),
        table.getTableName());
    // TODO: remove it when this bug is fixed
    sql += " SETTINGS enable_optimizer=0";
    executeSubQuery(context, sql);
#endif
    Statistics::CacheManager::invalidate(context, table);
}

void CatalogAdaptorCnch::invalidateServerStatsCache(const StatsTableIdentifier & table)
{
    Statistics::CacheManager::invalidate(context, table);
}

CatalogAdaptorPtr createCatalogAdaptorCnch(ContextPtr context)
{
    auto catalog = context->getCnchCatalog();
    if (!catalog)
    {
        throw Exception("getCnchCatalog returns nullptr", ErrorCodes::LOGICAL_ERROR);
    }
    return std::make_shared<CatalogAdaptorCnch>(context, std::move(catalog));
}

}
