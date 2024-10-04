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
#include <Interpreters/StorageID.h>
#include <Interpreters/executeQuery.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatisticsCollectorObjects.h>
#include <Statistics/StatsUdiCounter.h>
#include <Statistics/SubqueryHelper.h>
#include <Statistics/TypeUtils.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/regex.hpp>
#include "common/types.h"
#include "Databases/DatabaseExternalHive.h"
#include "Statistics/StatisticsBase.h"
#include "Storages/TableStatistics.h"

namespace DB::Statistics
{
using Catalog::CatalogPtr;
class CatalogAdaptorCnch : public CatalogAdaptor
{
public:
    bool hasStatsData(const StatsTableIdentifier & table) override;
    std::vector<String> readStatsColumnsKey(const StatsTableIdentifier & table) override;
    StatsData readStatsData(const StatsTableIdentifier & table) override;
    StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name) override;
    void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) override;
    void dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc) override;
    void dropStatsData(const StatsTableIdentifier & table) override;

    std::vector<StatsTableIdentifier> getAllTablesID(const String & database_name) override;
    std::optional<StatsTableIdentifier> getTableIdByName(const String & database_name, const String & table_name) override;
    std::optional<StatsTableIdentifier> getTableIdByUUID(const UUID & uuid) override;
    StoragePtr getStorageByTableId(const StatsTableIdentifier & identifier) override;
    StoragePtr tryGetStorageByUUID(const UUID & uuid) override;
    UInt64 getUpdateTime() override;
    const Settings & getSettingsRef() override { return context->getSettingsRef(); }
    static StatsData convertTableStats(const TableStatistics & table_stats);

    UInt64 fetchAddUdiCount(const StatsTableIdentifier & table, UInt64 count) override;
    void removeUdiCount(const StatsTableIdentifier & table) override;

    CatalogAdaptorCnch(ContextPtr context_, Catalog::CatalogPtr catalog_) : CatalogAdaptor(context_), catalog(catalog_)
    {
    }
    ~CatalogAdaptorCnch() override = default;


private:
    CatalogPtr catalog;
};


bool CatalogAdaptorCnch::hasStatsData(const StatsTableIdentifier & table)
{
    /// return whether table_stats of the corresponding table is non-empty
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    auto table_stats = catalog->getTableStatistics(uuid_str);
    return table_stats.count(StatisticsTag::TableBasic);
}

UInt64 CatalogAdaptorCnch::fetchAddUdiCount(const StatsTableIdentifier & table, UInt64 count)
{
    // this function is NOT atomic, and should be called only by daemon manager
    constexpr auto tag = StatisticsTag::UdiCounter;
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    auto stats_collection = catalog->getTableStatistics(uuid_str);

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
    StatsCollection c;
    // nullptr means delete
    c.emplace(tag, nullptr);
    catalog->updateTableStatistics(uuid_str, c);
}


StatsCollection CatalogAdaptorCnch::readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name_opt)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    if (!column_name_opt.has_value())
    {
        // table stats
        auto stats = catalog->getTableStatistics(uuid_str);
        return stats;
    }
    else
    {
        // column_stats
        auto column_name = column_name_opt.value();
        auto stats = catalog->getColumnStatistics(uuid_str, column_name);
        return stats;
    }
}

std::vector<String> CatalogAdaptorCnch::readStatsColumnsKey(const StatsTableIdentifier & table)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    return catalog->getAllColumnStatisticsKey(uuid_str);
}

StatsData CatalogAdaptorCnch::readStatsData(const StatsTableIdentifier & table)
{
    auto storage = getStorageByTableId(table);

    // only for Hive: if storage has stats
    if (storage->getName() == "HiveCluster")
    {
        auto columns_desc = this->getAllCollectableColumns(table);
        std::vector<String> cols_name;
        for (auto desc : columns_desc)
            cols_name.emplace_back(desc.name);
        auto table_stats = storage->getTableStats(cols_name, context);
        if (table_stats)
            return convertTableStats(*table_stats);
    }

    StatsData result;
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    // step 1: read table stats
    result.table_stats = catalog->getTableStatistics(uuid_str);
    result.column_stats = catalog->getAllColumnStatistics(uuid_str);

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
    catalog->removeTableStatistics(uuid_str);

    // step 2: write column stats
    catalog->removeAllColumnStatistics(uuid_str);
}

void CatalogAdaptorCnch::dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc)
{
    auto uuid_str = UUIDHelpers::UUIDToString(table.getUUID());
    for (auto & desc : cols_desc)
    {
        auto col_name = desc.name;
        catalog->removeColumnStatistics(uuid_str, col_name);
    }
}

std::vector<StatsTableIdentifier> CatalogAdaptorCnch::getAllTablesID(const String & database_name)
{
    auto db = DatabaseCatalog::instance().getDatabase(database_name, context);
    if (!db)
    {
        return {};
    }

    std::vector<StatsTableIdentifier> results;
    if(auto * db_hive = dynamic_cast<DatabaseExternalHive*>(db.get()); db_hive)
    {
        auto iter = db_hive->getTablesIterator(context, {});
        while(iter->isValid())
        {
            results.emplace_back(iter->table()->getStorageID());
            iter->next();
        }
        return results;
    }

    auto table_identifiers = context->getCnchCatalog()->getAllTablesID(database_name);
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

StatsData CatalogAdaptorCnch::convertTableStats(const TableStatistics & table_statistics)
{
    using namespace StatisticsImpl;

    StatsData result;

    auto table_basic = std::make_shared<StatsTableBasic>();
    table_basic->setRowCount(table_statistics.row_count);
    TableStats table_stats{.basic = table_basic};
    result.table_stats = table_stats.writeToCollection();

    for (const auto & column : table_statistics.column_statistics)
    {
        const auto & stats = column.second;

        auto column_basic = std::make_shared<StatsColumnBasic>();
        column_basic->mutableProto().set_min_as_double(stats.min);
        column_basic->mutableProto().set_max_as_double(stats.max);
        UInt64 ndv_exlude_null = stats.ndv - (stats.null_counts ? 1 : 0);
        column_basic->mutableProto().set_ndv_value(ndv_exlude_null);
        column_basic->mutableProto().set_nonnull_count(table_statistics.row_count - stats.null_counts);

        ColumnStats column_stats{.basic = std::move(column_basic)};
        result.column_stats[column.first] = column_stats.writeToCollection();
    }
    return result;
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
