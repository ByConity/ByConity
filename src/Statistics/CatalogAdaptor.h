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

#pragma once
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/TypeUtils.h>
#include <Storages/IStorage.h>

namespace DB::Statistics
{
class CatalogAdaptor
{
public:
    explicit CatalogAdaptor(ContextPtr context_) : context(context_)
    {
    }
    ContextPtr getContext()
    {
        return context;
    }
    virtual bool hasStatsData(const StatsTableIdentifier & table) = 0;
    virtual StatsData readStatsData(const StatsTableIdentifier & table) = 0;
    virtual std::vector<String> readStatsColumnsKey(const StatsTableIdentifier & table) = 0;
    virtual StatsCollection readSingleStats(const StatsTableIdentifier & table, const std::optional<String> & column_name) = 0;
    virtual void writeStatsData(const StatsTableIdentifier & table, const StatsData & stats_data) = 0;

    virtual void dropStatsColumnData(const StatsTableIdentifier & table, const ColumnDescVector & cols_desc) = 0;
    virtual void dropStatsData(const StatsTableIdentifier & table) = 0;

    // fast way to query row count
    std::optional<UInt64> queryRowCount(const StatsTableIdentifier & table_id);

    virtual std::vector<StatsTableIdentifier> getAllTablesID(const String & database_name) = 0;
    virtual std::optional<StatsTableIdentifier> getTableIdByName(const String & database_name, const String & table) = 0;
    virtual std::optional<StatsTableIdentifier> getTableIdByUUID(const UUID & uuid) = 0;
    virtual StoragePtr getStorageByTableId(const StatsTableIdentifier & identifier) = 0;
    virtual StoragePtr tryGetStorageByUUID(const UUID & uuid) = 0;
    virtual UInt64 getUpdateTime() = 0;
    ColumnDescVector getAllCollectableColumns(const StatsTableIdentifier & identifier);
    virtual const Settings & getSettingsRef() = 0;

    virtual UInt64 fetchAddUdiCount(const StatsTableIdentifier & table, UInt64 count) = 0;
    virtual void removeUdiCount(const StatsTableIdentifier & table) = 0;
    struct TableOptions
    {
        bool is_collectable = false;
        bool is_auto_updatable = false;
    };

    ColumnDescVector filterCollectableColumns(
        const StatsTableIdentifier & table, const std::vector<String> & target_columns, bool exception_on_unsupported = false);

    bool isTableCollectable(const StatsTableIdentifier & table)
    {
        return getTableOptions(table).is_collectable;
    }

    // fast filter
    static bool isDatabaseCollectable(const String & database_name);
    static TableOptions getTableOptionsForStorage(IStorage & storage);

    // normal filter
    TableOptions getTableOptions(const StatsTableIdentifier & table);

    bool isTableAutoUpdatable(const StatsTableIdentifier & table)
    {
        return getTableOptions(table).is_auto_updatable;
    }

    virtual void checkHealth(bool is_write) { (void)is_write; }
    virtual ~CatalogAdaptor() = default;

protected:
    ContextPtr context;
};


using CatalogAdaptorPtr = std::shared_ptr<CatalogAdaptor>;
using ConstCatalogAdaptorPtr = std::shared_ptr<CatalogAdaptor>;
CatalogAdaptorPtr createCatalogAdaptor(ContextPtr context);
inline ConstCatalogAdaptorPtr createConstCatalogAdaptor(ContextPtr context)
{
    // only const function can be used in this adaptor
    return createCatalogAdaptor(context);
}

}
