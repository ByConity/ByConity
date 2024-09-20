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

#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Context.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/SubqueryHelper.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/DataLakes/StorageCnchLakeBase.h>
#include <boost/regex.hpp>
#include "DataTypes/MapHelpers.h"
#include "Parsers/formatTenantDatabaseName.h"

namespace DB::ErrorCodes
{
extern const int LOGIGAL_ERROR;
}

namespace DB::Statistics
{
CatalogAdaptorPtr createCatalogAdaptorMemory(ContextPtr context);
CatalogAdaptorPtr createCatalogAdaptorCnch(ContextPtr context);
CatalogAdaptorPtr createCatalogAdaptor(ContextPtr context)
{
    if (context->getSettingsRef().enable_memory_catalog)
    {
        return createCatalogAdaptorMemory(context);
    }
    return createCatalogAdaptorCnch(context);
}


using TableOptions = CatalogAdaptor::TableOptions;

namespace
{

    const auto unsupported = TableOptions{false, false};
    const auto full_supported = TableOptions{true, true};
    const auto only_manual = TableOptions{true, false};
}

bool CatalogAdaptor::isDatabaseCollectable(const String & database_name)
{
    static const std::set<String> reject_dbs = {"system", "cnch_system", "admin"};
    return !reject_dbs.count(database_name);
}

TableOptions CatalogAdaptor::getTableOptionsForStorage(IStorage & storage)
{
    if (!isDatabaseCollectable(storage.getDatabaseName()))
    {
        return unsupported;
    }

    auto engine = storage.getName();
    if (dynamic_cast<StorageCnchMergeTree *>(&storage))
    {
        return full_supported;
    }
    else if (engine == "Memory")
        return only_manual;
    else if (dynamic_cast<StorageCnchLakeBase *>(&storage))
        return only_manual;
    else
        return unsupported;
}

TableOptions CatalogAdaptor::getTableOptions(const StatsTableIdentifier & table)
{
    auto storage = getStorageByTableId(table);
    if (!storage)
    {
        return unsupported;
    }

    auto options = getTableOptionsForStorage(*storage);

    if (!options.is_collectable)
        return options;

    const auto & pattern = context->getSettingsRef().statistics_exclude_tables_regex.value;
    if (!pattern.empty())
    {
        try
        {
            boost::regex re(pattern);
            boost::cmatch tmp;
            if (boost::regex_match(table.getTableName().data(), tmp, re))
            {
                return unsupported;
            }
        }
        catch (boost::wrapexcept<boost::regex_error> & e)
        {
            auto err_msg = std::string("statistics_exclude_tables_regex match error: ") + e.what();
            throw Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
        }
    }
    return options;
}

std::optional<UInt64> CatalogAdaptor::queryRowCount(const StatsTableIdentifier & table_id)
{
    auto storage = getStorageByTableId(table_id);
    const auto * cnch_merge_tree = dynamic_cast<const StorageCnchMergeTree *>(storage.get());
    if (!cnch_merge_tree)
        return std::nullopt;
    return cnch_merge_tree->totalRows(context);
}

ColumnDescVector CatalogAdaptor::filterCollectableColumns(
    const StatsTableIdentifier & table, const std::vector<String> & target_columns, bool exception_on_unsupported)
{
    std::unordered_map<String, DataTypePtr> name_to_type;
    ColumnDescVector result;
    std::vector<String> unsupported_columns;

    auto storage = getStorageByTableId(table);

    auto snapshot = storage->getInMemoryMetadataPtr();
    for (const auto & col_name : target_columns)
    {
        auto col_opt = snapshot->getColumns().tryGetColumn(GetColumnsOptions::All, col_name);
        if (!col_opt)
        {
            unsupported_columns.emplace_back(col_name);
            continue;
        }
        const auto & col = col_opt.value();
        if (Statistics::isCollectableType(col.type))
        {
            result.emplace_back(std::move(col));
        }
        else
        {
            unsupported_columns.emplace_back(col_name);
        }
    }

    if (exception_on_unsupported && !unsupported_columns.empty())
    {
        auto err_msg = fmt::format(FMT_STRING("columns ({}) is not collectable"), fmt::join(unsupported_columns, ", "));
        throw Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
    }

    return result;
}

ColumnDescVector CatalogAdaptor::getAllCollectableColumns(const StatsTableIdentifier & identifier)
{
    auto storage = getStorageByTableId(identifier);
    auto snapshot = storage->getInMemoryMetadataPtr();
    auto col_names = snapshot->getColumns().getNamesOfOrdinary();

    auto exists_col_names = this->readStatsColumnsKey(identifier);
    std::sort(exists_col_names.begin(), exists_col_names.end());

    for (const auto & name : exists_col_names)
    {
        if (isMapImplicitKey(name))
        {
            col_names.emplace_back(name);
        }
    }

    auto result = this->filterCollectableColumns(identifier, col_names);
    return result;
}
} // namespace DB::Statistics
