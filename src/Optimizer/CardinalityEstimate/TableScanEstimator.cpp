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

#include <Optimizer/CardinalityEstimate/TableScanEstimator.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsTableBasic.h>
#include <Poco/Logger.h>
#include <common/ErrorHandlers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

PlanNodeStatisticsPtr TableScanEstimator::estimate(ContextMutablePtr context, const TableScanStep & step)
{
    auto plan_node_stats_opt = estimate(context, step.getStorageID(), step.getColumnNames());
    if (!plan_node_stats_opt.has_value())
    {
        return nullptr;
    }
    auto plan_node_stats = std::move(plan_node_stats_opt.value());
    auto & stats = plan_node_stats->getSymbolStatistics();

    NameToNameMap alias_to_column;
    for (const auto & item : step.getColumnAlias())
    {
        alias_to_column.insert_or_assign(item.second, item.first);
    }

    for (const auto & col : step.getOutputStream().header)
    {
        const auto & alias = col.name;
        if (auto it = alias_to_column.find(alias); it != alias_to_column.end() && it->second != alias)
        {
            /// Only keep original column name in statistics
            const auto & name = it->second;
            if (auto jt = stats.find(alias); jt != stats.end()) {
                stats.insert_or_assign(name, jt->second);
                stats.erase(jt);

            }
        }
        if (auto jt = stats.find(alias); jt != stats.end())
        {
            jt->second->setType(col.type);
            jt->second->setDbTableColumn(step.getDatabase() + "-" + step.getTable() + "-" + alias);
        }
    }

    return plan_node_stats;
}

std::optional<PlanNodeStatisticsPtr> TableScanEstimator::estimate(
    ContextMutablePtr context, const StorageID & storage_id, const Names & columns)
{
    auto catalog = Statistics::createCatalogAdaptor(context);
    auto table_info_opt = catalog->getTableIdByName(storage_id.getDatabaseName(), storage_id.getTableName());
    if (!table_info_opt.has_value())
    {
        // TODO: give a warning here?
        return std::nullopt;
    }

    PlanNodeStatisticsPtr plan_node_stats;
    try {
        Statistics::StatisticsCollector collector(context, catalog, table_info_opt.value());
        collector.readFromCatalog(columns);
        auto plan_node_stats_opt = collector.toPlanNodeStatistics();
        if (!plan_node_stats_opt.has_value())
        {
            return std::nullopt;
        }
        plan_node_stats = std::move(plan_node_stats_opt.value());
    }
    catch(...)
    {
        auto * logger = &Poco::Logger::get("TableScanEstimator");
        tryLogCurrentException(logger);
        return std::nullopt;
    }

    return plan_node_stats;
}

}
