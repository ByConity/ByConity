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

#include <set>
#include <tuple>
#include <vector>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CachedStatsProxy.h>
#include <Statistics/CollectStep.h>
#include <Statistics/SimplifyHistogram.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsNdvBucketsImpl.h>
#include <Statistics/SubqueryHelper.h>
#include <Statistics/TypeUtils.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <boost/algorithm/string/join.hpp>
#include <common/logger_useful.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/now64.h>

namespace DB::Statistics
{

void StatisticsCollector::collect(const ColumnDescVector & cols_desc)
{
    auto step = [&] {
        if (settings.enable_sample)
        {
            return createStatisticsCollectorStepSample(*this);
        }
        else
        {
            return createStatisticsCollectorStepFull(*this);
        }
    }();

    step->collect(cols_desc);
    step->writeResult(table_stats, columns_stats);
    if (auto basic = table_stats.basic)
    {
        auto timestamp = nowSubsecondDt64(DataTypeDateTime64::default_scale);
        basic->setTimestamp(timestamp);
    }
}

void StatisticsCollector::writeToCatalog()
{
    if (!catalog)
    {
        throw Exception("catalog is NULL", ErrorCodes::LOGICAL_ERROR);
    }
    StatsData data;
    data.table_stats = table_stats.writeToCollection();
    std::vector<String> columns;
    for (auto & [name, stats] : columns_stats)
    {
        columns.emplace_back(name);
        data.column_stats[name] = stats.writeToCollection();
    }
    auto proxy = createCachedStatsProxy(catalog, settings.cache_policy);
    auto cols_desc = catalog->filterCollectableColumns(table_info, columns);
    // drop old columns to ensure other data is deleted
    // especially when collecting statistics_collect_histogram=0
    proxy->dropColumns(table_info, cols_desc);
    proxy->put(table_info, std::move(data));
    catalog->invalidateClusterStatsCache(table_info);
    // clear udi whenever it is to create/drop stats
    // since after manual drop stats, users just don't want statistics,
    // so we needn't care about udi for auto stats until next insertion
    catalog->removeUdiCount(table_info);
}

void StatisticsCollector::readAllFromCatalog()
{
    auto cols = catalog->getAllCollectableColumns(table_info);
    readFromCatalogImpl(cols);
}

void StatisticsCollector::readFromCatalog(const std::vector<String> & cols_name)
{
    auto cols_desc = catalog->filterCollectableColumns(table_info, cols_name);
    this->readFromCatalogImpl(cols_desc);
}

void StatisticsCollector::readFromCatalogImpl(const ColumnDescVector & cols_desc)
{
    auto proxy = createCachedStatsProxy(catalog, settings.cache_policy);
    auto data = proxy->get(table_info, true, cols_desc);

    if (data.table_stats.empty() && data.column_stats.empty())
    {
        // no stats collected, do nothing
        return;
    }

    table_stats.readFromCollection(data.table_stats);
    for (auto & [name, type] : cols_desc)
    {
        if (!data.column_stats.count(name))
        {
            // TODO: give a warning
            continue;
        }
        (void)type;
        auto & stats = data.column_stats.at(name);
        columns_stats[name].readFromCollection(stats);
    }
}

static bool isInteger(SerdeDataType type)
{
    switch (type)
    {
        case SerdeDataType::UInt8:
        case SerdeDataType::UInt16:
        case SerdeDataType::UInt32:
        case SerdeDataType::UInt64:
        case SerdeDataType::Int8:
        case SerdeDataType::Int16:
        case SerdeDataType::Int32:
        case SerdeDataType::Int64:
            return true;

        default:
            return false;
    }
}

std::optional<PlanNodeStatisticsPtr> StatisticsCollector::toPlanNodeStatistics() const
{
    if (!table_stats.basic)
    {
        // return empty
        return std::nullopt;
    }

    auto result = std::make_shared<PlanNodeStatistics>();
    auto table_row_count = table_stats.basic->getRowCount();
    result->updateRowCount(table_row_count);

    // whether to construct single bucket histogram from min/max if there is no histogram
    for (auto & [col, stats] : columns_stats)
    {
        auto symbol = std::make_shared<SymbolStatistics>();

        if (stats.basic)
        {
            auto nonnull_count = stats.basic->getProto().nonnull_count();
            symbol->null_counts = table_row_count - nonnull_count;
            symbol->min = stats.basic->getProto().min_as_double();
            symbol->max = stats.basic->getProto().max_as_double();
            symbol->ndv = AdjustNdvWithCount(stats.basic->getProto().ndv_value(), nonnull_count);
            symbol->unknown = false;
            auto construct_single_bucket_histogram = symbol->ndv == 1;

            if (stats.ndv_buckets_result)
            {
                stats.ndv_buckets_result->writeSymbolStatistics(*symbol);

                if (context->getSettingsRef().statistics_simplify_histogram)
                {
                    auto is_integer = isInteger(stats.ndv_buckets_result->getSerdeDataType());

                    auto ndv_thres = context->getSettingsRef().statistics_simplify_histogram_ndv_density_threshold;
                    auto range_thres = context->getSettingsRef().statistics_simplify_histogram_range_density_threshold;

                    symbol->histogram = simplifyHistogram(symbol->histogram, ndv_thres, range_thres, is_integer);
                }
            }
            else if (construct_single_bucket_histogram)
            {
                auto bucket = Bucket(symbol->min, symbol->max, symbol->ndv, nonnull_count, true, true);
                symbol->histogram.emplaceBackBucket(std::move(bucket));
            }


            result->updateSymbolStatistics(col, symbol);
        }
    }
    return result;
}

}
