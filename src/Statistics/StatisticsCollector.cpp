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

#include <chrono>
#include <cmath>
#include <set>
#include <tuple>
#include <vector>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/now64.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Statistics/Bucket.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptorProxy.h>
#include <Statistics/CollectStep.h>
#include <Statistics/SimplifyHistogram.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsNdvBucketsImpl.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/SubqueryHelper.h>
#include <Statistics/TypeUtils.h>
#include <boost/algorithm/string/join.hpp>
#include <common/logger_useful.h>
#include "Storages/ColumnsDescription.h"
#include <boost/range/adaptor/reversed.hpp>

namespace DB::Statistics
{
StorageMetadataPtr getStorageMetaData(ContextPtr context, StoragePtr storage)
{
    (void)context;
    StorageMetadataPtr storage_metadata = storage->getInMemoryMetadataPtr();
    // for Cnch, no need to workaround Distribtued
    return storage_metadata;
}

StatisticsCollector::StatisticsCollector(
    ContextPtr context_, CatalogAdaptorPtr catalog_, const StatsTableIdentifier & table_info_, const CollectorSettings & settings_)
    : context(context_), catalog(catalog_), table_info(table_info_), settings(settings_)
{
    if (settings.empty())
        settings.set_cache_policy(context->getSettingsRef().statistics_cache_policy);
    storage = catalog->getStorageByTableId(table_info);
#if 0
    auto metadata = getStorageMetaData(context, storage);
    if (settings.collect_in_partitions() && !metadata->hasPartitionKey())
    {
        settings.set_collect_in_partitions(false);
    }
#endif

    settings.set_collect_in_partitions(false);
    if (storage->getName() == "HiveCluster")
    {
        settings.set_enable_sample(false);
    }

    logger = getLogger("StatisticsLogger" + table_info.getDbTableName());
}


void StatisticsCollector::collect(const ColumnDescVector & cols_desc)
{
    auto step = [&] {
        if (settings.enable_sample())
        {
            return createSampleCollectStep(*this);
        }
        else
        {
            return createFullCollectStep(*this);
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
    auto proxy = createCatalogAdaptorProxy(catalog, settings.cache_policy());
    auto cols_desc = catalog->filterCollectableColumns(table_info, columns);
    // drop old columns to ensure other data is deleted
    // especially when collecting statistics_collect_histogram=0
    proxy->dropColumns(table_info, cols_desc);
    proxy->put(table_info, std::move(data));


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
    auto proxy = createCatalogAdaptorProxy(catalog, settings.cache_policy());
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

double getAverageLength(DataTypePtr type, UInt64 total_length, UInt64 nonnull_count)
{
    if (type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
    {
        auto avg_len = type->getSizeOfValueInMemory();
        return avg_len;
    }

    if (!total_length || !nonnull_count)
    {
        constexpr auto default_length_for_unknown = 8;
        return default_length_for_unknown;
    }

    return static_cast<double>(total_length) / nonnull_count;
}

PlanNodeStatisticsPtr expandToCurrent(ContextPtr context, PlanNodeStatisticsPtr plan_node_stats, time_t stats_time)
{
    auto [input_now, input_skip] = [&] {
        time_t now = context->getSettingsRef().statistics_current_timestamp.value;
        time_t skip_seconds = context->getSettingsRef().statistics_expand_to_current_threshold_days * 3600 * 24;
        time_t skip_time;

        if (now == 0)
        {
            now = time(nullptr);
            skip_time = stats_time - skip_seconds;
        }
        else
        {
            // for testing, stats_time is not useful, use now
            skip_time = now - skip_seconds;
        }
        return std::make_pair(now, skip_time);
    }();


    auto full_count = plan_node_stats->getRowCount();
    // first step: find the scale ratio

    struct TypeHelper
    {
        double now;
        double skip;
    };

    auto datetime_helper = TypeHelper{.now = static_cast<double>(input_now), .skip = static_cast<double>(input_skip)};
    auto date_helper = TypeHelper{.now = datetime_helper.now / (24 * 3600), .skip = datetime_helper.skip / (24 * 3600)};

    std::set<String> modified_cols;
    UInt64 all_delta_count = 0;
    for (auto & [col_name, symbol] : plan_node_stats->getSymbolStatistics())
    {
        auto type = symbol->getType();
        auto & symbol_stats = plan_node_stats->getSymbolStatistics().at(col_name);

        auto is_date = isDateOrDate32(type);
        auto is_datetime = isDateTime(type) || isDateTime64(type);

        if (is_date || is_datetime)
        {
            auto helper = is_date ? date_helper : datetime_helper;

            auto stats_max = symbol_stats->getMax();
            if (std::isnan(stats_max))
            {
                continue;
            }

            if (stats_max <= helper.skip)
            {
                // highly possible ancient data or unused column, skip it
                continue;
            }

            if (stats_max >= helper.now)
            {
                // up to date, no need to fix
                continue;
            }

            auto & histogram = symbol_stats->getHistogram();
            UInt64 delta_count = 0;
            if (histogram.empty())
            {
                auto stats_min = symbol_stats->getMin();
                if (isnan(stats_min))
                    continue;
                // +1 to avoid inf
                auto expand_factor = (helper.now - stats_max) / (stats_max - stats_min + 1);

                // we hard code expand_factor limit to 1, to avoid unexpected situations
                // like some column stores a fixed old date_time
                expand_factor = std::min(expand_factor, 1.0);

                // we just set max here, scale it in next step
                symbol_stats->setNdv(symbol_stats->getNdv() * (1 + expand_factor));
                symbol_stats->setMax(helper.now);
                delta_count = expand_factor * full_count;
            }
            else
            {
                double hist_ratio = context->getSettingsRef().statistics_expand_to_current_histogram_ratio;
                UInt64 bucket_id = static_cast<UInt64>(histogram.getBucketSize() * (1 - hist_ratio));

                if (bucket_id == histogram.getBucketSize())
                {
                    --bucket_id;
                }

                auto stats_min = histogram.getBucket(bucket_id)->getLowerBound();
                UInt64 tail_hist_count = 0;
                UInt64 tail_hist_ndv = 0;
                for(auto i = bucket_id; i < histogram.getBucketSize(); ++i)
                {
                    auto bucket = histogram.getBuckets()[i];
                    tail_hist_count += bucket.getCount();
                    tail_hist_ndv += bucket.getNumDistinct();
                }
                auto local_expand_factor = 1.0 * (helper.now - stats_max) / (stats_max - stats_min + 1);
                local_expand_factor = std::min(1.0 * full_count / tail_hist_count, local_expand_factor);
                auto local_count = local_expand_factor * tail_hist_count;
                auto local_ndv =  local_expand_factor * tail_hist_ndv;
                auto new_bucket = Bucket(stats_max, helper.now, local_ndv, local_count, false, true);
                histogram.emplaceBackBucket(std::move(new_bucket));

                delta_count = local_count;
                symbol_stats->setMax(helper.now);
                symbol_stats->setNdv(symbol_stats->getNdv() + local_ndv);
            }
            modified_cols.insert(col_name);

            all_delta_count = std::max(all_delta_count, delta_count);
        }
    }

    if (modified_cols.empty())
    {
        return plan_node_stats;
    }

    if (all_delta_count == 0)
    {
        return plan_node_stats;
    }

    plan_node_stats->updateRowCount(full_count + all_delta_count);

    auto scale_factor = 1.0 * (full_count + all_delta_count) / full_count;
    for (auto & [col, symbol] : plan_node_stats->getSymbolStatistics())
    {
        if (modified_cols.count(col))
        {
            continue;
        }

        // NOTE: we have made sure selectivity can be larger than 1 using this API
        // NOTE: future improvement should consider this scenario
        symbol = symbol->applySelectivity(scale_factor, 1);
    }

    return plan_node_stats;
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
    // Datetime64(3) to time_t, just do a simple division
    time_t stats_time = table_stats.basic->getTimestamp() / 1000;


    result->updateRowCount(table_row_count);
    // whether to construct single bucket histogram from min/max if there is no histogram
    auto meta = storage->getInMemoryMetadataPtr();

    for (const auto & [col, stats] : columns_stats)
    {
        auto symbol = std::make_shared<SymbolStatistics>();

        if (stats.basic)
        {
            const auto & basic_proto = stats.basic->getProto();
            auto nonnull_count = basic_proto.nonnull_count();
            symbol->null_counts = table_row_count - nonnull_count;
            symbol->min = basic_proto.min_as_double();
            symbol->max = basic_proto.max_as_double();
            symbol->ndv = AdjustNdvWithCount(basic_proto.ndv_value(), nonnull_count);
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

            UInt64 total_length = basic_proto.total_length();
            auto type = meta->getColumns().tryGetColumn(GetColumnsOptions::All, col)->type;
            auto decay_type = decayDataType(type);
            symbol->avg_len = getAverageLength(decay_type, total_length, nonnull_count);
            symbol->type = decay_type;

            result->updateSymbolStatistics(col, symbol);
        }
    }

    if (context->getSettingsRef().statistics_expand_to_current)
    {
        result = expandToCurrent(context, std::move(result), stats_time);
    }

    return result;
}
}
