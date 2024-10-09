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

#include <type_traits>
#include <Statistics/CollectStep.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/ParseUtils.h>
#include <Statistics/StatsHllSketch.h>
#include <Statistics/StatsNdvBuckets.h>
#include <Statistics/TableHandler.h>
#include <Statistics/TypeUtils.h>
#include <boost/noncopyable.hpp>
#include <fmt/format.h>

namespace DB::Statistics
{

class FirstFullColumnHandler : public ColumnHandlerBase
{
public:
    explicit FirstFullColumnHandler(HandlerContext & handler_context_, const NameAndTypePair & col_desc) : handler_context(handler_context_)
    {
        col_name = col_desc.name;
        config = getColumnConfig(handler_context.settings, col_desc.type);
        generateSqls();
    }

    void generateSqls()
    {
        auto quote_col_name = colNameForSql(col_name);
        auto wrapped_col_name = getWrappedColumnName(config, quote_col_name);

        auto count_sql = fmt::format(FMT_STRING("count({})"), quote_col_name);
        sqls.emplace_back(count_sql);
        auto ndv_sql = fmt::format(FMT_STRING("uniq({})"), wrapped_col_name);
        sqls.emplace_back(ndv_sql);
        if (config.need_histogram)
        {
            auto kll_log_k = handler_context.settings.kll_sketch_log_k();
            auto kll_name = getKllFuncNameWithConfig(kll_log_k);
            auto histogram_sql = fmt::format(FMT_STRING("{}({})"), kll_name, wrapped_col_name);
            sqls.emplace_back(histogram_sql);
        }

        if (config.need_minmax)
        {
            auto min_sql = fmt::format(FMT_STRING("toFloat64(min({}))"), wrapped_col_name);
            auto max_sql = fmt::format(FMT_STRING("toFloat64(max({}))"), wrapped_col_name);
            sqls.emplace_back(min_sql);
            sqls.emplace_back(max_sql);
        }

        if (config.need_length)
        {
            auto length_sql = fmt::format(FMT_STRING("sum({})"), config.getByteSizeSql(quote_col_name));
            sqls.emplace_back(length_sql);
        }

        // to estimate ndv
        LOG_INFO(
            getLogger("FirstFullColumnHandler"),
            fmt::format(
                FMT_STRING("col info: col={} && "
                           "sqls={}"),
                col_name,
                fmt::join(sqls, ", ")));
    }

    const std::vector<String> & getSqls() override { return sqls; }

    void parse(const Block & block, size_t index_offset_begin) override
    {
        auto index_offset = index_offset_begin;
        // count(col)
        auto nonnull_count = static_cast<double>(getSingleValue<UInt64>(block, index_offset++));
        HandlerColumnData result;
        result.nonnull_count = nonnull_count;

        // algorithm requires block_ndv as sample_nonnull
        if (nonnull_count != 0)
        {
            // select count(*)
            double full_count = handler_context.full_count;

            // hll(col)
            double ndv = getSingleValue<UInt64>(block, index_offset++);
            result.is_ndv_reliable = true;
            result.ndv_value = std::min(ndv, nonnull_count);

            if (config.need_histogram)
            {
                // kll(col)
                auto histogram_blob = getSingleValue<std::string_view>(block, index_offset++);

                if (!histogram_blob.empty())
                {
                    auto histogram = createStatisticsTyped<StatsKllSketch>(StatisticsTag::KllSketch, histogram_blob);
                    result.min_as_double = histogram->minAsDouble().value_or(std::nan(""));
                    result.max_as_double = histogram->maxAsDouble().value_or(std::nan(""));
                    auto histogram_bucket_size = handler_context.settings.histogram_bucket_size();
                    result.bucket_bounds = histogram->getBucketBounds(histogram_bucket_size);
                }
                LOG_INFO(
                    getLogger("FirstFullColumnHandler"),
                    fmt::format(
                        FMT_STRING("col info: col={} && "
                                   "context raw data: full_count={} && "
                                   "column raw data: nonnull_count={}, ndv=&& "
                                   "cast data: result.nonnull_count={}, "
                                   "result.is_ndv_reliable={}, "
                                   "result.ndv_value={}"),
                        col_name,
                        full_count,
                        nonnull_count,
                        ndv,
                        result.nonnull_count,
                        result.is_ndv_reliable,
                        result.ndv_value));
            }

            if (config.need_minmax)
            {
                auto min = getSingleValue<Float64>(block, index_offset++);
                auto max = getSingleValue<Float64>(block, index_offset++);
                result.min_as_double = min;
                result.max_as_double = max;
            }

            if (config.need_length)
            {
                auto length = getSingleValue<UInt64>(block, index_offset++);
                result.length_opt = length;
            }
        }
        else
        {
            result.is_ndv_reliable = true;
            result.ndv_value = 0;
            // use NaN for min/max
            result.min_as_double = std::numeric_limits<double>::quiet_NaN();
            result.max_as_double = std::numeric_limits<double>::quiet_NaN();
        }

        // write result to context
        handler_context.columns_data[col_name] = std::move(result);
    }

private:
    HandlerContext & handler_context;
    String col_name;
    ColumnCollectConfig config;
    std::vector<String> sqls;
};

class SecondFullColumnHandler : public ColumnHandlerBase
{
public:
    explicit SecondFullColumnHandler(
        HandlerContext & handler_context_, std::shared_ptr<BucketBounds> bucket_bounds_, const NameAndTypePair & col_desc)
        : handler_context(handler_context_), bucket_bounds(bucket_bounds_)
    {
        col_name = col_desc.name;
        config = getColumnConfig(handler_context.settings, col_desc.type);
        generateSqls();
    }

    void generateSqls()
    {
        auto quote_col_name = colNameForSql(col_name);
        auto wrapped_col_name = getWrappedColumnName(config, quote_col_name);

        auto bounds_b64 = base64Encode(bucket_bounds->serialize());

        auto ndv_buckets_sql = fmt::format(FMT_STRING("ndv_buckets('{}')({})"), bounds_b64, wrapped_col_name);
        // to estimate ndv
        sqls.emplace_back(ndv_buckets_sql);
    }

    const std::vector<String> & getSqls() override { return sqls; }

    void parse(const Block & block, size_t index_offset) override
    {
        auto ndv_buckets_blob = getSingleValue<std::string_view>(block, index_offset + 0);

        if (!handler_context.columns_data.count(col_name))
        {
            throw Exception("previous result not found", ErrorCodes::LOGICAL_ERROR);
        }

        // write result to context
        auto ndv_buckets = createStatisticsTyped<StatsNdvBuckets>(StatisticsTag::NdvBuckets, ndv_buckets_blob);

        auto result = ndv_buckets->asResult();

        handler_context.columns_data.at(col_name).ndv_buckets_result_opt = std::move(result);
    }

private:
    HandlerContext & handler_context;
    std::shared_ptr<BucketBounds> bucket_bounds;
    String col_name;
    ColumnCollectConfig config;
    std::vector<String> sqls;
};


class FullCollectStep : public CollectStep
{
public:
    explicit FullCollectStep(StatisticsCollector & core_) : CollectStep(core_) { }

    void collectFirstStep(const ColumnDescVector & cols_desc)
    {
        TableHandler table_handler(table_info);
        table_handler.registerHandler(std::make_unique<RowCountHandler>(handler_context));

        for (const auto & col_desc : cols_desc)
        {
            table_handler.registerHandler(std::make_unique<FirstFullColumnHandler>(handler_context, col_desc));
        }

        auto sql = table_handler.getFullSql();

        auto query_context = SubqueryHelper::createQueryContext(context);
        auto block = executeSubQueryWithOneRow(sql, query_context, false, false);

        table_handler.parse(block);
    }

    // judge info from step
    void collectSecondStep(const ColumnDescVector & cols_desc)
    {
        TableHandler table_handler(table_info);
        table_handler.registerHandler(std::make_unique<RowCountHandler>(handler_context));
        bool to_collect = false;
        for (const auto & col_desc : cols_desc)
        {
            auto & col_info = handler_context.columns_data.at(col_desc.name);
            if (std::llround(col_info.ndv_value) >= 2 && col_info.bucket_bounds)
            {
                table_handler.registerHandler(std::make_unique<SecondFullColumnHandler>(handler_context, col_info.bucket_bounds, col_desc));
                to_collect = true;
            }
        }
        if (!to_collect)
        {
            // no need to collect the second
            return;
        }
        auto sql = table_handler.getFullSql();

        auto query_context = SubqueryHelper::createQueryContext(context);
        auto block = executeSubQueryWithOneRow(sql, query_context, false, false);

        table_handler.parse(block);
    }


    // exported symbol
    void collect(const ColumnDescVector & all_cols_desc) override
    {
        collectTable();

        auto cols_desc_groups = split(all_cols_desc, context->getSettingsRef().statistics_batch_max_columns);

        for (auto & cols_desc_group : cols_desc_groups)
        {
            collectFirstStep(cols_desc_group);
            collectSecondStep(cols_desc_group);
        }
    }
};

std::unique_ptr<CollectStep> createFullCollectStep(StatisticsCollector & core)
{
    return std::make_unique<FullCollectStep>(core);
}

}
