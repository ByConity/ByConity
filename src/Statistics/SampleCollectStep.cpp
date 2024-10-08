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

#include <numeric>
#include <type_traits>
#include <Columns/ColumnNullable.h>
#include <Statistics/CollectStep.h>
#include <Statistics/ParseUtils.h>
#include <Statistics/ScaleAlgorithm.h>
#include <Statistics/StatsHllSketch.h>
#include <Statistics/StatsNdvBucketsExtend.h>
#include <Statistics/TypeUtils.h>
#include <boost/noncopyable.hpp>
#include <fmt/format.h>

namespace DB::Statistics
{

static const String virtual_mark_id = "cityHash64(blockNumber(), _part_uuid, intDiv(rowNumberInBlock(), 8192))";

// hll is like HyperLogLog, kll is to calculate bucket bounds for equal-height histogram
// this fetch data via the following sql:
// select
//      count(<col1>), hll(<col1>), hll(cityHash64(<col1>, __mark_id), kll(<col1>),
//      count(<col2>), hll(<col2>), hll(cityHash64(<col2>, __mark_id), kll(<col2>),
//      count(<col3>), hll(<col3>), hll(cityHash64(<col3>, __mark_id), kll(<col3>),
// from
//      <table>
class FirstSampleColumnHandler : public ColumnHandlerBase
{
public:
    explicit FirstSampleColumnHandler(HandlerContext & handler_context_, const NameAndTypePair & col_desc)
        : handler_context(handler_context_)
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
        auto ndv_sql = fmt::format(FMT_STRING("uniq({})"), wrapped_col_name);
        auto block_ndv_sql = fmt::format(FMT_STRING("hll(cityHash64({}, {}))"), wrapped_col_name, virtual_mark_id);

        sqls.emplace_back(count_sql);
        sqls.emplace_back(ndv_sql);
        sqls.emplace_back(block_ndv_sql);

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
            getLogger("FirstSampleColumnHandler"),
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
        auto wrapped_col_name = getWrappedColumnName(config, col_name);

        // count(col) SAMPLE
        auto sample_nonnull_count = static_cast<double>(getSingleValue<UInt64>(block, index_offset++));
        // hll(col) SAMPLE
        double sample_ndv = getSingleValue<UInt64>(block, index_offset++);
        // hll(cityHash64(col, _mark_id)  SAMPLE
        auto block_ndv_blob = getSingleValue<std::string_view>(block, index_offset++);

        // select count(*) SAMPLE
        double full_count = handler_context.full_count;
        // select count(*) FULL
        double sample_row_count = handler_context.query_row_count.value_or(0);

        // due to snapshot is not implemented,
        // and we get full_count from first sql and sample_row_count from second sql
        // there are possiblity that full_count < sample_row_count when insertion happens between them
        // then sample_ratio > 1, leading to unexpected exception
        // dirty hack this by assign full_count to sample_row_count when this case occurs

        /// TODO: remove this hack when snapshot is ready
        if (full_count < sample_row_count)
        {
            handler_context.full_count = sample_row_count;
            full_count = sample_row_count;
        }

        HandlerColumnData result;

        // algorithm requires block_ndv as sample_nonnull
        if (sample_nonnull_count != 0)
        {
            double block_ndv = getNdvFromSketchBinary(block_ndv_blob);
            block_ndv = std::min(block_ndv, sample_nonnull_count);

            result.nonnull_count = scaleCount(full_count, sample_row_count, sample_nonnull_count);

            // ensure it's a valid value
            sample_ndv = std::min(block_ndv, sample_ndv);
            auto estimated_ndv = scaleNdv(full_count, sample_row_count, sample_ndv, block_ndv);
            // 0.02 is hyperloglog error rate
            constexpr double err_rate = 0.02;
            auto sample_ndv_lb = std::min(block_ndv, sample_ndv * (1 - err_rate));
            auto estimated_ndv_lower_bound = scaleNdv(full_count, sample_row_count, sample_ndv_lb, block_ndv);
            auto sample_ndv_ub = std::min(block_ndv, sample_ndv * (1 + err_rate));
            auto estimated_ndv_upper_bound = scaleNdv(full_count, sample_row_count, sample_ndv_ub, block_ndv);

            LOG_INFO(
                getLogger("ThirdSampleColumnHandler"),
                fmt::format(
                    FMT_STRING("estimated_ndv={}, estimated_ndv_low_bound={}, estimated_ndv_upper_bound={}"),
                    estimated_ndv,
                    estimated_ndv_lower_bound,
                    estimated_ndv_upper_bound));

            bool use_accurate = false;
            switch (handler_context.settings.accurate_sample_ndv())
            {
                case StatisticsAccurateSampleNdvMode::AUTO:
                    // when error of estimated ndv more than 30% due to HyperLogLog,
                    // that is, low_bound is less than 70% of the upper bound
                    // use accurate sample ndv
                    // this case happens when full_ndv > k * sample_size, where k is a constant
                    use_accurate = estimated_ndv_lower_bound < estimated_ndv_upper_bound * 0.7;
                    break;
                case StatisticsAccurateSampleNdvMode::ALWAYS:
                    use_accurate = true;
                    break;
                case StatisticsAccurateSampleNdvMode::NEVER:
                    use_accurate = false;
                    break;
            }

            if (!use_accurate)
            {
                result.is_ndv_reliable = true;
                result.ndv_value = estimated_ndv;
            }
            else
            {
                result.is_ndv_reliable = false;
            }

            if (config.need_histogram)
            {
                // kll(col) SAMPLE
                auto histogram_blob = getSingleValue<std::string_view>(block, index_offset++);
                if (!histogram_blob.empty())
                {
                    auto histogram = createStatisticsTyped<StatsKllSketch>(StatisticsTag::KllSketch, histogram_blob);

                    result.min_as_double = histogram->minAsDouble().value_or(std::nan(""));
                    result.max_as_double = histogram->maxAsDouble().value_or(std::nan(""));
                    auto histogram_bucket_size = handler_context.settings.histogram_bucket_size();
                    result.bucket_bounds = histogram->getBucketBounds(histogram_bucket_size);
                }
            }

            if (config.need_minmax)
            {
                result.min_as_double = getSingleValue<Float64>(block, index_offset++);
                result.max_as_double = getSingleValue<Float64>(block, index_offset++);
            }

            if (config.need_length)
            {
                result.length_opt = getSingleValue<UInt64>(block, index_offset++) * 1.0 / sample_row_count * full_count;
            }

            LOG_INFO(
                getLogger("FirstSampleColumnHandler"),
                fmt::format(
                    FMT_STRING("col info: col={} && "
                               "context raw data: full_count={}, sample_row_count={} && "
                               "column raw data: sample_nonnull_count={}, block_ndv={}, sample_ndv={}&& "
                               "cast data: result.nonnull_count={}, result.ndv_value={}, result.length={}"),
                    col_name,
                    full_count,
                    sample_row_count,
                    sample_nonnull_count,
                    block_ndv,
                    sample_ndv,
                    result.nonnull_count,
                    result.ndv_value,
                    result.length_opt.value_or(0)));
        }
        else
        {
            result.nonnull_count = 0;
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

// ndv buckets extend contains count, ndv, block_ndv for each bucket
// this fetch data via the following sql:
// select
//      ndv_buckets_extend('<BASE64_OF_BUCKETS_1>', <col1>),
//      ndv_buckets_extend('<BASE64_OF_BUCKETS_2>', <col2>),
//      ndv_buckets_extend('<BASE64_OF_BUCKETS_3>', <col3>)
// from
//      <table>
//
// this sql is fast
class SecondSampleColumnHandler : public ColumnHandlerBase
{
public:
    explicit SecondSampleColumnHandler(
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

        auto ndv_buckets_extend_sql
            = fmt::format(FMT_STRING("ndv_buckets_extend('{}')({}, {})"), bounds_b64, wrapped_col_name, virtual_mark_id);
        // to estimate ndv
        sqls.emplace_back(ndv_buckets_extend_sql);
    }

    const std::vector<String> & getSqls() override { return sqls; }

    void parse(const Block & block, size_t index_offset) override
    {
        auto ndv_buckets_extend_blob = getSingleValue<std::string_view>(block, index_offset + 0);

        double full_count = handler_context.full_count;
        double sample_row_count = handler_context.query_row_count.value_or(0);
        // due to snapshot is not implemented,
        // and we get full_count from first sql and sample_row_count from second sql
        // there are possiblity that full_count < sample_row_count when insertion happens between them
        // then sample_ratio > 1, leading to unexpected exception
        // dirty hack this by assign full_count to sample_row_count when this case occurs

        /// TODO: remove this hack when snapshot is ready
        if (full_count < sample_row_count)
        {
            handler_context.full_count = sample_row_count;
            full_count = sample_row_count;
        }

        if (!handler_context.columns_data.count(col_name))
        {
            throw Exception("previous result not found", ErrorCodes::LOGICAL_ERROR);
        }

        // write result to context
        auto ndv_buckets_extend = createStatisticsTyped<StatsNdvBucketsExtend>(StatisticsTag::NdvBucketsExtend, ndv_buckets_extend_blob);
        auto nonnull_counts = ndv_buckets_extend->getCounts();
        auto ndvs = ndv_buckets_extend->getNdvs();
        auto block_ndvs = ndv_buckets_extend->getBlockNdvs();
        if (nonnull_counts.size() != ndvs.size() || nonnull_counts.size() != block_ndvs.size())
        {
            throw Exception("mismatched bucket size", ErrorCodes::LOGICAL_ERROR);
        }

        std::vector<UInt64> counts_estimate;
        std::vector<double> ndvs_estimate;
        for (size_t index = 0; index < nonnull_counts.size(); ++index)
        {
            auto sample_nonnull_count = nonnull_counts[index];
            auto sample_ndv = ndvs[index];
            auto sample_block_ndv = block_ndvs[index];
            auto count_estimate = std::llround(scaleCount(full_count, sample_row_count, sample_nonnull_count));
            auto ndv_estimate = scaleNdv(full_count, sample_row_count, sample_ndv, sample_block_ndv);
            ndv_estimate = std::min(ndv_estimate, static_cast<double>(count_estimate));
            counts_estimate.emplace_back(count_estimate);
            ndvs_estimate.emplace_back(ndv_estimate);
        }
        auto result = StatsNdvBucketsResult::create(*bucket_bounds, std::move(counts_estimate), std::move(ndvs_estimate));

        handler_context.columns_data.at(col_name).ndv_buckets_result_opt = result;
    }

private:
    HandlerContext & handler_context;
    std::shared_ptr<BucketBounds> bucket_bounds;
    String col_name;
    ColumnCollectConfig config;
    std::vector<String> sqls;
};


// bucket_bounds_search is to calculate bucket index of the histogram for an element
// this will generate the following sql, calculating result for ndv
//
// select
//     __tmp_tag,               // bucket index
//     count(*),                // bucket ndv
//     sum(__tmp_mark_cnt),     // bucket sum block ndv
//     sum(__tmp_freq)          // bucket count
// from
//     (
//         select
//             bucket_bounds_search('<BASE64_OF_BUCKET_BOUNDS>', <col>) as __tmp_tag,
//             uniq(__mark_id) as __tmp_mark_cnt,
//             count(*) as __tmp_freq
//         from
//             test.test_stats SAMPLE <SAMPLE_COUNT>
//         group by
//             <col>
//     )
// group by
//     __tmp_tag
// order by
//     __tmp_tag
//
// this sql is slow
String constructThirdSql(
    const CollectorSettings & settings,
    const StatsTableIdentifier & table_info,
    const NameAndTypePair & col_desc,
    const std::shared_ptr<BucketBounds> & bucket_bounds,
    const String & sample_tail)
{
    auto config = getColumnConfig(settings, col_desc.type);
    auto wrapped_col_name = getWrappedColumnName(config, colNameForSql(col_desc.name));
    auto tag_sql = [&]() -> String {
        if (bucket_bounds)
        {
            auto bounds_b64 = base64Encode(bucket_bounds->serialize());
            return fmt::format(FMT_STRING("bucket_bounds_search('{}', {})"), bounds_b64, wrapped_col_name);
        }
        else
        {
            // when bucket_bounds not exists, possibly statistics_collect_histogram=false,
            // we treat as if histogram has only one bucket, i.e., "group by 0"
            return "0";
        }
    }();

    auto mark_sql = fmt::format(FMT_STRING("uniq(cityHash64({}))"), virtual_mark_id);
    auto inner_sql = fmt::format(
        FMT_STRING("select {} as __tmp_tag, {} as __tmp_mark_cnt, count(*) as __tmp_freq from {} {} group by {}"),
        tag_sql,
        mark_sql,
        table_info.getDbTableName(),
        sample_tail,
        wrapped_col_name);
    auto full_sql = fmt::format(
        FMT_STRING("select __tmp_tag, count(*), sum(__tmp_mark_cnt), sum(__tmp_freq) from ({}) group by __tmp_tag order by __tmp_tag"),
        inner_sql);
    return full_sql;
}

class SampleCollectStep : public CollectStep
{
public:
    explicit SampleCollectStep(StatisticsCollector & core_) : CollectStep(core_) { }

    String getSampleTail(bool for_uniq_exact)
    {
        auto row_count = handler_context.full_count;
        const auto & settings = handler_context.settings;
        UInt64 sample_row_count;

        if (for_uniq_exact)
        {
            sample_row_count = settings.accurate_sample_ndv_row_limit();
        }
        else
        {
            sample_row_count = std::max<UInt64>(settings.sample_row_count(), row_count * settings.sample_ratio());
        }

        if (row_count <= sample_row_count)
        {
            return ""; // don't do sample
        }

        auto ratio = sample_row_count / static_cast<double>(row_count);

        // sample by rows is problematic, always use ratio instead
        // keep 3 significant digits
        return fmt::format(FMT_STRING(" SAMPLE {:.3g}"), ratio);
    }

    void firstCollectStep(const ColumnDescVector & cols_desc)
    {
        TableHandler table_handler(table_info);
        table_handler.registerHandler(std::make_unique<RowCountHandler>(handler_context));

        for (auto & col_desc : cols_desc)
        {
            table_handler.registerHandler(std::make_unique<FirstSampleColumnHandler>(handler_context, col_desc));
        }

        auto sql = table_handler.getFullSql();
        auto sample_tail_with_space = getSampleTail(false);

        sql += sample_tail_with_space;

        auto query_context = SubqueryHelper::createQueryContext(context);
        auto block = executeSubQueryWithOneRow(sql, query_context, false, false);

        table_handler.parse(block);
    }

    ColumnDescVector collectSecondStep(const ColumnDescVector & cols_desc)
    {
        TableHandler table_handler(table_info);
        table_handler.registerHandler(std::make_unique<RowCountHandler>(handler_context));

        bool to_collect = false;
        ColumnDescVector unhandled_cols;

        for (auto & col_desc : cols_desc)
        {
            auto & col_info = handler_context.columns_data.at(col_desc.name);

            // cannot handle col in second step whose total ndv is not reliable
            if (!col_info.is_ndv_reliable)
            {
                unhandled_cols.push_back(col_desc);
                continue;
            }

            auto ndv_value = col_info.ndv_value;

            if (std::llround(ndv_value) >= 2 && col_info.bucket_bounds)
            {
                table_handler.registerHandler(
                    std::make_unique<SecondSampleColumnHandler>(handler_context, col_info.bucket_bounds, col_desc));
                to_collect = true;
            }
            else
            {
                // no need to collect since ndv=1, skip
            }
        }

        if (to_collect)
        {
            auto sql = table_handler.getFullSql();
            sql += getSampleTail(false);

            auto query_context = SubqueryHelper::createQueryContext(context);
            auto block = executeSubQueryWithOneRow(sql, query_context, false, false);

            table_handler.parse(block);
        }

        return unhandled_cols;
    }


    void collectThirdStep(const ColumnDescVector & cols_desc)
    {
        // handle using special sql
        for (auto & col_desc : cols_desc)
        {
            auto & col_data = handler_context.columns_data.at(col_desc.name);
            auto full_sql = constructThirdSql(handler_context.settings, table_info, col_desc, col_data.bucket_bounds, getSampleTail(true));
            LOG_INFO(getLogger("thirdSampleColumnHandler"), full_sql);

            // when bucket_bounds not exists, we will use 0 as bucket_id
            // so there are always 1 buckets
            auto num_buckets = col_data.bucket_bounds ? col_data.bucket_bounds->numBuckets() : 1;
            std::vector<double> sample_ndvs(num_buckets);
            std::vector<double> sample_block_ndvs(num_buckets);
            std::vector<double> sample_counts(num_buckets);
            auto sample_null_count = 0;

            auto proc_block = [num_buckets, &sample_ndvs, &sample_block_ndvs, &sample_counts, &sample_null_count](Block & block) {
                // tag is bucket index
                auto col_tag = block.getByPosition(0).column;
                auto nested_col_tag = getNestedColumn(col_tag);
                auto nested_col_ndv = getNestedColumn(block.getByPosition(1).column);
                auto nested_col_block_ndv = getNestedColumn(block.getByPosition(2).column);
                auto nested_col_count = getNestedColumn(block.getByPosition(3).column);

                for (size_t index = 0; index < block.rows(); ++index)
                {
                    // when it is null, record null count
                    if (col_tag->isNullAt(index))
                    {
                        sample_null_count = nested_col_count->getUInt(index);
                        continue;
                    }
                    auto tag = nested_col_tag->getUInt(index);
                    if (tag >= num_buckets)
                    {
                        throw Exception("unexpected tag", ErrorCodes::LOGICAL_ERROR);
                    }
                    sample_ndvs[tag] = nested_col_ndv->getUInt(index);
                    sample_block_ndvs[tag] = nested_col_block_ndv->getUInt(index);
                    sample_counts[tag] = nested_col_count->getUInt(index);
                }
            };

            auto query_context = SubqueryHelper::createQueryContext(context);
            executeSubQuery(full_sql, query_context, proc_block);

            auto sample_nonnull_count = std::accumulate(sample_counts.begin(), sample_counts.end(), 0.0);
            auto sample_row_count = sample_nonnull_count + sample_null_count;

            // due to snapshot is not implemented,
            // and we get full_count from first sql and sample_row_count from second sql
            // there are possiblity that full_count < sample_row_count when insertion happens between them
            // then sample_ratio > 1, leading to unexpected exception
            // dirty hack this by assign full_count to sample_row_count when this case occurs

            /// TODO: remove this hack when snapshot is ready
            auto full_count = handler_context.full_count;
            if (full_count < sample_row_count)
            {
                handler_context.full_count = sample_row_count;
                full_count = sample_row_count;
            }

            {
                // column level stats
                auto sample_ndv = std::accumulate(sample_ndvs.begin(), sample_ndvs.end(), 0.0);
                auto sample_block_ndv = std::accumulate(sample_block_ndvs.begin(), sample_block_ndvs.end(), 0.0);
                col_data.nonnull_count = scaleCount(full_count, sample_row_count, sample_nonnull_count);
                col_data.is_ndv_reliable = true;
                col_data.ndv_value = scaleNdv(full_count, sample_row_count, sample_ndv, sample_block_ndv);
            }

            if (col_data.bucket_bounds)
            {
                std::vector<UInt64> res_counts(num_buckets);
                std::vector<double> res_ndvs(num_buckets);
                for (size_t index = 0; index < num_buckets; ++index)
                {
                    res_counts[index] = std::llround(scaleCount(full_count, sample_row_count, sample_counts[index]));
                    res_ndvs[index] = scaleNdv(full_count, sample_row_count, sample_ndvs[index], sample_block_ndvs[index]);
                }
                col_data.ndv_buckets_result_opt
                    = StatsNdvBucketsResult::create(*col_data.bucket_bounds, std::move(res_counts), std::move(res_ndvs));
            }
        }
    }

    // exported symbol
    void collect(const ColumnDescVector & all_cols_desc) override
    {
        // TODO: split them into several columns step
        collectTable();

        auto cols_desc_groups = split(all_cols_desc, context->getSettingsRef().statistics_batch_max_columns);

        ColumnDescVector all_unhandled_cols;

        for (auto & cols_desc_group : cols_desc_groups)
        {
            firstCollectStep(cols_desc_group);
            auto unhandled_cols = collectSecondStep(cols_desc_group);
            all_unhandled_cols.insert(all_unhandled_cols.end(), unhandled_cols.begin(), unhandled_cols.end());
        }

        auto unhandled_cols_groups = split(all_unhandled_cols, context->getSettingsRef().statistics_batch_max_columns);
        for (auto & unhandled_cols_group : unhandled_cols_groups)
        {
            collectThirdStep(unhandled_cols_group);
        }
    }

private:
};

std::unique_ptr<CollectStep> createSampleCollectStep(StatisticsCollector & core)
{
    return std::make_unique<SampleCollectStep>(core);
}

}
