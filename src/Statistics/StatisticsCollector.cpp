#include <set>
#include <tuple>
#include <unordered_set>
#include <vector>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Statistics/BlockParser.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CachedStatsProxy.h>
#include <Statistics/CommonTools.h>
#include <Statistics/ProfileCenter.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatisticsSql.h>
#include <Statistics/StatsNdvBucketsImpl.h>
#include <Statistics/SubqueryHelper.h>
#include <boost/algorithm/string/join.hpp>
#include <common/logger_useful.h>

namespace DB::Statistics
{
// return {left & right, left - right}
static std::pair<ColumnDescVector, ColumnDescVector> split_set(const ColumnDescVector & left, const Names & right)
{
    std::set<String> right_set{right.begin(), right.end()};
    ColumnDescVector result_and;
    ColumnDescVector result_minus;
    for (auto & desc : left)
    {
        if (right_set.count(desc.name))
        {
            result_and.emplace_back(desc);
        }
        else
        {
            result_minus.emplace_back(desc);
        }
    }
    return {std::move(result_and), std::move(result_minus)};
}


void StatisticsCollector::collectColumns(const ColumnDescVector & cols_desc)
{
    auto table = catalog->getStorageByTableId(table_info);

    auto & settings = catalog->getSettingsRef();
    auto enable_sample = settings.statistics_enable_sample;
    if (enable_sample)
    {
        this->collectTableStatsAndSample();
    }
    else
    {
        this->collectTableStats();
    }

    std::set<String> partitioned_column_set;

    if (sample_info_opt.has_value())
    {
        auto cols_need_full = sample_info_opt.value().partition_columns;
        auto [cols_full, cols_sample] = split_set(cols_desc, cols_need_full);
        this->collectColumnStats(cols_full, false);
        this->collectColumnStats(cols_sample, true);
        patchSampleResult(cols_sample);
    }
    else
    {
        this->collectColumnStats(cols_desc, false);
    }
}

void StatisticsCollector::collectFull()
{
    auto columns_desc = catalog->getCollectableColumns(table_info);
    collectColumns(columns_desc);
}

void StatisticsCollector::collectTableStats()
{
    auto table_name = getDatabaseDotTable();
    auto cnt_query = fmt::format(FMT_STRING("select count(*) from {}"), table_name);
    auto subquery_helper = SubqueryHelper::create(context, cnt_query);
    auto block = getOnlyRowFrom(subquery_helper);

    auto & tuple = block.getByPosition(0);
    auto & type = tuple.type;
    if (type->getTypeId() != TypeIndex::UInt64)
    {
        auto msg = "Logical Error: row count type mismatch";
        throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
    }
    auto row_count = tuple.column->getUInt(0);
    auto stats_table_basic = std::make_shared<StatsTableBasic>();
    stats_table_basic->setRowCount(row_count);

    table_stats = TableStats{stats_table_basic};
}

void StatisticsCollector::collectTableStatsAndSample()
{
    auto table_name = getDatabaseDotTable();

    auto cnt_query = fmt::format(
        FMT_STRING("select count(*) as _cnt, sipHash64(_part) as _key "
                   "from {} "
                   "group by sipHash64(_part) "
                   "with rollup "
                   "order by _key, _cnt desc"),
        table_name);

    auto subquery_helper = SubqueryHelper::create(context, cnt_query);
    auto [cnt_vec, hash_vec] = parseColumnsFromBlockIO<UInt64, UInt64>(subquery_helper);

    UInt64 row_count;
    do
    {
        if (cnt_vec.empty())
        {
            row_count = 0;
            break;
        }

        auto parts_count = cnt_vec.size() - 1;

        // read row_count from rollup
        auto rollup_count = cnt_vec[0];
        row_count = rollup_count;

        if (rollup_count <= catalog->getSettingsRef().statistics_sample_row_threshold)
        {
            // do nothing
            break;
        }

        UInt64 accumulate_count = 0;
        UInt64 current_key = 0;
        const auto expected_sample_row = catalog->getSettingsRef().statistics_sample_row_count;
        for (UInt64 row_id = 0; row_id < parts_count; ++row_id)
        {
            auto index = row_id + 1;
            auto cnt = cnt_vec[index];
            auto hash = hash_vec[index];
            accumulate_count += cnt;
            current_key = hash;
            if (accumulate_count >= expected_sample_row)
            {
                break;
            }
        }
        StatisticsSampleInfo info;
        info.predicates = fmt::format(FMT_STRING("where sipHash64(_part) <= {}"), current_key);
        info.partition_columns = catalog->getPartitionColumns(table_info);
        info.total_rows = row_count;
        info.sampled_rows = accumulate_count;
        this->sample_info_opt = std::move(info);
    } while (false);

    auto stats_table_basic = std::make_shared<StatsTableBasic>();
    stats_table_basic->setRowCount(row_count);
    table_stats = TableStats{stats_table_basic};
}

static ColumnCollectConfig get_column_config(CatalogAdaptorPtr catalog, const DataTypePtr & type)
{
    auto & settings = catalog->getSettingsRef();
    ColumnCollectConfig config;
    config.need_count = true;
    config.need_ndv = true;
    config.need_histogram = true;
    config.need_minmax = true;

    if (!settings.statistics_collect_histogram)
    {
        config.need_histogram = false;
    }
    else if (!settings.statistics_collect_floating_histogram)
    {
        if (isFloat(type) || isDecimal(type))
        {
            config.need_histogram = false;
        }
    }

    if (isStringOrFixedString(type))
    {
        config.wrapper_kind = WrapperKind::StringToHash64;
    }
    else if (isDecimal(type) || isDateTime64(type) || isTime(type))
    {
        // Note: DateTime64 is a Decimal, convert it to float64
        config.wrapper_kind = WrapperKind::DecimalToFloat64;
    }
    else
    {
        config.wrapper_kind = WrapperKind::None;
    }

    return config;
}

std::vector<FirstQueryResult> StatisticsCollector::collectFirstStage(const ColumnDescVector & cols_desc, bool do_sample)
{
    if (cols_desc.empty())
    {
        // DO NOTHING
        return {};
    }
    auto db_table = getDatabaseDotTable();
    FirstQueryBlockParser engine;
    for (auto & col_desc : cols_desc)
    {
        auto type = Statistics::decayDataType(col_desc.type);
        auto config = get_column_config(catalog, type);
        engine.appendColumn(col_desc.name, config);
    }

    auto all_sql = engine.getSql(db_table);
    if (do_sample)
    {
        if (!sample_info_opt.has_value())
        {
            throw Exception("sample info missing", ErrorCodes::LOGICAL_ERROR);
        }
        all_sql += " " + sample_info_opt.value().predicates;
    }

    auto subquery_helper = SubqueryHelper::create(context, all_sql);
    auto results = engine.parse(subquery_helper);
    return results;
}

void StatisticsCollector::collectColumnStats(const ColumnDescVector & cols_desc, bool do_sample)
{
    if (cols_desc.empty())
    {
        // DO NOTHING
        return;
    }
    auto & settings = catalog->getSettingsRef();

    auto results = this->collectFirstStage(cols_desc, do_sample);

    std::vector<std::pair<NameAndTypePair, std::shared_ptr<BucketBounds>>> second_cols_desc;
    for (auto & result : results)
    {
        auto col_id = result.col_id;
        if (col_id >= cols_desc.size())
        {
            throw Exception("col_id is unexpected", ErrorCodes::LOGICAL_ERROR);
        }
        auto & col_desc = cols_desc[col_id];
        auto type = Statistics::decayDataType(col_desc.type);
        ColumnStats column_stats;
        auto total_sampled_ndv = result.basic->get_proto().ndv_value();

        if (result.kll && !result.kll->isEmpty())
        {
            if (result.kll->min_as_double() == result.kll->max_as_double())
            {
                // singleton, do nothing
                // construct single bucket at `toPlanNodeStatistics` latter
            }
            else if (do_sample && total_sampled_ndv > settings.statistics_sample_histogram_enable_ndv_threshold)
            {
                // don't collect histogram when ndv is too large
                // because high ndv means histogram may be not reliable
            }
            else if ((isDecimal(type) || isFloat(type)) && !settings.statistics_collect_floating_histogram_ndv)
            {
                // construct ndv_buckets from kll
                column_stats.ndv_buckets_result = result.kll->generateNdvBucketsResult(total_sampled_ndv);
            }
            else
            {
                // default case: collect
                auto bucket_bounds = result.kll->get_bounds();
                second_cols_desc.emplace_back(col_desc, std::move(bucket_bounds));
            }
        }

        column_stats.basic = std::move(result.basic);
        columns_stats.emplace(col_desc.name, std::move(column_stats));
    }

    collectColumnsNdv(second_cols_desc, do_sample);
}

void StatisticsCollector::collectColumnsNdv(
    const std::vector<std::pair<NameAndTypePair, std::shared_ptr<BucketBounds>>> & cols_desc, bool do_sample)
{
    if (cols_desc.empty())
    {
        // DO NOTHING
        return;
    }

    std::vector<String> sub_sqls;
    for (auto & [col_desc, bounds] : cols_desc)
    {
        // ndv >= 2
        auto bucket_blob = bounds->serialize();
        auto bucket_blob_b64 = base64Encode(bucket_blob);
        auto text = col_desc.name;
        auto type = Statistics::decayDataType(col_desc.type);
        auto config = get_column_config(catalog, type);
        if (config.wrapper_kind == WrapperKind::DecimalToFloat64)
        {
            text = fmt::format(FMT_STRING("toFloat64({})"), text);
        }
        auto sub_sql = fmt::format(FMT_STRING("ndv_buckets('{}')({})"), bucket_blob_b64, text);
        sub_sqls.push_back(std::move(sub_sql));
    }

    auto table_name = getDatabaseDotTable();
    String all_sql = "select ";
    all_sql += boost::algorithm::join(sub_sqls, ", ");
    all_sql += " from " + table_name;
    if (do_sample)
    {
        if (!sample_info_opt.has_value())
        {
            throw Exception("sample info missing", ErrorCodes::LOGICAL_ERROR);
        }
        all_sql += " " + sample_info_opt.value().predicates;
    }

    auto subquery_helper = SubqueryHelper::create(context, all_sql);
    auto results = parseBlockIOV2<SecondQueryResult>(subquery_helper);

    if (results.size() != cols_desc.size())
    {
        throw Exception("Size not match", ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t col_id = 0; col_id < results.size(); ++col_id)
    {
        auto col_name = cols_desc[col_id].first.name;
        columns_stats.at(col_name).ndv_buckets_result = results[col_id].ndv_buckets->asResult();
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
    for (auto & [name, stats] : columns_stats)
    {
        data.column_stats[name] = stats.writeToCollection();
    }
    auto proxy = createCachedStatsProxy(catalog);
    proxy->put(table_info, std::move(data));
    catalog->invalidateClusterStatsCache(table_info);
}

void StatisticsCollector::readAllFromCatalog()
{
    auto proxy = createCachedStatsProxy(catalog);
    auto data = proxy->get(table_info);
    table_stats.readFromCollection(data.table_stats);
    for (auto & [name, stats] : data.column_stats)
    {
        columns_stats[name].readFromCollection(stats);
    }
}

void StatisticsCollector::readFromCatalog(const std::vector<String> & cols_name)
{
    ColumnDescVector cols_desc;
    auto full_cols_desc = catalog->getCollectableColumns(table_info);
    std::unordered_set<String> valid_set(cols_name.begin(), cols_name.end());
    for (const auto & col_desc : full_cols_desc)
    {
        if (valid_set.count(col_desc.name))
        {
            cols_desc.emplace_back(col_desc);
        }
    }

    this->readFromCatalog(cols_desc);
}

void StatisticsCollector::readFromCatalog(const ColumnDescVector & cols_desc)
{
    Stopwatch watch;
    auto proxy = createCachedStatsProxy(catalog);
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
            auto nonnull_count = stats.basic->get_proto().nonnull_count();
            symbol->null_counts = table_row_count - nonnull_count;
            symbol->min = stats.basic->get_proto().min_as_double();
            symbol->max = stats.basic->get_proto().max_as_double();
            symbol->ndv = AdjustNdvWithCount(stats.basic->get_proto().ndv_value(), nonnull_count);
            symbol->unknown = false;
            auto construct_single_bucket_histogram = symbol->ndv == 1;

            if (stats.ndv_buckets_result)
            {
                stats.ndv_buckets_result->writeSymbolStatistics(*symbol);
            }
            else if (construct_single_bucket_histogram)
            {
                auto bucket = std::make_shared<Bucket>(symbol->min, symbol->max, symbol->ndv, nonnull_count, true, true);
                symbol->histogram.emplaceBackBucket(std::move(bucket));
            }
        }
        result->updateSymbolStatistics(col, symbol);
    }
    return result;
}


static double scale_count(double sampled_rows, double total_rows, double count)
{
    if (sampled_rows == 0)
        return 0;

    auto result = count * total_rows / sampled_rows;
    return result;
}

// NDVsample = NDV * (1 - (1 - COUNTsample/ COUNT) ^ (COUNT/NDV) )
// NOTE: total_count, sample_count should be nonnull
double scale_ndv(double sample_count, double sample_ndv, double total_count)
{
    if (sample_count == 0)
        return 0;

    auto calc_sample_ndv
        = [=](double estimate) { return estimate * (1 - std::pow(1 - sample_count / total_count, total_count / estimate)); };

    if (sample_count > total_count)
    {
        throw Exception("ill-formed ndv", ErrorCodes::LOGICAL_ERROR);
    }

    if (sample_ndv >= total_count)
    {
        return total_count;
    }

    auto estimate_begin = std::max(1.0, sample_ndv);
    auto estimate_end = total_count;
    // Binary Search ndv
    while ((estimate_end - estimate_begin) / estimate_end > 0.0001)
    {
        auto mid = (estimate_begin + estimate_end) / 2;
        auto calc_result = calc_sample_ndv(mid);
        if (calc_result <= sample_ndv)
        {
            estimate_begin = mid;
        }
        else
        {
            estimate_end = mid;
        }
    }
    return (estimate_end + estimate_begin) / 2;
}

void StatisticsCollector::patchSampleResult(const ColumnDescVector & cols_desc)
{
    if (cols_desc.empty())
    {
        return;
    }
    if (!sample_info_opt.has_value())
    {
        throw Exception("missing sample info", ErrorCodes::LOGICAL_ERROR);
    }
    auto & sample_info = sample_info_opt.value();
    for (auto & col_desc : cols_desc)
    {
        auto & column_stat = columns_stats[col_desc.name];
        if (column_stat.basic)
        {
            auto & basic_proto = column_stat.basic->mutable_proto();
            double sample_nonnull_count = basic_proto.nonnull_count();
            double estimate_nonnull_count = scale_count(sample_info.sampled_rows, sample_info.total_rows, sample_nonnull_count);
            basic_proto.set_nonnull_count(std::llround(estimate_nonnull_count));
            double sample_ndv = basic_proto.ndv_value();
            double estimate_ndv = scale_ndv(sample_nonnull_count, sample_ndv, estimate_nonnull_count);
            basic_proto.set_ndv_value(estimate_ndv);
        }
        // scale histogram
        if (column_stat.ndv_buckets_result)
        {
            auto & result = *column_stat.ndv_buckets_result;
            for (size_t i = 0; i < result.num_buckets(); ++i)
            {
                double sample_nonnull_count = result.get_count(i);
                double estimate_nonnull_count = scale_count(sample_info.sampled_rows, sample_info.total_rows, sample_nonnull_count);
                result.set_count(i, std::llround(estimate_nonnull_count));
                double sample_ndv = result.get_ndv(i);
                double estimate_ndv = scale_ndv(sample_nonnull_count, sample_ndv, estimate_nonnull_count);
                result.set_ndv(i, estimate_ndv);
            }
        }
    }
}
}
