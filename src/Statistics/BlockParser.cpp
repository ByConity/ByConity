#include <vector>
#include <Statistics/BlockParser.h>
#include <Statistics/StatisticsSql.h>
#include <fmt/format.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB::Statistics
{

String get_wrapper(WrapperKind mode)
{
    switch (mode)
    {
        case WrapperKind::StringToHash64:
            return "cityHash64";
        case WrapperKind::DecimalToFloat64:
            return "toFloat64";
        default:
            throw Exception("Unknown wrapper mode", ErrorCodes::LOGICAL_ERROR);
    }
}

String FirstQueryBlockParser::getSql(const String & db_table)
{
    // like count(col1), cpc(col2)
    std::vector<String> select_items;
    auto inserter = [&](const String & agg_func, const std::vector<int> & col_ids, const std::set<WrapperKind> & modes) {
        for (auto id : col_ids)
        {
            auto wrapper_kind = this->column_wrapper_kinds[id];
            auto col_name = this->column_names[id];
            String text;
            if (modes.count(wrapper_kind))
            {
                auto wrapper = get_wrapper(wrapper_kind);
                text = fmt::format(FMT_STRING("{}({})"), wrapper, col_name);
            }
            else
            {
                text = col_name;
            }
            auto sql_agg_item = fmt::format(FMT_STRING("{}({})"), agg_func, text);
            select_items.emplace_back(sql_agg_item);
        }
    };

    inserter("count", cols_need_count, {});
    inserter("cpc", cols_need_ndv, {WrapperKind::DecimalToFloat64});
    inserter("kll", cols_need_histogram, {WrapperKind::DecimalToFloat64});
    inserter("min", cols_need_minmax, {WrapperKind::DecimalToFloat64, WrapperKind::StringToHash64});
    inserter("max", cols_need_minmax, {WrapperKind::DecimalToFloat64, WrapperKind::StringToHash64});
    auto sql = constructSelectSql(db_table, select_items);
    return sql;
}

void FirstQueryBlockParser::appendColumn(const String & column_name, const ColumnCollectConfig & config)
{
    auto column_id = static_cast<int>(column_names.size());
    column_names.emplace_back(column_name);
    column_wrapper_kinds.emplace_back(config.wrapper_kind);

    if (config.need_count)
    {
        cols_need_count.push_back(column_id);
    }
    if (config.need_ndv)
    {
        cols_need_ndv.push_back(column_id);
    }
    if (config.need_histogram)
    {
        cols_need_histogram.push_back(column_id);
    }
    if (config.need_minmax)
    {
        cols_need_minmax.push_back(column_id);
    }
}

static double FieldToDouble(const Field & field)
{
    if (field.isNull())
    {
        return std::numeric_limits<double>::quiet_NaN();
    }

    return applyVisitor(FieldVisitorConvertToNumber<double>(), field);
}

std::vector<FirstQueryResult> FirstQueryBlockParser::parse(SubqueryHelper & helper)
{
    std::vector<FirstQueryResult> results;
    auto block = getOnlyRowFrom(helper);
    results.clear();
    results.resize(column_names.size());
    auto offset = 0;
    for (UInt64 id = 0; id < results.size(); ++id)
    {
        results[id].col_id = id;
        results[id].basic = std::make_shared<StatsColumnBasic>();
    }

    for (auto id : cols_need_count)
    {
        auto count = block.getByPosition(offset).column->get64(0);
        results[id].basic->mutable_proto().set_nonnull_count(count);
        ++offset;
    }

    for (auto id : cols_need_ndv)
    {
        auto b64_blob = block.getByPosition(offset).column->getDataAt(0);
        if (b64_blob.size != 0)
        {
            auto blob = base64Decode(static_cast<std::string_view>(b64_blob));
            auto cpc = createStatisticsUntyped<StatsCpcSketch>(StatisticsTag::CpcSketch, blob);
            results[id].basic->mutable_proto().set_ndv_value(cpc->get_estimate());
        }
        ++offset;
    }
    for (auto id : cols_need_histogram)
    {
        auto b64_blob = block.getByPosition(offset).column->getDataAt(0);
        if (b64_blob.size != 0)
        {
            auto blob = base64Decode(static_cast<std::string_view>(b64_blob));
            auto kll = createStatisticsTyped<StatsKllSketch>(StatisticsTag::KllSketch, blob);
            auto min = kll->min_as_double().value_or(0);
            auto max = kll->max_as_double().value_or(0);
            results[id].kll = std::move(kll);
            auto & basic_proto = results[id].basic->mutable_proto();
            basic_proto.set_min_as_double(min);
            basic_proto.set_max_as_double(max);
        }
        ++offset;
    }

    for (auto id : cols_need_minmax)
    {
        auto value = FieldToDouble(block.getByPosition(offset).column->operator[](0));
        results[id].basic->mutable_proto().set_min_as_double(value);
        ++offset;
    }

    for (auto id : cols_need_minmax)
    {
        auto value = FieldToDouble(block.getByPosition(offset).column->operator[](0));
        results[id].basic->mutable_proto().set_max_as_double(value);
        ++offset;
    }

    return results;
}

String FirstQueryBlockParser::getSqlWithSample(const String & db_table, const String & predicate)
{
    auto sql = getSql(db_table);
    sql += " where " + predicate;
    return sql;
}

static std::shared_ptr<StatsNdvBuckets> StatsNdvBucketsFromImpl(
    SerdeDataType serde_data_type, const std::vector<std::tuple<size_t, size_t, String>> & data, const BucketBounds & bounds)
{
    switch (serde_data_type)
    {
#define CASE_DEF(TYPE, ALIAS_TYPE) \
    case SerdeDataType::TYPE: \
        return StatsNdvBucketsFromImpl<ALIAS_TYPE>(data, bounds);
#define CASE_DEF_SAME(TYPE) CASE_DEF(TYPE, TYPE)

        ALL_TYPE_ITERATE(CASE_DEF_SAME)
#undef CASE_DEF
#undef CASE_DEF_SAME
        default:
            throw Exception("unimplemented", ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::shared_ptr<StatsNdvBuckets> StatsNdvBucketsFrom(SerdeDataType serde_data_type, BlockIO & io, const BucketBounds & bounds)
{
    std::vector<std::tuple<size_t, size_t, String>> data;
    Block block;
    while ((block = io.in->read()))
    {
        auto size = block.rows();
        if (size == 0)
        {
            continue;
        }

        auto bucket_id_col = block.getByPosition(0).column;
        auto count_col = block.getByPosition(1).column;
        auto cpc_blob_col = block.getByPosition(2).column;
        for (size_t row_id = 0; row_id < size; ++row_id)
        {
            if (bucket_id_col->isNullAt(row_id))
            {
                continue;
            }
            auto bucket_id = bucket_id_col->get64(row_id);
            auto count = count_col->get64(row_id);
            auto cpc_blob_b64 = cpc_blob_col->getDataAt(row_id);
            data.emplace_back(bucket_id, count, cpc_blob_b64);
        }
    }
    return StatsNdvBucketsFromImpl(serde_data_type, data, bounds);
}

} // namespace DB
