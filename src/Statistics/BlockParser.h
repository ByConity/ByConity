#pragma once
#include <Core/Types.h>
#include <DataStreams/BlockIO.h>
#include <Statistics/CommonTools.h>
#include <Statistics/SerdeDataType.h>
#include <Statistics/StatsColumnBasic.h>
#include <Statistics/StatsNdvBucketsImpl.h>
#include <Statistics/SubqueryHelper.h>
#include <Statistics/TypeMacros.h>

namespace DB::Statistics
{
inline Block getOnlyRowFrom(SubqueryHelper & helper)
{
    Block block;
    do
    {
        block = helper.getNextBlock();
    } while (block && block.rows() == 0);

    if (!block)
    {
        throw Exception("Not a Valid Block", ErrorCodes::LOGICAL_ERROR);
    }

    if (block.rows() > 1)
    {
        throw Exception("Too much rows", ErrorCodes::LOGICAL_ERROR);
    }
    return block;
}

template <typename T>
inline std::shared_ptr<StatsNdvBucketsImpl<T>>
StatsNdvBucketsFromImpl(const std::vector<std::tuple<size_t, size_t, String>> & data, const BucketBounds & bounds)
{
    auto bounds_impl = dynamic_cast<const BucketBoundsImpl<T> &>(bounds);
    auto stats = std::make_shared<StatsNdvBucketsImpl<T>>();
    stats->initialize(bounds_impl);

    auto bucket_size = bounds_impl.numBuckets();
    for (auto & [bucket_id, count, cpc_blob_b64] : data)
    {
        auto cpc_blob = base64Decode(cpc_blob_b64);

        StatsCpcSketch cpc;
        cpc.deserialize(cpc_blob);
        if (bucket_id >= bucket_size)
        {
            // currently just skip invalid data
            // TODO: maybe we should throw Exception here
            continue;
        }
        stats->set_count(bucket_id, count);
        stats->set_cpc(bucket_id, std::move(cpc));
    }
    return stats;
}

// only for test
std::shared_ptr<StatsNdvBuckets> StatsNdvBucketsFrom(SerdeDataType serde_data_type, BlockIO & io, const BucketBounds & bounds);

struct FirstQueryResult
{
    size_t col_id = -1;
    std::shared_ptr<StatsKllSketch> kll;
    std::shared_ptr<StatsColumnBasic> basic;
};

struct SecondQueryResult
{
    std::shared_ptr<StatsNdvBuckets> ndv_buckets;
    void parseV2(const Columns & columns, int64_t col_id)
    {
        auto & column = columns[col_id];
        auto blob = column->getDataAt(0).toString();
        this->ndv_buckets = createStatisticsTyped<StatsNdvBuckets>(StatisticsTag::NdvBuckets, base64Decode(blob));
    }

    static size_t num_columnV2(const Columns & columns)
    {
        // only one column
        return columns.size();
    }
};

template <typename ResultType>
inline std::vector<ResultType> parseBlockIO(BlockIO & io)
{
    Block block;
    std::vector<ResultType> results;
    while ((block = io.in->read()))
    {
        if (block.rows() == 0)
        {
            continue;
        }
        // collect nonnull count
        auto columns = block.getColumns();
        for (size_t row_id = 0; row_id < block.rows(); row_id++)
        {
            ResultType result;
            result.parse(columns, row_id);
        }
    }
    return results;
}

template <typename ResultType>
inline std::vector<ResultType> parseBlockIOV2(SubqueryHelper & helper)
{
    std::vector<ResultType> results;
    auto block = getOnlyRowFrom(helper);
    auto columns = block.getColumns();
    auto num_column = ResultType::num_columnV2(columns);

    for (size_t col_id = 0; col_id < num_column; ++col_id)
    {
        ResultType result;
        result.parseV2(columns, col_id);

        results.emplace_back(std::move(result));
    }
    return results;
}

enum class WrapperKind
{
    Invalid = 0,
    None = 1,
    StringToHash64 = 2, // when necessary, apply "cityHash64" in sql
    DecimalToFloat64 = 3, // when necessary, apply "toFloat64" in sql
};

struct ColumnCollectConfig
{
    WrapperKind wrapper_kind = WrapperKind::Invalid;
    bool need_count = true;
    bool need_ndv = true;
    bool need_histogram = true;
    bool need_minmax = false;
};

class FirstQueryBlockParser
{
public:
    std::vector<FirstQueryResult> parse(SubqueryHelper & helper);

    void appendColumn(const String & column_name, const ColumnCollectConfig & config);

    String getSql(const String & db_table);

    String getSqlWithSample(const String & db_table, const String & predicates);

    size_t get_size() { return column_names.size(); }

private:
    std::vector<String> column_names;
    std::vector<WrapperKind> column_wrapper_kinds;
    // record column that need collection of xxx, in sequential order
    std::vector<int> cols_need_count;
    std::vector<int> cols_need_ndv;
    std::vector<int> cols_need_histogram;
    std::vector<int> cols_need_minmax;
};

inline void CheckType(const DataTypePtr & type, SerdeDataType required_serde)
{
    auto nested = Statistics::decayDataType(type);
    auto actual_serde = dataTypeToSerde(*type);
    if (actual_serde != required_serde)
    {
        auto msg = fmt::format(
            FMT_STRING("Type mismatch, currently {} expected {}"), nested->getName(), Protos::SerdeDataType_Name(required_serde));
        throw Exception(msg, ErrorCodes::TYPE_MISMATCH);
    }
}


namespace impl
{
    template <class>
    inline constexpr bool always_false_v = false;
}

template <typename T>
void appendColumn(const ColumnWithTypeAndName & column_info, UInt64 row_count, std::vector<T> & target)
{
    auto base_column = column_info.column;
    CheckType(column_info.type, SerdeDataTypeFrom<T>);

    if (base_column->size() != row_count)
    {
        throw Exception("wtf", ErrorCodes::LOGICAL_ERROR);
    }

    if constexpr (std::is_arithmetic_v<T>)
    {
        auto column = dynamic_cast<const ColumnVector<T> *>(base_column.get());
        if (!column)
        {
            throw Exception("wrong type", ErrorCodes::LOGICAL_ERROR);
        }
        auto & container = column->getData();
        target.insert(target.end(), container.begin(), container.end());
    }
    else
    {
        static_assert(impl::always_false_v<T>, "unimplemented");
    }
}

template <typename T1, typename T2>
std::tuple<std::vector<T1>, std::vector<T2>> parseColumnsFromBlockIO(SubqueryHelper & helper)
{
    std::vector<T1> result1;
    std::vector<T2> result2;

    for (Block block; (block = helper.getNextBlock());)
    {
        if (block.rows() == 0)
        {
            continue;
        }
        if (block.columns() != 2)
        {
            throw Exception("unexpected columns", ErrorCodes::LOGICAL_ERROR);
        }
        auto row_count = block.rows();
        appendColumn(block.getByPosition(0), row_count, result1);
        appendColumn(block.getByPosition(1), row_count, result2);
    }
    return {std::move(result1), std::move(result2)};
}

} // namespace DB
