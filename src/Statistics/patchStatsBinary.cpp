#include <Statistics/SerdeUtils.h>
#include <Statistics/TypeMacros.h>
#include <Statistics/patchStatsBinary.h>
#if 0
namespace DB::Statistics
{
namespace
{
    enum class CnchIndex
    {
        Nothing = 0,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        UInt128,
        Int8,
        Int16,
        Int32,
        Int64,
        Int128,
        Float32,
        Float64,
        Date,
        DateTime,
        String,
        FixedString,
        Enum8,
        Enum16,
        Decimal32,
        Decimal64,
        Decimal128,
        UUID,
        Array,
        Tuple,
        Set,
        Interval,
        Nullable,
        Function,
        AggregateFunction,
        LowCardinality,
        Map,
        BitMap64,

        UInt256 = 10000,
        Int256 = 10001
    };
}

static bool isPatchNeeded(StatisticsTag tag)
{
    switch (tag)
    {
        case StatisticsTag::KllSketch:
        case StatisticsTag::NdvBuckets:
        case StatisticsTag::NdvBucketsResult:
            return true;
        default:
            return false;
    }
}

String patchStatsBinary(StatisticsTag tag, String blob)
{
    if (!isPatchNeeded(tag))
    {
        return blob;
    }

    if (blob.empty())
    {
        return {};
    }
    auto [old_index, tail] = parseBlobWithHeader<CnchIndex>(blob);
    SerdeDataType new_serde_data_type;
    switch (old_index)
    {
#    define NEW_CASE(TYPE) \
        case CnchIndex::TYPE: { \
            new_serde_data_type = SerdeDataType::TYPE; \
            break; \
        }
        ALL_TYPE_ITERATE(NEW_CASE)
#    undef NEW_CASE

        default:
            throw Exception("unsupported", ErrorCodes::LOGICAL_ERROR);
    }

    std::string result;
    result.append(reinterpret_cast<const char *>(&new_serde_data_type), sizeof(new_serde_data_type));
    result += tail;
    if (result.size() != blob.size())
    {
        throw Exception("logical error", ErrorCodes::LOGICAL_ERROR);
    }
    return result;
}

void patchDbStats(Protos::DbStats & db_stats)
{
    auto version = db_stats.has_version() ? db_stats.version() : Protos::DbStats_Version_V1;
    if (version != Protos::DbStats_Version_V1)
    {
        throw Exception("wrong version for patch", ErrorCodes::LOGICAL_ERROR);
    }

    for (auto & table_stats : *db_stats.mutable_tables())
    {
        for (auto & blob : *table_stats.mutable_blobs())
        {
            auto tag = static_cast<StatisticsTag>(blob.first);
            blob.second = patchStatsBinary(tag, std::move(blob.second));
        }

        for (auto & column_stats : *table_stats.mutable_columns())
        {
            for (auto & blob : *column_stats.mutable_blobs())
            {
                auto tag = static_cast<StatisticsTag>(blob.first);
                blob.second = patchStatsBinary(tag, std::move(blob.second));
            }
        }
    }
    db_stats.set_version(Protos::DbStats_Version_V2);
}

}
#endif
