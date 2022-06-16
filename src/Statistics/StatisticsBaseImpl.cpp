#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatsColumnBasic.h>
#include <Statistics/StatsCpcSketch.h>
#include <Statistics/StatsDummy.h>
#include <Statistics/StatsKllSketchImpl.h>
#include <Statistics/StatsNdvBucketsImpl.h>
#include <Statistics/StatsTableBasic.h>
#include <Statistics/TypeMacros.h>
#include <Common/Exception.h>
namespace DB::Statistics
{
template <class StatsType>
std::shared_ptr<StatsType> createStatisticsUntyped(StatisticsTag tag, std::string_view blob)
{
    static_assert(std::is_base_of_v<StatisticsBase, StatsType>);
    CheckTag<StatsType>(tag);
    std::shared_ptr<StatsType> ptr = std::make_shared<StatsType>();
    ptr->deserialize(blob);
    return ptr;
}

template <class StatsType, typename T>
std::shared_ptr<StatsType> createStatisticsTypedImpl(StatisticsTag tag, std::string_view blob)
{
    static_assert(std::is_base_of_v<StatisticsBase, StatsType>);
    CheckTag<StatsType>(tag);
    using ImplType = typename StatsType::template Impl<T>;
    auto ptr = std::make_shared<ImplType>();
    ptr->deserialize(blob);
    return ptr;
}


#define UNTYPED_INSTANTIATION(STATS_TYPE) \
    template std::shared_ptr<Stats##STATS_TYPE> createStatisticsUntyped<Stats##STATS_TYPE>(StatisticsTag, std::string_view blob);
UNTYPED_STATS_ITERATE(UNTYPED_INSTANTIATION)
#undef UNTYPED_INSTANTIATION

#define TYPED_INSTANTIATION(STATS_TYPE) \
    template std::shared_ptr<Stats##STATS_TYPE> createStatisticsTyped<Stats##STATS_TYPE>(StatisticsTag, std::string_view blob);
TYPED_STATS_ITERATE(TYPED_INSTANTIATION)
#undef TYPED_INSTANTIATION

template <class StatsType>
std::shared_ptr<StatsType> createStatisticsTyped(StatisticsTag tag, std::string_view blob)
{
    if (blob.size() < sizeof(SerdeDataType))
    {
        throw Exception("statistics blob corrupted", ErrorCodes::LOGICAL_ERROR);
    }

    auto type_blob = blob.substr(0, sizeof(SerdeDataType));
    SerdeDataType serde_data_type;
    memcpy(&serde_data_type, type_blob.data(), type_blob.size());
    switch (serde_data_type)
    {
#define ENUM_CASE(TYPE) \
    case SerdeDataType::TYPE: { \
        return createStatisticsTypedImpl<StatsType, TYPE>(tag, blob); \
    }
        ALL_TYPE_ITERATE(ENUM_CASE)
#undef ENUM_CASE
        // NOTE: for compatibility, old version enum value is also supported
        case SerdeDataType::StringOldVersion:
            return createStatisticsTypedImpl<StatsType, String>(tag, blob);

        default: {
            throw Exception("unknown type", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}

StatisticsBasePtr createStatisticsBase(StatisticsTag tag, TxnTimestamp ts, std::string_view blob)
{
    auto ptr = [&]() -> StatisticsBasePtr {
        switch (tag)
        {
            case StatisticsTag::DummyAlpha:
                return createStatisticsUntyped<StatsDummyAlpha>(tag, blob);
            case StatisticsTag::DummyBeta:
                return createStatisticsUntyped<StatsDummyBeta>(tag, blob);
            case StatisticsTag::TableBasic:
                return createStatisticsUntyped<StatsTableBasic>(tag, blob);
            case StatisticsTag::ColumnBasic:
                return createStatisticsUntyped<StatsColumnBasic>(tag, blob);
            case StatisticsTag::CpcSketch:
                return createStatisticsUntyped<StatsCpcSketch>(tag, blob);
            case StatisticsTag::KllSketch:
                return createStatisticsTyped<StatsKllSketch>(tag, blob);
            case StatisticsTag::NdvBuckets:
                return createStatisticsTyped<StatsNdvBuckets>(tag, blob);
            case StatisticsTag::NdvBucketsResult:
                return createStatisticsTyped<StatsNdvBucketsResult>(tag, blob);
            default: {
                throw Exception("Unimplemented Statistics Tag", ErrorCodes::NOT_IMPLEMENTED);
            }
        }
    }();
    ptr->setTxnTimestamp(ts);

    return ptr;
}
} // namespace DB
