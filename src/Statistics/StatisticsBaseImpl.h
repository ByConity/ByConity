#pragma once
#include <Statistics/StatisticsBase.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
template <class StatsDerived>
inline void CheckTag(StatisticsTag tag)
{
    if (StatsDerived::tag != tag)
    {
        throw Exception("Statistics Tag mismatch", ErrorCodes::TYPE_MISMATCH);
    }
}

template <class StatsType>
std::shared_ptr<StatsType> createStatisticsTyped(StatisticsTag tag, std::string_view blob);

template <class StatsType>
std::shared_ptr<StatsType> createStatisticsUntyped(StatisticsTag tag, std::string_view blob);

}
