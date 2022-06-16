#pragma once

#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>

namespace DB
{
class PlanNodeStatistics;
using PlanNodeStatisticsPtr = std::shared_ptr<PlanNodeStatistics>;

class PlanNodeStatisticsEstimate
{
public:
    PlanNodeStatisticsEstimate() : statistics(), derived(false) { }
    PlanNodeStatisticsEstimate(std::optional<PlanNodeStatisticsPtr> statistics_) : statistics(std::move(statistics_)), derived(true) { }

    const std::optional<PlanNodeStatisticsPtr> & getStatistics() const { return statistics; }
    bool has_value() const { return statistics.has_value(); }
    PlanNodeStatisticsPtr value_or(const PlanNodeStatisticsPtr & value) const { return statistics.value_or(value); }
    const PlanNodeStatisticsPtr & value() const { return statistics.value(); }
    bool isDerived() const { return derived; }

    explicit operator bool() const { return has_value(); }
    bool operator!() const { return !this->operator bool(); }

private:
    std::optional<PlanNodeStatisticsPtr> statistics;
    bool derived;
};

}
