#pragma once

#include <common/types.h>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace LabelledMetrics
{

using MetricLabels = std::map<String, String>;
using Metric = size_t;
using Count = size_t;

struct MetricLabelHasher
{
    std::size_t operator()(const MetricLabels& labels) const;
};

std::string toString(const MetricLabels& labels);

using LabelledCounter = std::unordered_map<MetricLabels, Count, MetricLabelHasher>;

extern LabelledCounter labelled_counters[];
extern std::mutex labelled_mutexes[];

/// Get name of metric by identifier. Returns statically allocated string.
const char * getName(Metric metric);
/// Get name of metric by identifier in snake_case.
DB::String getSnakeName(Metric metric);
/// Get text description of metric by identifier. Returns statically allocated string.
const char * getDocumentation(Metric metric);
/// Get index just after last metric identifier.
Metric end();

inline LabelledCounter getCounter(Metric metric)
{
    auto lock = std::unique_lock(labelled_mutexes[metric]);
    return labelled_counters[metric];
}

inline void increment(Metric metric, Count value, const MetricLabels & labels)
{
    auto lock = std::unique_lock(labelled_mutexes[metric]);
    labelled_counters[metric][labels] += value;
}

}
