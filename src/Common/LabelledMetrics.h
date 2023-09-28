#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace LabelledMetrics
{

using MetricLabel = std::string;
using MetricLabels = std::map<MetricLabel, MetricLabel>;
using Count = size_t;

struct MetricLabelHasher
{
    std::size_t operator()(const MetricLabels& labels) const;
};

std::string toString(const MetricLabels& labels);

}
