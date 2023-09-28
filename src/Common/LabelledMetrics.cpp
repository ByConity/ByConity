#include <Common/LabelledMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/HistogramMetrics.h>
#include <boost/functional/hash.hpp>

namespace LabelledMetrics
{
std::size_t MetricLabelHasher::operator()(const MetricLabels &labels) const
{
  size_t seed = 0;
  for (const auto& label : labels)
  {
      boost::hash_combine(seed, label.first);
      boost::hash_combine(seed, label.second);
  }

  return seed;
}

std::string toString(const MetricLabels &labels)
{
    std::string tag;
    if (labels.empty())
        return tag;
    for (const auto & [key, val] : labels)
    {
        tag = tag + key + "=" + val + "|";
    }
    tag.pop_back();
    return tag;
}

}
