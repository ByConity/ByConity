#include <Common/LabelledMetrics.h>
#include <Common/StringUtils/StringUtils.h>
#include <boost/functional/hash.hpp>

#define APPLY_FOR_METRICS(M) \
    M(VwQuery, "Number of queries started to be interpreted and maybe executed that belongs to a virtual warehouse.") \
    M(UnlimitedQuery, "Number of queries that do not utilise a VW") \
    M(QueriesFailed, "Number of queries that have failed") \
    M(QueriesFailedBeforeStart, "Number of queries that have failed before start") \
    M(QueriesFailedWhileProcessing, "Number of queries that have failed while processing") \
    M(QueriesFailedFromUser, "Number of queries that have failed because of user side error") \
    M(QueriesFailedFromEngine, "Number of queries that have failed because of engine side error") \
    M(QueriesSucceeded, "Number of queries that have succeeded") \
    M(ErrorCodes, "Number of different error codes")

namespace LabelledMetrics
{

#define M(NAME, DOCUMENTATION) extern const Metric NAME = __COUNTER__;
    APPLY_FOR_METRICS(M)
#undef M
constexpr Metric END = __COUNTER__;

LabelledCounter labelled_counters[END];
std::mutex labelled_mutexes[END];

const char * getName(Metric metric)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_METRICS(M)
    #undef M
    };

    return strings[metric];
}

DB::String getSnakeName(Metric metric)
{
    DB::String res{getName(metric)};

    convertCamelToSnake(res);

    return res;
}

const char * getDocumentation(Metric metric)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) DOCUMENTATION,
        APPLY_FOR_METRICS(M)
    #undef M
    };

    return strings[metric];
}

Metric end() { return END; }

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

#undef APPLY_FOR_METRICS
