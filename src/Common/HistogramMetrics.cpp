#include <Common/HistogramMetrics.h>
#include <Common/StringUtils/StringUtils.h>

// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M) \
    M(QueryLatency, "Latency of queries", std::vector<Value>({750, 1500, 5000, 10000, 15000, 25000, 50000, 150000, 300000, std::numeric_limits<Value>::max()}))   \
    M(UnlimitedQueryLatency, "Latency of unlimited queries", std::vector<Value>({750, 1500, 5000, 10000, 15000, 25000, 50000, 150000, 300000, std::numeric_limits<Value>::max()}))   \
    M(QueryIOLatency, "IO Latency of queries", std::vector<Value>({750, 1500, 5000, 10000, 15000, 25000, 50000, 150000, 300000, std::numeric_limits<Value>::max()}))   \
    M(UnlimitedQueryIOLatency, "IO Latency of unlimited queries", std::vector<Value>({750, 1500, 5000, 10000, 15000, 25000, 50000, 150000, 300000, std::numeric_limits<Value>::max()}))   \

namespace HistogramMetrics
{
    #define M(NAME, DOCUMENTATION, HISTOGRAM_BUCKET_LIMITS) extern const Metric NAME = __COUNTER__;
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = __COUNTER__;

    LabelledHistogram histogram_metrics[END];

    std::mutex metric_mutexes[END];

    const char * getName(Metric metric)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION, HISTOGRAM_BUCKET_LIMITS) #NAME,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[metric];
    }

    const DB::String getSnakeName(Metric metric)
    {
        DB::String res{getName(metric)};

        convertCamelToSnake(res);

        return res;
    }

    const char * getDocumentation(Metric metric)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION, HISTOGRAM_BUCKET_LIMITS) DOCUMENTATION,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[metric];
    }

    const std::vector<Value> getHistogramBucketLimits(Metric metric)
    {
        static const std::vector<Value> histogram_bucket_limits[] =
        {
        #define M(NAME, DOCUMENTATION, HISTOGRAM_BUCKET_LIMITS) HISTOGRAM_BUCKET_LIMITS,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return histogram_bucket_limits[metric];

    }

    const std::unique_lock<std::mutex> getLock(Metric metric)
    {
        return std::unique_lock(metric_mutexes[metric]);
    }

    Metric end() { return END; }
}

#undef APPLY_FOR_METRICS
