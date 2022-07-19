#pragma once
#include <DataTypes/IDataType.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Common/Exception.h>

#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/Base64.h>
#include <Statistics/StatsKllSketch.h>
#include <Statistics/StatsNdvBucketsResult.h>


namespace DB::Statistics
{
template <typename T>
class StatsNdvBucketsImpl;
template <typename T>
class StatsNdvBucketsResultImpl;
class BucketBounds;

class StatsNdvBuckets : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::NdvBuckets;

    StatisticsTag getTag() const override { return tag; }

    virtual SerdeDataType getSerdeDataType() const = 0;

    template <typename T>
    using Impl = StatsNdvBucketsImpl<T>;

    virtual void writeSymbolStatistics(SymbolStatistics & symbol) = 0;

    virtual const BucketBounds & get_buckets_bounds() const = 0;
    virtual UInt64 get_count(size_t bucket_id) const = 0;
    virtual double get_ndv(size_t bucket_id) const = 0;
    virtual std::shared_ptr<StatsNdvBucketsResult> asResult() const = 0;

private:
};


}
