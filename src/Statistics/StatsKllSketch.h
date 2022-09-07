#pragma once
#include <DataTypes/IDataType.h>
#include <Statistics/Base64.h>
#include <Statistics/BucketBounds.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatsNdvBucketsResult.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
template <typename T>
class BucketBoundsImpl;


template <typename T>
class StatsKllSketchImpl;

class StatsKllSketch : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::KllSketch;

    StatisticsTag getTag() const override { return tag; }

    virtual bool isEmpty() const = 0;
    virtual std::optional<double> minAsDouble() const = 0;
    virtual std::optional<double> maxAsDouble() const = 0;

    virtual SerdeDataType getSerdeDataType() const = 0;

    template <typename T>
    using Impl = StatsKllSketchImpl<T>;

    virtual std::shared_ptr<BucketBounds> getBucketBounds() const = 0;

    virtual std::shared_ptr<StatsNdvBucketsResult> generateNdvBucketsResult(double total_ndv) const = 0;

    virtual int64_t getCount() const = 0;

private:
};


}
