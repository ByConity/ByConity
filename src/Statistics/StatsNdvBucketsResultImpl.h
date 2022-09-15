#pragma once
#include <Optimizer/Dump/Json2Pb.h>
#include <Statistics/BucketBoundsImpl.h>
#include <Statistics/SerdeUtils.h>
#include <Statistics/StatsCpcSketch.h>
#include <Statistics/StatsNdvBucketsResult.h>

namespace DB::Statistics
{


template <typename T>
class StatsNdvBucketsResultImpl : public StatsNdvBucketsResult
{
public:
    static std::shared_ptr<StatsNdvBucketsResultImpl<T>>
    createImpl(const BucketBounds & bounds, std::vector<UInt64> counts, std::vector<double> ndvs);

    String serialize() const override;
    void deserialize(std::string_view blob) override;
    String serializeToJson() const override;
    void deserializeFromJson(std::string_view json) override;

    SerdeDataType getSerdeDataType() const override { return SerdeDataTypeFrom<T>; }
    size_t numBuckets() const override { return bounds_.numBuckets(); }
    void writeSymbolStatistics(SymbolStatistics & symbol) override;

    const BucketBounds & getBucketBounds() const override { return bounds_; }
    uint64_t getCount(size_t bucket_id) const override { return counts_[bucket_id]; }
    double getNdv(size_t bucket_id) const override { return ndvs_[bucket_id]; }

    void setCount(size_t bucket_id, UInt64 count) override { counts_[bucket_id] = count; }
    void setNdv(size_t bucket_id, double ndv) override { ndvs_[bucket_id] = ndv; }

    void checkValid() const
    {
        bounds_.checkValid();
        auto num_bucket = bounds_.numBuckets();
        if (counts_.size() != num_bucket || ndvs_.size() != num_bucket)
        {
            throw Exception("failed init of Stats Bucket Result", ErrorCodes::LOGICAL_ERROR);
        }
    }

private:
    BucketBoundsImpl<T> bounds_;
    std::vector<uint64_t> counts_; // of size buckets
    std::vector<double> ndvs_; // of size buckets
};


} // namespace DB
