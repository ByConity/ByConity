#pragma once
#include <DataTypes/IDataType.h>
#include <Statistics/BucketBounds.h>
#include <Statistics/DataSketchesHelper.h>
#include <Statistics/SerdeUtils.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatsKllSketch.h>
#include <Statistics/VectorSerde.h>
#include <Common/Exception.h>

#include <city.h>
#include <IO/WriteIntText.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <common/itoa.h>

namespace DB::Statistics
{

// BucketBounds is to specify lower_bound and upper_bound for each bucket
// there are two types of buckets: normal buckets and singleton buckets
// when lower_bound < upper_bound, it's a normal bucket, holding data range from lower_bound to upper_bound
// when lower_bound == upper_bound, it's a singleton bucket, holding a specific frequent value (i.e., lower_bound)
// singleton buckets work as an alternative for TopN
template <typename T>
class BucketBoundsImpl : public BucketBounds
{
public:
    // for string type, use its cityHash64 value for calculation
    using EmbeddedType = std::conditional_t<std::is_same_v<T, std::string>, UInt64, T>;
    BucketBoundsImpl() = default;
    String serialize() const override;

    void deserialize(std::string_view raw_blob) override;

    // set internal data
    void setBounds(std::vector<EmbeddedType> && bounds);

    void checkValid() const;

    // generate sql like CAST([1, 2, 3], 'Array(UInt64)')
    String getSqlForBucketArray() const;

    // generate sql like arrayNdvBucketsSearch(CAST([1, 2, 3], 'Array(UInt64)'), u64_col)
    String getSqlForBucketId(const String & col_name) const override;

    // binary search bounds to get bucket id for searched value
    // e.g. for bounds [1, 2, 3, 3, 4, 5], with num_bucket=5
    // 1, 1.5 put into bucket 0 => [1, 2)
    // 2, 2.5 put into bucket 1 => [2, 3)
    // 3 put into bucket 2 => [3, 3] which is singleton
    // 3.5 put into bucket 3 => (3, 4)
    // 4, 4.5, 5 put into bucket 4 => [4, 5]
    template <typename Iter>
    static int64_t binarySearchBucketImpl(Iter bounds_beg, Iter bounds_end, const T & value);

    int64_t binarySearchBucket(const T & value) const;

    // calculate [lower_bound_inclusive, upper_bound_inclusive]
    std::pair<bool, bool> getBoundInclusive(size_t bucket_id) const override;

    size_t numBuckets() const override { return num_buckets_; }

    EmbeddedType operator[](int64_t index) const { return bounds_[index]; }

    // only for text output
    String getElementAsString(int64_t index) const override { return boost::lexical_cast<String>(operator[](index)); }

    bool equals(const BucketBoundsImpl<T> & right) const;

    // for debug
    std::string toString();

private:
    // notice that the upper_bound of a bucket,
    // is always equals to the lower_bound of its next bucket
    // so we store N+1 bounds for N buckets,
    // where lower_bound of Nth bucket is bounds[N]
    // and the upper_bound of Nth bucket is bounds[N+1]
    std::vector<EmbeddedType> bounds_;
    size_t num_buckets_ = 0;
};


// implementation put here since dual-template is hard to instantiate
template <typename T>
template <typename Iter>
int64_t BucketBoundsImpl<T>::binarySearchBucketImpl(Iter bounds_beg, Iter bounds_end, const T & value)
{
    // if string, use hash value to search
    auto search_value = [&] {
        if constexpr (std::is_same_v<T, std::string>)
        {
            return StatsKllSketchImpl<T>::hash(value);
        }
        else
        {
            return value;
        }
    }();
    // endpoints are not reliable as min/max, use internal data
    auto bucket_id = std::upper_bound(bounds_beg + 1, bounds_end - 1, search_value) - bounds_beg - 1;
    if (bucket_id >= 1 && bounds_beg[bucket_id - 1] == search_value)
    {
        // lower_bound of the previous bucket equals search_value
        // which means the previous bucket is singleton
        --bucket_id;
    }
    return bucket_id;
}
}
