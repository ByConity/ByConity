#pragma once
#include <optional>
#include <set>
#include <Core/Types.h>
#include <Statistics/Bucket.h>
#include <Statistics/CommonErrorCodes.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
/**
 * This class is an implementation of histogram.
 *
 * histogram represents the distribution of a column's values by a sequence of buckets.
 * Each bucket has a value range and contains approximately the same number of rows.
 *
 * - buckets histogram bucket
 */
class Histogram
{
public:
    Histogram(Buckets buckets = {});

    void emplaceBackBucket(BucketPtr ptr) { buckets.emplace_back(std::move(ptr)); }

    const Buckets & getBuckets() const { return buckets; }

    BucketPtr getBucket(size_t index) const
    {
        if (index < buckets.size())
        {
            return buckets[index];
        }
        return nullptr;
    }

    size_t getBucketSize() const { return buckets.size(); }
    bool empty() const { return buckets.empty(); }
    double getTotalCount() const;
    double getTotalNdv() const;
    double getMin() const;
    double getMax() const;

    double estimateEqual(double value) const;
    double estimateLessThanOrLessThanEqualFilter(double value, bool equal) const;
    double estimateGreaterThanOrGreaterThanEqualFilter(double value, bool equal) const;
    Buckets estimateJoin(const Histogram & right, double lower_bound, double upper_bound) const;

    Histogram createEqualFilter(double value) const;
    Histogram createNotEqualFilter(double value) const;
    Histogram createLessThanOrLessThanEqualFilter(double value, bool equal) const;
    Histogram createGreaterThanOrGreaterThanEqualFilter(double value, bool equal) const;
    Histogram createInFilter(std::set<double> & values) const;
    Histogram createNotInFilter(std::set<double> & values) const;
    Histogram createUnion(const Histogram & other) const;
    Histogram createNot(const Histogram & origin) const;

    Histogram applySelectivity(double rowcount_selectivity, double ndv_selectivity) const;

    Histogram copy() const;
    void clear() { buckets.clear(); }

private:
    Buckets buckets;
    void cleanupResidualBucket(BucketPtr & bucket, bool bucket_is_residual) const;
    BucketPtr getNextBucket(BucketPtr & new_bucket, bool & result_bucket_is_residual, size_t & current_bucket_index) const;
    size_t addResidualUnionAllBucket(Buckets & histogram_buckets, BucketPtr & bucket, bool bucket_is_residual, size_t index) const;
    static void addBuckets(const Buckets & src_buckets, Buckets & dest_buckets, size_t begin, size_t end);
    bool subsumes(const BucketPtr & bucket) const;
};

}
