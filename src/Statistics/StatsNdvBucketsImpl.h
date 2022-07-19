#pragma once
#include <DataTypes/IDataType.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatsNdvBuckets.h>
#include <Statistics/StatsNdvBucketsResultImpl.h>
#include <Common/Exception.h>

#include <algorithm>
#include <IO/WriteHelpers.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/Base64.h>
#include <Statistics/BucketBoundsImpl.h>
#include <Statistics/DataSketchesHelper.h>
#include <Statistics/StatsCpcSketch.h>
#include <Statistics/StatsKllSketchImpl.h>
#include <boost/algorithm/string/join.hpp>

namespace DB::Statistics
{
template <typename T>
class StatsNdvBucketsImpl : public StatsNdvBuckets
{
public:
    StatsNdvBucketsImpl() = default;

    String serialize() const override;
    void deserialize(std::string_view blob) override;

    void update(const T & value)
    {
        auto bucket_id = bounds_.binarySearchBucket(value);
        counts_[bucket_id] += 1;
        cpc_sketches_[bucket_id].update(value);
    }

    void merge(const StatsNdvBucketsImpl & rhs)
    {
        if (!bounds_.equals(rhs.bounds_))
        {
            throw Exception("Mismatch Kll Sketch", ErrorCodes::LOGICAL_ERROR);
        }

        for (size_t i = 0; i < num_buckets(); ++i)
        {
            counts_[i] += rhs.counts_[i];
            cpc_sketches_[i].merge(rhs.cpc_sketches_[i]);
        }
    }

    void set_count(size_t bucket_id, uint64_t count) { counts_[bucket_id] = count; }

    void set_cpc(size_t bucket_id, StatsCpcSketch cpc) { cpc_sketches_[bucket_id] = std::move(cpc); }

    void initialize(BucketBoundsImpl<T> bounds)
    {
        counts_.clear();
        cpc_sketches_.clear();
        auto num_buckets = bounds.numBuckets();
        counts_.resize(num_buckets);
        cpc_sketches_.resize(num_buckets);
        bounds_ = std::move(bounds);
    }

    SerdeDataType getSerdeDataType() const override { return SerdeDataTypeFrom<T>; }

    auto num_buckets() const { return bounds_.numBuckets(); }

    void writeSymbolStatistics(SymbolStatistics & symbol) override;

    void checkValid() const;

    const BucketBounds & get_buckets_bounds() const override { return bounds_; }

    uint64_t get_count(size_t bucket_id) const override { return counts_[bucket_id]; }
    double get_ndv(size_t bucket_id) const override { return cpc_sketches_[bucket_id].get_estimate(); }

    std::shared_ptr<StatsNdvBucketsResultImpl<T>> asResultImpl() const;

    std::shared_ptr<StatsNdvBucketsResult> asResult() const override { return asResultImpl(); }

private:
    BucketBoundsImpl<T> bounds_;
    std::vector<uint64_t> counts_; // of size buckets
    std::vector<StatsCpcSketch> cpc_sketches_; // of size buckets
};

template <typename T>
void StatsNdvBucketsImpl<T>::checkValid() const
{
    bounds_.checkValid();

    if (counts_.size() != num_buckets() || cpc_sketches_.size() != num_buckets())
    {
        throw Exception("counts/cpc size mismatch", ErrorCodes::LOGICAL_ERROR);
    }
}


template <typename T>
String StatsNdvBucketsImpl<T>::serialize() const
{
    checkValid();
    std::ostringstream ss;
    auto serde_data_type = getSerdeDataType();
    ss.write(reinterpret_cast<const char *>(&serde_data_type), sizeof(serde_data_type));
    Protos::StatsNdvBuckets pb;
    pb.set_bounds_blob(bounds_.serialize());

    for (auto & count : counts_)
    {
        pb.add_counts(count);
    }
    for (auto & cpc : cpc_sketches_)
    {
        pb.add_cpc_sketch_blobs(cpc.serialize());
    }
    pb.SerializeToOstream(&ss);
    return ss.str();
}

template <typename T>
void StatsNdvBucketsImpl<T>::deserialize(std::string_view raw_blob)
{
    std::tie(bounds_, counts_, cpc_sketches_) = [raw_blob] {
        if (raw_blob.size() <= sizeof(SerdeDataType))
        {
            throw Exception("corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        SerdeDataType serde_data_type;
        memcpy(&serde_data_type, raw_blob.data(), sizeof(serde_data_type));

        checkSerdeDataType<T>(serde_data_type);

        auto blob = raw_blob.substr(sizeof(serde_data_type), raw_blob.size() - sizeof(serde_data_type));
        Protos::StatsNdvBuckets pb;
        pb.ParseFromArray(blob.data(), blob.size());
        BucketBoundsImpl<T> bounds;
        bounds.deserialize(pb.bounds_blob());
        int64_t num_buckets = bounds.numBuckets();
        if (pb.counts_size() != num_buckets || pb.cpc_sketch_blobs_size() != num_buckets)
        {
            throw Exception("Corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        decltype(counts_) counts(num_buckets);
        decltype(cpc_sketches_) cpc_sketches(num_buckets);
        for (int64_t i = 0; i < num_buckets; ++i)
        {
            counts[i] = pb.counts(i);
            auto & cpc_blob = pb.cpc_sketch_blobs(i);
            cpc_sketches[i].deserialize(cpc_blob);
        }
        return std::tuple{std::move(bounds), std::move(counts), std::move(cpc_sketches)};
    }();
    checkValid();
}

template <typename T>
void StatsNdvBucketsImpl<T>::writeSymbolStatistics(SymbolStatistics & symbol)
{
    checkValid();
    Buckets buckets;
    for (size_t i = 0; i < num_buckets(); ++i)
    {
        auto lb = Statistics::toDouble(bounds_[i]);
        auto ub = Statistics::toDouble(bounds_[i + 1]);
        auto count = counts_[i];
        auto ndv_estimate = cpc_sketches_[i].get_estimate();
        auto [lb_inc, ub_inc] = bounds_.getBoundInclusive(i);

        // fix when ndv > count
        auto int_ndv = AdjustNdvWithCount(ndv_estimate, count);

        if (count > 0)
        {
            symbol.emplaceBackBucket(std::make_shared<Bucket>(lb, ub, int_ndv, count, lb_inc, ub_inc));
        }
    }
}

template <typename T>
std::shared_ptr<StatsNdvBucketsResultImpl<T>> StatsNdvBucketsImpl<T>::asResultImpl() const
{
    auto result = std::make_shared<StatsNdvBucketsResultImpl<T>>();
    std::vector<double> ndvs;
    for (auto & cpc : this->cpc_sketches_)
    {
        auto ndv = cpc.get_estimate();
        ndvs.emplace_back(ndv);
    }
    result->init(this->bounds_, this->counts_, std::move(ndvs));
    return result;
}

}
