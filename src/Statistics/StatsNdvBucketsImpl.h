/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
#include <Statistics/StatsHllSketch.h>
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
        auto bucket_id = bounds.binarySearchBucket(value);
        counts[bucket_id] += 1;
        hll_sketches[bucket_id].update(value);
    }

    void merge(const StatsNdvBucketsImpl & rhs)
    {
        if (!bounds.equals(rhs.bounds))
        {
            throw Exception("Mismatch Kll Sketch", ErrorCodes::LOGICAL_ERROR);
        }

        for (size_t i = 0; i < numBuckets(); ++i)
        {
            counts[i] += rhs.counts[i];
            hll_sketches[i].merge(rhs.hll_sketches[i]);
        }
    }

    void initialize(BucketBoundsImpl<T> bounds_)
    {
        counts.clear();
        hll_sketches.clear();
        auto num_buckets = bounds_.numBuckets();
        counts.resize(num_buckets);
        hll_sketches.resize(num_buckets);
        bounds = std::move(bounds_);
    }

    SerdeDataType getSerdeDataType() const override { return SerdeDataTypeFrom<T>; }

    auto numBuckets() const { return bounds.numBuckets(); }

    void checkValid() const;

    const BucketBounds & getBucketBounds() const override { return bounds; }

    std::shared_ptr<StatsNdvBucketsResultImpl<T>> asResultImpl() const;

    std::shared_ptr<StatsNdvBucketsResult> asResult() const override { return asResultImpl(); }

private:
    BucketBoundsImpl<T> bounds;
    std::vector<uint64_t> counts; // of size buckets
    std::vector<StatsHllSketch> hll_sketches; // of size buckets
};

template <typename T>
void StatsNdvBucketsImpl<T>::checkValid() const
{
    bounds.checkValid();

    if (counts.size() != numBuckets() || hll_sketches.size() != numBuckets())
    {
        throw Exception("counts/hll size mismatch", ErrorCodes::LOGICAL_ERROR);
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
    pb.set_bounds_blob(bounds.serialize());

    for (auto & count : counts)
    {
        pb.add_counts(count);
    }
    for (auto & hll : hll_sketches)
    {
        pb.add_hll_sketch_blobs(hll.serialize());
    }
    pb.SerializeToOstream(&ss);
    return ss.str();
}

template <typename T>
void StatsNdvBucketsImpl<T>::deserialize(std::string_view raw_blob)
{
    std::tie(bounds, counts, hll_sketches) = [raw_blob] {
        if (raw_blob.size() <= sizeof(SerdeDataType))
        {
            throw Exception("corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        SerdeDataType serde_data_type;
        memcpy(&serde_data_type, raw_blob.data(), sizeof(serde_data_type));

        checkSerdeDataType<T>(serde_data_type);

        auto blob = raw_blob.substr(sizeof(serde_data_type), raw_blob.size() - sizeof(serde_data_type));
        Protos::StatsNdvBuckets pb;
        ASSERT_PARSE(pb.ParseFromArray(blob.data(), blob.size()));
        BucketBoundsImpl<T> bounds_;
        bounds_.deserialize(pb.bounds_blob());
        int64_t num_buckets = bounds_.numBuckets();
        if (pb.counts_size() != num_buckets || pb.hll_sketch_blobs_size() != num_buckets)
        {
            throw Exception("Corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        decltype(counts) counts_(num_buckets);
        decltype(hll_sketches) hll_sketches_(num_buckets);
        for (int64_t i = 0; i < num_buckets; ++i)
        {
            counts_[i] = pb.counts(i);
            auto & hll_blob = pb.hll_sketch_blobs(i);
            hll_sketches_[i].deserialize(hll_blob);
        }
        return std::tuple{std::move(bounds_), std::move(counts_), std::move(hll_sketches_)};
    }();
    checkValid();
}

template <typename T>
std::shared_ptr<StatsNdvBucketsResultImpl<T>> StatsNdvBucketsImpl<T>::asResultImpl() const
{
    std::vector<double> ndvs;
    for (auto & hll : this->hll_sketches)
    {
        auto ndv = hll.getEstimate();
        ndvs.emplace_back(ndv);
    }
    return StatsNdvBucketsResultImpl<T>::createImpl(this->bounds, this->counts, std::move(ndvs));
}

}
