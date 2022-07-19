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
    void init(BucketBoundsImpl<T> bounds, std::vector<UInt64> counts, std::vector<double> ndvs)
    {
        bounds_ = std::move(bounds);
        counts_ = std::move(counts);
        ndvs_ = std::move(ndvs);
        checkValid();
    }

    String serialize() const override;
    void deserialize(std::string_view blob) override;
    String serializeToJson() const override;
    void deserializeFromJson(std::string_view json) override;

    SerdeDataType getSerdeDataType() const override { return SerdeDataTypeFrom<T>; }
    size_t num_buckets() const override { return bounds_.numBuckets(); }
    void writeSymbolStatistics(SymbolStatistics & symbol) override;

    const BucketBounds & get_buckets_bounds() const override { return bounds_; }
    uint64_t get_count(size_t bucket_id) const override { return counts_[bucket_id]; }
    double get_ndv(size_t bucket_id) const override { return ndvs_[bucket_id]; }

    void set_count(size_t bucket_id, UInt64 count) override { counts_[bucket_id] = count; }
    void set_ndv(size_t bucket_id, double ndv) override { ndvs_[bucket_id] = ndv; }

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

template <typename T>
String StatsNdvBucketsResultImpl<T>::serialize() const
{
    checkValid();
    std::ostringstream ss;
    auto serde_data_type = getSerdeDataType();
    ss.write(reinterpret_cast<const char *>(&serde_data_type), sizeof(serde_data_type));
    Protos::StatsNdvBucketsResult pb;
    pb.set_bounds_blob(bounds_.serialize());

    for (auto & count : counts_)
    {
        pb.add_counts(count);
    }
    for (auto & ndv : ndvs_)
    {
        pb.add_ndvs(ndv);
    }
    pb.SerializeToOstream(&ss);
    return ss.str();
}
template <typename T>
String StatsNdvBucketsResultImpl<T>::serializeToJson() const
{
    checkValid();
    Poco::JSON::Object::Ptr ptr_json = new Poco::JSON::Object(true);
    Poco::JSON::Parser parser;
    ptr_json->set("bounds_blob", parser.parse(bounds_.serializeToJson()));
    Poco::JSON::Array array_counts;
    for (auto & count : counts_)
    {
        array_counts.add(int64_t(count));
    }
    ptr_json->set("counts", Poco::Dynamic::Var(array_counts));

    Poco::JSON::Array array_ndvs;
    for (auto & ndv : ndvs_)
    {
        array_ndvs.add(double(ndv));
    }
    ptr_json->set("ndvs", Poco::Dynamic::Var(array_ndvs));
    Poco::Dynamic::Var result(*ptr_json);
    return result.toString();
}
template <typename T>
void StatsNdvBucketsResultImpl<T>::deserialize(std::string_view raw_blob)
{
    std::tie(bounds_, counts_, ndvs_) = [raw_blob] {
        if (raw_blob.size() <= sizeof(SerdeDataType))
        {
            throw Exception("corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        SerdeDataType serde_data_type;
        memcpy(&serde_data_type, raw_blob.data(), sizeof(serde_data_type));

        checkSerdeDataType<T>(serde_data_type);

        auto blob = raw_blob.substr(sizeof(serde_data_type), raw_blob.size() - sizeof(serde_data_type));
        Protos::StatsNdvBucketsResult pb;
        pb.ParseFromArray(blob.data(), blob.size());
        BucketBoundsImpl<T> bounds;
        bounds.deserialize(pb.bounds_blob());
        int64_t num_buckets = bounds.numBuckets();
        if (pb.counts_size() != num_buckets || pb.ndvs_size() != num_buckets)
        {
            throw Exception("Corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        decltype(counts_) counts(num_buckets);
        decltype(ndvs_) ndvs(num_buckets);
        for (int64_t i = 0; i < num_buckets; ++i)
        {
            counts[i] = pb.counts(i);
            auto ndv = pb.ndvs(i);
            ndvs[i] = ndv;
        }
        return std::tuple{std::move(bounds), std::move(counts), std::move(ndvs)};
    }();
    checkValid();
}
template <typename T>
void StatsNdvBucketsResultImpl<T>::deserializeFromJson(std::string_view raw_blob)
{
    std::tie(bounds_, counts_, ndvs_) = [raw_blob] {
        Pparser parser;
        PVar var = parser.parse(std::string{raw_blob.data(), raw_blob.size()});
        PObject json_object = *var.extract<PObject::Ptr>();

        if (raw_blob.size() <= sizeof(SerdeDataType))
        {
            throw Exception("corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        PVar var_bounds_blob = json_object.get("bounds_blob");
        PObject object_bounds_blob = *var_bounds_blob.extract<PObject::Ptr>();

        String type_str = object_bounds_blob.get("type_id").toString();
        SerdeDataType type_id = SerdeDataTypeFromString(type_str);
        //        auto it_pair = std::find_if(
        //            STRING_TYPE_INDEX.begin(), STRING_TYPE_INDEX.end(), [type_str](const std::pair<std::string, SerdeDataType> & element) {
        //                return element.first == type_str;
        //            });
        //        SerdeDataType type_id = it_pair->second;
        if (type_id != SerdeDataTypeFrom<T>)
        {
            throw Exception("blob header mismatch", ErrorCodes::TYPE_MISMATCH);
        }

        BucketBoundsImpl<T> bounds;

        bounds.deserializeFromJson(var_bounds_blob.toString());

        PVar counts_var, ndvs_var;
        counts_var = json_object.get("counts");
        ndvs_var = json_object.get("ndvs");
        PArray array_counts = *counts_var.extract<PArray::Ptr>();
        PArray array_ndvs = *ndvs_var.extract<PArray::Ptr>();

        size_t num_buckets = bounds.numBuckets();
        if (array_counts.size() != num_buckets || array_ndvs.size() != num_buckets)
        {
            throw Exception("Corrupted blob", ErrorCodes::LOGICAL_ERROR);
        }
        decltype(counts_) counts(num_buckets);
        decltype(ndvs_) ndvs(num_buckets);
        for (size_t i = 0; i < num_buckets; ++i)
        {
            counts[i] = array_counts.get(i).convert<uint64_t>();
            ndvs[i] = array_ndvs.get(i).convert<double>();
        }
        return std::tuple{std::move(bounds), std::move(counts), std::move(ndvs)};
    }();
    checkValid();
}

template <typename T>
void StatsNdvBucketsResultImpl<T>::writeSymbolStatistics(SymbolStatistics & symbol)
{
    checkValid();
    Buckets buckets;
    for (size_t i = 0; i < num_buckets(); ++i)
    {
        auto lb = static_cast<double>(bounds_[i]);
        auto ub = static_cast<double>(bounds_[i + 1]);
        auto count = counts_[i];
        auto ndv_estimate = ndvs_[i];
        auto [lb_inc, ub_inc] = bounds_.getBoundInclusive(i);

        // fix when ndv > count
        auto int_ndv = AdjustNdvWithCount(ndv_estimate, count);

        if (count > 0)
        {
            symbol.emplaceBackBucket(std::make_shared<Bucket>(lb, ub, int_ndv, count, lb_inc, ub_inc));
        }
    }
}

} // namespace DB
