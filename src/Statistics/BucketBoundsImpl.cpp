#include <Statistics/BucketBoundsImpl.h>
#include <Statistics/StatsKllSketchImpl.h>

namespace DB::Statistics
{


template <typename T>
String BucketBoundsImpl<T>::serialize() const
{
    checkValid();
    std::ostringstream ss;
    auto serde_data_type = SerdeDataTypeFrom<T>;
    ss.write(reinterpret_cast<const char *>(&serde_data_type), sizeof(serde_data_type));
    ss.write(reinterpret_cast<const char *>(&num_buckets_), sizeof(num_buckets_));
    auto blob = vector_serialize(bounds_);
    ss.write(blob.data(), blob.size());
    return ss.str();
}

template <typename T>
void BucketBoundsImpl<T>::deserialize(std::string_view raw_blob)
{
    auto [serde_data_type, raw_blob_2] = parseBlobWithHeader(raw_blob);
    auto [num_buckets, blob] = parseBlobWithHeader<decltype(num_buckets_)>(raw_blob_2);
    checkSerdeDataType<T>(serde_data_type);
    num_buckets_ = num_buckets;
    bounds_ = vector_deserialize<EmbeddedType>(blob);
    checkValid();
}

template <typename T>
void BucketBoundsImpl<T>::setBounds(std::vector<EmbeddedType> && bounds)
{
    num_buckets_ = bounds.size() - 1;
    bounds_ = std::move(bounds);
    checkValid();
}

template <typename T>
void BucketBoundsImpl<T>::checkValid() const
{
    if (bounds_.size() != num_buckets_ + 1)
    {
        throw Exception("Buckets is not initialized, maybe corrupted blob", ErrorCodes::LOGICAL_ERROR);
    }

    if (bounds_.size() < 2)
    {
        throw Exception("Buckets too few", ErrorCodes::LOGICAL_ERROR);
    }
}

template <typename T>
String BucketBoundsImpl<T>::getSqlForBucketArray() const
{
    auto numbers = boost::algorithm::join(
        bounds_ | boost::adaptors::transformed([](auto x) -> String {
            if constexpr (std::is_fundamental_v<T>)
            {
                return std::to_string(x);
            }
            else
            {
                WriteBufferFromOwnString out;
                writeIntText(x, out);
                return std::move(out.str());
            }
        }),
        ",");
    String sql = "CAST([";
    sql += numbers;
    sql += R"_(], 'Array()_";
    sql += TypeName<EmbeddedType>;
    sql += R"_()'))_";
    return sql;
}

template <typename T>
String BucketBoundsImpl<T>::getSqlForBucketId(const String & col_name) const
{
    String sql = "arrayNdvBucketsSearch(";
    sql += getSqlForBucketArray();
    sql += ", ";
    /// string column use cityHash64(col) instead
    if constexpr (std::is_same_v<T, String>)
    {
        sql += "cityHash64(";
        sql += col_name;
        sql += ")";
    }
    else
    {
        sql += col_name;
    }
    sql += ")";
    return sql;
}


template <typename T>
bool BucketBoundsImpl<T>::equals(const BucketBoundsImpl<T> & right) const
{
    this->checkValid();
    right.checkValid();
    return bounds_ == right.bounds_;
}
template <typename T>
int64_t BucketBoundsImpl<T>::binarySearchBucket(const T & value) const
{
    checkValid();
    return binarySearchBucketImpl(bounds_.begin(), bounds_.end(), value);
}
template <typename T>
std::string BucketBoundsImpl<T>::toString()
{
    std::vector<String> strs;
    for (UInt64 i = 0; i < num_buckets_; ++i)
    {
        String str;
        auto [lb_inc, ub_inc] = getBoundInclusive(i);
        str += lb_inc ? "[" : "(";
        str += getElementAsString(i);
        str += ", ";
        str += getElementAsString(i + 1);
        str += ub_inc ? "]" : ")";
        strs.emplace_back(std::move(str));
    }
    return boost::algorithm::join(strs, " ");
}

template <typename T>
std::pair<bool, bool> BucketBoundsImpl<T>::getBoundInclusive(size_t bucket_id) const
{
    // a singleton bucket is [lower, upper] where lower=upper.
    // usually a normal bucket is of [lower, upper),
    //      unless its previous bucket is singleton so "(lower".
    //      or it is the last element so "upper]",
    if (bucket_id >= numBuckets())
    {
        throw Exception("out of bucket id", ErrorCodes::LOGICAL_ERROR);
    }
    auto i = bucket_id;
    /// lower bound usually is inclusive
    /// unless its previous bucket is singleton
    auto lb_inc = i < 1 || bounds_[i - 1] != bounds_[i];
    /// upper bound usually is not inclusive
    /// unless it's singleton or the last
    auto ub_inc = i >= numBuckets() - 1 || bounds_[i] == bounds_[i + 1];
    return {lb_inc, ub_inc};
}

#define INSTANTIATION(Type) template class BucketBoundsImpl<Type>;

ALL_TYPE_ITERATE(INSTANTIATION)
#undef INSTANTIATION


} // namespace DB
