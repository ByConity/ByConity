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

#include <sstream>
#include <Optimizer/Dump/ProtoEnumUtils.h>
#include <Statistics/BucketBoundsImpl.h>
#include <Statistics/StatsKllSketchImpl.h>
#include <Poco/JSON/Parser.h>

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
    auto blob = vectorSerialize(bounds_);
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
    bounds_ = vectorDeserialize<EmbeddedType>(blob);
    checkValid();
}

//serialize as json
template <typename T>
String BucketBoundsImpl<T>::serializeToJson() const
{
    checkValid();
    Poco::JSON::Object object_json;
    auto type_id = SerdeDataTypeFrom<T>;
    String type_string = ProtoEnumUtils::serdeDataTypeToString(type_id);
    Poco::JSON::Array array_json;
    object_json.set("type_id", type_string);
    if constexpr (!std::is_same_v<T, UInt128> && !std::is_same_v<T, Int128> && !std::is_same_v<T, UInt256> && !std::is_same_v<T, Int256>)
    {
        for (auto ptr : bounds_)
        {
            if constexpr (std::is_same_v<T, UInt8>)
                array_json.add(static_cast<uint8_t>(ptr));
            else
                array_json.add(ptr);
        }
    }
    else if constexpr (std::is_same_v<T, UInt128> || std::is_same_v<T, Int128> || std::is_same_v<T, UInt256> || std::is_same_v<T, Int256>)
    {
        for (auto value : bounds_)
        {
            std::ostringstream out_str;
            out_str << value;
            array_json.add(out_str.str());
        }
    }
    else
    {
        throw Exception(
            " serializeToJson in BucketBoundsImpl.h is not successful, because the Type define is not in type_string set",
            ErrorCodes::LOGICAL_ERROR);
    }
    object_json.set("bounds_", array_json);
    std::stringstream json_string;
    object_json.stringify(json_string);
    return json_string.str();
}

//deserialize from json
template <typename T>
void BucketBoundsImpl<T>::deserializeFromJson(std::string_view blob)
{
    Poco::JSON::Parser json_parse;
    Poco::JSON::Object::Ptr object = json_parse.parse({blob.data(), blob.size()}).extract<Poco::JSON::Object::Ptr>();
    String type_id = object->getValue<String>("type_id");
    SerdeDataType serde_data_type = ProtoEnumUtils::serdeDataTypeFromString(type_id);
    checkSerdeDataType<T>(serde_data_type);

    Poco::JSON::Array::Ptr array = object->getArray("bounds_");
    if (array)
    {
        bounds_.clear();
        num_buckets_ = array->size() - 1;
        for (size_t j = 0; j < array->size(); ++j)
        {
            if constexpr (
                !std::is_same_v<
                    EmbeddedType,
                    UInt8> && !std::is_same_v<T, UInt128> && !std::is_same_v<T, Int128> && !std::is_same_v<T, UInt256> && !std::is_same_v<T, Int256>)
            {
                bounds_.push_back(array->getElement<EmbeddedType>(j));
            }
            else if constexpr (std::is_same_v<EmbeddedType, UInt8>)
            {
                bounds_.push_back(array->getElement<Poco::UInt8>(j));
            }
            else if constexpr (
                std::is_same_v<T, UInt128> || std::is_same_v<T, Int128> || std::is_same_v<T, UInt256> || std::is_same_v<T, Int256>)
            {
                String text = array->getElement<String>(j);
                T ele;
                wideIntFromString(ele, text);
                bounds_.push_back(ele);
            }
        }
    }
    else
    {
        throw Exception("the blob is not a array, deserialize failure", ErrorCodes::LOGICAL_ERROR);
    }
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
