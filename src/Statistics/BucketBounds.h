#pragma once
// generate bounds to be used in NdvBuckets

#include <string_view>
#include <DataTypes/DataTypeString.h>
#include <Statistics/SerdeDataType.h>

namespace DB::Statistics
{
template <typename T>
class BucketBoundsImpl;

// type erasure interface for BucketBoundsImpl
// for details, refer to definition of BucketBoundsImpl
class BucketBounds
{
public:
    template <typename T>
    using Impl = BucketBoundsImpl<T>;

    virtual String serialize() const = 0;
    virtual void deserialize(std::string_view blob) = 0;

    //serialize as json
    virtual String serializeToJson() const = 0;

    //deserialize from json
    virtual void deserializeFromJson(std::string_view) = 0;

    BucketBounds() = default;

    BucketBounds(const BucketBounds &) = default;
    BucketBounds(BucketBounds &&) = default;
    BucketBounds & operator=(const BucketBounds &) = default;
    BucketBounds & operator=(BucketBounds &&) = default;

    virtual SerdeDataType getSerdeDataType() const = 0;

    virtual ~BucketBounds() = default;

    virtual size_t numBuckets() const = 0;
    virtual String getElementAsString(int64_t index) const = 0;

    virtual std::pair<bool, bool> getBoundInclusive(size_t bucket_id) const = 0;
};

}
